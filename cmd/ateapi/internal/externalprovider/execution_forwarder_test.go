// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package externalprovider

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

type executionTestSender struct {
	frames chan *externalproviderpb.ServerFrame

	mu   sync.RWMutex
	hook func(context.Context, *externalproviderpb.ServerFrame) error

	active    atomic.Int32
	maxActive atomic.Int32
}

func newExecutionTestSender() *executionTestSender {
	return &executionTestSender{frames: make(chan *externalproviderpb.ServerFrame, 4096)}
}

func (s *executionTestSender) send(ctx context.Context, frame *externalproviderpb.ServerFrame) error {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		maximum := s.maxActive.Load()
		if active <= maximum || s.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	cloned := proto.Clone(frame).(*externalproviderpb.ServerFrame)
	select {
	case s.frames <- cloned:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	s.mu.RLock()
	hook := s.hook
	s.mu.RUnlock()
	if hook != nil {
		return hook(ctx, proto.Clone(cloned).(*externalproviderpb.ServerFrame))
	}
	return nil
}

func (s *executionTestSender) setHook(hook func(context.Context, *externalproviderpb.ServerFrame) error) {
	s.mu.Lock()
	s.hook = hook
	s.mu.Unlock()
}

type executionForwarderFixture struct {
	ctx         context.Context
	cancel      context.CancelFunc
	coordinator *sessionCoordinator
	registry    *sessionRegistry
	routes      *SessionRouteDirectory
	runtime     *coordinatorRuntime
	forwarder   *executionForwarder
	session     *coordinatedSession
	sender      *executionTestSender
	assignment  *ateapipb.WorkerAssignment
}

func newExecutionForwarderFixture(t *testing.T, edit func(*ExecutionForwardingLimits)) *executionForwarderFixture {
	t.Helper()
	registry := mustSessionRegistry(t, 2)
	routes := mustRouteDirectory(t, registry, 2, 8)
	runtime := newCoordinatorRuntime()
	lifecycle := mustWorkerLifecycle(t, registry, runtime, routes)
	limits := DefaultExecutionForwardingLimits()
	if edit != nil {
		edit(&limits)
	}
	forwarder, err := newExecutionForwarder(routes, limits)
	if err != nil {
		t.Fatalf("newExecutionForwarder() error = %v", err)
	}
	coordinator, err := newSessionCoordinatorWithForwarder(
		registry,
		runtime,
		routes,
		lifecycle,
		ChannelSessionLimits{MaxOpenChannels: 64, MaxDataBytes: 32, RememberedChannelLimit: 128},
		forwarder,
	)
	if err != nil {
		t.Fatalf("newSessionCoordinatorWithForwarder() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sender := newExecutionTestSender()
	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	session, err := coordinator.establish(ctx, claim, hello, sender.send)
	if err != nil {
		cancel()
		t.Fatalf("establish() error = %v", err)
	}
	ready := nextExecutionServerFrame(t, sender)
	if ready.GetReady() == nil || ready.GetSessionGeneration() != 1 {
		cancel()
		t.Fatalf("first server frame = %v, want generation 1 Ready", ready)
	}
	worker := onlyExecutionWorker(t, runtime)
	fixture := &executionForwarderFixture{
		ctx:         ctx,
		cancel:      cancel,
		coordinator: coordinator,
		registry:    registry,
		routes:      routes,
		runtime:     runtime,
		forwarder:   forwarder,
		session:     session,
		sender:      sender,
		assignment:  executionAssignment(worker),
	}
	t.Cleanup(func() {
		cancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_ = session.close(cleanupCtx)
	})
	return fixture
}

func onlyExecutionWorker(t *testing.T, runtime *coordinatorRuntime) *ateapipb.Worker {
	t.Helper()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.workers) != 1 {
		t.Fatalf("runtime Worker count = %d, want 1", len(runtime.workers))
	}
	for _, worker := range runtime.workers {
		return proto.Clone(worker).(*ateapipb.Worker)
	}
	return nil
}

func executionAssignment(worker *ateapipb.Worker) *ateapipb.WorkerAssignment {
	return &ateapipb.WorkerAssignment{
		Worker:            &ateapipb.ObjectRef{Name: worker.GetMetadata().GetName()},
		Provider:          ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
		ExternalSlot:      proto.Clone(worker.GetExternalSlot()).(*ateapipb.ExternalSlotIdentity),
		WorkerResourceUid: worker.GetMetadata().GetUid(),
	}
}

func nextExecutionServerFrame(t *testing.T, sender *executionTestSender) *externalproviderpb.ServerFrame {
	t.Helper()
	select {
	case frame := <-sender.frames:
		return frame
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server frame")
		return nil
	}
}

type executionDialResult struct {
	conn net.Conn
	err  error
}

func startExecutionDial(ctx context.Context, dialer *ExternalExecutionDialer, assignment *ateapipb.WorkerAssignment) <-chan executionDialResult {
	result := make(chan executionDialResult, 1)
	go func() {
		connection, err := dialer.DialContext(ctx, assignment)
		result <- executionDialResult{conn: connection, err: err}
	}()
	return result
}

func acceptNextExecutionDial(t *testing.T, fixture *executionForwarderFixture, result <-chan executionDialResult) (*executionConn, uint64) {
	t.Helper()
	open := nextExecutionServerFrame(t, fixture.sender)
	if open.GetOpen().GetKind() != externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC ||
		open.GetOpen().GetChannelId() == 0 || open.GetOpen().GetChannelId()%2 != 0 || open.GetOpen().GetSlotId() != "slot-a" {
		t.Fatalf("Open frame = %v, want even EXECUTION_GRPC slot-a", open)
	}
	if err := fixture.session.applyClientFrame(clientAckFrame(open.GetSessionGeneration(), open.GetOpen().GetChannelId(), true, "")); err != nil {
		t.Fatalf("apply accepted OpenAck: %v", err)
	}
	select {
	case dialed := <-result:
		if dialed.err != nil {
			t.Fatalf("DialContext() error = %v", dialed.err)
		}
		connection, ok := dialed.conn.(*executionConn)
		if !ok {
			t.Fatalf("DialContext() connection = %T, want *executionConn", dialed.conn)
		}
		return connection, open.GetOpen().GetChannelId()
	case <-time.After(5 * time.Second):
		t.Fatal("DialContext did not complete after OpenAck")
		return nil, 0
	}
}

func dialAcceptedExecution(t *testing.T, fixture *executionForwarderFixture) (*executionConn, uint64) {
	t.Helper()
	result := startExecutionDial(context.Background(), &ExternalExecutionDialer{forwarder: fixture.forwarder}, fixture.assignment)
	return acceptNextExecutionDial(t, fixture, result)
}

func TestExecutionForwardingLimitsAndBoundedSendQueue(t *testing.T) {
	invalid := []ExecutionForwardingLimits{
		{SendQueueDepth: maximumExecutionSendQueueDepth + 1},
		{ReceiveQueueDepth: maximumExecutionReceiveQueueDepth + 1},
		{MaxServerChannelID: 1},
		{MaxServerChannelID: 3},
		{CloseTimeout: time.Nanosecond},
		{CloseTimeout: maximumExecutionCloseTimeout + time.Millisecond},
	}
	for _, limits := range invalid {
		if normalized, err := normalizeExecutionForwardingLimits(limits); !errors.Is(err, ErrInvalidExecutionForwardingConfig) || normalized != (ExecutionForwardingLimits{}) {
			t.Errorf("normalizeExecutionForwardingLimits(%+v) = (%+v, %v)", limits, normalized, err)
		}
	}
	if normalized, err := normalizeExecutionForwardingLimits(ExecutionForwardingLimits{}); err != nil || normalized != DefaultExecutionForwardingLimits() {
		t.Fatalf("zero limits normalize to (%+v, %v), want defaults", normalized, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	var first sync.Once
	wire, err := newSessionWire(ctx, func(sendCtx context.Context, _ *externalproviderpb.ServerFrame) error {
		blocked := false
		first.Do(func() {
			blocked = true
			close(started)
		})
		if !blocked {
			return nil
		}
		select {
		case <-release:
			return nil
		case <-sendCtx.Done():
			return context.Cause(sendCtx)
		}
	}, 1)
	if err != nil {
		t.Fatalf("newSessionWire() error = %v", err)
	}
	defer wire.close(context.Canceled)
	frame := &externalproviderpb.ServerFrame{SessionGeneration: 1, Frame: &externalproviderpb.ServerFrame_Heartbeat{Heartbeat: &externalproviderpb.Heartbeat{Nonce: 1}}}
	firstResult := make(chan error, 1)
	go func() { firstResult <- wire.sendFrame(ctx, frame) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first sender did not block")
	}
	secondResult := make(chan error, 1)
	go func() { secondResult <- wire.sendFrame(ctx, frame) }()
	deadline := time.Now().Add(time.Second)
	for len(wire.queue) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(wire.queue); got != 1 {
		t.Fatalf("queued sends = %d, want bounded full queue", got)
	}
	thirdCtx, cancelThird := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelThird()
	if err := wire.sendFrame(thirdCtx, frame); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send beyond queue bound error = %v, want deadline", err)
	}
	close(release)
	if err := <-firstResult; err != nil {
		t.Fatalf("first send error = %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("second send error = %v", err)
	}
}

func TestExecutionForwarderAckDataHalfCloseAndReset(t *testing.T) {
	t.Run("accepted byte stream and half-close", func(t *testing.T) {
		fixture := newExecutionForwarderFixture(t, nil)
		connection, channelID := dialAcceptedExecution(t, fixture)

		if err := fixture.session.applyClientFrame(clientDataFrame(1, channelID, []byte("from-client"))); err != nil {
			t.Fatalf("apply client data: %v", err)
		}
		buffer := make([]byte, 32)
		count, err := connection.Read(buffer)
		if err != nil || string(buffer[:count]) != "from-client" {
			t.Fatalf("Read() = (%q, %v), want from-client", buffer[:count], err)
		}

		if count, err := connection.Write([]byte("from-server")); err != nil || count != len("from-server") {
			t.Fatalf("Write() = (%d, %v)", count, err)
		}
		data := nextExecutionServerFrame(t, fixture.sender)
		if data.GetData().GetChannelId() != channelID || string(data.GetData().GetData()) != "from-server" {
			t.Fatalf("server data frame = %v", data)
		}

		if err := fixture.session.applyClientFrame(clientHalfCloseFrame(1, channelID)); err != nil {
			t.Fatalf("apply client half-close: %v", err)
		}
		if count, err := connection.Read(buffer); count != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("Read() after half-close = (%d, %v), want EOF", count, err)
		}
		if err := connection.CloseWrite(); err != nil {
			t.Fatalf("CloseWrite() error = %v", err)
		}
		halfClose := nextExecutionServerFrame(t, fixture.sender)
		if halfClose.GetHalfClose().GetChannelId() != channelID {
			t.Fatalf("server half-close = %v", halfClose)
		}
		if err := connection.Close(); err != nil {
			t.Fatalf("Close() after both half-closes error = %v", err)
		}
	})

	t.Run("peer reset terminates exact channel", func(t *testing.T) {
		fixture := newExecutionForwarderFixture(t, nil)
		connection, channelID := dialAcceptedExecution(t, fixture)
		if err := fixture.session.applyClientFrame(clientResetFrame(1, channelID, uint32(codes.Canceled), "peer reset")); err != nil {
			t.Fatalf("apply client reset: %v", err)
		}
		buffer := make([]byte, 1)
		if _, err := connection.Read(buffer); !errors.Is(err, ErrExternalExecutionUnavailable) {
			t.Fatalf("Read() reset error = %v, want ErrExternalExecutionUnavailable", err)
		}
	})

	t.Run("rejected Open returns generic error", func(t *testing.T) {
		fixture := newExecutionForwarderFixture(t, nil)
		result := startExecutionDial(context.Background(), &ExternalExecutionDialer{forwarder: fixture.forwarder}, fixture.assignment)
		open := nextExecutionServerFrame(t, fixture.sender)
		if err := fixture.session.applyClientFrame(clientAckFrame(1, open.GetOpen().GetChannelId(), false, "untrusted peer detail")); err != nil {
			t.Fatalf("apply rejected OpenAck: %v", err)
		}
		dialed := <-result
		if !errors.Is(dialed.err, ErrExternalExecutionRejected) || dialed.conn != nil ||
			containsErrorText(dialed.err, "untrusted peer detail") {
			t.Fatalf("DialContext() = (%v, %v), want redacted rejection", dialed.conn, dialed.err)
		}
	})
}

func containsErrorText(err error, text string) bool {
	return err != nil && len(text) > 0 && strings.Contains(err.Error(), text)
}

func TestExecutionForwarderAssignmentAndRouteFencing(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	dialer := &ExternalExecutionDialer{forwarder: fixture.forwarder}

	invalid := []struct {
		name string
		edit func(*ateapipb.WorkerAssignment)
	}{
		{name: "Kubernetes provider", edit: func(a *ateapipb.WorkerAssignment) {
			a.Provider = ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD
		}},
		{name: "missing Worker", edit: func(a *ateapipb.WorkerAssignment) { a.Worker = nil }},
		{name: "namespaced Worker ref", edit: func(a *ateapipb.WorkerAssignment) { a.Worker.Atespace = "team-a" }},
		{name: "noncanonical Worker UID", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerResourceUid = "NOT-A-UUID" }},
		{name: "missing execution identity", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.ExecutionIdentity = "" }},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			assignment := proto.Clone(fixture.assignment).(*ateapipb.WorkerAssignment)
			test.edit(assignment)
			if connection, err := dialer.DialContext(context.Background(), assignment); !errors.Is(err, ErrInvalidExternalExecutionAssignment) || connection != nil {
				t.Fatalf("DialContext() = (%v, %v), want invalid assignment", connection, err)
			}
		})
	}

	stale := []struct {
		name string
		edit func(*ateapipb.WorkerAssignment)
	}{
		{name: "different Worker name", edit: func(a *ateapipb.WorkerAssignment) { a.Worker.Name = "different-worker" }},
		{name: "different canonical UID", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerResourceUid = "00000000-0000-4000-8000-000000000099" }},
		{name: "different execution identity", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.ExecutionIdentity = "different.execution" }},
	}
	for _, test := range stale {
		t.Run(test.name, func(t *testing.T) {
			assignment := proto.Clone(fixture.assignment).(*ateapipb.WorkerAssignment)
			test.edit(assignment)
			if connection, err := dialer.DialContext(context.Background(), assignment); !errors.Is(err, ErrExternalExecutionUnavailable) || connection != nil {
				t.Fatalf("DialContext() = (%v, %v), want unavailable", connection, err)
			}
		})
	}

	connection, _ := dialAcceptedExecution(t, fixture)
	fixture.routes.Close(fixture.session.route)
	select {
	case <-fixture.session.done():
	case <-time.After(5 * time.Second):
		t.Fatal("route close did not stop the coordinated session")
	}
	buffer := make([]byte, 1)
	if _, err := connection.Read(buffer); err == nil {
		t.Fatal("live connection survived exact route close")
	}
	if connection, err := dialer.DialContext(context.Background(), fixture.assignment); !errors.Is(err, ErrExternalExecutionUnavailable) || connection != nil {
		t.Fatalf("DialContext() after route close = (%v, %v)", connection, err)
	}
}

func TestExecutionForwarderBackpressureDeadlinesAndIDExhaustion(t *testing.T) {
	t.Run("receive backpressure resets one channel", func(t *testing.T) {
		fixture := newExecutionForwarderFixture(t, func(limits *ExecutionForwardingLimits) {
			limits.ReceiveQueueDepth = 1
		})
		connection, channelID := dialAcceptedExecution(t, fixture)
		if err := fixture.session.applyClientFrame(clientDataFrame(1, channelID, []byte("first"))); err != nil {
			t.Fatalf("first data: %v", err)
		}
		if err := fixture.session.applyClientFrame(clientDataFrame(1, channelID, []byte("second"))); err != nil {
			t.Fatalf("backpressure reset: %v", err)
		}
		reset := nextExecutionServerFrame(t, fixture.sender)
		if reset.GetReset_().GetChannelId() != channelID || reset.GetReset_().GetGrpcCode() != uint32(codes.ResourceExhausted) {
			t.Fatalf("backpressure reset frame = %v", reset)
		}
		if _, err := connection.Read(make([]byte, 8)); !errors.Is(err, ErrExternalExecutionBackpressure) {
			t.Fatalf("Read() error = %v, want ErrExternalExecutionBackpressure", err)
		}
	})

	t.Run("read and operation-gate deadlines", func(t *testing.T) {
		fixture := newExecutionForwarderFixture(t, nil)
		connection, _ := dialAcceptedExecution(t, fixture)
		if err := connection.SetReadDeadline(time.Now().Add(-time.Millisecond)); err != nil {
			t.Fatalf("SetReadDeadline() error = %v", err)
		}
		if _, err := connection.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Read() deadline error = %v", err)
		}
		if err := fixture.session.wire.lockOperation(fixture.ctx); err != nil {
			t.Fatalf("lock operation gate: %v", err)
		}
		defer fixture.session.wire.unlockOperation()
		writeResult := make(chan error, 1)
		go func() {
			_, err := connection.Write([]byte("blocked"))
			writeResult <- err
		}()
		deadline := time.Now().Add(time.Second)
		pending := false
		for time.Now().Before(deadline) {
			connection.stateMu.Lock()
			pending = connection.writeCancel != nil
			connection.stateMu.Unlock()
			if pending {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !pending {
			t.Fatal("Write did not reach the operation gate")
		}
		started := time.Now()
		if err := connection.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
			t.Fatalf("SetWriteDeadline() error = %v", err)
		}
		select {
		case err := <-writeResult:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("pending Write() deadline error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("SetWriteDeadline did not unblock pending Write")
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("Write() ignored bounded deadline for %v", elapsed)
		}
	})

	t.Run("even IDs are never reused and exhaust", func(t *testing.T) {
		fixture := newExecutionForwarderFixture(t, func(limits *ExecutionForwardingLimits) {
			limits.MaxServerChannelID = 2
			limits.CloseTimeout = 50 * time.Millisecond
		})
		connection, channelID := dialAcceptedExecution(t, fixture)
		if channelID != 2 {
			t.Fatalf("first channel ID = %d, want 2", channelID)
		}
		if err := connection.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		_ = nextExecutionServerFrame(t, fixture.sender)
		if connection, err := (&ExternalExecutionDialer{forwarder: fixture.forwarder}).DialContext(context.Background(), fixture.assignment); !errors.Is(err, ErrExternalExecutionChannelIDsExhausted) || connection != nil {
			t.Fatalf("second DialContext() = (%v, %v), want ID exhaustion", connection, err)
		}
	})
}

func TestExecutionConnectionCloseIsBoundedWhenPeerStopsReading(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, func(limits *ExecutionForwardingLimits) {
		limits.CloseTimeout = 25 * time.Millisecond
	})
	connection, channelID := dialAcceptedExecution(t, fixture)
	fixture.sender.setHook(func(ctx context.Context, frame *externalproviderpb.ServerFrame) error {
		if frame.GetReset_().GetChannelId() == channelID {
			<-ctx.Done()
			return context.Cause(ctx)
		}
		return nil
	})
	started := time.Now()
	err := connection.Close()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close() blocked for %v", elapsed)
	}
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v, want bounded deadline", err)
	}
	select {
	case <-fixture.session.done():
	case <-time.After(time.Second):
		t.Fatal("ambiguous reset did not fence the session generation")
	}
}

func TestExecutionForwarderBindsBeforeActivationAndUnbindsBeforeCleanup(t *testing.T) {
	registry := mustSessionRegistry(t, 1)
	routes := mustRouteDirectory(t, registry, 1, 1)
	runtime := newCoordinatorRuntime()
	lifecycle := mustWorkerLifecycle(t, registry, runtime, routes)
	forwarder, err := newExecutionForwarder(routes, DefaultExecutionForwardingLimits())
	if err != nil {
		t.Fatalf("newExecutionForwarder() error = %v", err)
	}
	coordinator, err := newSessionCoordinatorWithForwarder(
		registry,
		runtime,
		routes,
		lifecycle,
		ChannelSessionLimits{MaxOpenChannels: 4, MaxDataBytes: 32},
		forwarder,
	)
	if err != nil {
		t.Fatalf("newSessionCoordinatorWithForwarder() error = %v", err)
	}
	var closing atomic.Bool
	var activeBound atomic.Bool
	var cleanupUnbound atomic.Bool
	runtime.before = func(call availabilityCall) {
		worker := runtime.workerSnapshot(call.name)
		if worker == nil {
			return
		}
		route, _, routed := routes.LookupExecutionIdentity(worker.GetExternalSlot().GetExecutionIdentity())
		forwarder.mu.RLock()
		bound := route != nil && forwarder.sessions[route] != nil
		forwarder.mu.RUnlock()
		if call.state == ateapipb.WorkerState_WORKER_STATE_ACTIVE {
			activeBound.Store(routed && bound)
		}
		if closing.Load() && call.state == ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			cleanupUnbound.Store(!bound)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := newExecutionTestSender()
	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	session, err := coordinator.establish(ctx, claim, hello, sender.send)
	if err != nil {
		t.Fatalf("establish() error = %v", err)
	}
	_ = nextExecutionServerFrame(t, sender)
	if !activeBound.Load() {
		t.Fatal("Worker became ACTIVE before its exact forwarding transport was bound")
	}
	closing.Store(true)
	if err := session.close(context.Background()); err != nil {
		t.Fatalf("close() error = %v", err)
	}
	if !cleanupUnbound.Load() {
		t.Fatal("Worker cleanup began before execution channels were unbound")
	}
}

func TestExecutionForwarderReplacementPreservesNewGeneration(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	oldConnection, _ := dialAcceptedExecution(t, fixture)

	secondSender := newExecutionTestSender()
	claim, hello := coordinatorInput(t, "registration-a", 2, "slot-a")
	second, err := fixture.coordinator.establish(fixture.ctx, claim, hello, secondSender.send)
	if err != nil {
		t.Fatalf("establish(generation 2) error = %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = second.close(cleanupCtx)
	})
	ready := nextExecutionServerFrame(t, secondSender)
	if ready.GetSessionGeneration() != 2 || ready.GetReady() == nil {
		t.Fatalf("generation 2 first frame = %v", ready)
	}
	_ = oldConnection.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := oldConnection.Read(make([]byte, 1)); err == nil {
		t.Fatal("generation 1 connection survived route replacement")
	}
	if err := fixture.session.close(context.Background()); err != nil {
		t.Fatalf("close(generation 1) error = %v", err)
	}
	forwarder := fixture.forwarder
	forwarder.mu.RLock()
	newBound := forwarder.sessions[second.route] == second.forwarding
	oldBound := forwarder.sessions[fixture.session.route] != nil
	forwarder.mu.RUnlock()
	if !newBound || oldBound {
		t.Fatalf("forwarder bindings after stale cleanup = new:%v old:%v", newBound, oldBound)
	}

	secondFixture := *fixture
	secondFixture.session = second
	secondFixture.sender = secondSender
	secondFixture.assignment = executionAssignment(onlyExecutionWorker(t, fixture.runtime))
	result := startExecutionDial(context.Background(), &ExternalExecutionDialer{forwarder: forwarder}, secondFixture.assignment)
	connection, channelID := acceptNextExecutionDial(t, &secondFixture, result)
	if channelID != 2 {
		t.Fatalf("generation 2 first channel ID = %d, want independent ID 2", channelID)
	}
	_ = connection.Close()
	_ = nextExecutionServerFrame(t, secondSender)
}

func TestExecutionForwarderSendFailureFencesGeneration(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	injected := errors.New("injected send failure")
	fixture.sender.setHook(func(_ context.Context, frame *externalproviderpb.ServerFrame) error {
		if frame.GetOpen() != nil {
			return injected
		}
		return nil
	})
	connection, err := (&ExternalExecutionDialer{forwarder: fixture.forwarder}).DialContext(context.Background(), fixture.assignment)
	if connection != nil || !errors.Is(err, ErrExternalExecutionUnavailable) {
		t.Fatalf("DialContext() = (%v, %v), want unavailable after send failure", connection, err)
	}
	select {
	case <-fixture.session.done():
	case <-time.After(time.Second):
		t.Fatal("stream send failure did not fence the generation")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		fixture.forwarder.mu.RLock()
		bound := fixture.forwarder.sessions[fixture.session.route] != nil
		fixture.forwarder.mu.RUnlock()
		if !bound {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("send failure retained the route transport binding")
}

func TestSessionAuthorityExecutionForwardingIsExplicitAndSingleUse(t *testing.T) {
	config := SessionRuntimeConfig{
		MaxTrackedRegistrations: 1,
		RouteLimits:             SessionRouteDirectoryLimits{MaxRoutes: 1, MaxBindings: 1},
		ChannelLimits:           ChannelSessionLimits{MaxOpenChannels: 1, MaxDataBytes: 32},
		ExecutionLimits:         DefaultExecutionForwardingLimits(),
	}
	authority, err := NewSessionAuthority(config)
	if err != nil {
		t.Fatalf("NewSessionAuthority() error = %v", err)
	}
	runtime := newCoordinatorRuntime()
	sessionRuntime, dialer, err := authority.BindExecutionForwarding(runtime, runtime)
	if err != nil || sessionRuntime == nil || dialer == nil || dialer.forwarder == nil {
		t.Fatalf("BindExecutionForwarding() = (%v, %v, %v)", sessionRuntime, dialer, err)
	}
	if !sessionRuntime.coordinator.activateWorkers || sessionRuntime.coordinator.forwarder != dialer.forwarder {
		t.Fatal("execution runtime did not derive activation from its exact dialer authority")
	}
	if second, secondDialer, err := authority.BindExecutionForwarding(runtime, runtime); err == nil || second != nil || secondDialer != nil {
		t.Fatalf("second BindExecutionForwarding() = (%v, %v, %v), want nil/nil/error", second, secondDialer, err)
	}
}

func TestExecutionForwarderConcurrentChannelsSerializeSend(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	dialer := &ExternalExecutionDialer{forwarder: fixture.forwarder}
	clientDone := make(chan struct{})
	clientErrors := make(chan error, 1)
	go func() {
		defer close(clientDone)
		for {
			select {
			case <-fixture.session.done():
				return
			case frame := <-fixture.sender.frames:
				if frame.GetOpen() == nil {
					continue
				}
				if err := fixture.session.applyClientFrame(clientAckFrame(1, frame.GetOpen().GetChannelId(), true, "")); err != nil {
					select {
					case clientErrors <- err:
					default:
					}
					return
				}
			}
		}
	}()

	const count = 32
	connections := make([]net.Conn, count)
	var wait sync.WaitGroup
	dialErrors := make(chan error, count)
	for index := range count {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			connection, err := dialer.DialContext(context.Background(), fixture.assignment)
			if err != nil {
				dialErrors <- err
				return
			}
			connections[index] = connection
		}(index)
	}
	wait.Wait()
	for range count {
		select {
		case err := <-dialErrors:
			t.Errorf("concurrent DialContext() error = %v", err)
		default:
		}
	}

	writeErrors := make(chan error, count)
	for _, connection := range connections {
		if connection == nil {
			continue
		}
		wait.Add(1)
		go func(connection net.Conn) {
			defer wait.Done()
			if _, err := connection.Write([]byte("x")); err != nil {
				writeErrors <- err
			}
		}(connection)
	}
	wait.Wait()
	for range count {
		select {
		case err := <-writeErrors:
			t.Errorf("concurrent Write() error = %v", err)
		default:
		}
	}
	if got := fixture.sender.maxActive.Load(); got != 1 {
		t.Fatalf("maximum concurrent stream.Send calls = %d, want 1", got)
	}
	select {
	case err := <-clientErrors:
		t.Fatalf("client ack pump error = %v", err)
	default:
	}
	for _, connection := range connections {
		if connection != nil {
			_ = connection.Close()
		}
	}
	fixture.cancel()
	select {
	case <-clientDone:
	case <-time.After(time.Second):
		t.Fatal("client pump did not stop")
	}
}
