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

	mu         sync.RWMutex
	beforeHook func(context.Context, *externalproviderpb.ServerFrame) error
	hook       func(context.Context, *externalproviderpb.ServerFrame) error

	active    atomic.Int32
	maxActive atomic.Int32
	// Production Connect stream.Send does not consume the callback's request
	// context. Tests which exercise the exact transport boundary enable this
	// flag so cancellation cannot make the fake return before the simulated
	// Send completes.
	ignoreContext atomic.Bool
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
	s.mu.RLock()
	beforeHook := s.beforeHook
	s.mu.RUnlock()
	if beforeHook != nil {
		if err := beforeHook(ctx, proto.Clone(cloned).(*externalproviderpb.ServerFrame)); err != nil {
			return err
		}
	}
	if s.ignoreContext.Load() {
		s.frames <- cloned
	} else {
		select {
		case s.frames <- cloned:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	s.mu.RLock()
	hook := s.hook
	s.mu.RUnlock()
	if hook != nil {
		return hook(ctx, proto.Clone(cloned).(*externalproviderpb.ServerFrame))
	}
	return nil
}

func (s *executionTestSender) setBeforeHook(hook func(context.Context, *externalproviderpb.ServerFrame) error) {
	s.mu.Lock()
	s.beforeHook = hook
	s.mu.Unlock()
}

func (s *executionTestSender) setHook(hook func(context.Context, *externalproviderpb.ServerFrame) error) {
	s.mu.Lock()
	s.hook = hook
	s.mu.Unlock()
}

func (s *executionTestSender) ignoreRequestContext() {
	s.ignoreContext.Store(true)
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
		WorkerNamespace:   worker.GetWorkerNamespace(),
		WorkerPool:        worker.GetWorkerPool(),
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

func TestCanceledExecutionDialFencesAmbiguousOpenGeneration(t *testing.T) {
	tests := []struct {
		name    string
		context func() (context.Context, context.CancelFunc, func())
		want    error
	}{
		{name: "canceled", want: context.Canceled, context: func() (context.Context, context.CancelFunc, func()) {
			ctx, cancel := context.WithCancel(context.Background())
			return ctx, cancel, cancel
		}},
		{name: "deadline", want: context.DeadlineExceeded, context: func() (context.Context, context.CancelFunc, func()) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			return ctx, cancel, func() {}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutionForwarderFixture(t, nil)
			dialer := &ExternalExecutionDialer{forwarder: fixture.forwarder}
			ctx, cleanup, trigger := test.context()
			defer cleanup()
			result := startExecutionDial(ctx, dialer, fixture.assignment)
			open := nextExecutionServerFrame(t, fixture.sender)
			if open.GetOpen() == nil {
				t.Fatalf("first Dial frame = %v, want Open", open)
			}
			trigger()
			select {
			case dialed := <-result:
				if dialed.conn != nil || !errors.Is(dialed.err, test.want) {
					t.Fatalf("interrupted DialContext() = (%v, %v), want nil/%v", dialed.conn, dialed.err, test.want)
				}
			case <-time.After(time.Second):
				t.Fatal("interrupted DialContext did not return")
			}

			// Awaiting OpenAck cannot be reset unambiguously. The whole generation
			// must become unusable and release its process-owned connection map.
			select {
			case <-fixture.session.forwarding.done():
			default:
				t.Fatal("ambiguous interrupted Open retained a live execution generation")
			}
			fixture.session.forwarding.mu.Lock()
			connections := len(fixture.session.forwarding.conns)
			fixture.session.forwarding.mu.Unlock()
			if connections != 0 {
				t.Fatalf("ambiguous interrupted Open retained %d process connections", connections)
			}
			fixture.forwarder.mu.RLock()
			_, bound := fixture.forwarder.sessions[fixture.session.route]
			fixture.forwarder.mu.RUnlock()
			if bound {
				t.Fatal("ambiguous interrupted Open retained a reusable route binding")
			}
			if connection, err := dialer.DialContext(context.Background(), fixture.assignment); connection != nil ||
				!errors.Is(err, ErrExternalExecutionUnavailable) {
				t.Fatalf("DialContext() on fenced generation = (%v, %v), want unavailable", connection, err)
			}
		})
	}
}

func TestInterruptedExecutionDialDuringOpenSendPreservesContextCause(t *testing.T) {
	tests := []struct {
		name    string
		context func() (context.Context, context.CancelFunc, func())
		want    error
	}{
		{name: "canceled", want: context.Canceled, context: func() (context.Context, context.CancelFunc, func()) {
			ctx, cancel := context.WithCancel(context.Background())
			return ctx, cancel, cancel
		}},
		{name: "deadline", want: context.DeadlineExceeded, context: func() (context.Context, context.CancelFunc, func()) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			return ctx, cancel, func() {}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutionForwarderFixture(t, nil)
			fixture.sender.ignoreRequestContext()
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			fixture.sender.setBeforeHook(func(_ context.Context, frame *externalproviderpb.ServerFrame) error {
				if frame.GetOpen() != nil {
					once.Do(func() { close(started) })
					<-release // adversarial transport deliberately ignores request context
				}
				return nil
			})
			ctx, cleanup, trigger := test.context()
			defer cleanup()
			result := startExecutionDial(ctx, &ExternalExecutionDialer{forwarder: fixture.forwarder}, fixture.assignment)
			select {
			case <-started:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("Open did not enter the transport sender")
			}
			trigger()
			select {
			case dialed := <-result:
				if dialed.conn != nil || !errors.Is(dialed.err, test.want) {
					close(release)
					t.Fatalf("DialContext() interrupted in Open Send = (%v, %v), want nil/%v", dialed.conn, dialed.err, test.want)
				}
			case <-time.After(time.Second):
				close(release)
				t.Fatal("interrupted Open Send did not return to DialContext")
			}
			close(release)
			deadline := time.Now().Add(time.Second)
			for fixture.sender.active.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if active := fixture.sender.active.Load(); active != 0 {
				t.Fatalf("interrupted Open retained %d production Send calls", active)
			}
			open := nextExecutionServerFrame(t, fixture.sender)
			if open.GetOpen() == nil {
				t.Fatalf("interrupted production Send frame = %v, want ambiguous Open", open)
			}
			select {
			case <-fixture.session.forwarding.done():
			default:
				t.Fatal("interrupted Open Send did not fence the generation")
			}
			fixture.forwarder.mu.RLock()
			_, bound := fixture.forwarder.sessions[fixture.session.route]
			fixture.forwarder.mu.RUnlock()
			if bound {
				t.Fatal("interrupted Open Send retained a reusable route binding")
			}
			if connection, err := (&ExternalExecutionDialer{forwarder: fixture.forwarder}).DialContext(context.Background(), fixture.assignment); connection != nil ||
				!errors.Is(err, ErrExternalExecutionUnavailable) {
				t.Fatalf("DialContext() after interrupted Open Send = (%v, %v), want unavailable", connection, err)
			}
		})
	}
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
		{name: "unspecified provider", edit: func(a *ateapipb.WorkerAssignment) {
			a.Provider = ateapipb.WorkerProvider_WORKER_PROVIDER_UNSPECIFIED
		}},
		{name: "Kubernetes provider", edit: func(a *ateapipb.WorkerAssignment) {
			a.Provider = ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD
		}},
		{name: "unknown provider", edit: func(a *ateapipb.WorkerAssignment) { a.Provider = ateapipb.WorkerProvider(99) }},
		{name: "missing Worker", edit: func(a *ateapipb.WorkerAssignment) { a.Worker = nil }},
		{name: "namespaced Worker ref", edit: func(a *ateapipb.WorkerAssignment) { a.Worker.Atespace = "team-a" }},
		{name: "invalid Worker name", edit: func(a *ateapipb.WorkerAssignment) { a.Worker.Name = "Bad_Worker" }},
		{name: "noncanonical Worker UID", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerResourceUid = "NOT-A-UUID" }},
		{name: "uppercase Worker UID", edit: func(a *ateapipb.WorkerAssignment) {
			a.WorkerResourceUid = "00000000-0000-4000-8000-0000000000AA"
		}},
		{name: "missing Worker namespace", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerNamespace = "" }},
		{name: "invalid Worker namespace", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerNamespace = "Bad_NS" }},
		{name: "missing Worker pool", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerPool = "" }},
		{name: "invalid Worker pool", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerPool = "Bad_Pool" }},
		{name: "Kubernetes Pod name", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerPod = "forbidden-pod" }},
		{name: "Kubernetes Pod UID", edit: func(a *ateapipb.WorkerAssignment) {
			a.WorkerPodUid = "00000000-0000-4000-8000-000000000098"
		}},
		{name: "Kubernetes Pod IP", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerPodIp = "192.0.2.1" }},
		{name: "missing external slot", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot = nil }},
		{name: "missing execution identity", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.ExecutionIdentity = "" }},
		{name: "missing locality identity", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.LocalityIdentity = "" }},
		{name: "invalid locality identity", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.LocalityIdentity = "bad locality" }},
		{name: "missing owner Atespace", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.OwnerAtespace = "" }},
		{name: "invalid owner Atespace", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.OwnerAtespace = "Bad_Owner" }},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			assignment := proto.Clone(fixture.assignment).(*ateapipb.WorkerAssignment)
			test.edit(assignment)
			if connection, err := dialer.DialContext(context.Background(), assignment); !errors.Is(err, ErrInvalidExternalExecutionAssignment) || connection != nil {
				t.Fatalf("DialContext() = (%v, %v), want invalid assignment", connection, err)
			}
			select {
			case frame := <-fixture.sender.frames:
				t.Fatalf("invalid assignment emitted a frame: %v", frame)
			default:
			}
		})
	}

	stale := []struct {
		name string
		edit func(*ateapipb.WorkerAssignment)
	}{
		{name: "different Worker name", edit: func(a *ateapipb.WorkerAssignment) { a.Worker.Name = "different-worker" }},
		{name: "different canonical UID", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerResourceUid = "00000000-0000-4000-8000-000000000099" }},
		{name: "different Worker namespace", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerNamespace = "other-workers" }},
		{name: "different Worker pool", edit: func(a *ateapipb.WorkerAssignment) { a.WorkerPool = "other-pool" }},
		{name: "different execution identity", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.ExecutionIdentity = "different.execution" }},
		{name: "different locality identity", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.LocalityIdentity = "different.locality" }},
		{name: "different owner Atespace", edit: func(a *ateapipb.WorkerAssignment) { a.ExternalSlot.OwnerAtespace = "other-team" }},
	}
	for _, test := range stale {
		t.Run(test.name, func(t *testing.T) {
			assignment := proto.Clone(fixture.assignment).(*ateapipb.WorkerAssignment)
			test.edit(assignment)
			if connection, err := dialer.DialContext(context.Background(), assignment); !errors.Is(err, ErrExternalExecutionUnavailable) || connection != nil {
				t.Fatalf("DialContext() = (%v, %v), want unavailable", connection, err)
			}
			select {
			case frame := <-fixture.sender.frames:
				t.Fatalf("stale assignment emitted a frame: %v", frame)
			default:
			}
		})
	}

	connection, _ := dialAcceptedExecution(t, fixture)
	if err := fixture.session.wire.lockOperation(fixture.ctx); err != nil {
		t.Fatalf("lock operation gate: %v", err)
	}
	type writeOutcome struct {
		count int
		err   error
	}
	writeResult := make(chan writeOutcome, 1)
	go func() {
		count, err := connection.Write([]byte("racing-route-close"))
		writeResult <- writeOutcome{count: count, err: err}
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
		fixture.session.wire.unlockOperation()
		t.Fatal("Write did not reach the operation gate")
	}
	closeResult := make(chan bool, 1)
	go func() { closeResult <- fixture.routes.Close(fixture.session.route) }()
	select {
	case <-fixture.session.forwarding.done():
	case <-time.After(time.Second):
		fixture.session.wire.unlockOperation()
		t.Fatal("route Close did not synchronously cancel execution operations")
	}
	fixture.session.wire.unlockOperation()
	select {
	case closed := <-closeResult:
		if !closed {
			t.Fatal("Close(exact route) = false")
		}
	case <-time.After(time.Second):
		t.Fatal("route Close did not join the operation gate")
	}
	select {
	case outcome := <-writeResult:
		if outcome.count != 0 || outcome.err == nil {
			t.Fatalf("Write() racing route Close = (%d, %v), want fenced", outcome.count, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("route-fenced Write did not return")
	}
	if err := connection.CloseWrite(); err == nil {
		t.Fatal("CloseWrite() after route Close emitted from an old generation")
	}
	select {
	case frame := <-fixture.sender.frames:
		t.Fatalf("old generation emitted a frame after route Close: %v", frame)
	default:
	}
	select {
	case <-fixture.session.done():
	case <-time.After(5 * time.Second):
		t.Fatal("route close did not stop the coordinated session")
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		fixture.forwarder.mu.RLock()
		_, bound := fixture.forwarder.sessions[fixture.session.route]
		fixture.forwarder.mu.RUnlock()
		if !bound {
			break
		}
		time.Sleep(time.Millisecond)
	}
	fixture.forwarder.mu.RLock()
	_, bound := fixture.forwarder.sessions[fixture.session.route]
	fixture.forwarder.mu.RUnlock()
	if bound {
		t.Fatal("route close left the fenced execution session bound")
	}
	buffer := make([]byte, 1)
	if _, err := connection.Read(buffer); err == nil {
		t.Fatal("live connection survived exact route close")
	}
	if connection, err := dialer.DialContext(context.Background(), fixture.assignment); !errors.Is(err, ErrExternalExecutionUnavailable) || connection != nil {
		t.Fatalf("DialContext() after route close = (%v, %v)", connection, err)
	}
}

func TestRouteCloseWaitsForInFlightTransportSend(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	connection, channelID := dialAcceptedExecution(t, fixture)
	fixture.sender.ignoreRequestContext()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	fixture.sender.setBeforeHook(func(_ context.Context, frame *externalproviderpb.ServerFrame) error {
		if frame.GetData().GetChannelId() == channelID {
			once.Do(func() { close(started) })
			<-release // model production stream.Send, which has no per-send context
		}
		return nil
	})
	type writeOutcome struct {
		count int
		err   error
	}
	writeResult := make(chan writeOutcome, 1)
	go func() {
		count, err := connection.Write([]byte("in-flight"))
		writeResult <- writeOutcome{count: count, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("Write did not enter the transport sender")
	}

	closeResult := make(chan bool, 1)
	go func() { closeResult <- fixture.routes.Close(fixture.session.route) }()
	select {
	case closed := <-closeResult:
		close(release)
		t.Fatalf("route Close returned before in-flight Send completed: %v", closed)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case closed := <-closeResult:
		if !closed {
			t.Fatal("Close(exact route) = false")
		}
	case <-time.After(time.Second):
		t.Fatal("route Close did not return after in-flight Send completed")
	}
	if active := fixture.sender.active.Load(); active != 0 {
		t.Fatalf("route Close returned with %d production Send calls still active", active)
	}
	frame := nextExecutionServerFrame(t, fixture.sender)
	if frame.GetData().GetChannelId() != channelID || string(frame.GetData().GetData()) != "in-flight" {
		t.Fatalf("completed in-flight frame = %v", frame)
	}
	select {
	case outcome := <-writeResult:
		if outcome.err == nil && outcome.count != len("in-flight") {
			t.Fatalf("Write() = (%d, nil), want complete frame length", outcome.count)
		}
	case <-time.After(time.Second):
		t.Fatal("Write did not return after route fencing")
	}
	if err := connection.CloseWrite(); err == nil {
		t.Fatal("CloseWrite() succeeded after route Close")
	}
	select {
	case frame := <-fixture.sender.frames:
		t.Fatalf("old generation emitted after route Close returned: %v", frame)
	default:
	}
}

func TestRouteReplacementWaitsForInFlightTransportSend(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	oldConnection, oldChannelID := dialAcceptedExecution(t, fixture)
	fixture.sender.ignoreRequestContext()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	fixture.sender.setBeforeHook(func(_ context.Context, frame *externalproviderpb.ServerFrame) error {
		if frame.GetData().GetChannelId() == oldChannelID {
			once.Do(func() { close(started) })
			<-release // production stream.Send has no request-context cancellation
		}
		return nil
	})
	type writeOutcome struct {
		count int
		err   error
	}
	writeResult := make(chan writeOutcome, 1)
	go func() {
		count, err := oldConnection.Write([]byte("old-in-flight"))
		writeResult <- writeOutcome{count: count, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("old generation Write did not enter the transport sender")
	}

	secondSender := newExecutionTestSender()
	claim, hello := coordinatorInput(t, "registration-a", 2, "slot-a")
	type establishOutcome struct {
		session *coordinatedSession
		err     error
	}
	established := make(chan establishOutcome, 1)
	go func() {
		session, err := fixture.coordinator.establish(fixture.ctx, claim, hello, secondSender.send)
		established <- establishOutcome{session: session, err: err}
	}()
	select {
	case outcome := <-established:
		close(release)
		if outcome.session != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = outcome.session.close(cleanupCtx)
		}
		t.Fatalf("generation 2 establishment crossed an in-flight old Send: %v", outcome.err)
	case <-time.After(25 * time.Millisecond):
	}
	select {
	case frame := <-secondSender.frames:
		close(release)
		t.Fatalf("replacement emitted generation 2 frame before fencing old Send: %v", frame)
	default:
	}
	close(release)

	ready := nextExecutionServerFrame(t, secondSender)
	if ready.GetReady() == nil || ready.GetSessionGeneration() != 2 {
		t.Fatalf("generation 2 first frame = %v", ready)
	}
	var second *coordinatedSession
	select {
	case outcome := <-established:
		if outcome.err != nil || outcome.session == nil {
			t.Fatalf("establish(generation 2) = (%v, %v)", outcome.session, outcome.err)
		}
		second = outcome.session
	case <-time.After(time.Second):
		t.Fatal("generation 2 establishment did not resume after old Send completed")
	}
	if active := fixture.sender.active.Load(); active != 0 {
		t.Fatalf("route replacement returned with %d old production Send calls still active", active)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = second.close(cleanupCtx)
	})
	frame := nextExecutionServerFrame(t, fixture.sender)
	if frame.GetData().GetChannelId() != oldChannelID || string(frame.GetData().GetData()) != "old-in-flight" {
		t.Fatalf("completed old in-flight frame = %v", frame)
	}
	select {
	case <-writeResult:
	case <-time.After(time.Second):
		t.Fatal("old generation Write did not return after replacement")
	}
	if count, err := oldConnection.Write([]byte("after-replacement")); count != 0 || err == nil {
		t.Fatalf("old Write() after replacement = (%d, %v), want fenced", count, err)
	}
	if err := oldConnection.CloseWrite(); err == nil {
		t.Fatal("old CloseWrite() succeeded after replacement")
	}
	select {
	case frame := <-fixture.sender.frames:
		t.Fatalf("old generation emitted a frame after replacement: %v", frame)
	default:
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

	t.Run("SetDeadline updates an active write and stale timers are fenced", func(t *testing.T) {
		fixture := newExecutionForwarderFixture(t, nil)
		connection, _ := dialAcceptedExecution(t, fixture)
		if err := connection.SetWriteDeadline(time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("initial SetWriteDeadline() error = %v", err)
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
		var operation, staleGeneration uint64
		var staleCancel context.CancelCauseFunc
		for time.Now().Before(deadline) {
			connection.stateMu.Lock()
			operation = connection.writeOperation
			staleGeneration = connection.writeTimerGen
			staleCancel = connection.writeCancel
			connection.stateMu.Unlock()
			if staleCancel != nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if staleCancel == nil {
			t.Fatal("Write did not install its active deadline timer")
		}
		if err := connection.SetWriteDeadline(time.Now().Add(2 * time.Hour)); err != nil {
			t.Fatalf("rescheduled SetWriteDeadline() error = %v", err)
		}
		connection.expireWriteDeadline(operation, staleGeneration, staleCancel)
		select {
		case err := <-writeResult:
			t.Fatalf("stale timer canceled the current Write: %v", err)
		case <-time.After(20 * time.Millisecond):
		}

		if err := connection.SetDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
			t.Fatalf("SetDeadline() error = %v", err)
		}
		select {
		case err := <-writeResult:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("active Write after SetDeadline() error = %v, want deadline", err)
			}
		case <-time.After(time.Second):
			t.Fatal("SetDeadline did not update the active write timer")
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
	select {
	case <-fixture.session.forwarding.done():
	default:
		t.Fatal("route replacement returned before fencing generation 1 execution")
	}
	if count, err := oldConnection.Write([]byte("after-replacement")); count != 0 || err == nil {
		t.Fatalf("generation 1 Write() after replacement = (%d, %v), want fenced", count, err)
	}
	if err := oldConnection.CloseWrite(); err == nil {
		t.Fatal("generation 1 CloseWrite() succeeded after replacement")
	}
	select {
	case frame := <-fixture.sender.frames:
		t.Fatalf("generation 1 emitted a frame after replacement: %v", frame)
	default:
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
