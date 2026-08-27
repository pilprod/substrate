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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type connectTestReceive struct {
	frame *externalproviderpb.ClientFrame
	err   error
}

type connectTestStream struct {
	grpc.ServerStream
	ctx context.Context

	receive    chan connectTestReceive
	sendErr    error
	beforeSend func(*externalproviderpb.ServerFrame) error
	onSend     func(*externalproviderpb.ServerFrame)

	mu   sync.Mutex
	sent []*externalproviderpb.ServerFrame
}

func newConnectTestStream(ctx context.Context, receives ...connectTestReceive) *connectTestStream {
	queue := make(chan connectTestReceive, len(receives))
	for _, receive := range receives {
		queue <- receive
	}
	close(queue)
	return &connectTestStream{ctx: ctx, receive: queue}
}

func newIdleConnectTestStream(ctx context.Context, first *externalproviderpb.ClientFrame) *connectTestStream {
	queue := make(chan connectTestReceive, 1)
	queue <- connectTestReceive{frame: first}
	return &connectTestStream{ctx: ctx, receive: queue}
}

func (s *connectTestStream) Context() context.Context { return s.ctx }

func (s *connectTestStream) Recv() (*externalproviderpb.ClientFrame, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case receive, open := <-s.receive:
		if !open {
			return nil, io.EOF
		}
		return receive.frame, receive.err
	}
}

func (s *connectTestStream) Send(frame *externalproviderpb.ServerFrame) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	cloned := proto.Clone(frame).(*externalproviderpb.ServerFrame)
	if s.beforeSend != nil {
		if err := s.beforeSend(proto.Clone(cloned).(*externalproviderpb.ServerFrame)); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.sent = append(s.sent, cloned)
	s.mu.Unlock()
	if s.onSend != nil {
		s.onSend(proto.Clone(cloned).(*externalproviderpb.ServerFrame))
	}
	return nil
}

func (s *connectTestStream) sentSnapshot() []*externalproviderpb.ServerFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*externalproviderpb.ServerFrame, len(s.sent))
	for index, frame := range s.sent {
		result[index] = proto.Clone(frame).(*externalproviderpb.ServerFrame)
	}
	return result
}

func connectTestContext(parent context.Context, fill byte) context.Context {
	return metadata.NewIncomingContext(parent, metadata.Pairs("authorization", "Bearer "+string(testCredential(fill))))
}

func connectTestBroker(
	t *testing.T,
	coordinator *sessionCoordinator,
	claim func(context.Context, string, CredentialDigest) (SessionClaim, error),
	opts ...BrokerOption,
) (*Broker, *fakeStore) {
	t.Helper()
	store := &fakeStore{claim: claim}
	claimInstallGate, err := newClaimInstallGate(defaultMaxClaimInstallInFlight, defaultMaxClaimInstallKeys)
	if err != nil {
		t.Fatalf("newClaimInstallGate() error = %v", err)
	}
	brokerOptions := []BrokerOption{WithSessionRuntime(&SessionRuntime{coordinator: coordinator, claimInstallGate: claimInstallGate})}
	brokerOptions = append(brokerOptions, opts...)
	broker, err := newBroker(
		store,
		bytes.NewReader(make([]byte, credentialEntropyBytes)),
		time.Minute,
		brokerOptions...,
	)
	if err != nil {
		t.Fatalf("newBroker() error = %v", err)
	}
	return broker, store
}

func TestBrokerConnectClaimsAfterHelloThenRunsBoundedHeartbeatPump(t *testing.T) {
	coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim := validSessionClaim(1)
	credential := testCredential(0x81)
	var stream *connectTestStream
	broker, store := connectTestBroker(t, coordinator, func(_ context.Context, registrationUID string, digest CredentialDigest) (SessionClaim, error) {
		if registrationUID != claim.Registration.UID {
			t.Errorf("registration UID = %q, want %q", registrationUID, claim.Registration.UID)
		}
		if digest != digestCredential(sessionDigestDomain, credential) {
			t.Errorf("session digest = %x, want digest of presented credential", digest)
		}
		if len(stream.sentSnapshot()) != 0 {
			t.Error("Ready was sent before the atomic session claim")
		}
		return claim, nil
	})
	heartbeat := &externalproviderpb.ClientFrame{
		SessionGeneration: claim.Generation,
		Frame: &externalproviderpb.ClientFrame_Heartbeat{Heartbeat: &externalproviderpb.Heartbeat{
			Nonce: 91,
		}},
	}
	stream = newConnectTestStream(
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(credential))),
		connectTestReceive{frame: validClientFrame()},
		connectTestReceive{frame: heartbeat},
	)
	stream.onSend = func(frame *externalproviderpb.ServerFrame) {
		if frame.GetReady() != nil {
			runtime.record("ready")
		}
	}

	if err := broker.Connect(stream); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if store.claimCalls != 1 {
		t.Fatalf("ClaimExternalProviderSession calls = %d, want 1", store.claimCalls)
	}
	sent := stream.sentSnapshot()
	if len(sent) != 2 || sent[0].GetReady() == nil || sent[0].GetSessionGeneration() != claim.Generation ||
		sent[1].GetHeartbeat().GetNonce() != 91 || !sent[1].GetHeartbeat().GetAcknowledgement() {
		t.Fatalf("server frames = %v, want Ready then heartbeat acknowledgement", sent)
	}
	wantEvents := []string{
		"reconcile",
		"ready",
		"availability:WORKER_STATE_OFFLINE",
		"availability:WORKER_STATE_ACTIVE",
		"availability:WORKER_STATE_OFFLINE",
	}
	if got := runtime.eventSnapshot(); !slices.Equal(got, wantEvents) {
		t.Fatalf("events = %v, want %v", got, wantEvents)
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after EOF cleanup = %+v, want empty", stats)
	}
	if _, current := registry.lookup(claim.Registration.UID, claim.Generation); current {
		t.Fatal("session lease remained current after EOF cleanup")
	}
}

func TestBrokerConnectPrevalidatesBeforeClaimAndReady(t *testing.T) {
	coordinator, _, _, _ := newCoordinatorHarness(t, 1, 1, 1)
	broker, store := connectTestBroker(t, coordinator, func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		t.Fatal("ClaimExternalProviderSession called for invalid Hello")
		return SessionClaim{}, nil
	})
	invalid := validClientFrame()
	invalid.GetHello().RegistrationUid = ""
	stream := newConnectTestStream(
		connectTestContext(context.Background(), 0x82),
		connectTestReceive{frame: invalid},
	)
	err := broker.Connect(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Connect() code = %v, want InvalidArgument", status.Code(err))
	}
	if store.claimCalls != 0 || len(stream.sentSnapshot()) != 0 {
		t.Fatalf("invalid Hello caused claim/send = %d/%d", store.claimCalls, len(stream.sentSnapshot()))
	}
}

func TestBrokerConnectBoundsAndTimesOutPendingHandshakes(t *testing.T) {
	coordinator, _, _, _ := newCoordinatorHarness(t, 1, 1, 1)
	claim := validSessionClaim(1)
	broker, store := connectTestBroker(
		t,
		coordinator,
		func(context.Context, string, CredentialDigest) (SessionClaim, error) { return claim, nil },
		WithConnectHandshakeLimits(ConnectHandshakeLimits{MaxPending: 1, Timeout: 20 * time.Millisecond}),
	)
	firstParent, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	first := &connectTestStream{
		ctx:     connectTestContext(firstParent, 0x8b),
		receive: make(chan connectTestReceive),
	}
	firstResult := make(chan error, 1)
	go func() { firstResult <- broker.Connect(first) }()
	requirePendingHandshakeCount(t, broker, 1)

	second := newConnectTestStream(
		connectTestContext(context.Background(), 0x8c),
		connectTestReceive{frame: validClientFrame()},
	)
	if err := broker.Connect(second); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("concurrent Connect() code = %v, want ResourceExhausted", status.Code(err))
	}
	select {
	case err := <-firstResult:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("stalled Connect() code = %v, want DeadlineExceeded", status.Code(err))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled Connect did not reach its handshake deadline")
	}
	// The fake Recv is still blocked after the handler returns. Its slot must
	// remain held until cancellation actually releases that receiver.
	if got := len(broker.pendingHandshakes); got != 1 {
		t.Fatalf("pending handshakes after deadline = %d, want 1", got)
	}
	third := newConnectTestStream(
		connectTestContext(context.Background(), 0x8d),
		connectTestReceive{frame: validClientFrame()},
	)
	if err := broker.Connect(third); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("post-timeout Connect() code = %v, want ResourceExhausted until Recv exits", status.Code(err))
	}
	cancelFirst()
	requirePendingHandshakeCount(t, broker, 0)

	fourth := newConnectTestStream(
		connectTestContext(context.Background(), 0x8e),
		connectTestReceive{frame: validClientFrame()},
	)
	if err := broker.Connect(fourth); err != nil {
		t.Fatalf("Connect() after slot release error = %v", err)
	}
	if store.claimCalls != 1 {
		t.Fatalf("session claim calls = %d, want only final valid handshake", store.claimCalls)
	}
}

func TestBrokerConnectHandshakeDeadlineBoundsSessionClaim(t *testing.T) {
	coordinator, _, routes, _ := newCoordinatorHarness(t, 1, 1, 1)
	broker, store := connectTestBroker(
		t,
		coordinator,
		func(ctx context.Context, _ string, _ CredentialDigest) (SessionClaim, error) {
			<-ctx.Done()
			return SessionClaim{}, ctx.Err()
		},
		WithConnectHandshakeLimits(ConnectHandshakeLimits{MaxPending: 1, Timeout: 20 * time.Millisecond}),
	)
	stream := newConnectTestStream(
		connectTestContext(context.Background(), 0x8f),
		connectTestReceive{frame: validClientFrame()},
	)
	if err := broker.Connect(stream); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Connect() code = %v, want DeadlineExceeded", status.Code(err))
	}
	if store.claimCalls != 1 || len(stream.sentSnapshot()) != 0 {
		t.Fatalf("timed-out claim calls/sends = %d/%d, want 1/0", store.claimCalls, len(stream.sentSnapshot()))
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after claim deadline = %+v, want empty", stats)
	}
	requireClaimInstallGateStats(t, broker.sessionRuntime.claimInstallGate, claimInstallGateStats{})
}

func TestBrokerConnectReadyFailureCleansWithoutLeakingTransportError(t *testing.T) {
	coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim := validSessionClaim(1)
	broker, _ := connectTestBroker(t, coordinator, func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		return claim, nil
	})
	const secret = "peer supplied send failure secret"
	stream := newConnectTestStream(
		connectTestContext(context.Background(), 0x83),
		connectTestReceive{frame: validClientFrame()},
	)
	stream.sendErr = errors.New(secret)
	err := broker.Connect(stream)
	if status.Code(err) != codes.Unavailable || strings.Contains(err.Error(), secret) {
		t.Fatalf("Connect() error = %v, want redacted Unavailable", err)
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after Ready failure = %+v, want empty", stats)
	}
	if _, current := registry.lookup(claim.Registration.UID, claim.Generation); current {
		t.Fatal("session lease remained current after Ready failure")
	}
	for _, worker := range runtime.workers {
		if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			t.Fatalf("Worker state after Ready failure = %v, want OFFLINE", worker.GetStatus().GetState())
		}
	}
}

func TestBrokerConnectRedactsClaimFailureBeforeReady(t *testing.T) {
	coordinator, registry, routes, _ := newCoordinatorHarness(t, 1, 1, 1)
	const secret = "database session claim failure secret"
	broker, store := connectTestBroker(t, coordinator, func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		return SessionClaim{}, errors.New(secret)
	})
	stream := newConnectTestStream(
		connectTestContext(context.Background(), 0x89),
		connectTestReceive{frame: validClientFrame()},
	)
	err := broker.Connect(stream)
	if status.Code(err) != codes.Unavailable || strings.Contains(err.Error(), secret) {
		t.Fatalf("Connect() error = %v, want redacted Unavailable", err)
	}
	if store.claimCalls != 1 || len(stream.sentSnapshot()) != 0 {
		t.Fatalf("claim failure calls/sends = %d/%d, want 1/0", store.claimCalls, len(stream.sentSnapshot()))
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after claim failure = %+v, want empty", stats)
	}
	if _, current := registry.lookup("registration-a", 7); current {
		t.Fatal("claim failure installed a session lease")
	}
	requireClaimInstallGateStats(t, broker.sessionRuntime.claimInstallGate, claimInstallGateStats{})
}

func TestBrokerConnectRejectsUnsupportedClientChannelWithoutDroppingSession(t *testing.T) {
	coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim := validSessionClaim(1)
	broker, _ := connectTestBroker(t, coordinator, func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		return claim, nil
	})
	open := &externalproviderpb.ClientFrame{
		SessionGeneration: claim.Generation,
		Frame: &externalproviderpb.ClientFrame_Open{Open: &externalproviderpb.OpenChannel{
			ChannelId: 1,
			Kind:      externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS,
			SlotId:    "slot-a",
		}},
	}
	stream := newConnectTestStream(
		connectTestContext(context.Background(), 0x84),
		connectTestReceive{frame: validClientFrame()},
		connectTestReceive{frame: open},
	)
	err := broker.Connect(stream)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	sent := stream.sentSnapshot()
	if len(sent) != 2 || sent[0].GetReady() == nil || sent[1].GetOpenAck().GetChannelId() != 1 ||
		sent[1].GetOpenAck().GetAccepted() || sent[1].GetOpenAck().GetErrorMessage() == "" {
		t.Fatalf("server frames = %v, want Ready then bounded rejection", sent)
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after unsupported effect = %+v, want empty", stats)
	}
	if _, current := registry.lookup(claim.Registration.UID, claim.Generation); current {
		t.Fatal("session lease remained current after unsupported effect")
	}
	for _, worker := range runtime.workers {
		if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			t.Fatalf("Worker state after unsupported effect = %v, want OFFLINE", worker.GetStatus().GetState())
		}
	}
}

func TestBrokerConnectCleanupFailureLeavesClosingRouteForReplacementRecovery(t *testing.T) {
	coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim := validSessionClaim(1)
	var generation atomic.Uint64
	generation.Store(claim.Generation - 1)
	broker, _ := connectTestBroker(t, coordinator, func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		claimed := claim
		claimed.Generation = generation.Add(1)
		return claimed, nil
	})
	const secret = "offline storage failure secret"
	var armed atomic.Bool
	runtime.before = func(call availabilityCall) {
		if call.state != ateapipb.WorkerState_WORKER_STATE_ACTIVE || !armed.CompareAndSwap(false, true) {
			return
		}
		runtime.mu.Lock()
		runtime.offlineFails = 1
		runtime.offlineErr = errors.New(secret)
		runtime.mu.Unlock()
	}
	first := newConnectTestStream(
		connectTestContext(context.Background(), 0x85),
		connectTestReceive{frame: validClientFrame()},
	)
	err := broker.Connect(first)
	if status.Code(err) != codes.Unavailable || strings.Contains(err.Error(), secret) {
		t.Fatalf("first Connect() error = %v, want redacted cleanup failure", err)
	}
	worker := onlyCoordinatorWorker(t, runtime)
	lease, current := registry.lookup(claim.Registration.UID, claim.Generation)
	route := routes.routeForLease(lease)
	if !current || route == nil || route.CancellationCause() == nil || routes.Stats().Routes != 1 {
		t.Fatalf("cleanup failure route/current/stats = %v/%v/%+v, want retained CLOSING route", route, current, routes.Stats())
	}
	if routes.AssignmentGuard().AllowsCandidate(worker) {
		t.Fatal("CLOSING route remained eligible for assignment")
	}
	if _, current := registry.lookup(claim.Registration.UID, claim.Generation); !current {
		t.Fatal("cleanup failure fenced the lease before OFFLINE succeeded")
	}

	runtime.mu.Lock()
	runtime.before = nil
	runtime.offlineFails = 0
	runtime.offlineErr = nil
	runtime.mu.Unlock()
	second := newConnectTestStream(
		connectTestContext(context.Background(), 0x86),
		connectTestReceive{frame: validClientFrame()},
	)
	if err := broker.Connect(second); err != nil {
		t.Fatalf("replacement Connect() error = %v", err)
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after replacement cleanup = %+v, want empty", stats)
	}
	worker = onlyCoordinatorWorker(t, runtime)
	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		t.Fatalf("Worker state after replacement cleanup = %v, want OFFLINE", worker.GetStatus().GetState())
	}
}

func TestBrokerConnectCleanupFailurePreservesPrimaryStatus(t *testing.T) {
	coordinator, _, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim := validSessionClaim(1)
	broker, _ := connectTestBroker(t, coordinator, func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		return claim, nil
	})
	var armed atomic.Bool
	runtime.before = func(call availabilityCall) {
		if call.state != ateapipb.WorkerState_WORKER_STATE_ACTIVE || !armed.CompareAndSwap(false, true) {
			return
		}
		runtime.mu.Lock()
		runtime.offlineFails = 1
		runtime.offlineErr = errors.New("cleanup failure secret")
		runtime.mu.Unlock()
	}
	stream := newConnectTestStream(
		connectTestContext(context.Background(), 0x91),
		connectTestReceive{frame: validClientFrame()},
		connectTestReceive{frame: validClientFrame()},
	)
	err := broker.Connect(stream)
	if status.Code(err) != codes.InvalidArgument || strings.Contains(err.Error(), "cleanup failure secret") {
		t.Fatalf("Connect() error = %v, want redacted primary InvalidArgument", err)
	}
	if stats := routes.Stats(); stats.Routes != 1 {
		t.Fatalf("route stats after cleanup failure = %+v, want retained CLOSING route", stats)
	}
}

func TestBrokerConnectReplacementInterruptsIdleFrameReceive(t *testing.T) {
	coordinator, _, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim := validSessionClaim(1)
	firstCredential := testCredential(0x87)
	secondCredential := testCredential(0x88)
	broker, _ := connectTestBroker(t, coordinator, func(_ context.Context, _ string, digest CredentialDigest) (SessionClaim, error) {
		claimed := claim
		switch digest {
		case digestCredential(sessionDigestDomain, firstCredential):
			claimed.Generation = 7
		case digestCredential(sessionDigestDomain, secondCredential):
			claimed.Generation = 8
		default:
			return SessionClaim{}, ErrAuthenticationFailed
		}
		return claimed, nil
	})
	firstParent, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	first := newIdleConnectTestStream(
		metadata.NewIncomingContext(firstParent, metadata.Pairs("authorization", "Bearer "+string(firstCredential))),
		validClientFrame(),
	)
	ready := make(chan struct{})
	first.onSend = func(frame *externalproviderpb.ServerFrame) {
		if frame.GetReady() != nil {
			close(ready)
		}
	}
	firstResult := make(chan error, 1)
	go func() { firstResult <- broker.Connect(first) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("first Connect did not send Ready")
	}
	requireCoordinatorWorkerState(t, runtime, ateapipb.WorkerState_WORKER_STATE_ACTIVE)

	second := newConnectTestStream(
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(secondCredential))),
		connectTestReceive{frame: validClientFrame()},
	)
	if err := broker.Connect(second); err != nil {
		t.Fatalf("replacement Connect() error = %v", err)
	}
	select {
	case err := <-firstResult:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("idle first Connect() code = %v, want Aborted", status.Code(err))
		}
		cancelFirst()
	case <-time.After(5 * time.Second):
		t.Fatal("route replacement did not interrupt idle first Connect")
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after both streams closed = %+v, want empty", stats)
	}
}

func TestBrokerConnectReplacementAbortsBlockedProductionSendWithoutWaitCycle(t *testing.T) {
	coordinator, _, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim := validSessionClaim(1)
	firstCredential := testCredential(0x92)
	secondCredential := testCredential(0x93)
	broker, _ := connectTestBroker(t, coordinator, func(_ context.Context, _ string, digest CredentialDigest) (SessionClaim, error) {
		claimed := claim
		switch digest {
		case digestCredential(sessionDigestDomain, firstCredential):
			claimed.Generation = 9
		case digestCredential(sessionDigestDomain, secondCredential):
			claimed.Generation = 10
		default:
			return SessionClaim{}, ErrAuthenticationFailed
		}
		return claimed, nil
	})

	firstParent, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	first := newIdleConnectTestStream(
		metadata.NewIncomingContext(firstParent, metadata.Pairs("authorization", "Bearer "+string(firstCredential))),
		validClientFrame(),
	)
	ready := make(chan struct{})
	openFrames := make(chan *externalproviderpb.ServerFrame, 1)
	first.onSend = func(frame *externalproviderpb.ServerFrame) {
		switch {
		case frame.GetReady() != nil:
			close(ready)
		case frame.GetOpen() != nil:
			openFrames <- frame
		}
	}
	dataStarted := make(chan struct{})
	var dataOnce sync.Once
	var activeDataSends atomic.Int32
	first.beforeSend = func(frame *externalproviderpb.ServerFrame) error {
		if frame.GetData() == nil {
			return nil
		}
		activeDataSends.Add(1)
		defer activeDataSends.Add(-1)
		dataOnce.Do(func() { close(dataStarted) })
		// Model gRPC flow control: production Send ignores the request context
		// and unblocks only after the RPC handler returns and gRPC cancels the
		// actual stream context.
		<-first.Context().Done()
		return context.Cause(first.Context())
	}
	firstResult := make(chan error, 1)
	go func() {
		err := broker.Connect(first)
		// A real gRPC server cancels stream.Context after the handler returns.
		// Keeping this ordering in the fake exposes a synchronous-cleanup cycle.
		cancelFirst()
		firstResult <- err
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("generation 9 Connect did not send Ready")
	}
	requireCoordinatorWorkerState(t, runtime, ateapipb.WorkerState_WORKER_STATE_ACTIVE)

	assignment := executionAssignment(onlyCoordinatorWorker(t, runtime))
	dialResult := startExecutionDial(context.Background(), &ExternalExecutionDialer{forwarder: coordinator.forwarder}, assignment)
	var open *externalproviderpb.ServerFrame
	select {
	case open = <-openFrames:
	case <-time.After(5 * time.Second):
		t.Fatal("generation 9 did not send execution Open")
	}
	first.receive <- connectTestReceive{frame: clientAckFrame(open.GetSessionGeneration(), open.GetOpen().GetChannelId(), true, "")}
	var connection net.Conn
	select {
	case dialed := <-dialResult:
		if dialed.err != nil || dialed.conn == nil {
			t.Fatalf("DialContext() = (%v, %v)", dialed.conn, dialed.err)
		}
		connection = dialed.conn
	case <-time.After(5 * time.Second):
		t.Fatal("DialContext did not complete after OpenAck")
	}
	writeResult := make(chan error, 1)
	go func() {
		_, err := connection.Write([]byte("blocked-by-flow-control"))
		writeResult <- err
	}()
	select {
	case <-dataStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("production Data Send did not enter flow-control wait")
	}

	second := newConnectTestStream(
		metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(secondCredential))),
		connectTestReceive{frame: validClientFrame()},
	)
	secondResult := make(chan error, 1)
	go func() { secondResult <- broker.Connect(second) }()

	select {
	case err := <-firstResult:
		if status.Code(err) != codes.Aborted {
			t.Fatalf("generation 9 Connect code = %v, want Aborted", status.Code(err))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("generation 9 handler could not return to abort its blocked stream.Send")
	}
	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatalf("generation 10 Connect error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replacement remained blocked after old stream context cancellation")
	}
	select {
	case err := <-writeResult:
		if err == nil {
			t.Fatal("old Write succeeded after its production stream was aborted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old Write remained blocked after stream context cancellation")
	}
	if active := activeDataSends.Load(); active != 0 {
		t.Fatalf("replacement returned with %d old production Send calls active", active)
	}
	for _, frame := range first.sentSnapshot() {
		if frame.GetData() != nil {
			t.Fatalf("old Data frame crossed after replacement: %v", frame)
		}
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after replacement = %+v, want empty", stats)
	}
}

func TestBrokerConnectRevokesAssignmentBeforeDetachedBlockedSendCleanup(t *testing.T) {
	coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim := validSessionClaim(1)
	claim.Generation = 11
	credential := testCredential(0x94)
	broker, _ := connectTestBroker(t, coordinator, func(_ context.Context, _ string, digest CredentialDigest) (SessionClaim, error) {
		if digest != digestCredential(sessionDigestDomain, credential) {
			return SessionClaim{}, ErrAuthenticationFailed
		}
		return claim, nil
	})

	streamParent, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	stream := newIdleConnectTestStream(
		metadata.NewIncomingContext(streamParent, metadata.Pairs("authorization", "Bearer "+string(credential))),
		validClientFrame(),
	)
	ready := make(chan struct{})
	openFrames := make(chan *externalproviderpb.ServerFrame, 1)
	stream.onSend = func(frame *externalproviderpb.ServerFrame) {
		switch {
		case frame.GetReady() != nil:
			close(ready)
		case frame.GetOpen() != nil:
			openFrames <- frame
		}
	}
	dataStarted := make(chan struct{})
	var dataOnce sync.Once
	stream.beforeSend = func(frame *externalproviderpb.ServerFrame) error {
		if frame.GetData() == nil {
			return nil
		}
		dataOnce.Do(func() { close(dataStarted) })
		<-stream.Context().Done()
		return context.Cause(stream.Context())
	}
	connectResult := make(chan error, 1)
	go func() {
		err := broker.Connect(stream)
		cancelStream() // model gRPC canceling stream.Context after handler return
		connectResult <- err
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not send Ready")
	}
	requireCoordinatorWorkerState(t, runtime, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	worker := onlyCoordinatorWorker(t, runtime)
	dialResult := startExecutionDial(context.Background(), &ExternalExecutionDialer{forwarder: coordinator.forwarder}, executionAssignment(worker))
	var open *externalproviderpb.ServerFrame
	select {
	case open = <-openFrames:
	case dialed := <-dialResult:
		t.Fatalf("DialContext returned before execution Open: (%v, %v)", dialed.conn, dialed.err)
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not send execution Open")
	}
	stream.receive <- connectTestReceive{frame: clientAckFrame(open.GetSessionGeneration(), open.GetOpen().GetChannelId(), true, "")}
	var connection net.Conn
	select {
	case dialed := <-dialResult:
		if dialed.err != nil || dialed.conn == nil {
			t.Fatalf("DialContext() = (%v, %v)", dialed.conn, dialed.err)
		}
		connection = dialed.conn
	case <-time.After(5 * time.Second):
		t.Fatal("DialContext did not complete after OpenAck")
	}
	writeResult := make(chan error, 1)
	go func() {
		_, err := connection.Write([]byte("blocked-before-receive-error"))
		writeResult <- err
	}()
	select {
	case <-dataStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Data Send did not enter the production transport")
	}

	// EOF has no primary error. The detached branch must still return a
	// non-success status while retaining explicit cleanup ownership.
	close(stream.receive)
	select {
	case err := <-connectResult:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("Connect code = %v, want Unavailable", status.Code(err))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return to abort the blocked production Send")
	}
	if routes.AssignmentGuard().AllowsCandidate(worker) {
		t.Fatal("Connect returned while its old route still allowed assignment")
	}
	mutationCalled := false
	err := routes.AssignmentGuard().GuardAssignment(
		context.Background(),
		worker.GetExternalSlot().GetOwnerAtespace(),
		worker,
		func(func(*ateapipb.Worker) error) error {
			mutationCalled = true
			return nil
		},
	)
	if !errors.Is(err, ErrExternalRouteAssignmentUnavailable) || mutationCalled {
		t.Fatalf("GuardAssignment after handler return = (%v, mutation:%v), want unavailable/no mutation", err, mutationCalled)
	}
	select {
	case err := <-writeResult:
		if err == nil {
			t.Fatal("old Write succeeded after stream abort")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old Write remained blocked after handler returned")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if routes.Stats() == (SessionRouteDirectoryStats{}) {
			if _, current := registry.lookup(claim.Registration.UID, claim.Generation); !current {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("detached cleanup route stats = %+v, want empty", stats)
	}
	if _, current := registry.lookup(claim.Registration.UID, claim.Generation); current {
		t.Fatal("detached cleanup retained the exact session lease")
	}
	requireCoordinatorWorkerState(t, runtime, ateapipb.WorkerState_WORKER_STATE_OFFLINE)
}

func TestBrokerConnectSerializesClaimThroughInstallAcrossSharedRuntime(t *testing.T) {
	coordinator, registry, routes, _ := newCoordinatorHarness(t, 1, 1, 1)
	gate, err := newClaimInstallGate(2, 1)
	if err != nil {
		t.Fatalf("newClaimInstallGate() error = %v", err)
	}
	sharedRuntime := &SessionRuntime{coordinator: coordinator, claimInstallGate: gate}
	baseClaim := validSessionClaim(1)
	var sequence atomic.Uint64
	firstClaimEntered := make(chan struct{})
	releaseFirstClaim := make(chan struct{})
	secondClaimEntered := make(chan struct{})
	firstReady := make(chan struct{})
	claimObservation := make(chan error, 1)
	store := &fakeStore{claim: func(ctx context.Context, registrationUID string, _ CredentialDigest) (SessionClaim, error) {
		generation := sequence.Add(1)
		claim := baseClaim
		claim.Registration.UID = registrationUID
		claim.Generation = generation
		switch generation {
		case 1:
			close(firstClaimEntered)
			select {
			case <-releaseFirstClaim:
			case <-ctx.Done():
				return SessionClaim{}, ctx.Err()
			}
		case 2:
			if lease, current := registry.lookup(registrationUID, 1); !current || lease == nil {
				claimObservation <- errors.New("second durable claim ran before generation 1 registry install")
			}
			close(secondClaimEntered)
			select {
			case <-firstReady:
			case <-ctx.Done():
				return SessionClaim{}, ctx.Err()
			}
		default:
			return SessionClaim{}, errors.New("unexpected extra session claim")
		}
		return claim, nil
	}}
	newSharedBroker := func() *Broker {
		broker, err := newBroker(
			store,
			bytes.NewReader(make([]byte, credentialEntropyBytes)),
			time.Minute,
			WithSessionRuntime(sharedRuntime),
		)
		if err != nil {
			t.Fatalf("newBroker() error = %v", err)
		}
		return broker
	}
	firstBroker := newSharedBroker()
	secondBroker := newSharedBroker()

	firstParent, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	first := newIdleConnectTestStream(connectTestContext(firstParent, 0xa1), validClientFrame())
	first.onSend = func(frame *externalproviderpb.ServerFrame) {
		if frame.GetReady() != nil {
			close(firstReady)
		}
	}
	firstResult := make(chan error, 1)
	go func() { firstResult <- firstBroker.Connect(first) }()
	select {
	case <-firstClaimEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first Connect did not enter durable claim")
	}

	second := newConnectTestStream(
		connectTestContext(context.Background(), 0xa2),
		connectTestReceive{frame: validClientFrame()},
	)
	secondResult := make(chan error, 1)
	go func() { secondResult <- secondBroker.Connect(second) }()
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{InFlight: 2, DistinctKeys: 1})
	if got := sequence.Load(); got != 1 {
		t.Fatalf("durable claims before first release = %d, want 1", got)
	}
	select {
	case <-secondClaimEntered:
		t.Fatal("second durable claim bypassed the first claim-install gate")
	default:
	}

	close(releaseFirstClaim)
	select {
	case <-secondClaimEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("second durable claim did not proceed after generation 1 install")
	}
	select {
	case observation := <-claimObservation:
		t.Fatal(observation)
	default:
	}
	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatalf("second Connect() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second Connect did not finish")
	}
	select {
	case err := <-firstResult:
		if code := status.Code(err); code != codes.Aborted && code != codes.Unavailable {
			t.Fatalf("fenced first Connect() code = %v, want Aborted or Unavailable", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fenced first Connect did not finish")
	}
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{})
	requireSessionRegistryTracked(t, registry, 0, 0)
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after shared-runtime replacement = %+v, want empty", stats)
	}
}

func TestBrokerConnectFailsClosedAndRecoversAtClaimInstallKeyCapacity(t *testing.T) {
	coordinator, _, routes, _ := newCoordinatorHarness(t, 1, 1, 1)
	gate, err := newClaimInstallGate(2, 1)
	if err != nil {
		t.Fatalf("newClaimInstallGate() error = %v", err)
	}
	blockingLease, err := gate.acquire(context.Background(), "registration-a")
	if err != nil {
		t.Fatalf("blocking gate acquire error = %v", err)
	}
	claim := validSessionClaim(1)
	store := &fakeStore{claim: func(_ context.Context, registrationUID string, _ CredentialDigest) (SessionClaim, error) {
		claimed := claim
		claimed.Registration.UID = registrationUID
		return claimed, nil
	}}
	broker, err := newBroker(
		store,
		bytes.NewReader(make([]byte, credentialEntropyBytes)),
		time.Minute,
		WithSessionRuntime(&SessionRuntime{coordinator: coordinator, claimInstallGate: gate}),
	)
	if err != nil {
		t.Fatalf("newBroker() error = %v", err)
	}
	hello := validClientFrame()
	hello.GetHello().RegistrationUid = "registration-b"
	blocked := newConnectTestStream(
		connectTestContext(context.Background(), 0xa3),
		connectTestReceive{frame: hello},
	)
	err = broker.Connect(blocked)
	if status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "session admission is busy" {
		t.Fatalf("capacity Connect() error = %v, want fixed ResourceExhausted", err)
	}
	store.mu.Lock()
	claimCalls := store.claimCalls
	store.mu.Unlock()
	if claimCalls != 0 || len(blocked.sentSnapshot()) != 0 {
		t.Fatalf("capacity failure claim/send = %d/%d, want 0/0", claimCalls, len(blocked.sentSnapshot()))
	}

	blockingLease.release()
	retry := newConnectTestStream(
		connectTestContext(context.Background(), 0xa4),
		connectTestReceive{frame: hello},
	)
	if err := broker.Connect(retry); err != nil {
		t.Fatalf("Connect() after gate release error = %v", err)
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after capacity recovery = %+v, want empty", stats)
	}
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{})
}

func TestBrokerConnectReusesRegistryAndGateCapacityAcrossRegistrations(t *testing.T) {
	const capacity = 2
	coordinator, registry, routes, _ := newCoordinatorHarness(t, capacity, capacity, capacity)
	gate, err := newClaimInstallGate(capacity, capacity)
	if err != nil {
		t.Fatalf("newClaimInstallGate() error = %v", err)
	}
	claim := validSessionClaim(1)
	store := &fakeStore{claim: func(_ context.Context, registrationUID string, _ CredentialDigest) (SessionClaim, error) {
		claimed := claim
		claimed.Registration.UID = registrationUID
		return claimed, nil
	}}
	broker, err := newBroker(
		store,
		bytes.NewReader(make([]byte, credentialEntropyBytes)),
		time.Minute,
		WithSessionRuntime(&SessionRuntime{coordinator: coordinator, claimInstallGate: gate}),
	)
	if err != nil {
		t.Fatalf("newBroker() error = %v", err)
	}
	for index := range capacity * 4 {
		registrationUID := fmt.Sprintf("registration-%02d", index)
		hello := validClientFrame()
		hello.GetHello().RegistrationUid = registrationUID
		stream := newConnectTestStream(
			connectTestContext(context.Background(), byte(0xb0+index)),
			connectTestReceive{frame: hello},
		)
		if err := broker.Connect(stream); err != nil {
			t.Fatalf("Connect(%q) error = %v", registrationUID, err)
		}
		requireClaimInstallGateStats(t, gate, claimInstallGateStats{})
		requireSessionRegistryTracked(t, registry, 0, 0)
		if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
			t.Fatalf("route stats after %q = %+v, want empty", registrationUID, stats)
		}
	}
}

func TestBrokerConnectCancellationAndReceiveErrorCleanup(t *testing.T) {
	tests := []struct {
		name     string
		wantCode codes.Code
		stop     func(context.CancelFunc, *connectTestStream)
	}{
		{
			name:     "context cancellation",
			wantCode: codes.Canceled,
			stop: func(cancel context.CancelFunc, _ *connectTestStream) {
				cancel()
			},
		},
		{
			name:     "receive error",
			wantCode: codes.Unavailable,
			stop: func(_ context.CancelFunc, stream *connectTestStream) {
				stream.receive <- connectTestReceive{err: errors.New("peer receive failure secret")}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
			claim := validSessionClaim(1)
			broker, _ := connectTestBroker(t, coordinator, func(context.Context, string, CredentialDigest) (SessionClaim, error) {
				return claim, nil
			})
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream := newIdleConnectTestStream(connectTestContext(parent, 0x8a), validClientFrame())
			ready := make(chan struct{})
			stream.onSend = func(frame *externalproviderpb.ServerFrame) {
				if frame.GetReady() != nil {
					close(ready)
				}
			}
			result := make(chan error, 1)
			go func() { result <- broker.Connect(stream) }()
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("Connect did not send Ready")
			}
			requireCoordinatorWorkerState(t, runtime, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
			test.stop(cancel, stream)
			select {
			case err := <-result:
				if status.Code(err) != test.wantCode || strings.Contains(err.Error(), "peer receive failure secret") {
					t.Fatalf("Connect() error = %v, want redacted %v", err, test.wantCode)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Connect did not stop")
			}
			if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
				t.Fatalf("route stats after stop = %+v, want empty", stats)
			}
			if _, current := registry.lookup(claim.Registration.UID, claim.Generation); current {
				t.Fatal("session lease remained current after stop")
			}
			worker := onlyCoordinatorWorker(t, runtime)
			if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
				t.Fatalf("Worker state after stop = %v, want OFFLINE", worker.GetStatus().GetState())
			}
		})
	}
}

func onlyCoordinatorWorker(t *testing.T, runtime *coordinatorRuntime) *ateapipb.Worker {
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

func requireCoordinatorWorkerState(t *testing.T, runtime *coordinatorRuntime, want ateapipb.WorkerState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		worker := onlyCoordinatorWorker(t, runtime)
		if worker.GetStatus().GetState() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Worker did not reach %v", want)
}

func requirePendingHandshakeCount(t *testing.T, broker *Broker, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(broker.pendingHandshakes) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pending handshake count = %d, want %d", len(broker.pendingHandshakes), want)
}

func TestSessionAuthorityBindsWorkerRuntimeExactlyOnce(t *testing.T) {
	config := SessionRuntimeConfig{
		MaxTrackedRegistrations: 1,
		ClaimInstallGateLimits:  ClaimInstallGateLimits{MaxInFlight: 1, MaxDistinctKeys: 1},
		RouteLimits:             SessionRouteDirectoryLimits{MaxRoutes: 1, MaxBindings: 1},
		ChannelLimits:           ChannelSessionLimits{MaxOpenChannels: 1, MaxDataBytes: 1024},
	}
	authority, err := NewSessionAuthority(config)
	if err != nil {
		t.Fatalf("NewSessionAuthority() error = %v", err)
	}
	if authority.AssignmentGuard() == nil {
		t.Fatal("AssignmentGuard() is nil")
	}
	workerRuntime := newCoordinatorRuntime()
	runtime, err := authority.Bind(workerRuntime, workerRuntime)
	if err != nil || runtime == nil || runtime.coordinator == nil {
		t.Fatalf("Bind() runtime/error = %v/%v", runtime, err)
	}
	if runtime.coordinator.activateWorkers {
		t.Fatal("SessionAuthority enabled Workers without an execution-channel forwarder")
	}
	claim := validSessionClaim(1)
	store := &fakeStore{claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		return claim, nil
	}}
	broker, err := newBroker(
		store,
		bytes.NewReader(make([]byte, credentialEntropyBytes)),
		time.Minute,
		WithSessionRuntime(runtime),
	)
	if err != nil {
		t.Fatalf("newBroker() error = %v", err)
	}
	stream := newConnectTestStream(
		connectTestContext(context.Background(), 0x90),
		connectTestReceive{frame: validClientFrame()},
	)
	if err := broker.Connect(stream); err != nil {
		t.Fatalf("passive Connect() error = %v", err)
	}
	worker := onlyCoordinatorWorker(t, workerRuntime)
	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		t.Fatalf("passive Worker state = %v, want OFFLINE", worker.GetStatus().GetState())
	}
	if got := workerRuntime.eventSnapshot(); !slices.Equal(got, []string{"reconcile"}) {
		t.Fatalf("passive runtime events = %v, want reconciliation without availability writes", got)
	}
	if second, err := authority.Bind(workerRuntime, workerRuntime); err == nil || second != nil {
		t.Fatalf("second Bind() runtime/error = %v/%v, want nil/error", second, err)
	}
	if _, err := NewSessionAuthority(SessionRuntimeConfig{}); err == nil {
		t.Fatal("NewSessionAuthority() accepted unbounded zero config")
	}
}
