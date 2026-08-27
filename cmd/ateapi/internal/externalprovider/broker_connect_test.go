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
	"io"
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

	receive chan connectTestReceive
	sendErr error
	onSend  func(*externalproviderpb.ServerFrame)

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

func connectTestBroker(t *testing.T, coordinator *sessionCoordinator, claim func(context.Context, string, CredentialDigest) (SessionClaim, error)) (*Broker, *fakeStore) {
	t.Helper()
	store := &fakeStore{claim: claim}
	broker, err := newBroker(
		store,
		bytes.NewReader(make([]byte, credentialEntropyBytes)),
		time.Minute,
		WithSessionRuntime(&SessionRuntime{coordinator: coordinator}),
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
}

func TestBrokerConnectFailsClosedForUnwiredExecutionEffect(t *testing.T) {
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
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Connect() code = %v, want FailedPrecondition", status.Code(err))
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

func TestSessionAuthorityBindsWorkerRuntimeExactlyOnce(t *testing.T) {
	config := SessionRuntimeConfig{
		MaxTrackedRegistrations: 1,
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
	if second, err := authority.Bind(workerRuntime, workerRuntime); err == nil || second != nil {
		t.Fatalf("second Bind() runtime/error = %v/%v, want nil/error", second, err)
	}
	if _, err := NewSessionAuthority(SessionRuntimeConfig{}); err == nil {
		t.Fatal("NewSessionAuthority() accepted unbounded zero config")
	}
}
