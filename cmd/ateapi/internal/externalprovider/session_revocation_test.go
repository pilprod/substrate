// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type revocationFixture struct {
	authority      *SessionAuthority
	sessionRuntime *SessionRuntime
	execution      *ExternalExecutionDialer
	ingress        *ExternalActorIngressDialer
	runtime        *coordinatorRuntime
	session        *coordinatedSession
	sender         *executionTestSender
	assignment     *ateapipb.WorkerAssignment
	gatewayPeer    *net.TCPConn
}

func newRevocationFixture(t *testing.T) *revocationFixture {
	t.Helper()
	config := SessionRuntimeConfig{
		MaxTrackedRegistrations: 2,
		ClaimInstallGateLimits:  ClaimInstallGateLimits{MaxInFlight: 2, MaxDistinctKeys: 2},
		RouteLimits:             SessionRouteDirectoryLimits{MaxRoutes: 2, MaxBindings: 4},
		ChannelLimits:           ChannelSessionLimits{MaxOpenChannels: 8, MaxDataBytes: 4096, RememberedChannelLimit: 16},
		ExecutionLimits:         DefaultExecutionForwardingLimits(),
	}
	authority, err := NewSessionAuthority(config)
	if err != nil {
		t.Fatalf("NewSessionAuthority() error = %v", err)
	}
	runtime := newCoordinatorRuntime()
	serverGateway, gatewayPeer := actorEgressTCPPair(t)
	var gatewayOpened atomic.Bool
	gateway := actorEgressGatewayFunc(func(context.Context, SessionWorkerBinding, uint64) (net.Conn, error) {
		if !gatewayOpened.CompareAndSwap(false, true) {
			return nil, ErrExternalActorEgressUnavailable
		}
		return serverGateway, nil
	})
	sessionRuntime, execution, ingress, err := authority.BindProviderForwarding(runtime, runtime, gateway)
	if err != nil {
		t.Fatalf("BindProviderForwarding() error = %v", err)
	}

	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	gate, err := sessionRuntime.claimInstallGate.acquire(context.Background(), claim.Registration.UID)
	if err != nil {
		t.Fatalf("claim-install gate acquire error = %v", err)
	}
	claimStore := &fakeStore{claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		return claim, nil
	}}
	gated, err := gate.claimSession(context.Background(), claimStore, CredentialDigest{1})
	if err != nil {
		gate.release()
		t.Fatalf("claimSession() error = %v", err)
	}
	sender := newExecutionTestSender()
	session, err := sessionRuntime.coordinator.establish(context.Background(), gated, hello, sender.send)
	gate.release()
	if err != nil {
		t.Fatalf("establish() error = %v", err)
	}
	ready := nextExecutionServerFrame(t, sender)
	if ready.GetReady() == nil || ready.GetSessionGeneration() != 1 {
		t.Fatalf("Ready frame = %v", ready)
	}
	worker := onlyExecutionWorker(t, runtime)
	fixture := &revocationFixture{
		authority:      authority,
		sessionRuntime: sessionRuntime,
		execution:      execution,
		ingress:        ingress,
		runtime:        runtime,
		session:        session,
		sender:         sender,
		assignment:     executionAssignment(worker),
		gatewayPeer:    gatewayPeer,
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = session.close(cleanupCtx)
	})
	return fixture
}

type providerTestDialer interface {
	DialContext(context.Context, *ateapipb.WorkerAssignment) (net.Conn, error)
}

func openRevocationServerChannel(t *testing.T, fixture *revocationFixture, dialer providerTestDialer, kind externalproviderpb.ChannelKind) net.Conn {
	t.Helper()
	result := make(chan executionDialResult, 1)
	go func() {
		connection, err := dialer.DialContext(context.Background(), fixture.assignment)
		result <- executionDialResult{conn: connection, err: err}
	}()
	open := nextExecutionServerFrame(t, fixture.sender)
	if open.GetOpen().GetKind() != kind || open.GetOpen().GetSlotId() != "slot-a" {
		t.Fatalf("Open frame = %v, want %s for slot-a", open, kind)
	}
	if err := fixture.session.applyClientFrame(clientAckFrame(open.GetSessionGeneration(), open.GetOpen().GetChannelId(), true, "")); err != nil {
		t.Fatalf("apply OpenAck: %v", err)
	}
	select {
	case dialed := <-result:
		if dialed.err != nil || dialed.conn == nil {
			t.Fatalf("DialContext() = (%v, %v)", dialed.conn, dialed.err)
		}
		return dialed.conn
	case <-time.After(5 * time.Second):
		t.Fatal("DialContext did not finish after OpenAck")
		return nil
	}
}

func assertRevocationWorkerState(t *testing.T, fixture *revocationFixture, state ateapipb.WorkerState) {
	t.Helper()
	worker := fixture.runtime.workerSnapshot(fixture.assignment.GetWorker().GetName())
	if worker == nil || worker.GetStatus().GetState() != state {
		t.Fatalf("Worker state = %v, want %v", worker.GetStatus().GetState(), state)
	}
}

func waitCoordinatorWorkerState(t *testing.T, runtime *coordinatorRuntime, name string, state ateapipb.WorkerState) *ateapipb.Worker {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		worker := runtime.workerSnapshot(name)
		if worker != nil && worker.GetStatus().GetState() == state {
			return worker
		}
		if time.Now().After(deadline) {
			t.Fatalf("Worker %q did not reach %v; last snapshot = %v", name, state, worker)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSessionAuthorityRevocationPersistsThenFencesEveryLiveDataPlane(t *testing.T) {
	fixture := newRevocationFixture(t)
	execution := openRevocationServerChannel(t, fixture, fixture.execution, externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC)
	ingress := openRevocationServerChannel(t, fixture, fixture.ingress, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_INGRESS)
	if err := fixture.session.applyClientFrame(clientOpenFrame(1, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a")); err != nil {
		t.Fatalf("open Actor egress: %v", err)
	}
	if ack := nextExecutionServerFrame(t, fixture.sender).GetOpenAck(); ack == nil || !ack.GetAccepted() || ack.GetChannelId() != 1 {
		t.Fatalf("Actor egress OpenAck = %v", ack)
	}
	assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_ACTIVE)

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	store := &fakeStore{revokeRegistration: func(context.Context, string) error {
		if _, _, live := fixture.authority.routes.LookupExecutionIdentity(fixture.assignment.GetExternalSlot().GetExecutionIdentity()); !live {
			t.Error("live route was fenced before durable revocation committed")
		}
		assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
		cancelRequest()
		return nil
	}}
	if err := fixture.authority.RevokeExternalProviderRegistration(requestCtx, store, "registration-a"); err != nil {
		t.Fatalf("RevokeExternalProviderRegistration() error = %v", err)
	}
	if store.revokeRegistrationCalls != 1 {
		t.Fatalf("durable revoke calls = %d, want 1", store.revokeRegistrationCalls)
	}
	select {
	case <-fixture.session.done():
	default:
		t.Fatal("revocation returned before the live session was fenced")
	}
	if _, _, live := fixture.authority.routes.LookupExecutionIdentity(fixture.assignment.GetExternalSlot().GetExecutionIdentity()); live {
		t.Fatal("execution route remained live after revocation")
	}
	if stats := fixture.authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after revocation = %+v, want empty", stats)
	}
	if _, current := fixture.authority.registry.lookup("registration-a", 1); current {
		t.Fatal("revoked generation remained current")
	}
	assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_OFFLINE)
	for name, connection := range map[string]net.Conn{"execution": execution, "Actor ingress": ingress} {
		if _, err := connection.Write([]byte("after-revoke")); err == nil {
			t.Errorf("%s connection accepted bytes after revocation", name)
		}
	}
	if err := fixture.gatewayPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	if count, err := fixture.gatewayPeer.Read(buffer); count != 0 || (!errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed)) {
		t.Fatalf("Actor egress read after revoke = (%d, %v), want closed", count, err)
	}
	if connection, err := fixture.execution.DialContext(context.Background(), fixture.assignment); connection != nil || !errors.Is(err, ErrExternalExecutionUnavailable) {
		t.Fatalf("execution DialContext after revoke = (%v, %v), want unavailable", connection, err)
	}
	if connection, err := fixture.ingress.DialContext(context.Background(), fixture.assignment); connection != nil || !errors.Is(err, ErrExternalExecutionUnavailable) {
		t.Fatalf("Actor ingress DialContext after revoke = (%v, %v), want unavailable", connection, err)
	}
}

func TestSessionAuthorityRevocationDatabaseFailureLeavesLiveAuthorityUntouched(t *testing.T) {
	fixture := newRevocationFixture(t)
	storeErr := errors.New("database unavailable")
	store := &fakeStore{revokeRegistration: func(context.Context, string) error { return storeErr }}
	if err := fixture.authority.RevokeExternalProviderRegistration(context.Background(), store, "registration-a"); !errors.Is(err, storeErr) {
		t.Fatalf("RevokeExternalProviderRegistration() error = %v, want database error", err)
	}
	if _, _, live := fixture.authority.routes.LookupExecutionIdentity(fixture.assignment.GetExternalSlot().GetExecutionIdentity()); !live {
		t.Fatal("database failure fenced the live route")
	}
	select {
	case <-fixture.session.done():
		t.Fatal("database failure canceled the live session")
	default:
	}
	assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
}

func TestSessionAuthorityRevocationCleanupFailureIsFencedAndRetryable(t *testing.T) {
	fixture := newRevocationFixture(t)
	fixture.runtime.mu.Lock()
	fixture.runtime.offlineFails = 1
	fixture.runtime.offlineErr = errors.New("offline unavailable")
	fixture.runtime.mu.Unlock()
	store := &fakeStore{revokeRegistration: func(context.Context, string) error { return nil }}

	if err := fixture.authority.RevokeExternalProviderRegistration(context.Background(), store, "registration-a"); err == nil {
		t.Fatal("first revoke succeeded despite OFFLINE failure")
	}
	if _, _, live := fixture.authority.routes.LookupExecutionIdentity(fixture.assignment.GetExternalSlot().GetExecutionIdentity()); live {
		t.Fatal("OFFLINE failure left route authority live")
	}
	select {
	case <-fixture.session.done():
	default:
		t.Fatal("OFFLINE failure left session transport live")
	}
	if _, current := fixture.authority.registry.lookup("registration-a", 1); !current {
		t.Fatal("failed cleanup did not retain its retryable lease")
	}
	assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_ACTIVE)

	if err := fixture.authority.RevokeExternalProviderRegistration(context.Background(), store, "registration-a"); err != nil {
		t.Fatalf("retry revoke error = %v", err)
	}
	if store.revokeRegistrationCalls != 2 {
		t.Fatalf("durable revoke calls = %d, want idempotent retry", store.revokeRegistrationCalls)
	}
	if stats := fixture.authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after retry = %+v, want empty", stats)
	}
	if _, current := fixture.authority.registry.lookup("registration-a", 1); current {
		t.Fatal("retry left revoked generation current")
	}
	assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_OFFLINE)
}

func makeRevocationNoCurrentTombstone(t *testing.T, fixture *revocationFixture) {
	t.Helper()
	fixture.runtime.mu.Lock()
	fixture.runtime.offlineFails = 1
	fixture.runtime.offlineErr = errors.New("failed-establishment rollback unavailable")
	fixture.runtime.mu.Unlock()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := fixture.session.coordinator.lifecycle.cleanup(cleanupCtx, fixture.session.route); err == nil {
		t.Fatal("test setup cleanup unexpectedly succeeded")
	}
	if !fixture.authority.registry.remove("registration-a", 1, fixture.session.lease, false) {
		t.Fatal("failed to remove test lease with retained ownership")
	}
	if _, current := fixture.authority.registry.lookup("registration-a", 1); current {
		t.Fatal("test tombstone retained a current lease")
	}
	fixture.authority.registry.mu.RLock()
	lifecycle := fixture.authority.registry.lifecycleStates["registration-a"]
	owned := 0
	if lifecycle != nil {
		owned = len(lifecycle.ownedWorkers)
	}
	fixture.authority.registry.mu.RUnlock()
	if lifecycle == nil || owned != 1 {
		t.Fatalf("test tombstone = (%v, %d owned Workers), want retained ownership", lifecycle, owned)
	}
	if stats := fixture.authority.routes.Stats(); stats.Routes != 1 || stats.Bindings != 1 {
		t.Fatalf("test tombstone route stats = %+v, want retained closed route", stats)
	}
	if _, _, live := fixture.authority.routes.LookupExecutionIdentity(fixture.assignment.GetExternalSlot().GetExecutionIdentity()); live {
		t.Fatal("test tombstone retained live route authority")
	}
	assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
}

func TestSessionAuthorityRevocationCleansNoCurrentWorkerTombstone(t *testing.T) {
	t.Run("success reclaims tombstone", func(t *testing.T) {
		fixture := newRevocationFixture(t)
		makeRevocationNoCurrentTombstone(t, fixture)
		store := &fakeStore{revokeRegistration: func(context.Context, string) error { return nil }}
		if err := fixture.authority.RevokeExternalProviderRegistration(context.Background(), store, "registration-a"); err != nil {
			t.Fatalf("RevokeExternalProviderRegistration() error = %v", err)
		}
		assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_OFFLINE)
		fixture.authority.registry.mu.RLock()
		_, retained := fixture.authority.registry.lifecycleStates["registration-a"]
		fixture.authority.registry.mu.RUnlock()
		if retained {
			t.Fatal("successful tombstone cleanup was not reclaimed")
		}
		if stats := fixture.authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
			t.Fatalf("successful tombstone cleanup retained route stats %+v", stats)
		}
	})

	t.Run("failure retains tombstone for retry", func(t *testing.T) {
		fixture := newRevocationFixture(t)
		makeRevocationNoCurrentTombstone(t, fixture)
		fixture.runtime.mu.Lock()
		fixture.runtime.offlineFails = 1
		fixture.runtime.offlineErr = errors.New("offline unavailable")
		fixture.runtime.mu.Unlock()
		store := &fakeStore{revokeRegistration: func(context.Context, string) error { return nil }}
		if err := fixture.authority.RevokeExternalProviderRegistration(context.Background(), store, "registration-a"); err == nil {
			t.Fatal("revoke succeeded despite tombstone OFFLINE failure")
		}
		fixture.authority.registry.mu.RLock()
		lifecycle := fixture.authority.registry.lifecycleStates["registration-a"]
		pending := 0
		if lifecycle != nil {
			pending = len(lifecycle.ownedWorkers)
		}
		fixture.authority.registry.mu.RUnlock()
		if lifecycle == nil || pending != 1 {
			t.Fatalf("failed cleanup tombstone = (%v, %d pending), want retained", lifecycle, pending)
		}
		assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_ACTIVE)

		if err := fixture.authority.RevokeExternalProviderRegistration(context.Background(), store, "registration-a"); err != nil {
			t.Fatalf("retry revoke error = %v", err)
		}
		assertRevocationWorkerState(t, fixture, ateapipb.WorkerState_WORKER_STATE_OFFLINE)
		fixture.authority.registry.mu.RLock()
		_, retained := fixture.authority.registry.lifecycleStates["registration-a"]
		fixture.authority.registry.mu.RUnlock()
		if retained {
			t.Fatal("retry did not reclaim tombstone")
		}
		if stats := fixture.authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
			t.Fatalf("retry retained stale route stats %+v", stats)
		}
	})
}

func TestSessionAuthorityRevocationWithoutLiveSessionIsIdempotent(t *testing.T) {
	authority, err := NewSessionAuthority(DefaultSessionRuntimeConfig())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newCoordinatorRuntime()
	if _, err := authority.Bind(runtime, runtime); err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{revokeRegistration: func(context.Context, string) error { return nil }}
	for range 2 {
		if err := authority.RevokeExternalProviderRegistration(context.Background(), store, "registration-a"); err != nil {
			t.Fatalf("idempotent revoke error = %v", err)
		}
	}
	if store.revokeRegistrationCalls != 2 {
		t.Fatalf("durable revoke calls = %d, want 2", store.revokeRegistrationCalls)
	}
}

func TestSessionAuthorityRejectsRevocationBeforeBindingWithoutStoreMutation(t *testing.T) {
	authority, err := NewSessionAuthority(DefaultSessionRuntimeConfig())
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{revokeRegistration: func(context.Context, string) error { return nil }}
	if err := authority.RevokeExternalProviderRegistration(context.Background(), store, "registration-a"); !errors.Is(err, errSessionAuthorityNotBound) {
		t.Fatalf("RevokeExternalProviderRegistration() error = %v, want unbound", err)
	}
	if store.revokeRegistrationCalls != 0 {
		t.Fatalf("unbound authority mutated store %d times", store.revokeRegistrationCalls)
	}
}

func TestEnrollmentAdminRevocationTerminatesLiveBrokerAndDeniesMintAndReconnect(t *testing.T) {
	config := SessionRuntimeConfig{
		MaxTrackedRegistrations: 1,
		ClaimInstallGateLimits:  ClaimInstallGateLimits{MaxInFlight: 2, MaxDistinctKeys: 1},
		RouteLimits:             SessionRouteDirectoryLimits{MaxRoutes: 1, MaxBindings: 1},
		ChannelLimits:           ChannelSessionLimits{MaxOpenChannels: 4, MaxDataBytes: 4096, RememberedChannelLimit: 8},
		ExecutionLimits:         DefaultExecutionForwardingLimits(),
	}
	authority, err := NewSessionAuthority(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newCoordinatorRuntime()
	sessionRuntime, _, err := authority.BindExecutionForwarding(runtime, runtime)
	if err != nil {
		t.Fatal(err)
	}
	claim := validSessionClaim(1)
	var revoked atomic.Bool
	store := &fakeStore{
		claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) {
			if revoked.Load() {
				return SessionClaim{}, ErrAuthenticationFailed
			}
			return claim, nil
		},
		rotate: func(context.Context, string, CredentialDigest, CredentialDigest, time.Duration) (SessionAuthorization, error) {
			if revoked.Load() {
				return SessionAuthorization{}, ErrAuthenticationFailed
			}
			return SessionAuthorization{Registration: claim.Registration, ExpiresAt: time.Now().Add(time.Minute)}, nil
		},
		revokeRegistration: func(context.Context, string) error {
			revoked.Store(true)
			return nil
		},
	}
	broker, err := newBroker(store, bytes.NewReader(make([]byte, credentialEntropyBytes*4)), time.Minute, WithSessionRuntime(sessionRuntime))
	if err != nil {
		t.Fatal(err)
	}
	stream := newIdleConnectTestStream(connectTestContext(context.Background(), 0xb1), validClientFrame())
	ready := make(chan struct{}, 1)
	stream.onSend = func(frame *externalproviderpb.ServerFrame) {
		if frame.GetReady() != nil {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
	}
	connectResult := make(chan error, 1)
	go func() { connectResult <- broker.Connect(stream) }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("live Connect did not become Ready")
	}
	worker := onlyExecutionWorker(t, runtime)
	worker = waitCoordinatorWorkerState(t, runtime, worker.GetMetadata().GetName(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)

	admin, err := NewEnrollmentAdminServer(store, authority, []EnrollmentAdminPrincipal{{Provider: testAdminProvider, Issuer: testAdminIssuer, Subject: testAdminSubject}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.RevokeExternalProviderRegistration(adminContext(testAdminSubject), &externalproviderpb.RevokeExternalProviderRegistrationRequest{RegistrationUid: claim.Registration.UID}); err != nil {
		t.Fatalf("admin RevokeExternalProviderRegistration() error = %v", err)
	}
	if !revoked.Load() {
		t.Fatal("admin revocation returned before durable revocation")
	}
	select {
	case connectErr := <-connectResult:
		if code := status.Code(connectErr); code != codes.Aborted && code != codes.Unavailable {
			t.Fatalf("revoked Connect code = %v, want Aborted or Unavailable", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("revoked live Connect did not terminate")
	}
	if stats := authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after admin revoke = %+v", stats)
	}
	if _, current := authority.registry.lookup(claim.Registration.UID, claim.Generation); current {
		t.Fatal("admin revoke left the live generation current")
	}
	worker = waitCoordinatorWorkerState(t, runtime, worker.GetMetadata().GetName(), ateapipb.WorkerState_WORKER_STATE_OFFLINE)

	mintCtx := connectTestContext(context.Background(), 0xb2)
	if response, err := broker.MintSessionToken(mintCtx, &externalproviderpb.MintSessionTokenRequest{RegistrationUid: claim.Registration.UID}); response != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("MintSessionToken after revoke = (%v, %v), want Unauthenticated", response, err)
	}
	reconnect := newConnectTestStream(
		connectTestContext(context.Background(), 0xb3),
		connectTestReceive{frame: validClientFrame()},
	)
	if err := broker.Connect(reconnect); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Connect after revoke = %v, want Unauthenticated", err)
	}
	if stats := authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("denied reconnect published route stats %+v", stats)
	}
}

func TestSessionAuthorityRevocationSerializesClaimThroughInstall(t *testing.T) {
	config := SessionRuntimeConfig{
		MaxTrackedRegistrations: 1,
		ClaimInstallGateLimits:  ClaimInstallGateLimits{MaxInFlight: 2, MaxDistinctKeys: 1},
		RouteLimits:             SessionRouteDirectoryLimits{MaxRoutes: 1, MaxBindings: 1},
		ChannelLimits:           ChannelSessionLimits{MaxOpenChannels: 4, MaxDataBytes: 4096, RememberedChannelLimit: 8},
		ExecutionLimits:         DefaultExecutionForwardingLimits(),
	}
	authority, err := NewSessionAuthority(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newCoordinatorRuntime()
	sessionRuntime, _, err := authority.BindExecutionForwarding(runtime, runtime)
	if err != nil {
		t.Fatal(err)
	}
	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	claimGate, err := sessionRuntime.claimInstallGate.acquire(context.Background(), claim.Registration.UID)
	if err != nil {
		t.Fatal(err)
	}
	claimStore := &fakeStore{claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) { return claim, nil }}
	gatedClaim, err := claimGate.claimSession(context.Background(), claimStore, CredentialDigest{1})
	if err != nil {
		claimGate.release()
		t.Fatal(err)
	}

	storeEntered := make(chan struct{})
	store := &fakeStore{revokeRegistration: func(context.Context, string) error {
		close(storeEntered)
		return nil
	}}
	revokeResult := make(chan error, 1)
	go func() {
		revokeResult <- authority.RevokeExternalProviderRegistration(context.Background(), store, claim.Registration.UID)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for sessionRuntime.claimInstallGate.stats().InFlight != 2 {
		if time.Now().After(deadline) {
			claimGate.release()
			t.Fatal("revocation did not join the occupied claim-install gate")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-storeEntered:
		claimGate.release()
		t.Fatal("durable revoke crossed a claim which had not installed")
	default:
	}

	sender := newExecutionTestSender()
	establishResult := make(chan struct {
		session *coordinatedSession
		err     error
	}, 1)
	go func() {
		session, err := sessionRuntime.coordinator.establish(context.Background(), gatedClaim, hello, sender.send)
		establishResult <- struct {
			session *coordinatedSession
			err     error
		}{session: session, err: err}
	}()
	select {
	case <-storeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("revocation did not persist after install released the gate")
	}
	if err := <-revokeResult; err != nil {
		t.Fatalf("RevokeExternalProviderRegistration() error = %v", err)
	}
	established := <-establishResult
	if established.session != nil {
		select {
		case <-established.session.done():
		default:
			t.Fatal("racing establishment returned a live session after revoke")
		}
	}
	if stats := authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("racing establishment left routes %+v", stats)
	}
	if _, current := authority.registry.lookup(claim.Registration.UID, claim.Generation); current {
		t.Fatal("racing establishment left a generation current")
	}
	if stats := sessionRuntime.claimInstallGate.stats(); stats != (claimInstallGateStats{}) {
		t.Fatalf("claim-install gate leaked after race: %+v", stats)
	}
}

func TestSessionAuthorityRevocationCancelsInstalledGenerationBeforePublish(t *testing.T) {
	config := SessionRuntimeConfig{
		MaxTrackedRegistrations: 1,
		ClaimInstallGateLimits:  ClaimInstallGateLimits{MaxInFlight: 2, MaxDistinctKeys: 1},
		RouteLimits:             SessionRouteDirectoryLimits{MaxRoutes: 1, MaxBindings: 1},
		ChannelLimits:           ChannelSessionLimits{MaxOpenChannels: 4, MaxDataBytes: 4096, RememberedChannelLimit: 8},
		ExecutionLimits:         DefaultExecutionForwardingLimits(),
	}
	authority, err := NewSessionAuthority(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newCoordinatorRuntime()
	blocking := &blockingCoordinatorReconciler{
		delegate: runtime,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	sessionRuntime, _, err := authority.BindExecutionForwarding(blocking, runtime)
	if err != nil {
		t.Fatal(err)
	}
	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	gate, err := sessionRuntime.claimInstallGate.acquire(context.Background(), claim.Registration.UID)
	if err != nil {
		t.Fatal(err)
	}
	claimStore := &fakeStore{claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) { return claim, nil }}
	gatedClaim, err := gate.claimSession(context.Background(), claimStore, CredentialDigest{1})
	if err != nil {
		gate.release()
		t.Fatal(err)
	}
	establishResult := make(chan error, 1)
	go func() {
		_, err := sessionRuntime.coordinator.establish(context.Background(), gatedClaim, hello, newExecutionTestSender().send)
		establishResult <- err
	}()
	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("establishment did not reach reconcile after install")
	}
	if _, current := authority.registry.lookup(claim.Registration.UID, claim.Generation); !current {
		t.Fatal("generation was not installed before blocked reconciliation")
	}
	if stats := authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route published before blocked reconciliation: %+v", stats)
	}
	store := &fakeStore{revokeRegistration: func(context.Context, string) error { return nil }}
	if err := authority.RevokeExternalProviderRegistration(context.Background(), store, claim.Registration.UID); err != nil {
		t.Fatalf("RevokeExternalProviderRegistration() error = %v", err)
	}
	if _, current := authority.registry.lookup(claim.Registration.UID, claim.Generation); current {
		t.Fatal("revocation left installed generation current")
	}
	close(blocking.release)
	if err := <-establishResult; err == nil {
		t.Fatal("revoked establishment succeeded after reconciliation resumed")
	}
	if stats := authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("revoked establishment published routes %+v", stats)
	}
}

type blockingActiveAvailability struct {
	delegate *coordinatorRuntime
	entered  chan struct{}
	release  chan struct{}
	once     atomic.Bool
}

func (b *blockingActiveAvailability) SetExternalWorkerAvailability(ctx context.Context, name, uid string, state ateapipb.WorkerState) (*ateapipb.Worker, error) {
	updated, err := b.delegate.SetExternalWorkerAvailability(ctx, name, uid, state)
	if state == ateapipb.WorkerState_WORKER_STATE_ACTIVE && b.once.CompareAndSwap(false, true) {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return updated, err
}

func TestSessionAuthorityRevocationWaitsForActivationThenOfflines(t *testing.T) {
	config := SessionRuntimeConfig{
		MaxTrackedRegistrations: 1,
		ClaimInstallGateLimits:  ClaimInstallGateLimits{MaxInFlight: 2, MaxDistinctKeys: 1},
		RouteLimits:             SessionRouteDirectoryLimits{MaxRoutes: 1, MaxBindings: 1},
		ChannelLimits:           ChannelSessionLimits{MaxOpenChannels: 4, MaxDataBytes: 4096, RememberedChannelLimit: 8},
		ExecutionLimits:         DefaultExecutionForwardingLimits(),
	}
	authority, err := NewSessionAuthority(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime := newCoordinatorRuntime()
	availability := &blockingActiveAvailability{delegate: runtime, entered: make(chan struct{}), release: make(chan struct{})}
	sessionRuntime, _, err := authority.BindExecutionForwarding(runtime, availability)
	if err != nil {
		t.Fatal(err)
	}
	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	gate, err := sessionRuntime.claimInstallGate.acquire(context.Background(), claim.Registration.UID)
	if err != nil {
		t.Fatal(err)
	}
	claimStore := &fakeStore{claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) { return claim, nil }}
	gatedClaim, err := gate.claimSession(context.Background(), claimStore, CredentialDigest{1})
	if err != nil {
		gate.release()
		t.Fatal(err)
	}
	sender := newExecutionTestSender()
	establishResult := make(chan struct {
		session *coordinatedSession
		err     error
	}, 1)
	go func() {
		session, err := sessionRuntime.coordinator.establish(context.Background(), gatedClaim, hello, sender.send)
		establishResult <- struct {
			session *coordinatedSession
			err     error
		}{session: session, err: err}
	}()
	select {
	case <-availability.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("establishment did not reach ACTIVE transition")
	}
	worker := onlyExecutionWorker(t, runtime)
	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Fatalf("blocked activation Worker state = %v, want ACTIVE", worker.GetStatus().GetState())
	}
	if _, _, live := authority.routes.LookupExecutionIdentity(worker.GetExternalSlot().GetExecutionIdentity()); !live {
		t.Fatal("activation did not have a published route")
	}
	storePersisted := make(chan struct{})
	store := &fakeStore{revokeRegistration: func(context.Context, string) error {
		close(storePersisted)
		return nil
	}}
	revokeResult := make(chan error, 1)
	go func() {
		revokeResult <- authority.RevokeExternalProviderRegistration(context.Background(), store, claim.Registration.UID)
	}()
	select {
	case <-storePersisted:
	case <-time.After(5 * time.Second):
		t.Fatal("durable revocation did not commit during activation")
	}
	select {
	case err := <-revokeResult:
		t.Fatalf("revocation returned before activation lifecycle gate released: %v", err)
	default:
	}
	close(availability.release)
	established := <-establishResult
	if established.err != nil {
		t.Fatalf("establishment after released activation error = %v", established.err)
	}
	if err := <-revokeResult; err != nil {
		t.Fatalf("RevokeExternalProviderRegistration() error = %v", err)
	}
	if established.session != nil {
		select {
		case <-established.session.done():
		default:
			t.Fatal("revocation did not fence session completed during activation race")
		}
	}
	worker = onlyExecutionWorker(t, runtime)
	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		t.Fatalf("Worker state after activation race = %v, want OFFLINE", worker.GetStatus().GetState())
	}
	if stats := authority.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("activation race left routes %+v", stats)
	}
	if _, current := authority.registry.lookup(claim.Registration.UID, claim.Generation); current {
		t.Fatal("activation race left generation current")
	}
}
