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
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/proto"
)

type coordinatorRuntime struct {
	mu sync.Mutex

	workers       map[string]*ateapipb.Worker
	events        []string
	reconcileErr  error
	activeErr     error
	offlineErr    error
	offlineFails  int
	mutateWorkers func([]*ateapipb.Worker)
	before        func(availabilityCall)
}

type fencingCoordinatorReconciler struct {
	delegate   WorkerPlanReconciler
	registry   *sessionRegistry
	generation uint64
	lease      *sessionLease
}

type blockingCoordinatorReconciler struct {
	delegate WorkerPlanReconciler
	entered  chan struct{}
	release  chan struct{}
	once     atomic.Bool
}

func (r *blockingCoordinatorReconciler) ReconcileExternalWorkers(ctx context.Context, plan *WorkerPlan) ([]*ateapipb.Worker, error) {
	if r.once.CompareAndSwap(false, true) {
		close(r.entered)
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return r.delegate.ReconcileExternalWorkers(ctx, plan)
}

func (r *fencingCoordinatorReconciler) ReconcileExternalWorkers(ctx context.Context, plan *WorkerPlan) ([]*ateapipb.Worker, error) {
	workers, err := r.delegate.ReconcileExternalWorkers(ctx, plan)
	if err != nil {
		return nil, err
	}
	r.lease, err = r.registry.install(plan.Registration().UID, r.generation)
	if err != nil {
		return nil, err
	}
	return workers, nil
}

func newCoordinatorRuntime() *coordinatorRuntime {
	return &coordinatorRuntime{workers: make(map[string]*ateapipb.Worker)}
}

func (r *coordinatorRuntime) ReconcileExternalWorkers(_ context.Context, plan *WorkerPlan) ([]*ateapipb.Worker, error) {
	r.record("reconcile")
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reconcileErr != nil {
		return nil, r.reconcileErr
	}
	desired := plan.Workers()
	result := make([]*ateapipb.Worker, len(desired))
	for index, worker := range desired {
		name := worker.GetMetadata().GetName()
		stored := r.workers[name]
		if stored == nil {
			stored = proto.Clone(worker).(*ateapipb.Worker)
			stored.Metadata = &ateapipb.ResourceMetadata{
				Name:    name,
				Uid:     coordinatorWorkerUID(len(r.workers) + 1),
				Version: 1,
			}
			stored.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_OFFLINE}
			r.workers[name] = stored
		}
		result[index] = proto.Clone(stored).(*ateapipb.Worker)
	}
	if r.mutateWorkers != nil {
		r.mutateWorkers(result)
	}
	return result, nil
}

func (r *coordinatorRuntime) SetExternalWorkerAvailability(
	_ context.Context,
	name string,
	uid string,
	state ateapipb.WorkerState,
) (*ateapipb.Worker, error) {
	call := availabilityCall{name: name, uid: uid, state: state}
	r.record("availability:" + state.String())
	r.mu.Lock()
	before := r.before
	r.mu.Unlock()
	if before != nil {
		before(call)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if state == ateapipb.WorkerState_WORKER_STATE_ACTIVE && r.activeErr != nil {
		return nil, r.activeErr
	}
	if state == ateapipb.WorkerState_WORKER_STATE_OFFLINE && r.offlineFails > 0 {
		r.offlineFails--
		return nil, r.offlineErr
	}
	worker := r.workers[name]
	if worker == nil || worker.GetMetadata().GetUid() != uid {
		return nil, errors.New("coordinator test Worker identity mismatch")
	}
	worker.Status.State = state
	worker.Metadata.Version++
	return proto.Clone(worker).(*ateapipb.Worker), nil
}

func (r *coordinatorRuntime) record(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *coordinatorRuntime) eventSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

func (r *coordinatorRuntime) workerSnapshot(name string) *ateapipb.Worker {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.workers[name] == nil {
		return nil
	}
	return proto.Clone(r.workers[name]).(*ateapipb.Worker)
}

func coordinatorWorkerUID(sequence int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", sequence)
}

func coordinatorInput(t *testing.T, registrationUID string, generation uint64, slotIDs ...string) (SessionClaim, *prevalidatedConnectHello) {
	t.Helper()
	claim := validSessionClaim(uint32(len(slotIDs)))
	claim.Registration.UID = registrationUID
	claim.Generation = generation
	frame := validClientFrame()
	frame.GetHello().RegistrationUid = registrationUID
	frame.GetHello().Slots = make([]*externalproviderpb.ExternalSlot, len(slotIDs))
	for index, slotID := range slotIDs {
		frame.GetHello().Slots[index] = validExternalSlot(slotID)
	}
	hello, err := prevalidateConnectHello(frame)
	if err != nil {
		t.Fatalf("prevalidateConnectHello() error = %v", err)
	}
	return claim, hello
}

func coordinatorGatedClaim(t *testing.T, claim SessionClaim) *gatedSessionClaim {
	t.Helper()
	gate, err := newClaimInstallGate(1, 1)
	if err != nil {
		t.Fatalf("newClaimInstallGate() error = %v", err)
	}
	lease, err := gate.acquire(context.Background(), claim.Registration.UID)
	if err != nil {
		t.Fatalf("claim-install gate acquire error = %v", err)
	}
	t.Cleanup(lease.release)
	store := &fakeStore{claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		return claim, nil
	}}
	gated, err := lease.claimSession(context.Background(), store, CredentialDigest{1})
	if err != nil {
		t.Fatalf("claimSession() error = %v", err)
	}
	return gated
}

func newCoordinatorHarness(t *testing.T, maxRegistrations, maxRoutes, maxBindings uint32) (*sessionCoordinator, *sessionRegistry, *SessionRouteDirectory, *coordinatorRuntime) {
	t.Helper()
	registry := mustSessionRegistry(t, maxRegistrations)
	routes := mustRouteDirectory(t, registry, maxRoutes, maxBindings)
	runtime := newCoordinatorRuntime()
	lifecycle := mustWorkerLifecycle(t, registry, runtime, routes)
	coordinator, err := newSessionCoordinator(
		registry,
		runtime,
		routes,
		lifecycle,
		ChannelSessionLimits{MaxOpenChannels: 8, MaxDataBytes: 4096},
	)
	if err != nil {
		t.Fatalf("newSessionCoordinator() error = %v", err)
	}
	return coordinator, registry, routes, runtime
}

func TestSessionCoordinatorEstablishAndCloseOrder(t *testing.T) {
	coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 2)
	claim, hello := coordinatorInput(t, "registration-a", 11, "slot-a", "slot-b")
	var closing atomic.Bool
	var orderingFailure atomic.Bool
	runtime.before = func(call availabilityCall) {
		worker := runtime.workerSnapshot(call.name)
		if worker == nil {
			orderingFailure.Store(true)
			return
		}
		_, _, routed := routes.LookupExecutionIdentity(worker.GetExternalSlot().GetExecutionIdentity())
		_, leaseCurrent := registry.lookup("registration-a", 11)
		if closing.Load() {
			if routed || !leaseCurrent {
				orderingFailure.Store(true)
			}
			return
		}
		if !routed || !leaseCurrent {
			orderingFailure.Store(true)
		}
	}

	session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), hello, func(_ context.Context, frame *externalproviderpb.ServerFrame) error {
		runtime.record("ready")
		if got := runtime.eventSnapshot(); !slices.Equal(got, []string{"reconcile", "ready"}) {
			t.Errorf("events at Ready = %v, want reconcile then ready", got)
		}
		if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
			t.Errorf("route was published before Ready: %+v", stats)
		}
		if frame.GetSessionGeneration() != 11 || frame.GetReady().GetMaxOpenChannels() != 8 || frame.GetReady().GetMaxDataBytes() != 4096 {
			t.Errorf("Ready frame = %v", frame)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("establish() error = %v", err)
	}
	if session.channelState() == nil || session.channelState().Generation() != 11 {
		t.Fatalf("channel state = %v, want generation 11", session.channelState())
	}
	wantEstablished := []string{
		"reconcile",
		"ready",
		"availability:WORKER_STATE_OFFLINE",
		"availability:WORKER_STATE_OFFLINE",
		"availability:WORKER_STATE_ACTIVE",
		"availability:WORKER_STATE_ACTIVE",
	}
	if got := runtime.eventSnapshot(); !slices.Equal(got, wantEstablished) {
		t.Fatalf("establish events = %v, want %v", got, wantEstablished)
	}
	if orderingFailure.Load() {
		t.Fatal("Worker availability changed outside a published route and current lease")
	}

	closing.Store(true)
	if err := session.close(context.Background()); err != nil {
		t.Fatalf("close() error = %v", err)
	}
	if orderingFailure.Load() {
		t.Fatal("cleanup did not withdraw route before OFFLINE or removed lease too early")
	}
	if _, ok := registry.lookup("registration-a", 11); ok {
		t.Fatal("lease remained after cleanup")
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after close = %+v, want empty", stats)
	}
	for _, worker := range runtime.workers {
		if got := runtime.workerSnapshot(worker.GetMetadata().GetName()).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			t.Errorf("Worker state after close = %s, want OFFLINE", got)
		}
	}
	beforeSecondClose := len(runtime.eventSnapshot())
	if err := session.close(context.Background()); err != nil {
		t.Fatalf("second close() error = %v", err)
	}
	if got := len(runtime.eventSnapshot()); got != beforeSecondClose {
		t.Fatalf("second close added %d events", got-beforeSecondClose)
	}
}

func TestSessionCoordinatorReleasesClaimGateBeforeReconcile(t *testing.T) {
	coordinator, _, _, runtime := newCoordinatorHarness(t, 1, 1, 1)
	blocking := &blockingCoordinatorReconciler{
		delegate: runtime,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	coordinator.reconciler = blocking
	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	gate, err := newClaimInstallGate(1, 1)
	if err != nil {
		t.Fatalf("newClaimInstallGate() error = %v", err)
	}
	gateLease, err := gate.acquire(context.Background(), claim.Registration.UID)
	if err != nil {
		t.Fatalf("gate acquire error = %v", err)
	}
	t.Cleanup(gateLease.release)
	store := &fakeStore{claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		return claim, nil
	}}
	gatedClaim, err := gateLease.claimSession(context.Background(), store, CredentialDigest{1})
	if err != nil {
		t.Fatalf("claimSession() error = %v", err)
	}
	type establishResult struct {
		session *coordinatedSession
		err     error
	}
	result := make(chan establishResult, 1)
	go func() {
		session, err := coordinator.establish(context.Background(), gatedClaim, hello, func(context.Context, *externalproviderpb.ServerFrame) error {
			return nil
		})
		result <- establishResult{session: session, err: err}
	}()
	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("coordinator did not enter reconciliation")
	}
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{})
	close(blocking.release)
	completed := <-result
	if completed.err != nil || completed.session == nil {
		t.Fatalf("establish() = (%v, %v), want session/nil", completed.session, completed.err)
	}
	if err := completed.session.close(context.Background()); err != nil {
		t.Fatalf("session close error = %v", err)
	}
}

func TestNewSessionCoordinatorRequiresOneAuthorityAndValidBounds(t *testing.T) {
	registry := mustSessionRegistry(t, 1)
	otherRegistry := mustSessionRegistry(t, 1)
	routes := mustRouteDirectory(t, registry, 1, 1)
	runtime := newCoordinatorRuntime()
	lifecycle := mustWorkerLifecycle(t, registry, runtime, routes)

	tests := []struct {
		name      string
		registry  *sessionRegistry
		routes    *SessionRouteDirectory
		lifecycle *workerSessionLifecycle
		limits    ChannelSessionLimits
	}{
		{name: "nil registry", routes: routes, lifecycle: lifecycle, limits: ChannelSessionLimits{MaxOpenChannels: 1, MaxDataBytes: 1}},
		{name: "mismatched registry", registry: otherRegistry, routes: routes, lifecycle: lifecycle, limits: ChannelSessionLimits{MaxOpenChannels: 1, MaxDataBytes: 1}},
		{name: "invalid limits", registry: registry, routes: routes, lifecycle: lifecycle},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, err := newSessionCoordinator(test.registry, runtime, test.routes, test.lifecycle, test.limits)
			if !errors.Is(err, errInvalidSessionCoordinator) || coordinator != nil {
				t.Fatalf("newSessionCoordinator() = (%v, %v), want (nil, errInvalidSessionCoordinator)", coordinator, err)
			}
		})
	}
}

func TestSessionCoordinatorFailuresStopAtOrderedBoundary(t *testing.T) {
	t.Run("invalid admission", func(t *testing.T) {
		coordinator, _, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
		claim, _ := coordinatorInput(t, "registration-a", 1, "slot-a")
		if session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), nil, func(context.Context, *externalproviderpb.ServerFrame) error { return nil }); !errors.Is(err, ErrInvalidConnectAdmission) || session != nil {
			t.Fatalf("establish(invalid admission) = (%v, %v)", session, err)
		}
		if len(runtime.eventSnapshot()) != 0 || routes.Stats() != (SessionRouteDirectoryStats{}) {
			t.Fatal("invalid admission crossed a side-effect boundary")
		}
	})

	t.Run("reconcile", func(t *testing.T) {
		coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
		injected := errors.New("injected reconcile failure")
		runtime.reconcileErr = injected
		claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
		readyCalled := false
		if session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), hello, func(context.Context, *externalproviderpb.ServerFrame) error {
			readyCalled = true
			return nil
		}); !errors.Is(err, injected) || session != nil {
			t.Fatalf("establish(reconcile failure) = (%v, %v)", session, err)
		}
		if readyCalled || !slices.Equal(runtime.eventSnapshot(), []string{"reconcile"}) || routes.Stats() != (SessionRouteDirectoryStats{}) {
			t.Fatal("reconcile failure crossed Ready or route boundary")
		}
		if _, ok := registry.lookup("registration-a", 1); ok {
			t.Fatal("failed reconciliation retained the lease")
		}
	})

	t.Run("reconciled identity", func(t *testing.T) {
		coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
		runtime.mutateWorkers = func(workers []*ateapipb.Worker) { workers[0].Metadata.Uid = "not-a-uuid" }
		claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
		readyCalled := false
		if session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), hello, func(context.Context, *externalproviderpb.ServerFrame) error {
			readyCalled = true
			return nil
		}); !errors.Is(err, ErrInvalidSessionWorkerBindings) || session != nil {
			t.Fatalf("establish(invalid reconciled identity) = (%v, %v)", session, err)
		}
		if readyCalled || routes.Stats() != (SessionRouteDirectoryStats{}) {
			t.Fatal("invalid reconciled identity crossed Ready or route boundary")
		}
		if _, ok := registry.lookup("registration-a", 1); ok {
			t.Fatal("invalid reconciled identity retained the lease")
		}
	})

	t.Run("ready", func(t *testing.T) {
		coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
		injected := errors.New("injected Ready failure")
		claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
		if session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), hello, func(context.Context, *externalproviderpb.ServerFrame) error {
			runtime.record("ready")
			return injected
		}); !errors.Is(err, injected) || session != nil {
			t.Fatalf("establish(Ready failure) = (%v, %v)", session, err)
		}
		if got := runtime.eventSnapshot(); !slices.Equal(got, []string{"reconcile", "ready"}) {
			t.Fatalf("Ready failure events = %v", got)
		}
		if routes.Stats() != (SessionRouteDirectoryStats{}) {
			t.Fatal("Ready failure published a route")
		}
		if _, ok := registry.lookup("registration-a", 1); ok {
			t.Fatal("Ready failure retained the lease")
		}
	})
}

func TestSessionCoordinatorDoesNotSendReadyAfterReconcileWasFenced(t *testing.T) {
	registry := mustSessionRegistry(t, 1)
	routes := mustRouteDirectory(t, registry, 1, 1)
	runtime := newCoordinatorRuntime()
	lifecycle := mustWorkerLifecycle(t, registry, runtime, routes)
	reconciler := &fencingCoordinatorReconciler{delegate: runtime, registry: registry, generation: 2}
	coordinator, err := newSessionCoordinator(
		registry,
		reconciler,
		routes,
		lifecycle,
		ChannelSessionLimits{MaxOpenChannels: 1, MaxDataBytes: 1},
	)
	if err != nil {
		t.Fatalf("newSessionCoordinator() error = %v", err)
	}
	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	readyCalled := false
	session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), hello, func(context.Context, *externalproviderpb.ServerFrame) error {
		readyCalled = true
		return nil
	})
	if !errors.Is(err, errSessionFenced) || session != nil {
		t.Fatalf("establish(fenced during reconcile) = (%v, %v), want (nil, errSessionFenced)", session, err)
	}
	if readyCalled {
		t.Fatal("fenced generation crossed Ready boundary")
	}
	if got := runtime.eventSnapshot(); !slices.Equal(got, []string{"reconcile"}) {
		t.Fatalf("fenced events = %v, want only reconcile", got)
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("fenced generation published route: %+v", stats)
	}
	if current, ok := registry.lookup("registration-a", 2); !ok || current != reconciler.lease {
		t.Fatalf("newer generation = (%p, %v), want (%p, true)", current, ok, reconciler.lease)
	}
	if !registry.remove("registration-a", 2, reconciler.lease, true) {
		t.Fatal("could not remove test replacement lease")
	}
}

func TestSessionCoordinatorRouteAndActivationFailuresFailClosed(t *testing.T) {
	t.Run("route capacity after Ready", func(t *testing.T) {
		coordinator, registry, routes, runtime := newCoordinatorHarness(t, 2, 1, 1)
		otherAdmission := workerPlanAdmission(t, "registration-b", "slot-a")
		otherLease := mustInstallSession(t, registry, "registration-b", 1)
		mustPublishRoute(t, routes, otherLease, mustRouteBindings(t, otherAdmission, 20))

		claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
		if session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), hello, func(context.Context, *externalproviderpb.ServerFrame) error {
			runtime.record("ready")
			return nil
		}); !errors.Is(err, ErrSessionRouteDirectoryFull) || session != nil {
			t.Fatalf("establish(route full) = (%v, %v)", session, err)
		}
		if got := runtime.eventSnapshot(); !slices.Equal(got, []string{"reconcile", "ready"}) {
			t.Fatalf("route failure events = %v", got)
		}
		if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{Routes: 1, Bindings: 1}) {
			t.Fatalf("route failure damaged existing owner: %+v", stats)
		}
		if _, ok := registry.lookup("registration-a", 1); ok {
			t.Fatal("route failure retained candidate lease")
		}
	})

	t.Run("activation rollback after route withdrawal", func(t *testing.T) {
		coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
		injected := errors.New("injected activation failure")
		runtime.activeErr = injected
		var rollbackObserved atomic.Bool
		offlineCalls := atomic.Int32{}
		runtime.before = func(call availabilityCall) {
			if call.state != ateapipb.WorkerState_WORKER_STATE_OFFLINE || offlineCalls.Add(1) != 2 {
				return
			}
			worker := runtime.workerSnapshot(call.name)
			_, _, routed := routes.LookupExecutionIdentity(worker.GetExternalSlot().GetExecutionIdentity())
			rollbackObserved.Store(!routed)
		}
		claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
		if session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), hello, func(context.Context, *externalproviderpb.ServerFrame) error {
			runtime.record("ready")
			return nil
		}); !errors.Is(err, injected) || session != nil {
			t.Fatalf("establish(activation failure) = (%v, %v)", session, err)
		}
		if !rollbackObserved.Load() {
			t.Fatal("activation rollback ran before route withdrawal")
		}
		if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
			t.Fatalf("failed activation retained route: %+v", stats)
		}
		if _, ok := registry.lookup("registration-a", 1); ok {
			t.Fatal("failed activation retained lease")
		}
		plan := mustWorkerPlan(t, "registration-a", "slot-a")
		if got := runtime.workerSnapshot(plan.Workers()[0].GetMetadata().GetName()).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			t.Fatalf("Worker state after activation rollback = %s, want OFFLINE", got)
		}
	})
}

func TestSessionCoordinatorFailedReplacementOfflinesInheritedWorkers(t *testing.T) {
	coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim1, hello1 := coordinatorInput(t, "registration-a", 1, "slot-a")
	first, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim1), hello1, func(context.Context, *externalproviderpb.ServerFrame) error { return nil })
	if err != nil {
		t.Fatalf("establish(generation 1) error = %v", err)
	}
	plan := mustWorkerPlan(t, "registration-a", "slot-a")
	name := plan.Workers()[0].GetMetadata().GetName()
	if got := runtime.workerSnapshot(name).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Fatalf("generation 1 Worker state = %s, want ACTIVE", got)
	}

	claim2, hello2 := coordinatorInput(t, "registration-a", 2, "slot-a")
	injected := errors.New("replacement Ready failure")
	if second, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim2), hello2, func(context.Context, *externalproviderpb.ServerFrame) error { return injected }); !errors.Is(err, injected) || second != nil {
		t.Fatalf("establish(generation 2 failure) = (%v, %v)", second, err)
	}
	if got := runtime.workerSnapshot(name).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		t.Fatalf("inherited Worker state = %s, want OFFLINE", got)
	}
	if _, _, ok := routes.LookupExecutionIdentity(plan.Workers()[0].GetExternalSlot().GetExecutionIdentity()); ok {
		t.Fatal("fenced generation 1 route remained resolvable")
	}
	if _, ok := registry.lookup("registration-a", 2); ok {
		t.Fatal("failed generation 2 retained its lease")
	}
	select {
	case <-first.done():
	default:
		t.Fatal("generation 1 session was not fenced")
	}
	if err := first.close(context.Background()); err != nil {
		t.Fatalf("close(fenced generation 1) error = %v", err)
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("fenced cleanup retained stale route: %+v", stats)
	}
}

func TestSessionCoordinatorConcurrentCloseIsIdempotent(t *testing.T) {
	coordinator, _, _, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), hello, func(context.Context, *externalproviderpb.ServerFrame) error { return nil })
	if err != nil {
		t.Fatalf("establish() error = %v", err)
	}

	const closers = 64
	start := make(chan struct{})
	errorsFound := make(chan error, closers)
	var wait sync.WaitGroup
	for range closers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			if err := session.close(context.Background()); err != nil {
				errorsFound <- err
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent close error = %v", err)
	}
	if got := runtime.eventSnapshot(); !slices.Equal(got, []string{
		"reconcile",
		"availability:WORKER_STATE_OFFLINE",
		"availability:WORKER_STATE_ACTIVE",
		"availability:WORKER_STATE_OFFLINE",
	}) {
		t.Fatalf("concurrent close events = %v", got)
	}
}

func TestSessionCoordinatorCloseFailureKeepsLeaseForRetry(t *testing.T) {
	coordinator, registry, routes, runtime := newCoordinatorHarness(t, 1, 1, 1)
	claim, hello := coordinatorInput(t, "registration-a", 1, "slot-a")
	session, err := coordinator.establish(context.Background(), coordinatorGatedClaim(t, claim), hello, func(context.Context, *externalproviderpb.ServerFrame) error { return nil })
	if err != nil {
		t.Fatalf("establish() error = %v", err)
	}
	injected := errors.New("injected OFFLINE failure")
	runtime.mu.Lock()
	runtime.offlineErr = injected
	runtime.offlineFails = 1
	runtime.mu.Unlock()

	if err := session.close(context.Background()); !errors.Is(err, injected) {
		t.Fatalf("close(OFFLINE failure) error = %v, want injected error", err)
	}
	if stats := routes.Stats(); stats != (SessionRouteDirectoryStats{Routes: 1, Bindings: 1}) {
		t.Fatalf("failed close route stats = %+v, want closed bindings retained for retry", stats)
	}
	plan := mustWorkerPlan(t, "registration-a", "slot-a")
	if _, _, routed := routes.LookupExecutionIdentity(plan.Workers()[0].GetExternalSlot().GetExecutionIdentity()); routed {
		t.Fatal("failed close retained a schedulable route")
	}
	if lease, ok := registry.lookup("registration-a", 1); !ok || lease != session.lease {
		t.Fatalf("failed close lease = (%p, %v), want retryable current lease", lease, ok)
	}

	if err := session.close(context.Background()); err != nil {
		t.Fatalf("close(retry) error = %v", err)
	}
	if _, ok := registry.lookup("registration-a", 1); ok {
		t.Fatal("successful retry retained lease")
	}
	if got := runtime.workerSnapshot(plan.Workers()[0].GetMetadata().GetName()).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		t.Fatalf("Worker state after close retry = %s, want OFFLINE", got)
	}
}
