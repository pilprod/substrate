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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

type availabilityCall struct {
	name  string
	uid   string
	state ateapipb.WorkerState
}

type availabilityFailure struct {
	name       string
	state      ateapipb.WorkerState
	occurrence int
	err        error
}

type fakeWorkerAvailability struct {
	mu       sync.Mutex
	workers  map[string]*ateapipb.Worker
	calls    []availabilityCall
	counts   map[availabilityCall]int
	failures []availabilityFailure
	before   func(availabilityCall)
}

var errFakeRouteWithdrawn = errors.New("fake session route withdrawn")

type fakeWorkerSessionRoute struct {
	lease    *sessionLease
	ctx      context.Context
	cancel   context.CancelCauseFunc
	bindings map[string]fakeWorkerRouteBinding
}

type fakeWorkerRouteBinding struct {
	uid               string
	executionIdentity string
}

func (r *fakeWorkerSessionRoute) RegistrationUID() string       { return r.lease.registration() }
func (r *fakeWorkerSessionRoute) Generation() uint64            { return r.lease.sessionGeneration() }
func (r *fakeWorkerSessionRoute) Done() <-chan struct{}         { return r.ctx.Done() }
func (r *fakeWorkerSessionRoute) CancellationCause() error      { return context.Cause(r.ctx) }
func (r *fakeWorkerSessionRoute) lifecycleLease() *sessionLease { return r.lease }

type fakeWorkerRouteAuthority struct {
	mu      sync.Mutex
	current *fakeWorkerSessionRoute
}

func (a *fakeWorkerRouteAuthority) route(lease *sessionLease, workers []*ateapipb.Worker) *fakeWorkerSessionRoute {
	ctx, cancel := context.WithCancelCause(lease.ctx)
	bindings := make(map[string]fakeWorkerRouteBinding, len(workers))
	for _, worker := range workers {
		bindings[worker.GetMetadata().GetName()] = fakeWorkerRouteBinding{
			uid:               worker.GetMetadata().GetUid(),
			executionIdentity: worker.GetExternalSlot().GetExecutionIdentity(),
		}
	}
	return &fakeWorkerSessionRoute{lease: lease, ctx: ctx, cancel: cancel, bindings: bindings}
}

func (a *fakeWorkerRouteAuthority) publish(route *fakeWorkerSessionRoute) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current != nil {
		a.current.cancel(errSessionFenced)
	}
	a.current = route
}

func (a *fakeWorkerRouteAuthority) AuthorizesWorker(route workerSessionRoute, name, uid, executionIdentity string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	concrete, ok := route.(*fakeWorkerSessionRoute)
	if !ok || a.current != concrete || routeLiveError(concrete) != nil {
		return false
	}
	binding, found := concrete.bindings[name]
	return found && binding.uid == uid && binding.executionIdentity == executionIdentity
}

func (a *fakeWorkerRouteAuthority) Close(route workerSessionRoute) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	concrete, ok := route.(*fakeWorkerSessionRoute)
	if !ok || a.current != concrete {
		return false
	}
	concrete.cancel(ErrSessionRouteClosing)
	return true
}

func (a *fakeWorkerRouteAuthority) CurrentRoute(lease *sessionLease) workerSessionRoute {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current == nil || a.current.lease != lease {
		return nil
	}
	return a.current
}

func (a *fakeWorkerRouteAuthority) Withdraw(route workerSessionRoute) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	concrete, ok := route.(*fakeWorkerSessionRoute)
	if !ok || a.current != concrete {
		return false
	}
	a.current = nil
	concrete.cancel(errFakeRouteWithdrawn)
	return true
}

func newFakeWorkerAvailability() *fakeWorkerAvailability {
	return &fakeWorkerAvailability{
		workers: make(map[string]*ateapipb.Worker),
		counts:  make(map[availabilityCall]int),
	}
}

func (f *fakeWorkerAvailability) reconcile(plan *WorkerPlan) []*ateapipb.Worker {
	f.mu.Lock()
	defer f.mu.Unlock()
	workers := plan.Workers()
	for index, desired := range workers {
		name := desired.GetMetadata().GetName()
		stored := f.workers[name]
		if stored == nil {
			stored = proto.Clone(desired).(*ateapipb.Worker)
			stored.Metadata = &ateapipb.ResourceMetadata{Name: name, Uid: fmt.Sprintf("worker-uid-%d", len(f.workers)+1), Version: 1}
			stored.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_OFFLINE}
			f.workers[name] = stored
		}
		workers[index] = proto.Clone(stored).(*ateapipb.Worker)
	}
	return workers
}

func (f *fakeWorkerAvailability) SetExternalWorkerAvailability(
	_ context.Context,
	name string,
	uid string,
	state ateapipb.WorkerState,
) (*ateapipb.Worker, error) {
	call := availabilityCall{name: name, uid: uid, state: state}
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.counts[call]++
	occurrence := f.counts[call]
	before := f.before
	f.mu.Unlock()

	if before != nil {
		before(call)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	for _, failure := range f.failures {
		if failure.name == name && failure.state == state && failure.occurrence == occurrence {
			return nil, failure.err
		}
	}
	worker := f.workers[name]
	if worker == nil {
		return nil, errors.New("worker not found")
	}
	if worker.GetMetadata().GetUid() != uid {
		return nil, errors.New("worker UID conflict")
	}
	worker.Status.State = state
	worker.Metadata.Version++
	return proto.Clone(worker).(*ateapipb.Worker), nil
}

func (f *fakeWorkerAvailability) setFailures(failures ...availabilityFailure) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = slices.Clone(failures)
}

func (f *fakeWorkerAvailability) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeWorkerAvailability) worker(name string) *ateapipb.Worker {
	f.mu.Lock()
	defer f.mu.Unlock()
	return proto.Clone(f.workers[name]).(*ateapipb.Worker)
}

func (f *fakeWorkerAvailability) assign(name, actorUID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.workers[name].Status.Assignment = &ateapipb.ActorAssignment{ActorUid: actorUID}
}

func mustWorkerPlan(t *testing.T, registrationUID string, slots ...string) *WorkerPlan {
	t.Helper()
	plan, err := PlanExternalWorkers(workerPlanAdmission(t, registrationUID, slots...))
	if err != nil {
		t.Fatalf("PlanExternalWorkers() error = %v", err)
	}
	return plan
}

func mustWorkerLifecycle(
	t *testing.T,
	registry *sessionRegistry,
	availability externalWorkerAvailabilityController,
	routes workerSessionRouteAuthority,
) *workerSessionLifecycle {
	t.Helper()
	lifecycle, err := newWorkerSessionLifecycle(registry, availability, routes)
	if err != nil {
		t.Fatalf("newWorkerSessionLifecycle() error = %v", err)
	}
	return lifecycle
}

func TestWorkerSessionLifecycleActivatesOnlyCurrentInstalledLease(t *testing.T) {
	ctx := context.Background()
	registry := mustSessionRegistry(t, 1)
	availability := newFakeWorkerAvailability()
	routes := &fakeWorkerRouteAuthority{}
	lifecycle := mustWorkerLifecycle(t, registry, availability, routes)
	plan := mustWorkerPlan(t, "registration-a", "slot-a")
	reconciled := availability.reconcile(plan)

	forgedLease := &sessionLease{registrationUID: "registration-a", generation: 1, ctx: context.Background()}
	forged := routes.route(forgedLease, reconciled)
	if _, err := lifecycle.activate(ctx, forged, plan, reconciled); !errors.Is(err, errSessionNotCurrent) {
		t.Fatalf("activate(forged lease) error = %v, want errSessionNotCurrent", err)
	}
	if got := availability.callCount(); got != 0 {
		t.Fatalf("forged activation made %d availability calls, want 0", got)
	}

	lease := mustInstallSession(t, registry, "registration-a", 1)
	unpublished := routes.route(lease, reconciled)
	if _, err := lifecycle.activate(ctx, unpublished, plan, reconciled); !errors.Is(err, errSessionRouteNotPublished) {
		t.Fatalf("activate(unpublished route) error = %v, want errSessionRouteNotPublished", err)
	}
	if got := availability.callCount(); got != len(reconciled) {
		t.Fatalf("unpublished activation made %d fail-closed OFFLINE calls, want %d", got, len(reconciled))
	}
	requireSessionDone(t, lease, errSessionActivationFailed)

	lease = mustInstallSession(t, registry, "registration-a", 2)
	route := routes.route(lease, reconciled)
	routes.publish(route) // Represents Ready sent followed by atomic publication.
	if _, err := lifecycle.activate(ctx, route, plan, reconciled); err != nil {
		t.Fatalf("activate(current lease) error = %v", err)
	}
	worker := availability.worker(plan.Workers()[0].GetMetadata().GetName())
	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Fatalf("Worker state = %s, want ACTIVE", worker.GetStatus().GetState())
	}

	cleanup, err := lifecycle.cleanup(ctx, route)
	if err != nil {
		t.Fatalf("cleanup(current lease) error = %v", err)
	}
	if cleanup.superseded || len(cleanup.pending) != 0 || len(cleanup.offlined) != 1 {
		t.Fatalf("cleanup = %+v, want one offlined Worker", cleanup)
	}
	requireSessionDone(t, lease, errSessionClosing)
	if _, err := lifecycle.activate(ctx, route, plan, reconciled); !errors.Is(err, errSessionClosing) {
		t.Fatalf("activate(withdrawn route) error = %v, want errSessionClosing", err)
	}
	if !registry.remove("registration-a", 2, lease, true) {
		t.Fatal("remove(cleaned lease) = false, want true")
	}
}

func TestWorkerSessionLifecycleFencedCleanupCannotOfflineNewGeneration(t *testing.T) {
	ctx := context.Background()
	registry := mustSessionRegistry(t, 1)
	availability := newFakeWorkerAvailability()
	routes := &fakeWorkerRouteAuthority{}
	lifecycle := mustWorkerLifecycle(t, registry, availability, routes)

	firstPlan := mustWorkerPlan(t, "registration-a", "slot-a", "slot-b")
	firstWorkers := availability.reconcile(firstPlan)
	for _, worker := range firstWorkers {
		availability.assign(worker.GetMetadata().GetName(), "actor-"+worker.GetMetadata().GetUid())
	}
	first := mustInstallSession(t, registry, "registration-a", 1)
	firstRoute := routes.route(first, firstWorkers)
	routes.publish(firstRoute)
	if _, err := lifecycle.activate(ctx, firstRoute, firstPlan, firstWorkers); err != nil {
		t.Fatalf("activate(generation 1) error = %v", err)
	}

	// Generation 2 intentionally omits slot-b. It inherits the generation 1
	// ownership set and must offline both old Workers before reactivating only a.
	secondPlan := mustWorkerPlan(t, "registration-a", "slot-a")
	secondWorkers := availability.reconcile(secondPlan)
	second := mustInstallSession(t, registry, "registration-a", 2)
	secondRoute := routes.route(second, secondWorkers)
	routes.publish(secondRoute)

	start := make(chan struct{})
	activationResult := make(chan error, 1)
	cleanupResult := make(chan workerCleanupResult, 1)
	cleanupErrors := make(chan error, 1)
	go func() {
		<-start
		_, err := lifecycle.activate(ctx, secondRoute, secondPlan, secondWorkers)
		activationResult <- err
	}()
	go func() {
		<-start
		result, err := lifecycle.cleanup(ctx, firstRoute)
		cleanupResult <- result
		cleanupErrors <- err
	}()
	close(start)

	if err := <-activationResult; err != nil {
		t.Fatalf("activate(generation 2) error = %v", err)
	}
	oldCleanup := <-cleanupResult
	if err := <-cleanupErrors; err != nil {
		t.Fatalf("cleanup(fenced generation 1) error = %v", err)
	}
	if !oldCleanup.superseded || len(oldCleanup.offlined) != 0 || len(oldCleanup.pending) != 0 {
		t.Fatalf("fenced cleanup = %+v, want superseded no-op", oldCleanup)
	}

	secondName := secondPlan.Workers()[0].GetMetadata().GetName()
	if got := availability.worker(secondName).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("generation 2 Worker state = %s, want ACTIVE", got)
	}
	for _, worker := range firstPlan.Workers() {
		name := worker.GetMetadata().GetName()
		stored := availability.worker(name)
		if name != secondName && stored.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			t.Errorf("omitted Worker %q state = %s, want OFFLINE", name, stored.GetStatus().GetState())
		}
		if stored.GetStatus().GetAssignment().GetActorUid() == "" {
			t.Errorf("availability transition cleared assignment for %q", name)
		}
	}
	if registry.remove("registration-a", 1, first, true) {
		t.Fatal("old exact remove deleted generation 2")
	}
	if got, ok := registry.lookup("registration-a", 2); !ok || got != second {
		t.Fatalf("generation 2 route = (%p, %v), want (%p, true)", got, ok, second)
	}
}

func TestWorkerSessionLifecyclePartialActivationReturnsDeterministicCleanup(t *testing.T) {
	ctx := context.Background()
	registry := mustSessionRegistry(t, 1)
	availability := newFakeWorkerAvailability()
	routes := &fakeWorkerRouteAuthority{}
	lifecycle := mustWorkerLifecycle(t, registry, availability, routes)
	plan := mustWorkerPlan(t, "registration-a", "slot-a", "slot-b", "slot-c")
	reconciled := availability.reconcile(plan)
	lease := mustInstallSession(t, registry, "registration-a", 1)
	route := routes.route(lease, reconciled)
	routes.publish(route)

	ordered := plan.Workers()
	firstName := ordered[0].GetMetadata().GetName()
	secondName := ordered[1].GetMetadata().GetName()
	activateFailure := errors.New("injected activation failure")
	rollbackFailure := errors.New("injected rollback failure")
	availability.setFailures(
		availabilityFailure{name: secondName, state: ateapipb.WorkerState_WORKER_STATE_ACTIVE, occurrence: 1, err: activateFailure},
		// OFFLINE occurrence 1 is the preflight; occurrence 2 is rollback.
		availabilityFailure{name: firstName, state: ateapipb.WorkerState_WORKER_STATE_OFFLINE, occurrence: 2, err: rollbackFailure},
	)

	cleanup, err := lifecycle.activate(ctx, route, plan, reconciled)
	if !errors.Is(err, activateFailure) || !errors.Is(err, rollbackFailure) {
		t.Fatalf("activate(partial) error = %v, want both injected errors", err)
	}
	if cause := lease.cancellationCause(); cause != nil {
		t.Fatalf("lease was fenced before failed OFFLINE rollback completed: %v", cause)
	}
	if _, ok := registry.lookup("registration-a", 1); !ok {
		t.Fatal("failed OFFLINE rollback did not retain the current lease for retry")
	}
	if err := routeLiveError(route); !errors.Is(err, ErrSessionRouteClosing) {
		t.Fatalf("route after failed rollback error = %v, want ErrSessionRouteClosing", err)
	}
	wantOfflined := []string{ordered[1].GetMetadata().GetName(), ordered[2].GetMetadata().GetName()}
	if got := cleanupNames(cleanup.offlined); !slices.Equal(got, wantOfflined) {
		t.Errorf("offlined cleanup = %v, want %v", got, wantOfflined)
	}
	if len(cleanup.pending) != 1 || cleanup.pending[0].name() != firstName {
		t.Errorf("pending cleanup = %v, want only %q", cleanupNames(cleanup.pending), firstName)
	}
	if got := availability.worker(firstName).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("ambiguous rollback Worker state = %s, want ACTIVE in fake", got)
	}
	for _, worker := range ordered[1:] {
		if got := availability.worker(worker.GetMetadata().GetName()).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			t.Errorf("Worker %q state = %s, want OFFLINE", worker.GetMetadata().GetName(), got)
		}
	}

	availability.setFailures()
	retry, err := lifecycle.cleanup(ctx, route)
	if err != nil {
		t.Fatalf("cleanup retry error = %v", err)
	}
	if len(retry.pending) != 0 || len(retry.offlined) != 1 || retry.offlined[0].name() != firstName {
		t.Fatalf("cleanup retry = offlined %v pending %v", cleanupNames(retry.offlined), cleanupNames(retry.pending))
	}
	if got := availability.worker(firstName).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		t.Errorf("Worker state after retry = %s, want OFFLINE", got)
	}
}

func TestWorkerSessionLifecycleRejectsIdentityCollisionBeforeMutation(t *testing.T) {
	ctx := context.Background()
	registry := mustSessionRegistry(t, 1)
	availability := newFakeWorkerAvailability()
	routes := &fakeWorkerRouteAuthority{}
	lifecycle := mustWorkerLifecycle(t, registry, availability, routes)
	plan := mustWorkerPlan(t, "registration-a", "slot-a")
	reconciled := availability.reconcile(plan)
	reconciled[0].ExternalSlot.ExecutionIdentity = "different-execution"
	lease := mustInstallSession(t, registry, "registration-a", 1)
	route := routes.route(lease, reconciled)
	routes.publish(route)

	if _, err := lifecycle.activate(ctx, route, plan, reconciled); !errors.Is(err, ErrWorkerIdentityCollision) {
		t.Fatalf("activate(identity collision) error = %v, want ErrWorkerIdentityCollision", err)
	}
	if got := availability.callCount(); got != 0 {
		t.Fatalf("identity collision made %d availability calls, want 0", got)
	}
}

func TestWorkerSessionLifecycleRequiresCompletePublishedBindings(t *testing.T) {
	ctx := context.Background()
	registry := mustSessionRegistry(t, 1)
	availability := newFakeWorkerAvailability()
	routes := &fakeWorkerRouteAuthority{}
	lifecycle := mustWorkerLifecycle(t, registry, availability, routes)
	plan := mustWorkerPlan(t, "registration-a", "slot-a", "slot-b")
	reconciled := availability.reconcile(plan)
	lease := mustInstallSession(t, registry, "registration-a", 1)
	route := routes.route(lease, reconciled)
	delete(route.bindings, reconciled[0].GetMetadata().GetName())
	routes.publish(route)

	if _, err := lifecycle.activate(ctx, route, plan, reconciled); !errors.Is(err, errSessionRouteNotPublished) {
		t.Fatalf("activate(incomplete route) error = %v, want errSessionRouteNotPublished", err)
	}
	if got := availability.callCount(); got != len(reconciled) {
		t.Fatalf("incomplete route made %d fail-closed OFFLINE calls, want %d", got, len(reconciled))
	}
	requireSessionDone(t, lease, errSessionActivationFailed)
	if err := routeLiveError(route); !errors.Is(err, ErrSessionRouteClosing) {
		t.Fatalf("route after rejected activation error = %v, want closing", err)
	}
	routes.mu.Lock()
	currentRoute := routes.current
	routes.mu.Unlock()
	if currentRoute != nil {
		t.Fatal("incomplete route remained physically published after OFFLINE rollback")
	}
}

func TestWorkerSessionLifecycleCleanupWithdrawsRouteBeforeOffline(t *testing.T) {
	ctx := context.Background()
	registry := mustSessionRegistry(t, 1)
	availability := newFakeWorkerAvailability()
	routes := &fakeWorkerRouteAuthority{}
	lifecycle := mustWorkerLifecycle(t, registry, availability, routes)
	plan := mustWorkerPlan(t, "registration-a", "slot-a")
	reconciled := availability.reconcile(plan)
	lease := mustInstallSession(t, registry, "registration-a", 1)
	route := routes.route(lease, reconciled)
	routes.publish(route)
	if _, err := lifecycle.activate(ctx, route, plan, reconciled); err != nil {
		t.Fatal(err)
	}

	var routeWasLive bool
	availability.mu.Lock()
	availability.before = func(call availabilityCall) {
		if call.state != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			return
		}
		select {
		case <-route.Done():
		default:
			routeWasLive = true
		}
	}
	availability.mu.Unlock()
	if _, err := lifecycle.cleanup(ctx, route); err != nil {
		t.Fatalf("cleanup() error = %v", err)
	}
	if routeWasLive {
		t.Fatal("cleanup attempted OFFLINE before withdrawing the route")
	}
}

func cleanupNames(workers []sessionWorkerRef) []string {
	names := make([]string, len(workers))
	for index, worker := range workers {
		names[index] = worker.name()
	}
	return names
}
