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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestBuildSessionWorkerBindingsPinsWorkerIncarnationsWithoutAliasing(t *testing.T) {
	admission := workerPlanAdmission(t, "registration-a", "slot-a", "slot-b")
	workers := reconciledRouteWorkers(t, admission, 1)
	bindings, err := BuildSessionWorkerBindings(admission, workers)
	if err != nil {
		t.Fatalf("BuildSessionWorkerBindings() error = %v", err)
	}
	if len(bindings) != 2 || bindings[0].ExecutionIdentity() >= bindings[1].ExecutionIdentity() {
		t.Fatalf("bindings are not sorted by execution identity: %+v", bindings)
	}

	want := append([]SessionWorkerBinding(nil), bindings...)
	workers[0].Metadata.Uid = routeWorkerUID(99)
	workers[0].ExternalSlot.ExecutionIdentity = "mutated-execution"
	workers[0].Metadata.Name = "mutated-worker"
	bindings[0] = SessionWorkerBinding{}

	second, err := BuildSessionWorkerBindings(admission, reconciledRouteWorkers(t, admission, 1))
	if err != nil {
		t.Fatalf("second BuildSessionWorkerBindings() error = %v", err)
	}
	if !equalSessionBindings(second, want) {
		t.Fatalf("binding builder retained mutable input: got %+v, want %+v", second, want)
	}
	for _, binding := range second {
		if binding.RegistrationUID() != "registration-a" || binding.SlotID() == "" ||
			binding.WorkerName() == "" || binding.WorkerUID() == "" || binding.ExecutionIdentity() == "" {
			t.Fatalf("incomplete binding: %+v", binding)
		}
	}
}

func TestBuildSessionWorkerBindingsRejectsInconsistentWorkers(t *testing.T) {
	admission := workerPlanAdmission(t, "registration-a", "slot-a", "slot-b")
	tests := []struct {
		name   string
		mutate func([]*ateapipb.Worker) []*ateapipb.Worker
	}{
		{name: "missing", mutate: func(workers []*ateapipb.Worker) []*ateapipb.Worker { return workers[:1] }},
		{name: "nil", mutate: func(workers []*ateapipb.Worker) []*ateapipb.Worker { workers[0] = nil; return workers }},
		{name: "duplicate name", mutate: func(workers []*ateapipb.Worker) []*ateapipb.Worker {
			workers[1].Metadata.Name = workers[0].GetMetadata().GetName()
			return workers
		}},
		{name: "duplicate uid", mutate: func(workers []*ateapipb.Worker) []*ateapipb.Worker {
			workers[1].Metadata.Uid = workers[0].GetMetadata().GetUid()
			return workers
		}},
		{name: "noncanonical uid", mutate: func(workers []*ateapipb.Worker) []*ateapipb.Worker {
			workers[0].Metadata.Uid = "00000000-0000-4000-8000-0000000000AA"
			return workers
		}},
		{name: "changed execution identity", mutate: func(workers []*ateapipb.Worker) []*ateapipb.Worker {
			workers[0].ExternalSlot.ExecutionIdentity = "different-execution"
			return workers
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workers := test.mutate(reconciledRouteWorkers(t, admission, 1))
			if bindings, err := BuildSessionWorkerBindings(admission, workers); !errors.Is(err, ErrInvalidSessionWorkerBindings) || bindings != nil {
				t.Fatalf("BuildSessionWorkerBindings() = (%+v, %v), want (nil, ErrInvalidSessionWorkerBindings)", bindings, err)
			}
		})
	}
	if bindings, err := BuildSessionWorkerBindings(nil, nil); !errors.Is(err, ErrInvalidSessionWorkerBindings) || bindings != nil {
		t.Fatalf("BuildSessionWorkerBindings(nil) = (%+v, %v), want (nil, ErrInvalidSessionWorkerBindings)", bindings, err)
	}
}

func TestSessionRouteDirectoryValidatesAndBoundsCapacity(t *testing.T) {
	registry := mustSessionRegistry(t, 3)
	invalidLimits := []SessionRouteDirectoryLimits{
		{},
		{MaxRoutes: 1},
		{MaxRoutes: 2, MaxBindings: 1},
		{MaxRoutes: maximumSessionRoutes + 1, MaxBindings: maximumSessionRoutes + 1},
		{MaxRoutes: 1, MaxBindings: maximumSessionBindings + 1},
	}
	if directory, err := newSessionRouteDirectory(nil, SessionRouteDirectoryLimits{MaxRoutes: 1, MaxBindings: 1}); !errors.Is(err, ErrInvalidSessionRouteConfig) || directory != nil {
		t.Fatalf("newSessionRouteDirectory(nil) = (%v, %v), want (nil, ErrInvalidSessionRouteConfig)", directory, err)
	}
	for _, limits := range invalidLimits {
		if directory, err := newSessionRouteDirectory(registry, limits); !errors.Is(err, ErrInvalidSessionRouteConfig) || directory != nil {
			t.Fatalf("newSessionRouteDirectory(%+v) = (%v, %v), want (nil, ErrInvalidSessionRouteConfig)", limits, directory, err)
		}
	}

	directory := mustRouteDirectory(t, registry, 2, 2)
	admissionA := workerPlanAdmission(t, "registration-a", "slot-a", "slot-b")
	bindingsA := mustRouteBindings(t, admissionA, 1)
	leaseA1 := mustInstallSession(t, registry, "registration-a", 1)
	routeA1 := mustPublishRoute(t, directory, leaseA1, bindingsA)
	if stats := directory.Stats(); stats != (SessionRouteDirectoryStats{Routes: 1, Bindings: 2}) {
		t.Fatalf("Stats() = %+v, want 1 route and 2 bindings", stats)
	}

	admissionB := workerPlanAdmission(t, "registration-b", "slot-a")
	bindingsB := mustRouteBindings(t, admissionB, 10)
	leaseB := mustInstallSession(t, registry, "registration-b", 1)
	if route, err := directory.publish(leaseB, bindingsB); !errors.Is(err, ErrSessionRouteDirectoryFull) || route != nil {
		t.Fatalf("publish(over binding capacity) = (%v, %v), want (nil, ErrSessionRouteDirectoryFull)", route, err)
	}

	leaseA2 := mustInstallSession(t, registry, "registration-a", 2)
	bindingsA2 := mustRouteBindings(t, workerPlanAdmission(t, "registration-a", "slot-a"), 1)
	routeA2 := mustPublishRoute(t, directory, leaseA2, bindingsA2)
	if directory.withdraw(routeA1) {
		t.Fatal("stale route cleanup removed a replacement")
	}
	if stats := directory.Stats(); stats != (SessionRouteDirectoryStats{Routes: 1, Bindings: 1}) {
		t.Fatalf("Stats() after replacement = %+v, want 1 route and 1 binding", stats)
	}
	removed := routeBindingForSlot(t, bindingsA, "slot-b")
	if _, _, ok := directory.LookupExecutionIdentity(removed.ExecutionIdentity()); ok {
		t.Fatal("removed binding remained in the reverse index")
	}
	gotRoute, gotBinding, ok := directory.LookupExecutionIdentity(bindingsA2[0].ExecutionIdentity())
	if !ok || gotRoute != routeA2 || gotBinding != bindingsA2[0] {
		t.Fatalf("LookupExecutionIdentity() = (%v, %+v, %v), want current route/binding", gotRoute, gotBinding, ok)
	}
	if !directory.AuthorizesWorker(routeA2, gotBinding.WorkerName(), gotBinding.WorkerUID(), gotBinding.ExecutionIdentity()) {
		t.Fatal("current route did not authorize its exact Worker binding")
	}
	if directory.AuthorizesWorker(routeA2, gotBinding.WorkerName(), routeWorkerUID(999), gotBinding.ExecutionIdentity()) {
		t.Fatal("route authorized a recreated Worker UID")
	}
	if bound, found := routeA2.BindingForWorker(gotBinding.WorkerName(), gotBinding.WorkerUID()); !found || bound != gotBinding {
		t.Fatalf("BindingForWorker() = (%+v, %v), want exact binding", bound, found)
	}

	if !directory.withdraw(routeA2) {
		t.Fatal("withdraw(current route) = false, want true")
	}
	requireRouteDone(t, routeA2, ErrSessionRouteWithdrawn)
	if directory.AuthorizesWorker(routeA2, gotBinding.WorkerName(), gotBinding.WorkerUID(), gotBinding.ExecutionIdentity()) {
		t.Fatal("withdrawn route still authorized a Worker")
	}
	if _, _, ok := directory.LookupExecutionIdentity(bindingsA2[0].ExecutionIdentity()); ok {
		t.Fatal("withdrawn execution identity remained resolvable")
	}
	if route, err := directory.publish(leaseB, bindingsB); err != nil || route == nil {
		t.Fatalf("publish(after freeing capacity) = (%v, %v), want a route", route, err)
	}
	if route, err := directory.publish(leaseB, bindingsB); err != nil || route == nil {
		t.Fatalf("idempotent publish = (%v, %v), want the existing route", route, err)
	}

	routeLimitedRegistry := mustSessionRegistry(t, 2)
	routeLimited := mustRouteDirectory(t, routeLimitedRegistry, 1, 2)
	mustPublishRoute(t, routeLimited, mustInstallSession(t, routeLimitedRegistry, "registration-a", 1), bindingsA2)
	if route, err := routeLimited.publish(mustInstallSession(t, routeLimitedRegistry, "registration-b", 1), bindingsB); !errors.Is(err, ErrSessionRouteDirectoryFull) || route != nil {
		t.Fatalf("publish(over route capacity) = (%v, %v), want (nil, ErrSessionRouteDirectoryFull)", route, err)
	}
}

func TestSessionRouteDirectoryDefensivelyOwnsPublishedBindings(t *testing.T) {
	registry := mustSessionRegistry(t, 1)
	directory := mustRouteDirectory(t, registry, 1, 1)
	bindings := mustRouteBindings(t, workerPlanAdmission(t, "registration-a", "slot-a"), 1)
	want := bindings[0]
	route := mustPublishRoute(t, directory, mustInstallSession(t, registry, "registration-a", 1), bindings)
	bindings[0] = SessionWorkerBinding{}
	routeBindings := route.Bindings()
	if len(routeBindings) != 1 || routeBindings[0] != want {
		t.Fatalf("Bindings() = %+v after input mutation, want %+v", routeBindings, want)
	}
	routeBindings[0] = SessionWorkerBinding{}
	gotRoute, gotBinding, ok := directory.LookupExecutionIdentity(want.ExecutionIdentity())
	if !ok || gotRoute != route || gotBinding != want {
		t.Fatalf("lookup after accessor mutation = (%v, %+v, %v), want unchanged route/binding", gotRoute, gotBinding, ok)
	}
}

func TestSessionRouteDirectoryFencesLookupBeforeReplacementPublishes(t *testing.T) {
	registry := mustSessionRegistry(t, 1)
	directory := mustRouteDirectory(t, registry, 1, 1)
	bindings := mustRouteBindings(t, workerPlanAdmission(t, "registration-a", "slot-a"), 1)
	firstLease := mustInstallSession(t, registry, "registration-a", 1)
	firstRoute := mustPublishRoute(t, directory, firstLease, bindings)
	if _, _, ok := directory.LookupExecutionIdentity(bindings[0].ExecutionIdentity()); !ok {
		t.Fatal("current route was not resolvable")
	}

	secondLease := mustInstallSession(t, registry, "registration-a", 2)
	requireRouteDone(t, firstRoute, errSessionFenced)
	if _, _, ok := directory.LookupExecutionIdentity(bindings[0].ExecutionIdentity()); ok {
		t.Fatal("fenced route remained resolvable before replacement publication")
	}
	secondRoute := mustPublishRoute(t, directory, secondLease, bindings)
	if firstRoute == secondRoute || secondRoute.Generation() != 2 {
		t.Fatalf("replacement route = %p generation %d, want new route generation 2", secondRoute, secondRoute.Generation())
	}
	if directory.withdraw(firstRoute) {
		t.Fatal("old cleanup withdrew a newer route")
	}
	if got, _, ok := directory.LookupExecutionIdentity(bindings[0].ExecutionIdentity()); !ok || got != secondRoute {
		t.Fatalf("lookup after stale cleanup = (%p, %v), want (%p, true)", got, ok, secondRoute)
	}
}

func TestSessionRouteDirectoryRejectsRemovedLease(t *testing.T) {
	registry := mustSessionRegistry(t, 1)
	directory := mustRouteDirectory(t, registry, 1, 1)
	bindings := mustRouteBindings(t, workerPlanAdmission(t, "registration-a", "slot-a"), 1)
	lease := mustInstallSession(t, registry, "registration-a", 1)
	if !registry.remove("registration-a", 1, lease) {
		t.Fatal("remove(current lease) = false, want true")
	}
	if route, err := directory.publish(lease, bindings); !errors.Is(err, ErrSessionRouteNotCurrent) || route != nil {
		t.Fatalf("publish(removed lease) = (%v, %v), want (nil, ErrSessionRouteNotCurrent)", route, err)
	}
}

func TestSessionRouteDirectoryExecutionCollisionFailsClosed(t *testing.T) {
	registry := mustSessionRegistry(t, 2)
	directory := mustRouteDirectory(t, registry, 2, 2)
	leaseA := mustInstallSession(t, registry, "registration-a", 1)
	leaseB := mustInstallSession(t, registry, "registration-b", 1)
	bindingsA := mustRouteBindings(t, workerPlanAdmission(t, "registration-a", "slot-a"), 1)
	bindingsB := mustRouteBindings(t, workerPlanAdmission(t, "registration-b", "slot-a"), 2)
	bindingsB[0].executionIdentity = bindingsA[0].executionIdentity

	start := make(chan struct{})
	results := make(chan routePublishResult, 2)
	var wait sync.WaitGroup
	for _, candidate := range []struct {
		lease    *sessionLease
		bindings []SessionWorkerBinding
	}{{leaseA, bindingsA}, {leaseB, bindingsB}} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			route, err := directory.publish(candidate.lease, candidate.bindings)
			results <- routePublishResult{route: route, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	winners := 0
	collisions := 0
	for result := range results {
		switch {
		case result.err == nil && result.route != nil:
			winners++
		case errors.Is(result.err, ErrSessionRouteCollision) && result.route == nil:
			collisions++
		default:
			t.Fatalf("publish collision result = (%v, %v)", result.route, result.err)
		}
	}
	if winners != 1 || collisions != 1 {
		t.Fatalf("collision results = %d winners, %d collisions; want 1 and 1", winners, collisions)
	}
	if stats := directory.Stats(); stats != (SessionRouteDirectoryStats{Routes: 1, Bindings: 1}) {
		t.Fatalf("Stats() = %+v after collision, want one intact owner", stats)
	}
	if route, binding, ok := directory.LookupExecutionIdentity(bindingsA[0].ExecutionIdentity()); !ok || route == nil || binding.ExecutionIdentity() != bindingsA[0].ExecutionIdentity() {
		t.Fatalf("collision winner lookup = (%v, %+v, %v), want one intact route", route, binding, ok)
	}
}

func TestSessionRouteDirectoryConcurrentReplaceWithdrawAndLookup(t *testing.T) {
	const generations = 200
	registry := mustSessionRegistry(t, 1)
	directory := mustRouteDirectory(t, registry, 1, 1)
	bindings := mustRouteBindings(t, workerPlanAdmission(t, "registration-a", "slot-a"), 1)
	executionIdentity := bindings[0].ExecutionIdentity()
	lease := mustInstallSession(t, registry, "registration-a", 1)
	currentRoute := mustPublishRoute(t, directory, lease, bindings)

	stop := make(chan struct{})
	var failed atomic.Bool
	var lookups sync.WaitGroup
	for range 16 {
		lookups.Add(1)
		go func() {
			defer lookups.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				route, binding, ok := directory.LookupExecutionIdentity(executionIdentity)
				if ok && (route == nil || route.Generation() == 0 || binding != bindings[0]) {
					failed.Store(true)
					return
				}
			}
		}()
	}

	var staleCleanup sync.WaitGroup
	for generation := uint64(2); generation <= generations; generation++ {
		old := currentRoute
		lease = mustInstallSession(t, registry, "registration-a", generation)
		currentRoute = mustPublishRoute(t, directory, lease, bindings)
		staleCleanup.Add(1)
		go func() {
			defer staleCleanup.Done()
			if directory.withdraw(old) {
				failed.Store(true)
			}
		}()
	}
	staleCleanup.Wait()
	close(stop)
	lookups.Wait()
	if failed.Load() {
		t.Fatal("concurrent lookup or stale cleanup observed an invalid route")
	}
	got, binding, ok := directory.LookupExecutionIdentity(executionIdentity)
	if !ok || got != currentRoute || got.Generation() != generations || binding != bindings[0] {
		var gotGeneration uint64
		if got != nil {
			gotGeneration = got.Generation()
		}
		t.Fatalf("final lookup = (%p generation %d, %+v, %v), want current generation %d", got, gotGeneration, binding, ok, generations)
	}
}

type routePublishResult struct {
	route *SessionRoute
	err   error
}

func reconciledRouteWorkers(t *testing.T, admission *ConnectAdmission, firstUID int) []*ateapipb.Worker {
	t.Helper()
	plan, err := PlanExternalWorkers(admission)
	if err != nil {
		t.Fatalf("PlanExternalWorkers() error = %v", err)
	}
	workers := plan.Workers()
	for index, worker := range workers {
		worker.Metadata.Uid = routeWorkerUID(firstUID + index)
		worker.Metadata.Version = 1
		worker.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_OFFLINE}
	}
	return workers
}

func routeWorkerUID(sequence int) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", sequence)
}

func mustRouteBindings(t *testing.T, admission *ConnectAdmission, firstUID int) []SessionWorkerBinding {
	t.Helper()
	bindings, err := BuildSessionWorkerBindings(admission, reconciledRouteWorkers(t, admission, firstUID))
	if err != nil {
		t.Fatalf("BuildSessionWorkerBindings() error = %v", err)
	}
	return bindings
}

func mustRouteDirectory(t *testing.T, registry *sessionRegistry, maxRoutes, maxBindings uint32) *SessionRouteDirectory {
	t.Helper()
	directory, err := newSessionRouteDirectory(registry, SessionRouteDirectoryLimits{MaxRoutes: maxRoutes, MaxBindings: maxBindings})
	if err != nil {
		t.Fatalf("newSessionRouteDirectory() error = %v", err)
	}
	return directory
}

func mustPublishRoute(t *testing.T, directory *SessionRouteDirectory, lease *sessionLease, bindings []SessionWorkerBinding) *SessionRoute {
	t.Helper()
	route, err := directory.publish(lease, bindings)
	if err != nil {
		t.Fatalf("publish() error = %v", err)
	}
	return route
}

func requireRouteDone(t *testing.T, route *SessionRoute, want error) {
	t.Helper()
	select {
	case <-route.Done():
	default:
		t.Fatal("route is live, want done")
	}
	if got := route.CancellationCause(); !errors.Is(got, want) {
		t.Fatalf("route cancellation cause = %v, want %v", got, want)
	}
}

func routeBindingForSlot(t *testing.T, bindings []SessionWorkerBinding, slotID string) SessionWorkerBinding {
	t.Helper()
	for _, binding := range bindings {
		if binding.SlotID() == slotID {
			return binding
		}
	}
	t.Fatalf("binding for slot %q not found", slotID)
	return SessionWorkerBinding{}
}
