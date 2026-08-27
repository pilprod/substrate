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
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type routeGuardHarness struct {
	registry     *sessionRegistry
	routes       *SessionRouteDirectory
	availability *fakeWorkerAvailability
	lifecycle    *workerSessionLifecycle
	plan         *WorkerPlan
	worker       *ateapipb.Worker
	lease        *sessionLease
	route        *SessionRoute
	guard        *ExternalRouteAssignmentGuard
}

func newRouteGuardHarness(t *testing.T) *routeGuardHarness {
	t.Helper()
	ctx := context.Background()
	registry := mustSessionRegistry(t, 2)
	routes := mustRouteDirectory(t, registry, 2, 4)
	availability := newFakeWorkerAvailability()
	lifecycle := mustWorkerLifecycle(t, registry, availability, routes)
	admission := workerPlanAdmission(t, "registration-a", "slot-a")
	plan, err := PlanExternalWorkers(admission)
	if err != nil {
		t.Fatalf("PlanExternalWorkers() error = %v", err)
	}
	reconciled := availability.reconcile(plan)
	workerUID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(reconciled[0].GetMetadata().GetName())).String()
	availability.mu.Lock()
	availability.workers[reconciled[0].GetMetadata().GetName()].Metadata.Uid = workerUID
	availability.mu.Unlock()
	reconciled[0].Metadata.Uid = workerUID
	lease := mustInstallSession(t, registry, "registration-a", 1)
	bindings, err := BuildSessionWorkerBindings(admission, reconciled)
	if err != nil {
		t.Fatalf("BuildSessionWorkerBindings() error = %v", err)
	}
	route, err := routes.publish(lease, bindings)
	if err != nil {
		t.Fatalf("publish() error = %v", err)
	}
	if _, err := lifecycle.activate(ctx, route, plan, reconciled); err != nil {
		t.Fatalf("activate() error = %v", err)
	}
	worker := availability.worker(reconciled[0].GetMetadata().GetName())
	guard, ok := routes.AssignmentGuard().(*ExternalRouteAssignmentGuard)
	if !ok {
		t.Fatal("AssignmentGuard() did not return ExternalRouteAssignmentGuard")
	}
	return &routeGuardHarness{
		registry: registry, routes: routes, availability: availability,
		lifecycle: lifecycle, plan: plan, worker: worker, lease: lease,
		route: route, guard: guard,
	}
}

func TestExternalRouteAssignmentGuardRequiresExactOpenActiveRoute(t *testing.T) {
	h := newRouteGuardHarness(t)
	if !h.guard.AllowsCandidate(h.worker) {
		t.Fatal("AllowsCandidate(current Worker) = false, want true")
	}

	for name, mutate := range map[string]func(*ateapipb.Worker){
		"offline": func(worker *ateapipb.Worker) {
			worker.Status.State = ateapipb.WorkerState_WORKER_STATE_OFFLINE
		},
		"different uid": func(worker *ateapipb.Worker) { worker.Metadata.Uid = "different-worker-uid" },
		"different execution": func(worker *ateapipb.Worker) {
			worker.ExternalSlot.ExecutionIdentity = "different-execution"
		},
		"kubernetes": func(worker *ateapipb.Worker) {
			worker.Provider = ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := proto.Clone(h.worker).(*ateapipb.Worker)
			mutate(candidate)
			if h.guard.AllowsCandidate(candidate) {
				t.Fatal("AllowsCandidate(mutated Worker) = true, want false")
			}
		})
	}

	if !h.routes.Close(h.route) {
		t.Fatal("Close() = false")
	}
	if h.guard.AllowsCandidate(h.worker) {
		t.Fatal("AllowsCandidate(closed route) = true, want false")
	}
}

func TestExternalRouteAssignmentGuardSerializesClaimBeforeCleanup(t *testing.T) {
	h := newRouteGuardHarness(t)
	claimEntered := make(chan struct{})
	releaseClaim := make(chan struct{})
	claimDone := make(chan error, 1)
	var mutationFinished atomic.Bool

	go func() {
		claimDone <- h.guard.GuardAssignment(context.Background(), h.worker.GetExternalSlot().GetOwnerAtespace(), h.worker, func(validate func(*ateapipb.Worker) error) error {
			if err := validate(h.availability.worker(h.worker.GetMetadata().GetName())); err != nil {
				return err
			}
			close(claimEntered)
			<-releaseClaim
			mutationFinished.Store(true)
			return nil
		})
	}()
	<-claimEntered

	offlineEntered := make(chan struct{}, 1)
	h.availability.mu.Lock()
	h.availability.before = func(call availabilityCall) {
		if call.state == ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			if !mutationFinished.Load() {
				t.Error("OFFLINE crossed the lifecycle gate before assignment mutation completed")
			}
			offlineEntered <- struct{}{}
		}
	}
	h.availability.mu.Unlock()
	cleanupDone := make(chan error, 1)
	go func() {
		_, err := h.lifecycle.cleanup(context.Background(), h.route)
		cleanupDone <- err
	}()

	select {
	case <-offlineEntered:
		t.Fatal("cleanup reached OFFLINE while assignment held the lifecycle gate")
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseClaim)
	if err := <-claimDone; err != nil {
		t.Fatalf("GuardAssignment() error = %v", err)
	}
	if err := <-cleanupDone; err != nil {
		t.Fatalf("cleanup() error = %v", err)
	}
	if got := h.availability.worker(h.worker.GetMetadata().GetName()).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		t.Fatalf("Worker state = %s, want OFFLINE", got)
	}
}

func TestExternalRouteAssignmentGuardRechecksAfterConcurrentClose(t *testing.T) {
	h := newRouteGuardHarness(t)
	lookupDone := make(chan struct{})
	releaseLookup := make(chan struct{})
	h.guard.afterLookup = func() {
		close(lookupDone)
		<-releaseLookup
	}
	var mutationCalled atomic.Bool
	guardDone := make(chan error, 1)
	go func() {
		guardDone <- h.guard.GuardAssignment(context.Background(), h.worker.GetExternalSlot().GetOwnerAtespace(), h.worker, func(func(*ateapipb.Worker) error) error {
			mutationCalled.Store(true)
			return nil
		})
	}()
	<-lookupDone

	cleanupDone := make(chan error, 1)
	go func() {
		_, err := h.lifecycle.cleanup(context.Background(), h.route)
		cleanupDone <- err
	}()
	if err := <-cleanupDone; err != nil {
		t.Fatalf("cleanup() error = %v", err)
	}
	close(releaseLookup)
	if err := <-guardDone; !errors.Is(err, ErrExternalRouteAssignmentUnavailable) {
		t.Fatalf("GuardAssignment() error = %v, want ErrExternalRouteAssignmentUnavailable", err)
	}
	if mutationCalled.Load() {
		t.Fatal("assignment mutation ran after route cleanup")
	}
}

func TestExternalRouteAssignmentGuardRejectsDifferentOwnerAtespace(t *testing.T) {
	h := newRouteGuardHarness(t)
	var mutationCalled atomic.Bool
	err := h.guard.GuardAssignment(context.Background(), "other-tenant", h.worker, func(func(*ateapipb.Worker) error) error {
		mutationCalled.Store(true)
		return nil
	})
	if !errors.Is(err, ErrExternalRouteAssignmentUnavailable) {
		t.Fatalf("GuardAssignment(other owner) error = %v, want ErrExternalRouteAssignmentUnavailable", err)
	}
	if mutationCalled.Load() {
		t.Fatal("owner mismatch reached the assignment mutation")
	}

	err = h.guard.GuardAssignment(context.Background(), h.worker.GetExternalSlot().GetOwnerAtespace(), h.worker, func(validate func(*ateapipb.Worker) error) error {
		current := proto.Clone(h.worker).(*ateapipb.Worker)
		current.ExternalSlot.OwnerAtespace = "other-tenant"
		return validate(current)
	})
	if !errors.Is(err, ErrExternalRouteAssignmentUnavailable) {
		t.Fatalf("GuardAssignment(authoritative owner drift) error = %v, want ErrExternalRouteAssignmentUnavailable", err)
	}
}

func TestWorkerSessionReplacementOfflinesBeforeWithdrawAndFence(t *testing.T) {
	h := newRouteGuardHarness(t)
	var observed atomic.Bool
	h.availability.mu.Lock()
	h.availability.before = func(call availabilityCall) {
		if call.state != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			return
		}
		observed.Store(true)
		if _, _, open := h.routes.LookupExecutionIdentity(h.worker.GetExternalSlot().GetExecutionIdentity()); open {
			t.Error("closing replacement route remained eligible during OFFLINE")
		}
		if stats := h.routes.Stats(); stats != (SessionRouteDirectoryStats{Routes: 1, Bindings: 1}) {
			t.Errorf("route stats during OFFLINE = %+v, want retained binding", stats)
		}
		if current, ok := h.registry.lookup("registration-a", 1); !ok || current != h.lease {
			t.Error("old generation was fenced before OFFLINE")
		}
	}
	h.availability.mu.Unlock()

	second, err := h.lifecycle.install(context.Background(), "registration-a", 2)
	if err != nil {
		t.Fatalf("install(generation 2) error = %v", err)
	}
	if !observed.Load() {
		t.Fatal("replacement did not make the old Worker OFFLINE")
	}
	if stats := h.routes.Stats(); stats != (SessionRouteDirectoryStats{}) {
		t.Fatalf("route stats after replacement = %+v, want empty", stats)
	}
	if cause := h.lease.cancellationCause(); !errors.Is(cause, errSessionFenced) {
		t.Fatalf("old lease cause = %v, want errSessionFenced", cause)
	}
	if current, ok := h.registry.lookup("registration-a", 2); !ok || current != second {
		t.Fatalf("new generation = (%p, %v), want (%p, true)", current, ok, second)
	}
}

func TestWorkerSessionReplacementFailureDoesNotConsumeGeneration(t *testing.T) {
	h := newRouteGuardHarness(t)
	injected := errors.New("injected replacement OFFLINE failure")
	h.availability.setFailures(availabilityFailure{
		name:       h.worker.GetMetadata().GetName(),
		state:      ateapipb.WorkerState_WORKER_STATE_OFFLINE,
		occurrence: 2, // occurrence 1 is activation preflight
		err:        injected,
	})
	if lease, err := h.lifecycle.install(context.Background(), "registration-a", 2); !errors.Is(err, injected) || lease != nil {
		t.Fatalf("install(failing replacement) = (%v, %v), want (nil, injected)", lease, err)
	}
	if current, ok := h.registry.lookup("registration-a", 1); !ok || current != h.lease {
		t.Fatalf("current lease after failure = (%p, %v), want old lease", current, ok)
	}
	if h.guard.AllowsCandidate(h.worker) {
		t.Fatal("failed replacement left the old route eligible")
	}
	if stats := h.routes.Stats(); stats != (SessionRouteDirectoryStats{Routes: 1, Bindings: 1}) {
		t.Fatalf("route stats after failure = %+v, want closed route retained", stats)
	}

	h.availability.setFailures()
	if lease, err := h.lifecycle.install(context.Background(), "registration-a", 2); err != nil || lease == nil {
		t.Fatalf("install(retry same generation) = (%v, %v), want success", lease, err)
	}
}
