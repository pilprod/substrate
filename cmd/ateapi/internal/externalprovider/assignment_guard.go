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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

var (
	// ErrExternalRouteAssignmentUnavailable reports that an ExternalSlot
	// Worker cannot be proven to have an OPEN current-generation route.
	ErrExternalRouteAssignmentUnavailable = errors.New("external provider route is unavailable for assignment")

	// ErrInvalidExternalAssignmentMutation reports an incomplete guard call.
	ErrInvalidExternalAssignmentMutation = errors.New("invalid external provider assignment mutation")
)

// RouteAssignmentGuard couples ExternalSlot scheduling to the exact live
// session route. GuardAssignment holds the registration lifecycle gate across
// the final route recheck and durable Worker mutation. The mutation must invoke
// validateCurrent from inside its authoritative store update before writing.
type RouteAssignmentGuard interface {
	AllowsCandidate(*ateapipb.Worker) bool
	GuardAssignment(
		context.Context,
		string,
		*ateapipb.Worker,
		func(validateCurrent func(*ateapipb.Worker) error) error,
	) error
}

// ExternalRouteAssignmentGuard is the route-directory backed implementation.
// afterLookup is a test seam used to force close/replace races before the
// lifecycle gate is acquired.
type ExternalRouteAssignmentGuard struct {
	registry    *sessionRegistry
	routes      *SessionRouteDirectory
	afterLookup func()
}

var _ RouteAssignmentGuard = (*ExternalRouteAssignmentGuard)(nil)

// AssignmentGuard returns a guard sharing this directory's route and
// registration authority. A nil directory returns nil so callers fail closed.
func (d *SessionRouteDirectory) AssignmentGuard() RouteAssignmentGuard {
	if d == nil || d.registry == nil {
		return nil
	}
	return &ExternalRouteAssignmentGuard{registry: d.registry, routes: d}
}

// AllowsCandidate reports whether worker is ACTIVE and bound by the exact
// OPEN current route. It is a point-in-time scheduling filter; the final store
// mutation must still use GuardAssignment.
func (g *ExternalRouteAssignmentGuard) AllowsCandidate(worker *ateapipb.Worker) bool {
	route, binding, ok := g.lookup(worker)
	return ok && routeLiveError(route) == nil &&
		g.routes.AuthorizesBinding(route, binding)
}

// GuardAssignment linearizes one final assignment mutation with route close,
// replacement, and cleanup. Lock order is deliberately lookup (released),
// registration lifecycle gate, registry/route recheck, then store write.
func (g *ExternalRouteAssignmentGuard) GuardAssignment(
	ctx context.Context,
	expectedActorAtespace string,
	worker *ateapipb.Worker,
	mutation func(validateCurrent func(*ateapipb.Worker) error) error,
) error {
	if g == nil || g.registry == nil || g.routes == nil || ctx == nil || expectedActorAtespace == "" || mutation == nil {
		return ErrInvalidExternalAssignmentMutation
	}
	route, binding, ok := g.lookup(worker)
	if !ok || binding.OwnerAtespace() != expectedActorAtespace {
		return ErrExternalRouteAssignmentUnavailable
	}
	if g.afterLookup != nil {
		g.afterLookup()
	}

	err := g.registry.withCurrentLease(route.lifecycleLease(), func(_ *sessionLifecycleState, _ sessionEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		route.assignmentGate.RLock()
		defer route.assignmentGate.RUnlock()
		if routeLiveError(route) != nil || !g.routes.AuthorizesBinding(route, binding) {
			return ErrExternalRouteAssignmentUnavailable
		}

		validateCurrent := func(current *ateapipb.Worker) error {
			// OwnerAtespace is persisted from the authenticated registration. The
			// exact equality check belongs inside the authoritative store mutation;
			// client labels must never supply tenant authority.
			if expectedActorAtespace == "" {
				return ErrInvalidExternalAssignmentMutation
			}
			if !bindingMatchesWorker(binding, current) || binding.OwnerAtespace() != expectedActorAtespace ||
				current.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
				return fmt.Errorf("%w: authoritative Worker identity or state changed", ErrExternalRouteAssignmentUnavailable)
			}
			if routeLiveError(route) != nil || !g.routes.AuthorizesBinding(route, binding) {
				return ErrExternalRouteAssignmentUnavailable
			}
			return nil
		}
		return mutation(validateCurrent)
	})
	if err != nil {
		if errors.Is(err, errSessionFenced) || errors.Is(err, errSessionNotCurrent) ||
			errors.Is(err, ErrSessionRouteClosing) || errors.Is(err, ErrSessionRouteWithdrawn) ||
			errors.Is(err, ErrSessionRouteReplaced) {
			return fmt.Errorf("%w: %v", ErrExternalRouteAssignmentUnavailable, err)
		}
		return err
	}
	return nil
}

func (g *ExternalRouteAssignmentGuard) lookup(worker *ateapipb.Worker) (*SessionRoute, SessionWorkerBinding, bool) {
	if g == nil || g.registry == nil || g.routes == nil || worker == nil ||
		worker.GetProvider() != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT ||
		worker.GetMetadata().GetName() == "" || worker.GetMetadata().GetUid() == "" ||
		worker.GetExternalSlot().GetExecutionIdentity() == "" ||
		worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		return nil, SessionWorkerBinding{}, false
	}
	route, binding, ok := g.routes.LookupExecutionIdentity(worker.GetExternalSlot().GetExecutionIdentity())
	if !ok || !bindingMatchesWorker(binding, worker) {
		return nil, SessionWorkerBinding{}, false
	}
	return route, binding, true
}

func bindingMatchesWorker(binding SessionWorkerBinding, worker *ateapipb.Worker) bool {
	return worker != nil && worker.GetMetadata() != nil &&
		worker.GetProvider() == ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT &&
		worker.GetMetadata().GetAtespace() == "" &&
		worker.GetMetadata().GetName() == binding.WorkerName() &&
		worker.GetMetadata().GetUid() == binding.WorkerUID() &&
		worker.GetWorkerNamespace() == binding.WorkerNamespace() &&
		worker.GetWorkerPool() == binding.WorkerPool() &&
		worker.GetWorkerPod() == "" && worker.GetWorkerPodUid() == "" && worker.GetNodeName() == "" && worker.GetIp() == "" &&
		worker.GetExternalSlot() != nil &&
		worker.GetExternalSlot().GetExecutionIdentity() == binding.ExecutionIdentity() &&
		worker.GetExternalSlot().GetLocalityIdentity() == binding.LocalityIdentity() &&
		worker.GetExternalSlot().GetOwnerAtespace() == binding.OwnerAtespace()
}
