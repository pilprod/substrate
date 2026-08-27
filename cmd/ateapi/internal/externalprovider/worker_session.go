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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

var (
	errInvalidWorkerSessionLifecycle = errors.New("invalid external provider Worker session lifecycle")
	errSessionRouteNotPublished      = errors.New("external provider session route is not published")
)

// ExternalWorkerAvailabilityController is the ateapi-private status boundary.
// The provider stream never supplies Worker status; the control plane applies
// the transition with its own UID/version guards and immutable-field checks.
type ExternalWorkerAvailabilityController interface {
	SetExternalWorkerAvailability(context.Context, string, string, ateapipb.WorkerState) (*ateapipb.Worker, error)
}

type externalWorkerAvailabilityController = ExternalWorkerAvailabilityController

// workerSessionRoute is a transport-neutral proof returned only after the
// session router has sent Ready and atomically published all Worker bindings.
// The private lease accessor prevents a caller from substituting registration
// and generation strings for the registry-owned lease identity.
type workerSessionRoute interface {
	RegistrationUID() string
	Generation() uint64
	Done() <-chan struct{}
	CancellationCause() error
	lifecycleLease() *sessionLease
}

// workerSessionRouteAuthority adapts the authoritative route directory. It is
// intentionally behavior-only: this lifecycle owns no binding or reverse
// index and cannot construct a publication proof itself.
type workerSessionRouteAuthority interface {
	AuthorizesWorker(workerSessionRoute, string, string, string) bool
	Close(workerSessionRoute) bool
	Withdraw(workerSessionRoute) bool
}

// workerSessionLifecycle couples a Ready, published route to Worker
// availability. It has no transport and cannot construct or publish a route.
type workerSessionLifecycle struct {
	registry     *sessionRegistry
	availability externalWorkerAvailabilityController
	routes       workerSessionRouteAuthority
}

// sessionWorkerRef is a bounded, immutable snapshot of one planned Worker
// incarnation. Status, assignment, labels, and sandbox class are not retained.
type sessionWorkerRef struct {
	identity *ateapipb.Worker
}

// workerCleanupResult identifies the deterministic result of a best-effort
// fail-closed OFFLINE pass. pending is conservative: every entry may still be
// ACTIVE and remains owned by the current registration lifecycle state.
type workerCleanupResult struct {
	offlined   []sessionWorkerRef
	pending    []sessionWorkerRef
	superseded bool
}

func newWorkerSessionLifecycle(
	registry *sessionRegistry,
	availability externalWorkerAvailabilityController,
	routes workerSessionRouteAuthority,
) (*workerSessionLifecycle, error) {
	if registry == nil || availability == nil || routes == nil {
		return nil, fmt.Errorf("%w: registry, availability controller, and route authority are required", errInvalidWorkerSessionLifecycle)
	}
	return &workerSessionLifecycle{registry: registry, availability: availability, routes: routes}, nil
}

// install replaces a provider generation only after the old route has stopped
// accepting assignments and every conservatively owned Worker is OFFLINE. The
// old route remains indexed while OFFLINE is in progress, then is withdrawn
// immediately before the registry fences the old lease.
func (l *workerSessionLifecycle) install(ctx context.Context, registrationUID string, generation uint64) (*sessionLease, error) {
	if l == nil || ctx == nil {
		return nil, errInvalidWorkerSessionLifecycle
	}
	return l.registry.installPrepared(registrationUID, generation, func(state *sessionLifecycleState, current sessionEntry) error {
		route := l.currentRoute(current.lease)
		if route != nil && !l.routes.Close(route) {
			return errSessionRouteNotPublished
		}
		cleanup, err := l.offline(ctx, state.ownedWorkers)
		state.ownedWorkers = cloneSessionWorkerRefs(cleanup.pending)
		if err != nil {
			return fmt.Errorf("offlining replaced external Workers: %w", err)
		}
		if route != nil && !l.routes.Withdraw(route) {
			return errSessionRouteNotPublished
		}
		state.ownedWorkers = nil
		return nil
	})
}

func (l *workerSessionLifecycle) currentRoute(lease *sessionLease) workerSessionRoute {
	if directory, ok := l.routes.(*SessionRouteDirectory); ok {
		return directory.routeForLease(lease)
	}
	if authority, ok := l.routes.(interface {
		CurrentRoute(*sessionLease) workerSessionRoute
	}); ok {
		return authority.CurrentRoute(lease)
	}
	return nil
}

// activate makes exactly the reconciled plan available for a Ready, published,
// live route. The per-registration gate first offlines every Worker possibly
// owned by the previous generation, including slots omitted from this plan,
// and then preflights the desired set to OFFLINE before any ACTIVE transition.
// Any activation failure first closes the route, then runs a deterministic
// OFFLINE rollback, and withdraws/fences only after the rollback succeeds.
func (l *workerSessionLifecycle) activate(
	ctx context.Context,
	route workerSessionRoute,
	plan *WorkerPlan,
	reconciled []*ateapipb.Worker,
) (workerCleanupResult, error) {
	lease := routeLease(route)
	if lease == nil || plan == nil ||
		route.RegistrationUID() != lease.registration() || route.Generation() != lease.sessionGeneration() ||
		plan.Registration().UID != lease.registration() {
		err := fmt.Errorf("%w: route, lease, and plan authority do not match", errInvalidWorkerSessionLifecycle)
		l.cleanupFailedActivation(ctx, route, lease, nil)
		return workerCleanupResult{}, err
	}
	desired, err := sessionWorkerRefs(plan, reconciled)
	if err != nil {
		l.cleanupFailedActivation(ctx, route, lease, nil)
		return workerCleanupResult{}, err
	}

	var cleanup workerCleanupResult
	err = l.registry.withCurrentLease(lease, func(state *sessionLifecycleState, current sessionEntry) error {
		activationErr := func() error {
			if cause := lease.cancellationCause(); cause != nil {
				return cause
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := l.validatePublishedRoute(route, desired); err != nil {
				return err
			}

			// A replacement inherits the conservative ACTIVE set. Complete this
			// pass before touching the new plan so omitted slots cannot remain
			// schedulable and state never grows across failed generations.
			inherited, inheritedErr := l.offline(ctx, state.ownedWorkers)
			state.ownedWorkers = cloneSessionWorkerRefs(inherited.pending)
			if inheritedErr != nil {
				return fmt.Errorf("offlining inherited external Workers: %w", inheritedErr)
			}
			state.ownedWorkers = nil

			// Reconciliation preserves status. An explicit OFFLINE preflight makes
			// this activation fail closed even when a prior process left one of the
			// desired durable Workers ACTIVE.
			preflight, preflightErr := l.offline(ctx, desired)
			state.ownedWorkers = cloneSessionWorkerRefs(preflight.pending)
			if preflightErr != nil {
				return fmt.Errorf("preflighting external Workers OFFLINE: %w", preflightErr)
			}
			state.ownedWorkers = nil

			for _, worker := range desired {
				if err := l.validatePublishedWorker(route, worker); err != nil {
					return err
				}
				updated, transitionErr := l.availability.SetExternalWorkerAvailability(
					ctx,
					worker.name(),
					worker.uid(),
					ateapipb.WorkerState_WORKER_STATE_ACTIVE,
				)
				if transitionErr == nil {
					transitionErr = worker.validateState(updated, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
				}
				if transitionErr == nil {
					transitionErr = l.validatePublishedWorker(route, worker)
				}
				if transitionErr != nil {
					return fmt.Errorf("activating external Worker %q: %w", worker.name(), transitionErr)
				}
			}

			state.ownedWorkers = cloneSessionWorkerRefs(desired)
			return nil
		}()
		if activationErr == nil {
			return nil
		}
		var cleanupErr error
		cleanup, cleanupErr = l.failActivationLocked(ctx, route, state, current, desired)
		return errors.Join(activationErr, cleanupErr)
	})
	if err != nil {
		return cleanup, err
	}
	return workerCleanupResult{}, nil
}

func (l *workerSessionLifecycle) cleanupFailedActivation(ctx context.Context, route workerSessionRoute, lease *sessionLease, workers []sessionWorkerRef) {
	if ctx == nil || route == nil || lease == nil {
		return
	}
	_ = l.registry.withCurrentLease(lease, func(state *sessionLifecycleState, current sessionEntry) error {
		_, err := l.failActivationLocked(ctx, route, state, current, workers)
		return err
	})
}

func (l *workerSessionLifecycle) failActivationLocked(
	ctx context.Context,
	route workerSessionRoute,
	state *sessionLifecycleState,
	current sessionEntry,
	workers []sessionWorkerRef,
) (workerCleanupResult, error) {
	closed := l.routes.Close(route)
	candidates := mergeSessionWorkerRefs(state.ownedWorkers, workers)
	cleanup, cleanupErr := l.offline(ctx, candidates)
	state.ownedWorkers = cloneSessionWorkerRefs(cleanup.pending)
	if cleanupErr != nil {
		return cleanup, fmt.Errorf("rolling back external Workers OFFLINE: %w", cleanupErr)
	}
	if closed && !l.routes.Withdraw(route) {
		return cleanup, errSessionRouteNotPublished
	}
	state.ownedWorkers = nil
	current.cancel(errSessionActivationFailed)
	return cleanup, nil
}

func (l *workerSessionLifecycle) validatePublishedRoute(route workerSessionRoute, workers []sessionWorkerRef) error {
	if err := routeLiveError(route); err != nil {
		return err
	}
	for _, worker := range workers {
		if err := l.validatePublishedWorker(route, worker); err != nil {
			return err
		}
	}
	return nil
}

func (l *workerSessionLifecycle) validatePublishedWorker(route workerSessionRoute, worker sessionWorkerRef) error {
	if err := routeLiveError(route); err != nil {
		return err
	}
	if !l.routes.AuthorizesWorker(route, worker.name(), worker.uid(), worker.executionIdentity()) {
		return fmt.Errorf("%w: route does not authorize Worker %q", errSessionRouteNotPublished, worker.name())
	}
	return nil
}

// cleanup closes the exact current route, makes every owned Worker OFFLINE,
// and only then withdraws the route and fences the lease.
// A fenced generation is already owned by its replacement and therefore
// returns a successful superseded no-op instead of touching shared Workers.
// The caller may exact-remove the underlying lease after this method returns.
func (l *workerSessionLifecycle) cleanup(ctx context.Context, route workerSessionRoute) (workerCleanupResult, error) {
	lease := routeLease(route)
	if lease == nil || route.RegistrationUID() != lease.registration() || route.Generation() != lease.sessionGeneration() {
		return workerCleanupResult{}, errSessionNotCurrent
	}
	var cleanup workerCleanupResult
	err := l.registry.withCurrentLease(lease, func(state *sessionLifecycleState, current sessionEntry) error {
		closed := l.routes.Close(route)
		if !closed && routeLiveError(route) == nil {
			return errSessionRouteNotPublished
		}
		var cleanupErr error
		cleanup, cleanupErr = l.offline(ctx, state.ownedWorkers)
		state.ownedWorkers = cloneSessionWorkerRefs(cleanup.pending)
		if cleanupErr != nil {
			return cleanupErr
		}
		if closed && !l.routes.Withdraw(route) {
			return errSessionRouteNotPublished
		}
		current.cancel(errSessionClosing)
		return nil
	})
	if errors.Is(err, errSessionFenced) {
		return workerCleanupResult{superseded: true}, nil
	}
	return cleanup, err
}

// cleanupUnpublished makes inherited Worker ownership unavailable when the
// current generation fails before publishing a route. It cannot activate a
// Worker and deliberately has no route authority. The caller may exact-remove
// the lease after this method returns.
func (l *workerSessionLifecycle) cleanupUnpublished(ctx context.Context, lease *sessionLease) (workerCleanupResult, error) {
	if lease == nil {
		return workerCleanupResult{}, errSessionNotCurrent
	}
	var cleanup workerCleanupResult
	err := l.registry.withCurrentLease(lease, func(state *sessionLifecycleState, current sessionEntry) error {
		var cleanupErr error
		cleanup, cleanupErr = l.offline(ctx, state.ownedWorkers)
		state.ownedWorkers = cloneSessionWorkerRefs(cleanup.pending)
		if cleanupErr != nil {
			return cleanupErr
		}
		current.cancel(errSessionClosing)
		return nil
	})
	if errors.Is(err, errSessionFenced) {
		return workerCleanupResult{superseded: true}, nil
	}
	return cleanup, err
}

func routeLease(route workerSessionRoute) *sessionLease {
	if route == nil {
		return nil
	}
	return route.lifecycleLease()
}

func routeLiveError(route workerSessionRoute) error {
	if route == nil {
		return errSessionRouteNotPublished
	}
	select {
	case <-route.Done():
		if cause := route.CancellationCause(); cause != nil {
			return cause
		}
		return errSessionRouteNotPublished
	default:
	}
	if cause := route.CancellationCause(); cause != nil {
		return cause
	}
	return nil
}

func (l *workerSessionLifecycle) offline(ctx context.Context, workers []sessionWorkerRef) (workerCleanupResult, error) {
	workers = cloneSessionWorkerRefs(workers)
	result := workerCleanupResult{
		offlined: make([]sessionWorkerRef, 0, len(workers)),
		pending:  make([]sessionWorkerRef, 0, len(workers)),
	}
	transitionErrors := make([]error, 0)
	for _, worker := range workers {
		updated, err := l.availability.SetExternalWorkerAvailability(
			ctx,
			worker.name(),
			worker.uid(),
			ateapipb.WorkerState_WORKER_STATE_OFFLINE,
		)
		if err == nil {
			err = worker.validateState(updated, ateapipb.WorkerState_WORKER_STATE_OFFLINE)
		}
		if err != nil {
			result.pending = append(result.pending, worker.clone())
			transitionErrors = append(transitionErrors, fmt.Errorf("offlining external Worker %q: %w", worker.name(), err))
			continue
		}
		result.offlined = append(result.offlined, worker.clone())
	}
	return result, errors.Join(transitionErrors...)
}

func sessionWorkerRefs(plan *WorkerPlan, reconciled []*ateapipb.Worker) ([]sessionWorkerRef, error) {
	if plan == nil || len(reconciled) == 0 || len(reconciled) > maxSlots {
		return nil, fmt.Errorf("%w: reconciled Worker set is empty or exceeds the slot bound", errInvalidWorkerSessionLifecycle)
	}
	desired := plan.Workers()
	if len(desired) != len(reconciled) {
		return nil, fmt.Errorf("%w: reconciled Worker set does not exactly match the plan", errInvalidWorkerSessionLifecycle)
	}

	byName := make(map[string]*ateapipb.Worker, len(reconciled))
	for _, worker := range reconciled {
		if worker == nil || worker.GetMetadata() == nil || worker.GetMetadata().GetName() == "" {
			return nil, fmt.Errorf("%w: reconciled Worker identity is missing", errInvalidWorkerSessionLifecycle)
		}
		name := worker.GetMetadata().GetName()
		if _, duplicate := byName[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate reconciled Worker %q", errInvalidWorkerSessionLifecycle, name)
		}
		byName[name] = worker
	}

	refs := make([]sessionWorkerRef, 0, len(desired))
	for _, expected := range desired {
		name := expected.GetMetadata().GetName()
		worker, found := byName[name]
		if !found {
			return nil, fmt.Errorf("%w: planned Worker %q was not reconciled", errInvalidWorkerSessionLifecycle, name)
		}
		if err := plan.ValidateExisting(worker); err != nil {
			return nil, err
		}
		uid := worker.GetMetadata().GetUid()
		if uid == "" {
			return nil, fmt.Errorf("%w: reconciled Worker %q has no UID", errInvalidWorkerSessionLifecycle, name)
		}
		identity := proto.Clone(expected).(*ateapipb.Worker)
		identity.Metadata = &ateapipb.ResourceMetadata{Name: name, Uid: uid}
		identity.SandboxClass = ""
		identity.Labels = nil
		identity.Status = nil
		refs = append(refs, sessionWorkerRef{identity: identity})
	}
	slices.SortFunc(refs, func(left, right sessionWorkerRef) int {
		return compareStrings(left.name(), right.name())
	})
	return refs, nil
}

func (r sessionWorkerRef) name() string {
	return r.identity.GetMetadata().GetName()
}

func (r sessionWorkerRef) uid() string {
	return r.identity.GetMetadata().GetUid()
}

func (r sessionWorkerRef) executionIdentity() string {
	return r.identity.GetExternalSlot().GetExecutionIdentity()
}

func (r sessionWorkerRef) clone() sessionWorkerRef {
	if r.identity == nil {
		return sessionWorkerRef{}
	}
	return sessionWorkerRef{identity: proto.Clone(r.identity).(*ateapipb.Worker)}
}

func (r sessionWorkerRef) validateState(worker *ateapipb.Worker, state ateapipb.WorkerState) error {
	if worker == nil || worker.GetMetadata().GetUid() != r.uid() {
		return fmt.Errorf("%w: Worker %q incarnation changed", ErrWorkerIdentityCollision, r.name())
	}
	if err := validatePlannedWorkerIdentity(r.identity, worker); err != nil {
		return err
	}
	if worker.GetStatus().GetState() != state {
		return fmt.Errorf("%w: Worker %q returned state %s, want %s", errInvalidWorkerSessionLifecycle, r.name(), worker.GetStatus().GetState(), state)
	}
	return nil
}

func cloneSessionWorkerRefs(workers []sessionWorkerRef) []sessionWorkerRef {
	if len(workers) == 0 {
		return nil
	}
	cloned := make([]sessionWorkerRef, len(workers))
	for index, worker := range workers {
		cloned[index] = worker.clone()
	}
	return cloned
}

func mergeSessionWorkerRefs(groups ...[]sessionWorkerRef) []sessionWorkerRef {
	byIdentity := make(map[string]sessionWorkerRef)
	for _, workers := range groups {
		for _, worker := range workers {
			key := worker.name() + "\x00" + worker.uid()
			if key == "\x00" {
				continue
			}
			byIdentity[key] = worker.clone()
		}
	}
	merged := make([]sessionWorkerRef, 0, len(byIdentity))
	for _, worker := range byIdentity {
		merged = append(merged, worker)
	}
	slices.SortFunc(merged, func(left, right sessionWorkerRef) int {
		return compareStrings(left.name()+"\x00"+left.uid(), right.name()+"\x00"+right.uid())
	})
	return merged
}

func compareStrings(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
