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
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
)

const (
	maximumSessionRoutes   uint32 = 65535
	maximumSessionBindings uint32 = 1 << 20
)

var (
	// ErrInvalidSessionRouteConfig reports unsafe route-directory bounds.
	ErrInvalidSessionRouteConfig = errors.New("invalid external provider session route configuration")

	// ErrInvalidSessionWorkerBindings reports incomplete or inconsistent
	// reconciled Worker identities.
	ErrInvalidSessionWorkerBindings = errors.New("invalid external provider session Worker bindings")

	// ErrSessionRouteNotCurrent reports an attempted publication by a fenced or
	// removed session generation.
	ErrSessionRouteNotCurrent = errors.New("external provider session route is not current")

	// ErrSessionRouteDirectoryFull reports that publishing a route would exceed
	// an explicit route or binding bound.
	ErrSessionRouteDirectoryFull = errors.New("external provider session route directory is full")

	// ErrSessionRouteCollision reports an execution identity already owned by
	// another live registration. The existing owner is retained unchanged.
	ErrSessionRouteCollision = errors.New("external provider session execution identity collision")

	// ErrSessionRouteReplaced is the cancellation cause for a route replaced by
	// a newer generation.
	ErrSessionRouteReplaced = errors.New("external provider session route was replaced")

	// ErrSessionRouteWithdrawn is the cancellation cause for an exactly-owned
	// route removed during session cleanup.
	ErrSessionRouteWithdrawn = errors.New("external provider session route was withdrawn")
)

// SessionRouteDirectoryLimits bounds all in-process route state. A route has
// at least one and at most 256 Worker bindings. Both fields are required.
type SessionRouteDirectoryLimits struct {
	MaxRoutes   uint32
	MaxBindings uint32
}

// SessionWorkerBinding is an immutable association between one advertised
// slot and the exact durable Worker incarnation used to route it.
type SessionWorkerBinding struct {
	registrationUID   string
	slotID            string
	workerName        string
	workerUID         string
	executionIdentity string
}

// RegistrationUID returns the non-secret provider registration identity.
func (b SessionWorkerBinding) RegistrationUID() string { return b.registrationUID }

// SlotID returns the registration-scoped slot identity.
func (b SessionWorkerBinding) SlotID() string { return b.slotID }

// WorkerName returns the durable global Worker resource name.
func (b SessionWorkerBinding) WorkerName() string { return b.workerName }

// WorkerUID returns the exact server-created Worker incarnation.
func (b SessionWorkerBinding) WorkerUID() string { return b.workerUID }

// ExecutionIdentity returns the stable opaque reverse-index key.
func (b SessionWorkerBinding) ExecutionIdentity() string { return b.executionIdentity }

// BuildSessionWorkerBindings validates reconciled Workers against admission
// and returns bindings ordered by execution identity. Input protobufs and the
// returned slice are not retained.
func BuildSessionWorkerBindings(admission *ConnectAdmission, reconciled []*ateapipb.Worker) ([]SessionWorkerBinding, error) {
	plan, err := PlanExternalWorkers(admission)
	if err != nil {
		return nil, fmt.Errorf("%w: admission does not produce a Worker plan", ErrInvalidSessionWorkerBindings)
	}
	desired := plan.Workers()
	if len(reconciled) != len(desired) {
		return nil, fmt.Errorf("%w: reconciled Worker count does not match the plan", ErrInvalidSessionWorkerBindings)
	}

	byName := make(map[string]*ateapipb.Worker, len(reconciled))
	seenUIDs := make(map[string]struct{}, len(reconciled))
	for _, worker := range reconciled {
		if worker == nil || worker.GetMetadata() == nil {
			return nil, fmt.Errorf("%w: a reconciled Worker is missing metadata", ErrInvalidSessionWorkerBindings)
		}
		name := worker.GetMetadata().GetName()
		if _, duplicate := byName[name]; duplicate {
			return nil, fmt.Errorf("%w: reconciled Worker names are not unique", ErrInvalidSessionWorkerBindings)
		}
		uid := worker.GetMetadata().GetUid()
		parsedUID, parseErr := uuid.Parse(uid)
		if parseErr != nil || parsedUID.String() != uid {
			return nil, fmt.Errorf("%w: a reconciled Worker UID is not canonical", ErrInvalidSessionWorkerBindings)
		}
		if _, duplicate := seenUIDs[uid]; duplicate {
			return nil, fmt.Errorf("%w: reconciled Worker UIDs are not unique", ErrInvalidSessionWorkerBindings)
		}
		seenUIDs[uid] = struct{}{}
		byName[name] = worker
	}

	registrationUID := admission.registration.UID
	bindings := make([]SessionWorkerBinding, 0, len(admission.slots))
	for _, slot := range admission.slots {
		workerName := deriveOpaqueIdentity("ext-", workerNameDomain, registrationUID, slot.slotID)
		worker, exists := byName[workerName]
		if !exists {
			return nil, fmt.Errorf("%w: a planned Worker is missing", ErrInvalidSessionWorkerBindings)
		}
		if err := plan.ValidateExisting(worker); err != nil {
			return nil, fmt.Errorf("%w: reconciled Worker identity differs from the plan", ErrInvalidSessionWorkerBindings)
		}
		bindings = append(bindings, SessionWorkerBinding{
			registrationUID:   registrationUID,
			slotID:            slot.slotID,
			workerName:        workerName,
			workerUID:         worker.GetMetadata().GetUid(),
			executionIdentity: worker.GetExternalSlot().GetExecutionIdentity(),
		})
	}
	slices.SortFunc(bindings, func(left, right SessionWorkerBinding) int {
		return cmp.Compare(left.executionIdentity, right.executionIdentity)
	})
	return bindings, nil
}

// SessionRoute is an immutable live generation and its Worker bindings. Done
// closes when the generation is fenced, replaced, or explicitly withdrawn.
type SessionRoute struct {
	lease    *sessionLease
	bindings []SessionWorkerBinding
	ctx      context.Context
}

// RegistrationUID returns the route's provider registration identity.
func (r *SessionRoute) RegistrationUID() string {
	if r == nil || r.lease == nil {
		return ""
	}
	return r.lease.registrationUID
}

// Generation returns the route's nonzero session fencing generation.
func (r *SessionRoute) Generation() uint64 {
	if r == nil || r.lease == nil {
		return 0
	}
	return r.lease.generation
}

// lifecycleLease binds this proof to the exact sessionRegistry lease. It is
// intentionally package-private so only the broker and Worker lifecycle can
// consume route authority.
func (r *SessionRoute) lifecycleLease() *sessionLease {
	if r == nil {
		return nil
	}
	return r.lease
}

// Bindings returns an independent slice ordered by execution identity.
func (r *SessionRoute) Bindings() []SessionWorkerBinding {
	if r == nil {
		return nil
	}
	return slices.Clone(r.bindings)
}

// BindingForWorker returns the route proof for one exact durable Worker
// incarnation. A Worker deleted and recreated under the same name does not
// match the old UID.
func (r *SessionRoute) BindingForWorker(workerName, workerUID string) (SessionWorkerBinding, bool) {
	if r == nil || workerName == "" || workerUID == "" {
		return SessionWorkerBinding{}, false
	}
	for _, binding := range r.bindings {
		if binding.workerName == workerName && binding.workerUID == workerUID {
			return binding, true
		}
	}
	return SessionWorkerBinding{}, false
}

// Done closes when this route can no longer be used for new execution.
func (r *SessionRoute) Done() <-chan struct{} {
	if r == nil || r.ctx == nil {
		return closedRouteDone
	}
	return r.ctx.Done()
}

// CancellationCause reports why Done closed, or nil while the route is live.
func (r *SessionRoute) CancellationCause() error {
	if r == nil || r.ctx == nil {
		return ErrSessionRouteNotCurrent
	}
	return context.Cause(r.ctx)
}

var closedRouteDone = func() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}()

type sessionRouteEntry struct {
	route  *SessionRoute
	cancel context.CancelCauseFunc
}

type sessionExecutionEntry struct {
	route   *SessionRoute
	binding SessionWorkerBinding
}

// SessionRouteDirectory atomically owns live registration routes and the
// execution-identity reverse index. It has no transport or persistence role.
type SessionRouteDirectory struct {
	registry *sessionRegistry
	limits   SessionRouteDirectoryLimits

	mu            sync.RWMutex
	routes        map[string]sessionRouteEntry
	executions    map[string]sessionExecutionEntry
	totalBindings uint32
}

func newSessionRouteDirectory(registry *sessionRegistry, limits SessionRouteDirectoryLimits) (*SessionRouteDirectory, error) {
	if registry == nil || limits.MaxRoutes == 0 || limits.MaxRoutes > maximumSessionRoutes ||
		limits.MaxBindings < limits.MaxRoutes || limits.MaxBindings > maximumSessionBindings {
		return nil, ErrInvalidSessionRouteConfig
	}
	return &SessionRouteDirectory{
		registry:   registry,
		limits:     limits,
		routes:     make(map[string]sessionRouteEntry),
		executions: make(map[string]sessionExecutionEntry),
	}, nil
}

// publish atomically installs bindings for the exact current lease. Repeating
// the same publication is idempotent; changing bindings for one lease fails.
func (d *SessionRouteDirectory) publish(lease *sessionLease, bindings []SessionWorkerBinding) (*SessionRoute, error) {
	validated, err := validateRouteBindings(lease, bindings)
	if err != nil {
		return nil, err
	}

	var published *SessionRoute
	var publishErr error
	current := d != nil && d.registry.whileCurrent(lease, func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		existing, replacing := d.routes[lease.registrationUID]
		if replacing && existing.route.lease == lease {
			if equalSessionBindings(existing.route.bindings, validated) {
				published = existing.route
				return
			}
			publishErr = fmt.Errorf("%w: one lease cannot change its bindings", ErrSessionRouteCollision)
			return
		}
		if replacing && existing.route.Generation() >= lease.generation {
			publishErr = ErrSessionRouteNotCurrent
			return
		}

		for _, binding := range validated {
			owner, occupied := d.executions[binding.executionIdentity]
			if occupied && (!replacing || owner.route != existing.route) {
				publishErr = ErrSessionRouteCollision
				return
			}
		}

		routeCount := uint64(len(d.routes))
		bindingCount := uint64(d.totalBindings) + uint64(len(validated))
		if replacing {
			bindingCount -= uint64(len(existing.route.bindings))
		} else {
			routeCount++
		}
		if routeCount > uint64(d.limits.MaxRoutes) || bindingCount > uint64(d.limits.MaxBindings) {
			publishErr = ErrSessionRouteDirectoryFull
			return
		}

		ctx, cancel := context.WithCancelCause(lease.ctx)
		route := &SessionRoute{lease: lease, bindings: slices.Clone(validated), ctx: ctx}
		if replacing {
			for _, binding := range existing.route.bindings {
				delete(d.executions, binding.executionIdentity)
			}
		}
		for _, binding := range validated {
			d.executions[binding.executionIdentity] = sessionExecutionEntry{route: route, binding: binding}
		}
		d.routes[lease.registrationUID] = sessionRouteEntry{route: route, cancel: cancel}
		d.totalBindings = uint32(bindingCount)
		published = route
		if replacing {
			existing.cancel(ErrSessionRouteReplaced)
		}
	})
	if !current {
		return nil, ErrSessionRouteNotCurrent
	}
	if publishErr != nil {
		return nil, publishErr
	}
	return published, nil
}

// Withdraw removes only the exact concrete route pointer currently owned by
// its registration and generation. The interface boundary lets the Worker
// lifecycle consume publication proofs without granting it construction or
// index authority; a substituted implementation always fails closed. Stale
// cleanup cannot remove a replacement.
func (d *SessionRouteDirectory) Withdraw(proof workerSessionRoute) bool {
	route, valid := proof.(*SessionRoute)
	if d == nil || !valid || route == nil || route.lifecycleLease() == nil {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	entry, exists := d.routes[route.RegistrationUID()]
	if !exists || entry.route != route || entry.route.Generation() != route.Generation() {
		return false
	}
	for _, binding := range route.bindings {
		indexed, found := d.executions[binding.executionIdentity]
		if !found || indexed.route != route || indexed.binding != binding {
			return false
		}
	}
	for _, binding := range route.bindings {
		delete(d.executions, binding.executionIdentity)
	}
	delete(d.routes, route.RegistrationUID())
	d.totalBindings -= uint32(len(route.bindings))
	entry.cancel(ErrSessionRouteWithdrawn)
	return true
}

// LookupExecutionIdentity resolves one opaque execution identity only while
// its route is atomically verified as the current session generation. The
// returned binding is a value copy; callers must observe route.Done while
// using any transport derived from it.
func (d *SessionRouteDirectory) LookupExecutionIdentity(executionIdentity string) (*SessionRoute, SessionWorkerBinding, bool) {
	if d == nil || !IsValidIdentity(executionIdentity) {
		return nil, SessionWorkerBinding{}, false
	}

	d.mu.RLock()
	candidate, exists := d.executions[executionIdentity]
	d.mu.RUnlock()
	if !exists {
		return nil, SessionWorkerBinding{}, false
	}

	var route *SessionRoute
	var binding SessionWorkerBinding
	current := d.registry.whileCurrent(candidate.route.lease, func() {
		d.mu.RLock()
		defer d.mu.RUnlock()
		indexed, stillPublished := d.executions[executionIdentity]
		if !stillPublished || indexed.route != candidate.route || indexed.binding != candidate.binding {
			return
		}
		route = indexed.route
		binding = indexed.binding
	})
	if !current || route == nil {
		return nil, SessionWorkerBinding{}, false
	}
	return route, binding, true
}

// AuthorizesWorker reports whether route is the exact current publication and
// binds the named Worker incarnation to executionIdentity. It is a point-in-
// time generation-fenced proof; lifecycle callers must also observe
// route.Done across longer operations.
func (d *SessionRouteDirectory) AuthorizesWorker(proof workerSessionRoute, workerName, workerUID, executionIdentity string) bool {
	route, valid := proof.(*SessionRoute)
	if d == nil || !valid || route == nil || route.lifecycleLease() == nil || !IsValidIdentity(executionIdentity) {
		return false
	}
	var authorized bool
	current := d.registry.whileCurrent(route.lifecycleLease(), func() {
		d.mu.RLock()
		defer d.mu.RUnlock()
		published, exists := d.routes[route.RegistrationUID()]
		if !exists || published.route != route || published.route.Generation() != route.Generation() {
			return
		}
		indexed, exists := d.executions[executionIdentity]
		if !exists || indexed.route != route || indexed.binding.workerName != workerName || indexed.binding.workerUID != workerUID {
			return
		}
		authorized = true
	})
	return current && authorized
}

// SessionRouteDirectoryStats is a non-secret point-in-time capacity snapshot.
type SessionRouteDirectoryStats struct {
	Routes   uint32
	Bindings uint32
}

// Stats returns current route and reverse-index cardinalities.
func (d *SessionRouteDirectory) Stats() SessionRouteDirectoryStats {
	if d == nil {
		return SessionRouteDirectoryStats{}
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return SessionRouteDirectoryStats{Routes: uint32(len(d.routes)), Bindings: d.totalBindings}
}

func validateRouteBindings(lease *sessionLease, bindings []SessionWorkerBinding) ([]SessionWorkerBinding, error) {
	if lease == nil || !IsValidIdentity(lease.registrationUID) || lease.generation == 0 || len(bindings) == 0 || len(bindings) > maxSlots {
		return nil, fmt.Errorf("%w: lease and 1..256 bindings are required", ErrInvalidSessionWorkerBindings)
	}
	validated := slices.Clone(bindings)
	seenSlots := make(map[string]struct{}, len(validated))
	seenNames := make(map[string]struct{}, len(validated))
	seenUIDs := make(map[string]struct{}, len(validated))
	seenExecutions := make(map[string]struct{}, len(validated))
	for _, binding := range validated {
		uid, err := uuid.Parse(binding.workerUID)
		if binding.registrationUID != lease.registrationUID || !IsValidIdentity(binding.slotID) ||
			!IsValidIdentity(binding.workerName) || err != nil || uid.String() != binding.workerUID ||
			!IsValidIdentity(binding.executionIdentity) {
			return nil, fmt.Errorf("%w: a binding identity is invalid", ErrInvalidSessionWorkerBindings)
		}
		if _, duplicate := seenSlots[binding.slotID]; duplicate {
			return nil, fmt.Errorf("%w: slot identities are not unique", ErrInvalidSessionWorkerBindings)
		}
		if _, duplicate := seenNames[binding.workerName]; duplicate {
			return nil, fmt.Errorf("%w: Worker names are not unique", ErrInvalidSessionWorkerBindings)
		}
		if _, duplicate := seenUIDs[binding.workerUID]; duplicate {
			return nil, fmt.Errorf("%w: Worker UIDs are not unique", ErrInvalidSessionWorkerBindings)
		}
		if _, duplicate := seenExecutions[binding.executionIdentity]; duplicate {
			return nil, fmt.Errorf("%w: execution identities are not unique", ErrInvalidSessionWorkerBindings)
		}
		seenSlots[binding.slotID] = struct{}{}
		seenNames[binding.workerName] = struct{}{}
		seenUIDs[binding.workerUID] = struct{}{}
		seenExecutions[binding.executionIdentity] = struct{}{}
	}
	slices.SortFunc(validated, func(left, right SessionWorkerBinding) int {
		return cmp.Compare(left.executionIdentity, right.executionIdentity)
	})
	return validated, nil
}

func equalSessionBindings(left, right []SessionWorkerBinding) bool {
	return slices.Equal(left, right)
}
