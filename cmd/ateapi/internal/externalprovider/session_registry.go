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
	"sync"
)

var (
	errInvalidSessionRegistryCapacity = errors.New("external provider session registry capacity is invalid")
	errInvalidSessionRegistration     = errors.New("external provider session registration is invalid")
	errInvalidSessionGeneration       = errors.New("external provider session generation is invalid")
	errSessionGenerationNotNewer      = errors.New("external provider session generation is not newer")
	errSessionRegistryFull            = errors.New("external provider session registry is full")
	errSessionFenced                  = errors.New("external provider session was fenced")
	errSessionActivationFailed        = errors.New("external provider session activation failed")
	errSessionClosing                 = errors.New("external provider session is closing")
	errSessionRemoved                 = errors.New("external provider session was removed")
	errSessionNotCurrent              = errors.New("external provider session is not current")
)

// sessionRegistry owns the current in-process session lease, generation fence,
// and bounded conservative Worker ownership for each tracked registration. A
// current lease is not proof that a route was published. The registry has no
// transport, persistence, or credential authority.
type sessionRegistry struct {
	mu                      sync.RWMutex
	maxTrackedRegistrations uint32
	highestGenerations      map[string]uint64
	sessions                map[string]sessionEntry
	lifecycleStates         map[string]*sessionLifecycleState
}

// sessionLifecycleState serializes route replacement and Worker availability
// transitions for one registration without blocking unrelated registrations.
// ownedWorkers is a bounded, conservative set of Workers which may still be
// ACTIVE; it survives route replacement so the new owner can make omitted
// slots unavailable. activeOperations is protected by sessionRegistry.mu and
// pins this pointer while an operation waits for or holds mu.
type sessionLifecycleState struct {
	mu               sync.Mutex
	ownedWorkers     []sessionWorkerRef
	activeOperations uint32
	reclaimEligible  bool
}

// sessionLease is an immutable generation identity. Its cancellation function
// is retained separately so only the registry can fence or remove it.
type sessionLease struct {
	registrationUID string
	generation      uint64
	ctx             context.Context
}

type sessionEntry struct {
	lease  *sessionLease
	cancel context.CancelCauseFunc
}

func newSessionRegistry(maxTrackedRegistrations uint32) (*sessionRegistry, error) {
	if maxTrackedRegistrations == 0 {
		return nil, errInvalidSessionRegistryCapacity
	}
	return &sessionRegistry{
		maxTrackedRegistrations: maxTrackedRegistrations,
		highestGenerations:      make(map[string]uint64),
		sessions:                make(map[string]sessionEntry),
		lifecycleStates:         make(map[string]*sessionLifecycleState),
	}, nil
}

// install reserves generation if it is newer than the current generation for
// registrationUID. The returned lease is not routable by itself and is owned
// by the session handler, which must pass it back to remove during cleanup.
func (r *sessionRegistry) install(registrationUID string, generation uint64) (*sessionLease, error) {
	return r.installPrepared(registrationUID, generation, nil)
}

// installPrepared installs a newer generation only after prepare has made the
// previous generation fail closed. prepare runs under the stable
// per-registration lifecycle gate and without the registry's global lock, so
// it may perform bounded persistence I/O. If prepare fails, the previous lease
// remains current and the generation is not consumed.
func (r *sessionRegistry) installPrepared(
	registrationUID string,
	generation uint64,
	prepare func(*sessionLifecycleState, sessionEntry) error,
) (*sessionLease, error) {
	if !IsValidIdentity(registrationUID) {
		return nil, errInvalidSessionRegistration
	}
	if generation == 0 {
		return nil, errInvalidSessionGeneration
	}

	lifecycle, err := r.acquireLifecycle(registrationUID, true)
	if err != nil {
		return nil, err
	}

	// The stable per-registration gate prevents a newer generation from being
	// installed in the middle of an older generation's Worker transition.
	lifecycle.mu.Lock()
	defer func() {
		r.releaseLifecycleLocked(registrationUID, lifecycle)
		lifecycle.mu.Unlock()
	}()

	r.mu.RLock()
	highest, tracked := r.highestGenerations[registrationUID]
	previous := r.sessions[registrationUID]
	r.mu.RUnlock()
	if tracked && generation <= highest {
		return nil, errSessionGenerationNotNewer
	}
	if previous.lease != nil && prepare != nil {
		if err := prepare(lifecycle, previous); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	lease := &sessionLease{
		registrationUID: registrationUID,
		generation:      generation,
		ctx:             ctx,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// All same-registration mutators take lifecycle.mu first. Recheck anyway so
	// this invariant fails closed if a future mutator omits that contract.
	highest, tracked = r.highestGenerations[registrationUID]
	current := r.sessions[registrationUID]
	if (tracked && generation <= highest) || current.lease != previous.lease {
		cancel(errSessionRemoved)
		return nil, errSessionGenerationNotNewer
	}
	r.highestGenerations[registrationUID] = generation
	r.sessions[registrationUID] = sessionEntry{lease: lease, cancel: cancel}
	lifecycle.reclaimEligible = false
	if previous.lease != nil {
		previous.cancel(errSessionFenced)
	}
	return lease, nil
}

// lookup returns a lease only while the exact registration and generation are
// current. It does not prove route publication. A returned lease may be fenced
// immediately afterward, so users must also observe done.
func (r *sessionRegistry) lookup(registrationUID string, generation uint64) (*sessionLease, bool) {
	if !IsValidIdentity(registrationUID) || generation == 0 {
		return nil, false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, exists := r.sessions[registrationUID]
	if !exists || entry.lease.generation != generation {
		return nil, false
	}
	return entry.lease, true
}

// remove drops and cancels only the exact current lease. Comparing the
// registration, generation, and lease identity prevents cleanup from an older
// session from deleting a newer generation. cleanupComplete authorizes idle
// lifecycle reclamation; a failed cleanup keeps its conservative tombstone.
func (r *sessionRegistry) remove(registrationUID string, generation uint64, lease *sessionLease, cleanupComplete bool) bool {
	if lease == nil || lease.registrationUID != registrationUID || lease.generation != generation {
		return false
	}

	lifecycle, _ := r.acquireLifecycle(registrationUID, false)
	if lifecycle == nil {
		return false
	}

	lifecycle.mu.Lock()
	defer func() {
		r.releaseLifecycleLocked(registrationUID, lifecycle)
		lifecycle.mu.Unlock()
	}()

	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.sessions[registrationUID]
	if !exists || current.lease.generation != generation || current.lease != lease {
		return false
	}
	delete(r.sessions, registrationUID)
	lifecycle.reclaimEligible = cleanupComplete
	current.cancel(errSessionRemoved)
	return true
}

// whileCurrent runs fn while lease is still the exact current lease. Holding
// the registry read lock across fn makes a route publication or lookup atomic
// with respect to a newer generation fencing the lease. Callers must not call
// a sessionRegistry mutator from fn.
func (r *sessionRegistry) whileCurrent(lease *sessionLease, fn func()) bool {
	if r == nil || lease == nil || fn == nil || !IsValidIdentity(lease.registrationUID) || lease.generation == 0 {
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	current, exists := r.sessions[lease.registrationUID]
	if !exists || current.lease != lease || current.lease.generation != lease.generation {
		return false
	}
	select {
	case <-lease.ctx.Done():
		return false
	default:
	}
	fn()
	return true
}

// withCurrentLease serializes one lifecycle operation with install and remove
// for the lease's registration, then rechecks exact session ownership. It never
// retains state for an untracked registration and never holds the registry's
// global session lock while the callback performs persistence I/O.
func (r *sessionRegistry) withCurrentLease(lease *sessionLease, operation func(*sessionLifecycleState, sessionEntry) error) error {
	if lease == nil || operation == nil || !IsValidIdentity(lease.registrationUID) || lease.generation == 0 {
		return errSessionNotCurrent
	}

	lifecycle, _ := r.acquireLifecycle(lease.registrationUID, false)
	if lifecycle == nil {
		if cause := lease.cancellationCause(); cause != nil {
			return cause
		}
		return errSessionNotCurrent
	}

	lifecycle.mu.Lock()
	defer func() {
		r.releaseLifecycleLocked(lease.registrationUID, lifecycle)
		lifecycle.mu.Unlock()
	}()

	r.mu.RLock()
	current, exists := r.sessions[lease.registrationUID]
	isCurrent := exists && current.lease == lease && current.lease.generation == lease.generation
	r.mu.RUnlock()
	if !isCurrent {
		if cause := lease.cancellationCause(); cause != nil {
			return cause
		}
		return errSessionNotCurrent
	}
	return operation(lifecycle, current)
}

// withRegistrationLifecycle serializes an operator lifecycle mutation with
// install, activation, cleanup, and removal for one registration. Unlike
// withCurrentLease, the caller intentionally does not possess a session lease:
// it must visit both the current generation and a no-current lifecycle
// tombstone retaining conservative Worker ownership. Callers must also hold
// the registration's claim-install gate so a durable claim cannot install a
// newer generation after this lookup.
func (r *sessionRegistry) withRegistrationLifecycle(registrationUID string, operation func(*sessionLifecycleState, sessionEntry) error) error {
	if r == nil || operation == nil || !IsValidIdentity(registrationUID) {
		return errSessionNotCurrent
	}

	lifecycle, _ := r.acquireLifecycle(registrationUID, false)
	if lifecycle == nil {
		return errSessionNotCurrent
	}

	lifecycle.mu.Lock()
	defer func() {
		r.releaseLifecycleLocked(registrationUID, lifecycle)
		lifecycle.mu.Unlock()
	}()

	r.mu.RLock()
	current := r.sessions[registrationUID]
	r.mu.RUnlock()
	return operation(lifecycle, current)
}

// acquireLifecycle pins the current per-registration lifecycle before
// releasing the global map lock. This prevents reclamation and same-key pointer
// recreation while the caller waits for lifecycle.mu.
func (r *sessionRegistry) acquireLifecycle(registrationUID string, create bool) (*sessionLifecycleState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lifecycle := r.lifecycleStates[registrationUID]
	if lifecycle == nil {
		if !create {
			return nil, nil
		}
		if uint64(len(r.lifecycleStates)) >= uint64(r.maxTrackedRegistrations) {
			return nil, errSessionRegistryFull
		}
		lifecycle = &sessionLifecycleState{}
		r.lifecycleStates[registrationUID] = lifecycle
	}
	lifecycle.activeOperations++
	return lifecycle, nil
}

// releaseLifecycleLocked drops an operation pin while lifecycle.mu is held.
// The generation watermark is reclaimed only when no current lease, pending
// Worker ownership, or other operation can still depend on this pointer. The
// Broker claim-install gate makes deletion safe from delayed durable claims.
func (r *sessionRegistry) releaseLifecycleLocked(registrationUID string, lifecycle *sessionLifecycleState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lifecycle == nil || r.lifecycleStates[registrationUID] != lifecycle || lifecycle.activeOperations == 0 {
		return
	}
	lifecycle.activeOperations--
	if lifecycle.activeOperations != 0 || len(lifecycle.ownedWorkers) != 0 || !lifecycle.reclaimEligible {
		return
	}
	if current, exists := r.sessions[registrationUID]; exists && current.lease != nil {
		return
	}
	delete(r.lifecycleStates, registrationUID)
	delete(r.highestGenerations, registrationUID)
}

func (l *sessionLease) registration() string {
	return l.registrationUID
}

func (l *sessionLease) sessionGeneration() uint64 {
	return l.generation
}

func (l *sessionLease) done() <-chan struct{} {
	return l.ctx.Done()
}

func (l *sessionLease) cancellationCause() error {
	return context.Cause(l.ctx)
}
