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
	errSessionRemoved                 = errors.New("external provider session was removed")
)

// sessionRegistry owns the current in-process route lease and generation fence
// for each tracked registration. It has no transport, persistence, or
// credential authority.
type sessionRegistry struct {
	mu                      sync.RWMutex
	maxTrackedRegistrations uint32
	highestGenerations      map[string]uint64
	sessions                map[string]sessionEntry
}

// sessionLease is an immutable routing identity. Its cancellation function is
// retained separately so only the registry can fence or remove it.
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
	}, nil
}

// install publishes generation if it is newer than the current generation for
// registrationUID. The returned lease is owned by the session handler, which
// must pass it back to remove during cleanup.
func (r *sessionRegistry) install(registrationUID string, generation uint64) (*sessionLease, error) {
	if !IsValidIdentity(registrationUID) {
		return nil, errInvalidSessionRegistration
	}
	if generation == 0 {
		return nil, errInvalidSessionGeneration
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	highest, tracked := r.highestGenerations[registrationUID]
	if tracked && generation <= highest {
		return nil, errSessionGenerationNotNewer
	}
	if !tracked && uint64(len(r.highestGenerations)) >= uint64(r.maxTrackedRegistrations) {
		return nil, errSessionRegistryFull
	}

	previous := r.sessions[registrationUID]
	ctx, cancel := context.WithCancelCause(context.Background())
	lease := &sessionLease{
		registrationUID: registrationUID,
		generation:      generation,
		ctx:             ctx,
	}
	r.highestGenerations[registrationUID] = generation
	r.sessions[registrationUID] = sessionEntry{lease: lease, cancel: cancel}
	if previous.lease != nil {
		previous.cancel(errSessionFenced)
	}
	return lease, nil
}

// lookup returns a lease only while the exact registration and generation are
// current. A returned lease may be fenced immediately afterward, so routing
// users must also observe done.
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
// session from deleting a newer route.
func (r *sessionRegistry) remove(registrationUID string, generation uint64, lease *sessionLease) bool {
	if lease == nil || lease.registrationUID != registrationUID || lease.generation != generation {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.sessions[registrationUID]
	if !exists || current.lease.generation != generation || current.lease != lease {
		return false
	}
	delete(r.sessions, registrationUID)
	current.cancel(errSessionRemoved)
	return true
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
