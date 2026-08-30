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

const (
	defaultMaxClaimInstallInFlight uint32 = 64
	defaultMaxClaimInstallKeys     uint32 = 64
	maximumClaimInstallGateLimit   uint32 = 1024
)

var (
	errInvalidClaimInstallGate = errors.New("external provider claim-install gate is invalid")
	errClaimInstallGateFull    = errors.New("external provider claim-install gate is full")
	errInvalidGatedClaim       = errors.New("external provider gated session claim is invalid")
)

// claimInstallGate serializes the durable claim and in-memory generation
// install for each registration. It is process-local and therefore relies on
// the Broker's single-replica deployment fence.
type claimInstallGate struct {
	mu              sync.Mutex
	maxDistinctKeys uint32
	inFlight        chan struct{}
	entries         map[string]*claimInstallGateEntry
}

// ClaimInstallGateLimits bounds claims waiting to install their generation in
// the process-local session registry.
type ClaimInstallGateLimits struct {
	MaxInFlight     uint32
	MaxDistinctKeys uint32
}

type claimInstallGateEntry struct {
	permit chan struct{}
	refs   uint32
}

// claimInstallGateLease pins one exact key and one global in-flight slot. A
// lease is one-shot: it can issue at most one claim and begin at most one
// registry install.
type claimInstallGateLease struct {
	gate            *claimInstallGate
	entry           *claimInstallGateEntry
	registrationUID string

	mu             sync.Mutex
	claimStarted   bool
	installStarted bool
	released       bool
}

// gatedSessionClaim proves that ClaimExternalProviderSession ran while the
// registration's gate was held. Only sessionCoordinator may consume it.
type gatedSessionClaim struct {
	lease *claimInstallGateLease
	claim SessionClaim
}

func newClaimInstallGate(maxInFlight, maxDistinctKeys uint32) (*claimInstallGate, error) {
	if maxInFlight == 0 || maxInFlight > maximumClaimInstallGateLimit ||
		maxDistinctKeys == 0 || maxDistinctKeys > maxInFlight {
		return nil, errInvalidClaimInstallGate
	}
	return &claimInstallGate{
		maxDistinctKeys: maxDistinctKeys,
		inFlight:        make(chan struct{}, maxInFlight),
		entries:         make(map[string]*claimInstallGateEntry),
	}, nil
}

// acquire waits for the global in-flight bound with context cancellation, then
// joins or creates the exact registration gate. Unknown registrations fail
// closed when the distinct-key bound is full; waiters for an existing key are
// still bounded by the global slot acquired first.
func (g *claimInstallGate) acquire(ctx context.Context, registrationUID string) (*claimInstallGateLease, error) {
	if g == nil || ctx == nil || !IsValidIdentity(registrationUID) {
		return nil, errInvalidClaimInstallGate
	}
	select {
	case g.inFlight <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	g.mu.Lock()
	entry := g.entries[registrationUID]
	if entry == nil {
		if uint64(len(g.entries)) >= uint64(g.maxDistinctKeys) {
			g.mu.Unlock()
			<-g.inFlight
			return nil, errClaimInstallGateFull
		}
		entry = &claimInstallGateEntry{permit: make(chan struct{}, 1)}
		entry.permit <- struct{}{}
		g.entries[registrationUID] = entry
	}
	entry.refs++
	g.mu.Unlock()

	select {
	case <-entry.permit:
		if err := ctx.Err(); err != nil {
			entry.permit <- struct{}{}
			g.releaseReference(registrationUID, entry)
			return nil, err
		}
		return &claimInstallGateLease{
			gate:            g,
			entry:           entry,
			registrationUID: registrationUID,
		}, nil
	case <-ctx.Done():
		g.releaseReference(registrationUID, entry)
		return nil, ctx.Err()
	}
}

func (g *claimInstallGate) releaseReference(registrationUID string, entry *claimInstallGateEntry) {
	g.mu.Lock()
	if current := g.entries[registrationUID]; current == entry && entry.refs > 0 {
		entry.refs--
		if entry.refs == 0 {
			delete(g.entries, registrationUID)
		}
	}
	g.mu.Unlock()
	<-g.inFlight
}

func (l *claimInstallGateLease) claimSession(
	ctx context.Context,
	store ExternalProviderStore,
	digest CredentialDigest,
) (*gatedSessionClaim, error) {
	if l == nil || ctx == nil || store == nil {
		return nil, errInvalidGatedClaim
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.claimStarted {
		return nil, errInvalidGatedClaim
	}
	l.claimStarted = true

	claim, err := store.ClaimExternalProviderSession(ctx, l.registrationUID, digest)
	if err != nil {
		return nil, err
	}
	if claim.Registration.UID != l.registrationUID || claim.Generation == 0 {
		return nil, errInvalidGatedClaim
	}
	return &gatedSessionClaim{lease: l, claim: claim}, nil
}

func (c *gatedSessionClaim) beginInstall() (SessionClaim, *claimInstallGateLease, error) {
	if c == nil || c.lease == nil || c.claim.Registration.UID != c.lease.registrationUID {
		return SessionClaim{}, nil, errInvalidGatedClaim
	}
	l := c.lease
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || !l.claimStarted || l.installStarted {
		return SessionClaim{}, nil, errInvalidGatedClaim
	}
	l.installStarted = true
	return c.claim, l, nil
}

func (l *claimInstallGateLease) release() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	gate := l.gate
	entry := l.entry
	registrationUID := l.registrationUID
	l.mu.Unlock()

	if gate == nil || entry == nil {
		return
	}
	entry.permit <- struct{}{}
	gate.releaseReference(registrationUID, entry)
}

type claimInstallGateStats struct {
	InFlight     uint32
	DistinctKeys uint32
}

func (g *claimInstallGate) stats() claimInstallGateStats {
	if g == nil {
		return claimInstallGateStats{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return claimInstallGateStats{
		InFlight:     uint32(len(g.inFlight)),
		DistinctKeys: uint32(len(g.entries)),
	}
}
