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
	"sync"
	"sync/atomic"
	"testing"
)

func TestSessionRegistryValidatesAndBoundsEntries(t *testing.T) {
	if registry, err := newSessionRegistry(0); !errors.Is(err, errInvalidSessionRegistryCapacity) || registry != nil {
		t.Fatalf("newSessionRegistry(0) = (%v, %v), want (nil, errInvalidSessionRegistryCapacity)", registry, err)
	}

	registry := mustSessionRegistry(t, 2)
	if lease, err := registry.install("/invalid", 1); !errors.Is(err, errInvalidSessionRegistration) || lease != nil {
		t.Fatalf("install(invalid registration) = (%v, %v), want (nil, errInvalidSessionRegistration)", lease, err)
	}
	if lease, err := registry.install("registration-a", 0); !errors.Is(err, errInvalidSessionGeneration) || lease != nil {
		t.Fatalf("install(zero generation) = (%v, %v), want (nil, errInvalidSessionGeneration)", lease, err)
	}

	first := mustInstallSession(t, registry, "registration-a", 1)
	second := mustInstallSession(t, registry, "registration-b", 1)
	if lease, err := registry.install("registration-c", 1); !errors.Is(err, errSessionRegistryFull) || lease != nil {
		t.Fatalf("install(over capacity) = (%v, %v), want (nil, errSessionRegistryFull)", lease, err)
	}

	replacement := mustInstallSession(t, registry, "registration-a", 2)
	requireSessionDone(t, first, errSessionFenced)
	if !registry.remove("registration-b", 1, second) {
		t.Fatal("remove(registration-b) = false, want true")
	}
	if lease, err := registry.install("registration-c", 1); !errors.Is(err, errSessionRegistryFull) || lease != nil {
		t.Fatalf("install(over capacity after live removal) = (%v, %v), want (nil, errSessionRegistryFull)", lease, err)
	}
	reconnected := mustInstallSession(t, registry, "registration-b", 2)
	if got, ok := registry.lookup("registration-a", 2); !ok || got != replacement {
		t.Fatalf("lookup(registration-a, 2) = (%p, %v), want (%p, true)", got, ok, replacement)
	}
	if got, ok := registry.lookup("registration-b", 2); !ok || got != reconnected {
		t.Fatalf("lookup(registration-b, 2) = (%p, %v), want (%p, true)", got, ok, reconnected)
	}
}

func TestSessionRegistryRejectsStaleAndFencesPrevious(t *testing.T) {
	registry := mustSessionRegistry(t, 1)
	first := mustInstallSession(t, registry, "registration-a", 7)
	if got := first.registration(); got != "registration-a" {
		t.Errorf("registration() = %q, want registration-a", got)
	}
	if got := first.sessionGeneration(); got != 7 {
		t.Errorf("sessionGeneration() = %d, want 7", got)
	}
	requireSessionLive(t, first)

	for _, generation := range []uint64{6, 7} {
		lease, err := registry.install("registration-a", generation)
		if !errors.Is(err, errSessionGenerationNotNewer) || lease != nil {
			t.Errorf("install(generation %d) = (%v, %v), want (nil, errSessionGenerationNotNewer)", generation, lease, err)
		}
	}
	requireSessionLive(t, first)

	second := mustInstallSession(t, registry, "registration-a", 8)
	requireSessionDone(t, first, errSessionFenced)
	requireSessionLive(t, second)
	if got, ok := registry.lookup("registration-a", 7); ok || got != nil {
		t.Errorf("lookup(stale generation) = (%v, %v), want (nil, false)", got, ok)
	}
	if got, ok := registry.lookup("registration-a", 8); !ok || got != second {
		t.Errorf("lookup(current generation) = (%p, %v), want (%p, true)", got, ok, second)
	}
	for _, lookup := range []struct {
		uid        string
		generation uint64
	}{
		{uid: "/invalid", generation: 8},
		{uid: "registration-a", generation: 0},
		{uid: "registration-b", generation: 8},
	} {
		if got, ok := registry.lookup(lookup.uid, lookup.generation); ok || got != nil {
			t.Errorf("lookup(%q, %d) = (%v, %v), want (nil, false)", lookup.uid, lookup.generation, got, ok)
		}
	}
}

func TestSessionRegistryCompareAndDeletePreventsABACleanup(t *testing.T) {
	registry := mustSessionRegistry(t, 1)
	first := mustInstallSession(t, registry, "registration-a", 1)
	second := mustInstallSession(t, registry, "registration-a", 2)

	if registry.remove("registration-a", 1, first) {
		t.Fatal("remove(old generation) = true, want false")
	}
	if registry.remove("registration-a", 2, first) {
		t.Fatal("remove(mismatched lease generation) = true, want false")
	}
	if registry.remove("registration-b", 2, second) {
		t.Fatal("remove(mismatched registration) = true, want false")
	}
	if registry.remove("registration-a", 2, nil) {
		t.Fatal("remove(nil lease) = true, want false")
	}
	forged := &sessionLease{
		registrationUID: "registration-a",
		generation:      2,
		ctx:             context.Background(),
	}
	if registry.remove("registration-a", 2, forged) {
		t.Fatal("remove(forged identity) = true, want false")
	}
	if got, ok := registry.lookup("registration-a", 2); !ok || got != second {
		t.Fatalf("lookup after stale cleanup = (%p, %v), want (%p, true)", got, ok, second)
	}

	if !registry.remove("registration-a", 2, second) {
		t.Fatal("remove(exact current lease) = false, want true")
	}
	requireSessionDone(t, second, errSessionRemoved)
	if registry.remove("registration-a", 2, second) {
		t.Fatal("remove(already removed lease) = true, want false")
	}
	if got, ok := registry.lookup("registration-a", 2); ok || got != nil {
		t.Fatalf("lookup after removal = (%v, %v), want (nil, false)", got, ok)
	}
	for _, generation := range []uint64{1, 2} {
		if lease, err := registry.install("registration-a", generation); !errors.Is(err, errSessionGenerationNotNewer) || lease != nil {
			t.Errorf("install stale generation %d after removal = (%v, %v), want (nil, errSessionGenerationNotNewer)", generation, lease, err)
		}
	}
	third := mustInstallSession(t, registry, "registration-a", 3)
	if got, ok := registry.lookup("registration-a", 3); !ok || got != third {
		t.Fatalf("lookup after tombstone replacement = (%p, %v), want (%p, true)", got, ok, third)
	}
}

func TestSessionRegistrySimultaneousInstallsKeepHighestGeneration(t *testing.T) {
	const racers = 128
	registry := mustSessionRegistry(t, 1)
	start := make(chan struct{})
	results := make(chan sessionInstallResult, racers)
	var wait sync.WaitGroup
	for generation := uint64(1); generation <= racers; generation++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			lease, err := registry.install("registration-a", generation)
			results <- sessionInstallResult{generation: generation, lease: lease, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var highest *sessionLease
	var installed []*sessionLease
	for result := range results {
		if result.err != nil {
			if !errors.Is(result.err, errSessionGenerationNotNewer) || result.lease != nil {
				t.Fatalf("install generation %d = (%v, %v), want a lease or errSessionGenerationNotNewer", result.generation, result.lease, result.err)
			}
			continue
		}
		installed = append(installed, result.lease)
		if result.generation == racers {
			highest = result.lease
		}
	}
	if highest == nil {
		t.Fatal("highest generation did not install")
	}
	if got, ok := registry.lookup("registration-a", racers); !ok || got != highest {
		t.Fatalf("lookup(highest generation) = (%p, %v), want (%p, true)", got, ok, highest)
	}
	for _, lease := range installed {
		if lease == highest {
			requireSessionLive(t, lease)
			continue
		}
		requireSessionDone(t, lease, errSessionFenced)
	}
}

func TestSessionRegistryEqualGenerationHasOneWinner(t *testing.T) {
	const racers = 64
	registry := mustSessionRegistry(t, 1)
	start := make(chan struct{})
	results := make(chan sessionInstallResult, racers)
	var wait sync.WaitGroup
	for range racers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			lease, err := registry.install("registration-a", 11)
			results <- sessionInstallResult{generation: 11, lease: lease, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	winners := 0
	var winner *sessionLease
	for result := range results {
		switch {
		case result.err == nil && result.lease != nil:
			winners++
			winner = result.lease
		case errors.Is(result.err, errSessionGenerationNotNewer) && result.lease == nil:
		default:
			t.Fatalf("install equal generation = (%v, %v), want winner or errSessionGenerationNotNewer", result.lease, result.err)
		}
	}
	if winners != 1 {
		t.Fatalf("equal-generation winners = %d, want 1", winners)
	}
	if got, ok := registry.lookup("registration-a", 11); !ok || got != winner {
		t.Fatalf("lookup(equal-generation winner) = (%p, %v), want (%p, true)", got, ok, winner)
	}
}

func TestSessionRegistryConcurrentLookupAndOldCleanupPreserveCurrent(t *testing.T) {
	const operations = 256
	registry := mustSessionRegistry(t, 1)
	old := mustInstallSession(t, registry, "registration-a", 1)
	current := mustInstallSession(t, registry, "registration-a", 2)
	start := make(chan struct{})
	errorsFound := make(chan error, operations*2)
	var wait sync.WaitGroup
	for range operations {
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			if registry.remove("registration-a", 1, old) {
				errorsFound <- errors.New("old cleanup removed the current route")
			}
		}()
		go func() {
			defer wait.Done()
			<-start
			lease, ok := registry.lookup("registration-a", 2)
			if !ok || lease != current {
				errorsFound <- fmt.Errorf("current lookup = (%p, %v), want (%p, true)", lease, ok, current)
			}
		}()
	}
	close(start)
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	if got, ok := registry.lookup("registration-a", 2); !ok || got != current {
		t.Fatalf("lookup after concurrent old cleanup = (%p, %v), want (%p, true)", got, ok, current)
	}

	var removals atomic.Int64
	start = make(chan struct{})
	for range operations {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			if registry.remove("registration-a", 2, current) {
				removals.Add(1)
			}
		}()
	}
	close(start)
	wait.Wait()
	if got := removals.Load(); got != 1 {
		t.Fatalf("successful concurrent removals = %d, want 1", got)
	}
	requireSessionDone(t, current, errSessionRemoved)
	if got, ok := registry.lookup("registration-a", 2); ok || got != nil {
		t.Fatalf("lookup after concurrent removal = (%v, %v), want (nil, false)", got, ok)
	}
}

type sessionInstallResult struct {
	generation uint64
	lease      *sessionLease
	err        error
}

func mustSessionRegistry(t *testing.T, maxTrackedRegistrations uint32) *sessionRegistry {
	t.Helper()
	registry, err := newSessionRegistry(maxTrackedRegistrations)
	if err != nil {
		t.Fatalf("newSessionRegistry(%d) error = %v", maxTrackedRegistrations, err)
	}
	return registry
}

func mustInstallSession(t *testing.T, registry *sessionRegistry, registrationUID string, generation uint64) *sessionLease {
	t.Helper()
	lease, err := registry.install(registrationUID, generation)
	if err != nil {
		t.Fatalf("install(%q, %d) error = %v", registrationUID, generation, err)
	}
	return lease
}

func requireSessionLive(t *testing.T, lease *sessionLease) {
	t.Helper()
	select {
	case <-lease.done():
		t.Fatalf("session is done with cause %v, want live", lease.cancellationCause())
	default:
	}
	if cause := lease.cancellationCause(); cause != nil {
		t.Fatalf("live session cancellation cause = %v, want nil", cause)
	}
}

func requireSessionDone(t *testing.T, lease *sessionLease, want error) {
	t.Helper()
	select {
	case <-lease.done():
	default:
		t.Fatal("session is live, want done")
	}
	if got := lease.cancellationCause(); !errors.Is(got, want) {
		t.Fatalf("session cancellation cause = %v, want %v", got, want)
	}
}
