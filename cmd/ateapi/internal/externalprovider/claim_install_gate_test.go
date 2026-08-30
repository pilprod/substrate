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
)

func TestClaimInstallGateValidatesBounds(t *testing.T) {
	for _, limits := range []ClaimInstallGateLimits{
		{},
		{MaxInFlight: 1},
		{MaxInFlight: 1, MaxDistinctKeys: 2},
		{MaxInFlight: maximumClaimInstallGateLimit + 1, MaxDistinctKeys: 1},
	} {
		if gate, err := newClaimInstallGate(limits.MaxInFlight, limits.MaxDistinctKeys); !errors.Is(err, errInvalidClaimInstallGate) || gate != nil {
			t.Errorf("newClaimInstallGate(%+v) = (%v, %v), want nil/invalid", limits, gate, err)
		}
	}
	if gate, err := newClaimInstallGate(2, 1); err != nil || gate == nil {
		t.Fatalf("newClaimInstallGate(valid) = (%v, %v), want gate/nil", gate, err)
	}
}

func TestClaimInstallGateBoundsDistinctKeysAndContextWaiters(t *testing.T) {
	gate, err := newClaimInstallGate(2, 1)
	if err != nil {
		t.Fatalf("newClaimInstallGate() error = %v", err)
	}
	first, err := gate.acquire(context.Background(), "registration-a")
	if err != nil {
		t.Fatalf("first acquire error = %v", err)
	}
	if lease, err := gate.acquire(context.Background(), "registration-b"); !errors.Is(err, errClaimInstallGateFull) || lease != nil {
		t.Fatalf("unknown-key acquire at capacity = (%v, %v), want nil/full", lease, err)
	}
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{InFlight: 1, DistinctKeys: 1})

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterResult := make(chan error, 1)
	go func() {
		lease, err := gate.acquire(waiterCtx, "registration-a")
		if lease != nil {
			lease.release()
		}
		waiterResult <- err
	}()
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{InFlight: 2, DistinctKeys: 1})

	globalCtx, cancelGlobal := context.WithCancel(context.Background())
	globalResult := make(chan error, 1)
	go func() {
		lease, err := gate.acquire(globalCtx, "registration-a")
		if lease != nil {
			lease.release()
		}
		globalResult <- err
	}()
	select {
	case err := <-globalResult:
		t.Fatalf("global-bound acquire returned before cancellation: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	cancelGlobal()
	if err := <-globalResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("global-bound cancellation error = %v, want context.Canceled", err)
	}

	cancelWaiter()
	if err := <-waiterResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("same-key waiter cancellation error = %v, want context.Canceled", err)
	}
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{InFlight: 1, DistinctKeys: 1})
	first.release()
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{})

	replacement, err := gate.acquire(context.Background(), "registration-b")
	if err != nil {
		t.Fatalf("acquire after release error = %v", err)
	}
	replacement.release()
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{})
}

func TestClaimInstallGateAllowsDifferentKeysAndOneShotClaim(t *testing.T) {
	gate, err := newClaimInstallGate(2, 2)
	if err != nil {
		t.Fatalf("newClaimInstallGate() error = %v", err)
	}
	first, err := gate.acquire(context.Background(), "registration-a")
	if err != nil {
		t.Fatalf("first acquire error = %v", err)
	}
	second, err := gate.acquire(context.Background(), "registration-b")
	if err != nil {
		t.Fatalf("different-key acquire error = %v", err)
	}
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{InFlight: 2, DistinctKeys: 2})
	second.release()

	claim := validSessionClaim(1)
	claim.Registration.UID = "registration-a"
	var claimCalls atomic.Int64
	store := &fakeStore{claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) {
		claimCalls.Add(1)
		return claim, nil
	}}
	gated, err := first.claimSession(context.Background(), store, CredentialDigest{1})
	if err != nil {
		t.Fatalf("claimSession() error = %v", err)
	}
	if duplicate, err := first.claimSession(context.Background(), store, CredentialDigest{2}); !errors.Is(err, errInvalidGatedClaim) || duplicate != nil {
		t.Fatalf("duplicate claimSession() = (%v, %v), want nil/invalid", duplicate, err)
	}
	if got := claimCalls.Load(); got != 1 {
		t.Fatalf("store claim calls = %d, want 1", got)
	}
	got, lease, err := gated.beginInstall()
	if err != nil || got != claim || lease != first {
		t.Fatalf("beginInstall() = (%+v, %v, %v), want claim/lease/nil", got, lease, err)
	}
	if duplicate, duplicateLease, err := gated.beginInstall(); !errors.Is(err, errInvalidGatedClaim) || duplicate != (SessionClaim{}) || duplicateLease != nil {
		t.Fatalf("duplicate beginInstall() = (%+v, %v, %v), want zero/nil/invalid", duplicate, duplicateLease, err)
	}
	first.release()
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{})
}

func TestClaimInstallGateReleasesAdmissionAfterStorePanic(t *testing.T) {
	gate, err := newClaimInstallGate(1, 1)
	if err != nil {
		t.Fatalf("newClaimInstallGate() error = %v", err)
	}
	lease, err := gate.acquire(context.Background(), "registration-a")
	if err != nil {
		t.Fatalf("first acquire error = %v", err)
	}

	panicMarker := errors.New("injected durable claim panic")
	panicResult := make(chan any, 1)
	go func() {
		defer func() { panicResult <- recover() }()
		defer lease.release()
		store := &fakeStore{claim: func(context.Context, string, CredentialDigest) (SessionClaim, error) {
			panic(panicMarker)
		}}
		_, _ = lease.claimSession(context.Background(), store, CredentialDigest{1})
	}()

	select {
	case recovered := <-panicResult:
		if recovered != panicMarker {
			t.Fatalf("recovered panic = %v, want injected marker", recovered)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("store panic left claim lease locked during deferred release")
	}
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{})

	reacquireCtx, cancelReacquire := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReacquire()
	replacement, err := gate.acquire(reacquireCtx, "registration-b")
	if err != nil {
		t.Fatalf("acquire after recovered store panic error = %v", err)
	}
	replacement.release()
	requireClaimInstallGateStats(t, gate, claimInstallGateStats{})
}

func requireClaimInstallGateStats(t *testing.T, gate *claimInstallGate, want claimInstallGateStats) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := gate.stats(); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("claim-install gate stats = %+v, want %+v", gate.stats(), want)
}
