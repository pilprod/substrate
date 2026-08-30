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

package atepg

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
)

func externalProviderDigest(fill byte) externalprovider.CredentialDigest {
	var digest externalprovider.CredentialDigest
	for i := range digest {
		digest[i] = fill
	}
	return digest
}

func externalProviderScope() externalprovider.Scope {
	policy, err := externalprovider.NewSlotCapabilityPolicy(externalprovider.SlotCapabilityPolicyVersion, []externalprovider.SlotProfile{{
		ProfileID:    "standard",
		SandboxClass: "gvisor",
		Labels:       map[string]string{"security.example/tier": "managed"},
		MaxSlots:     4,
		CPUMilli:     4_000,
		MemoryBytes:  16 << 30,
	}})
	if err != nil {
		panic(err)
	}
	return externalprovider.Scope{
		OwnerAtespace:   "tenant-a",
		WorkerNamespace: "workers",
		WorkerPool:      "pool-a",
		MaxSlots:        4,
		SlotPolicy:      policy,
	}
}

func setupExternalProviderPersistence(t *testing.T) *Persistence {
	t.Helper()
	persistence := setupPostgresPersistence(t)
	createTestAtespace(t, persistence, externalProviderScope().OwnerAtespace)
	return persistence
}

func TestExternalProviderStoreLifecycle(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	ctx := context.Background()
	scope := externalProviderScope()
	enrollmentDigest := externalProviderDigest(1)
	refreshDigest := externalProviderDigest(2)
	sessionOne := externalProviderDigest(3)
	sessionTwo := externalProviderDigest(4)

	enrollment, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-a", enrollmentDigest, scope, time.Hour)
	if err != nil {
		t.Fatalf("CreateExternalProviderEnrollment() error = %v", err)
	}
	if enrollment.UID != "enrollment-a" || enrollment.Scope != scope {
		t.Errorf("enrollment = %+v, want UID/scope preserved", enrollment)
	}
	if !enrollment.ExpiresAt.After(time.Now()) {
		t.Errorf("enrollment expiry = %v, want future DB-clock expiry", enrollment.ExpiresAt)
	}
	var storedPolicy, storedPolicyDigest []byte
	if err := persistence.pool.QueryRow(ctx, `
		SELECT slot_policy_canonical, slot_policy_digest
		FROM external_provider_enrollments
		WHERE enrollment_uid = $1`, enrollment.UID).Scan(&storedPolicy, &storedPolicyDigest); err != nil {
		t.Fatalf("reading enrollment slot policy: %v", err)
	}
	if !bytes.Equal(storedPolicy, scope.SlotPolicy.CanonicalBytes()) || !bytes.Equal(storedPolicyDigest, scope.SlotPolicy.DigestBytes()) {
		t.Fatalf("stored slot policy/digest = %s/%x, want %s/%x", storedPolicy, storedPolicyDigest, scope.SlotPolicy.CanonicalBytes(), scope.SlotPolicy.DigestBytes())
	}

	registration, err := persistence.ConsumeExternalProviderEnrollment(ctx, enrollmentDigest, "registration-a", refreshDigest)
	if err != nil {
		t.Fatalf("ConsumeExternalProviderEnrollment() error = %v", err)
	}
	if registration.UID != "registration-a" || registration.EnrollmentUID != enrollment.UID || registration.Scope != scope {
		t.Errorf("registration = %+v, want linked immutable scope", registration)
	}
	if _, err := persistence.ConsumeExternalProviderEnrollment(ctx, enrollmentDigest, "registration-b", externalProviderDigest(9)); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("second ConsumeExternalProviderEnrollment() error = %v, want ErrAuthenticationFailed", err)
	}

	first, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, sessionOne, time.Minute)
	if err != nil {
		t.Fatalf("first RotateExternalProviderSession() error = %v", err)
	}
	if first.Registration != registration || !first.ExpiresAt.After(time.Now()) {
		t.Errorf("first session authorization = %+v, want registration/future expiry", first)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, sessionOne, time.Minute); !errors.Is(err, externalprovider.ErrCredentialCollision) {
		t.Errorf("reusing current session digest error = %v, want ErrCredentialCollision", err)
	}
	second, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, sessionTwo, time.Minute)
	if err != nil {
		t.Fatalf("second RotateExternalProviderSession() error = %v", err)
	}
	if second.Registration.Scope != scope {
		t.Errorf("registration scope changed during rotation: %+v", second.Registration.Scope)
	}

	var currentDigest []byte
	var consumedAt *time.Time
	var generation int64
	if err := persistence.pool.QueryRow(ctx, `
		SELECT current_session_digest, session_consumed_at, session_generation
		FROM external_provider_registrations
		WHERE registration_uid = $1`, registration.UID).Scan(&currentDigest, &consumedAt, &generation); err != nil {
		t.Fatalf("reading current session state: %v", err)
	}
	if !bytes.Equal(currentDigest, sessionTwo[:]) {
		t.Errorf("current session digest = %x, want rotated digest %x", currentDigest, sessionTwo)
	}
	if consumedAt != nil || generation != 0 {
		t.Errorf("unclaimed session consumed_at/generation = %v/%d, want nil/0", consumedAt, generation)
	}

	if err := persistence.RevokeExternalProviderRegistration(ctx, registration.UID); err != nil {
		t.Fatalf("RevokeExternalProviderRegistration() error = %v", err)
	}
	if err := persistence.RevokeExternalProviderRegistration(ctx, registration.UID); err != nil {
		t.Fatalf("idempotent RevokeExternalProviderRegistration() error = %v", err)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, externalProviderDigest(5), time.Minute); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("RotateExternalProviderSession() after revoke error = %v, want ErrAuthenticationFailed", err)
	}
	if _, err := persistence.ClaimExternalProviderSession(ctx, registration.UID, sessionTwo); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("ClaimExternalProviderSession() after registration revoke error = %v, want ErrAuthenticationFailed", err)
	}
	var revokedSessionExpiry, revokedSessionConsumed *time.Time
	var revokedDigest []byte
	if err := persistence.pool.QueryRow(ctx, `
		SELECT current_session_digest, current_session_expires_at, session_consumed_at
		FROM external_provider_registrations
		WHERE registration_uid = $1`, registration.UID).Scan(&revokedDigest, &revokedSessionExpiry, &revokedSessionConsumed); err != nil {
		t.Fatalf("reading directly revoked session state: %v", err)
	}
	if revokedDigest != nil || revokedSessionExpiry != nil || revokedSessionConsumed != nil {
		t.Errorf("direct revoke retained session state: digest=%x expiry=%v consumed=%v", revokedDigest, revokedSessionExpiry, revokedSessionConsumed)
	}
}

func TestExternalProviderStoreRejectsCorruptPersistedSlotPolicy(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	ctx := context.Background()
	enrollmentDigest := externalProviderDigest(101)
	refreshDigest := externalProviderDigest(102)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-policy-corrupt", enrollmentDigest, externalProviderScope(), time.Hour); err != nil {
		t.Fatalf("creating enrollment: %v", err)
	}
	registration, err := persistence.ConsumeExternalProviderEnrollment(ctx, enrollmentDigest, "registration-policy-corrupt", refreshDigest)
	if err != nil {
		t.Fatalf("consuming enrollment: %v", err)
	}
	if _, err := persistence.pool.Exec(ctx, `
		UPDATE external_provider_registrations
		SET slot_policy_digest = $2
		WHERE registration_uid = $1`, registration.UID, make([]byte, 32)); err != nil {
		t.Fatalf("corrupting registration policy digest: %v", err)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, externalProviderDigest(103), time.Minute); !errors.Is(err, externalprovider.ErrInvalidSlotCapabilityPolicy) {
		t.Fatalf("RotateExternalProviderSession() error = %v, want invalid slot policy", err)
	}
}

func TestExternalProviderEnrollmentExpiryUsesDatabaseClock(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	ctx := context.Background()
	digest := externalProviderDigest(8)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-expiring", digest, externalProviderScope(), 5*time.Millisecond); err != nil {
		t.Fatalf("creating expiring enrollment: %v", err)
	}
	if _, err := persistence.pool.Exec(ctx, `SELECT pg_sleep(0.02)`); err != nil {
		t.Fatalf("advancing database clock: %v", err)
	}
	if _, err := persistence.ConsumeExternalProviderEnrollment(ctx, digest, "registration-expired", externalProviderDigest(9)); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("consuming expired enrollment error = %v, want ErrAuthenticationFailed", err)
	}
}

func TestExternalProviderEnrollmentRevokeBlocksRedeemAndMint(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	ctx := context.Background()
	scope := externalProviderScope()

	unconsumed := externalProviderDigest(11)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-unconsumed", unconsumed, scope, time.Hour); err != nil {
		t.Fatalf("creating unconsumed enrollment: %v", err)
	}
	if err := persistence.RevokeExternalProviderEnrollment(ctx, "enrollment-unconsumed"); err != nil {
		t.Fatalf("revoking unconsumed enrollment: %v", err)
	}
	if _, err := persistence.ConsumeExternalProviderEnrollment(ctx, unconsumed, "registration-rejected", externalProviderDigest(12)); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("consuming revoked enrollment error = %v, want ErrAuthenticationFailed", err)
	}

	consumed := externalProviderDigest(13)
	refresh := externalProviderDigest(14)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-consumed", consumed, scope, time.Hour); err != nil {
		t.Fatalf("creating consumed enrollment: %v", err)
	}
	registration, err := persistence.ConsumeExternalProviderEnrollment(ctx, consumed, "registration-consumed", refresh)
	if err != nil {
		t.Fatalf("consuming enrollment: %v", err)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refresh, externalProviderDigest(15), time.Minute); err != nil {
		t.Fatalf("minting session before enrollment revoke: %v", err)
	}
	if err := persistence.RevokeExternalProviderEnrollment(ctx, "enrollment-consumed"); err != nil {
		t.Fatalf("revoking consumed enrollment: %v", err)
	}
	if err := persistence.RevokeExternalProviderEnrollment(ctx, "enrollment-consumed"); err != nil {
		t.Fatalf("idempotent enrollment revoke: %v", err)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refresh, externalProviderDigest(16), time.Minute); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("mint after enrollment revoke error = %v, want ErrAuthenticationFailed", err)
	}
	if _, err := persistence.ClaimExternalProviderSession(ctx, registration.UID, externalProviderDigest(15)); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("claim after enrollment revoke error = %v, want ErrAuthenticationFailed", err)
	}

	var enrollmentRevoked, registrationRevoked, sessionExpiry, sessionConsumed *time.Time
	var sessionDigest []byte
	if err := persistence.pool.QueryRow(ctx, `
		SELECT enrollment.revoked_at, registration.revoked_at,
		       registration.current_session_digest,
		       registration.current_session_expires_at,
		       registration.session_consumed_at
		FROM external_provider_enrollments AS enrollment
		JOIN external_provider_registrations AS registration USING (enrollment_uid)
		WHERE enrollment.enrollment_uid = $1`, "enrollment-consumed").Scan(&enrollmentRevoked, &registrationRevoked, &sessionDigest, &sessionExpiry, &sessionConsumed); err != nil {
		t.Fatalf("reading revocation timestamps: %v", err)
	}
	if enrollmentRevoked == nil || registrationRevoked == nil {
		t.Errorf("revocation timestamps = %v/%v, want both set", enrollmentRevoked, registrationRevoked)
	}
	if sessionDigest != nil || sessionExpiry != nil || sessionConsumed != nil {
		t.Errorf("enrollment revoke retained session state: digest=%x expiry=%v consumed=%v", sessionDigest, sessionExpiry, sessionConsumed)
	}
}

func TestExternalProviderEnrollmentSingleUseAcrossReplicas(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	replica, err := NewPersistence(context.Background(), persistence.pool)
	if err != nil {
		t.Fatalf("NewPersistence(replica) error = %v", err)
	}
	t.Cleanup(replica.Close)

	ctx := context.Background()
	digest := externalProviderDigest(21)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-race", digest, externalProviderScope(), time.Hour); err != nil {
		t.Fatalf("creating enrollment: %v", err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for index, candidate := range []*Persistence{persistence, replica} {
		index, candidate := index, candidate
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := candidate.ConsumeExternalProviderEnrollment(
				ctx,
				digest,
				[]string{"registration-race-a", "registration-race-b"}[index],
				externalProviderDigest(byte(22+index)),
			)
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var successes, rejected int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, externalprovider.ErrAuthenticationFailed):
			rejected++
		default:
			t.Fatalf("concurrent consume error = %v", err)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("concurrent consume outcomes = %d success/%d rejected, want 1/1", successes, rejected)
	}
}

func TestExternalProviderSessionRotationAcrossReplicasKeepsOneCurrent(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	replica, err := NewPersistence(context.Background(), persistence.pool)
	if err != nil {
		t.Fatalf("NewPersistence(replica) error = %v", err)
	}
	t.Cleanup(replica.Close)

	ctx := context.Background()
	enrollmentDigest := externalProviderDigest(31)
	refreshDigest := externalProviderDigest(32)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-session-race", enrollmentDigest, externalProviderScope(), time.Hour); err != nil {
		t.Fatalf("creating enrollment: %v", err)
	}
	registration, err := persistence.ConsumeExternalProviderEnrollment(ctx, enrollmentDigest, "registration-session-race", refreshDigest)
	if err != nil {
		t.Fatalf("consuming enrollment: %v", err)
	}

	sessions := []externalprovider.CredentialDigest{externalProviderDigest(33), externalProviderDigest(34)}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for index, candidate := range []*Persistence{persistence, replica} {
		index, candidate := index, candidate
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := candidate.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, sessions[index], time.Minute)
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent session rotation error = %v", err)
		}
	}

	var current []byte
	if err := persistence.pool.QueryRow(ctx, `
		SELECT current_session_digest
		FROM external_provider_registrations
		WHERE registration_uid = $1`, registration.UID).Scan(&current); err != nil {
		t.Fatalf("reading current session: %v", err)
	}
	if !bytes.Equal(current, sessions[0][:]) && !bytes.Equal(current, sessions[1][:]) {
		t.Fatalf("current session digest = %x, want one concurrently minted digest", current)
	}
}

func TestExternalProviderSessionClaimAdvancesFencingGeneration(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	ctx := context.Background()
	enrollmentDigest := externalProviderDigest(51)
	refreshDigest := externalProviderDigest(52)
	sessionOne := externalProviderDigest(53)
	sessionTwo := externalProviderDigest(54)

	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-claim", enrollmentDigest, externalProviderScope(), time.Hour); err != nil {
		t.Fatalf("creating enrollment: %v", err)
	}
	registration, err := persistence.ConsumeExternalProviderEnrollment(ctx, enrollmentDigest, "registration-claim", refreshDigest)
	if err != nil {
		t.Fatalf("consuming enrollment: %v", err)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, sessionOne, time.Minute); err != nil {
		t.Fatalf("minting first session: %v", err)
	}

	first, err := persistence.ClaimExternalProviderSession(ctx, registration.UID, sessionOne)
	if err != nil {
		t.Fatalf("claiming first session: %v", err)
	}
	if first.Registration != registration || first.Generation != 1 {
		t.Errorf("first claim = %+v, want registration and generation 1", first)
	}
	if _, err := persistence.ClaimExternalProviderSession(ctx, registration.UID, sessionOne); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("reusing first session error = %v, want ErrAuthenticationFailed", err)
	}

	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, sessionTwo, time.Minute); err != nil {
		t.Fatalf("minting second session: %v", err)
	}
	if _, err := persistence.ClaimExternalProviderSession(ctx, registration.UID, sessionOne); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("claiming rotated-out session error = %v, want ErrAuthenticationFailed", err)
	}
	second, err := persistence.ClaimExternalProviderSession(ctx, registration.UID, sessionTwo)
	if err != nil {
		t.Fatalf("claiming second session: %v", err)
	}
	if second.Registration != registration || second.Generation != 2 {
		t.Errorf("second claim = %+v, want registration and generation 2", second)
	}

	var consumedAt *time.Time
	var generation int64
	if err := persistence.pool.QueryRow(ctx, `
		SELECT session_consumed_at, session_generation
		FROM external_provider_registrations
		WHERE registration_uid = $1`, registration.UID).Scan(&consumedAt, &generation); err != nil {
		t.Fatalf("reading claimed session state: %v", err)
	}
	if consumedAt == nil || generation != 2 {
		t.Errorf("claimed session consumed_at/generation = %v/%d, want nonnil/2", consumedAt, generation)
	}
}

func TestExternalProviderSessionClaimSingleWinnerAcrossReplicas(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	replica, err := NewPersistence(context.Background(), persistence.pool)
	if err != nil {
		t.Fatalf("NewPersistence(replica) error = %v", err)
	}
	t.Cleanup(replica.Close)

	ctx := context.Background()
	enrollmentDigest := externalProviderDigest(61)
	refreshDigest := externalProviderDigest(62)
	sessionDigest := externalProviderDigest(63)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-claim-race", enrollmentDigest, externalProviderScope(), time.Hour); err != nil {
		t.Fatalf("creating enrollment: %v", err)
	}
	registration, err := persistence.ConsumeExternalProviderEnrollment(ctx, enrollmentDigest, "registration-claim-race", refreshDigest)
	if err != nil {
		t.Fatalf("consuming enrollment: %v", err)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, sessionDigest, time.Minute); err != nil {
		t.Fatalf("minting session: %v", err)
	}

	type result struct {
		claim externalprovider.SessionClaim
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for _, candidate := range []*Persistence{persistence, replica} {
		candidate := candidate
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			claim, err := candidate.ClaimExternalProviderSession(ctx, registration.UID, sessionDigest)
			results <- result{claim: claim, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var successes, rejected int
	for result := range results {
		switch {
		case result.err == nil:
			successes++
			if result.claim.Registration != registration || result.claim.Generation != 1 {
				t.Errorf("winning claim = %+v, want registration and generation 1", result.claim)
			}
		case errors.Is(result.err, externalprovider.ErrAuthenticationFailed):
			rejected++
		default:
			t.Fatalf("concurrent claim error = %v", result.err)
		}
	}
	if successes != 1 || rejected != 1 {
		t.Fatalf("concurrent claim outcomes = %d success/%d rejected, want 1/1", successes, rejected)
	}
}

func TestExternalProviderSessionRevocationRemainsAuthoritativeAcrossReplicas(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	replica, err := NewPersistence(context.Background(), persistence.pool)
	if err != nil {
		t.Fatalf("NewPersistence(replica) error = %v", err)
	}
	t.Cleanup(replica.Close)

	ctx := context.Background()
	enrollmentDigest := externalProviderDigest(81)
	refreshDigest := externalProviderDigest(82)
	sessionDigest := externalProviderDigest(83)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-claim-revoke-race", enrollmentDigest, externalProviderScope(), time.Hour); err != nil {
		t.Fatalf("creating enrollment: %v", err)
	}
	registration, err := persistence.ConsumeExternalProviderEnrollment(ctx, enrollmentDigest, "registration-claim-revoke-race", refreshDigest)
	if err != nil {
		t.Fatalf("consuming enrollment: %v", err)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, sessionDigest, time.Minute); err != nil {
		t.Fatalf("minting session: %v", err)
	}

	start := make(chan struct{})
	claimResult := make(chan error, 1)
	revokeResult := make(chan error, 1)
	go func() {
		<-start
		_, err := persistence.ClaimExternalProviderSession(ctx, registration.UID, sessionDigest)
		claimResult <- err
	}()
	go func() {
		<-start
		revokeResult <- replica.RevokeExternalProviderRegistration(ctx, registration.UID)
	}()
	close(start)

	if err := <-revokeResult; err != nil {
		t.Fatalf("concurrent revoke error = %v", err)
	}
	if err := <-claimResult; err != nil && !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Fatalf("concurrent claim error = %v, want success or ErrAuthenticationFailed", err)
	}
	if _, err := persistence.ClaimExternalProviderSession(ctx, registration.UID, sessionDigest); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Fatalf("claim after completed revoke error = %v, want ErrAuthenticationFailed", err)
	}

	var revokedAt *time.Time
	var currentDigest []byte
	if err := persistence.pool.QueryRow(ctx, `
		SELECT revoked_at, current_session_digest
		FROM external_provider_registrations
		WHERE registration_uid = $1`, registration.UID).Scan(&revokedAt, &currentDigest); err != nil {
		t.Fatalf("reading final revoked state: %v", err)
	}
	if revokedAt == nil || currentDigest != nil {
		t.Errorf("final revoked_at/current_session_digest = %v/%x, want nonnil/nil", revokedAt, currentDigest)
	}
}

func TestExternalProviderSessionClaimRejectsInvalidAndExpiredCredentials(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	ctx := context.Background()
	enrollmentDigest := externalProviderDigest(71)
	refreshDigest := externalProviderDigest(72)
	sessionDigest := externalProviderDigest(73)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "enrollment-expired-session", enrollmentDigest, externalProviderScope(), time.Hour); err != nil {
		t.Fatalf("creating enrollment: %v", err)
	}
	registration, err := persistence.ConsumeExternalProviderEnrollment(ctx, enrollmentDigest, "registration-expired-session", refreshDigest)
	if err != nil {
		t.Fatalf("consuming enrollment: %v", err)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, refreshDigest, sessionDigest, 5*time.Millisecond); err != nil {
		t.Fatalf("minting expiring session: %v", err)
	}

	tests := []struct {
		name   string
		uid    string
		digest externalprovider.CredentialDigest
	}{
		{name: "missing registration", uid: "registration-missing", digest: sessionDigest},
		{name: "invalid registration", uid: "/invalid", digest: sessionDigest},
		{name: "zero digest", uid: registration.UID},
		{name: "wrong digest", uid: registration.UID, digest: externalProviderDigest(74)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := persistence.ClaimExternalProviderSession(ctx, test.uid, test.digest); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
				t.Errorf("ClaimExternalProviderSession() error = %v, want ErrAuthenticationFailed", err)
			}
		})
	}

	if _, err := persistence.pool.Exec(ctx, `SELECT pg_sleep(0.02)`); err != nil {
		t.Fatalf("advancing database clock: %v", err)
	}
	if _, err := persistence.ClaimExternalProviderSession(ctx, registration.UID, sessionDigest); !errors.Is(err, externalprovider.ErrAuthenticationFailed) {
		t.Errorf("claiming expired session error = %v, want ErrAuthenticationFailed", err)
	}
}

func TestExternalProviderSchemaKeepsAuditAndNoPlaintextCredential(t *testing.T) {
	persistence := setupExternalProviderPersistence(t)
	ctx := context.Background()
	issuer := externalprovider.NewIssuer(persistence)
	issued, err := issuer.IssueEnrollment(ctx, externalProviderScope(), time.Hour)
	if err != nil {
		t.Fatalf("IssueEnrollment() error = %v", err)
	}
	defer issued.Credential.Destroy()

	var storedDigest []byte
	if err := persistence.pool.QueryRow(ctx, `
		SELECT credential_digest
		FROM external_provider_enrollments
		WHERE enrollment_uid = $1`, issued.UID).Scan(&storedDigest); err != nil {
		t.Fatalf("reading stored enrollment digest: %v", err)
	}
	if bytes.Equal(storedDigest, issued.Credential.Bytes()) || bytes.Contains(storedDigest, issued.Credential.Bytes()) {
		t.Fatal("database stored plaintext enrollment credential")
	}

	if err := issuer.RevokeEnrollment(ctx, issued.UID); err != nil {
		t.Fatalf("RevokeEnrollment() error = %v", err)
	}
	_, err = persistence.DeleteAtespace(ctx, externalProviderScope().OwnerAtespace)
	if !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("DeleteAtespace() with retained enrollment error = %v, want ErrFailedPrecondition", err)
	}
}

func TestExternalProviderStoreEnforcesScopeFKAndTTLs(t *testing.T) {
	persistence := setupPostgresPersistence(t)
	ctx := context.Background()
	scope := externalProviderScope()
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "missing-owner", externalProviderDigest(41), scope, time.Hour); err == nil {
		t.Fatal("CreateExternalProviderEnrollment() accepted missing owner atespace")
	}
	createTestAtespace(t, persistence, scope.OwnerAtespace)
	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "long-enrollment", externalProviderDigest(42), scope, externalprovider.MaxEnrollmentTTL+time.Microsecond); err == nil {
		t.Fatal("CreateExternalProviderEnrollment() accepted excessive TTL")
	}

	if _, err := persistence.CreateExternalProviderEnrollment(ctx, "valid-enrollment", externalProviderDigest(43), scope, time.Hour); err != nil {
		t.Fatalf("creating valid enrollment: %v", err)
	}
	registration, err := persistence.ConsumeExternalProviderEnrollment(ctx, externalProviderDigest(43), "valid-registration", externalProviderDigest(44))
	if err != nil {
		t.Fatalf("consuming valid enrollment: %v", err)
	}
	if _, err := persistence.RotateExternalProviderSession(ctx, registration.UID, externalProviderDigest(44), externalProviderDigest(45), externalprovider.MaxSessionTTL+time.Microsecond); err == nil {
		t.Fatal("RotateExternalProviderSession() accepted excessive TTL")
	}
	if err := persistence.RevokeExternalProviderEnrollment(ctx, "missing-enrollment"); !errors.Is(err, externalprovider.ErrNotFound) {
		t.Errorf("RevokeExternalProviderEnrollment(missing) error = %v, want ErrNotFound", err)
	}
	if err := persistence.RevokeExternalProviderRegistration(ctx, "missing-registration"); !errors.Is(err, externalprovider.ErrNotFound) {
		t.Errorf("RevokeExternalProviderRegistration(missing) error = %v, want ErrNotFound", err)
	}
}
