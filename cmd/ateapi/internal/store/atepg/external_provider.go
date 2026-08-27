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
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/jackc/pgx/v5"
)

// CreateExternalProviderEnrollment persists only the enrollment digest and
// uses the database clock to establish its validity window.
func (p *Persistence) CreateExternalProviderEnrollment(ctx context.Context, enrollmentUID string, digest externalprovider.CredentialDigest, scope externalprovider.Scope, ttl time.Duration) (externalprovider.Enrollment, error) {
	if !externalprovider.IsValidIdentity(enrollmentUID) || digest == (externalprovider.CredentialDigest{}) {
		return externalprovider.Enrollment{}, errors.New("external provider enrollment identity or digest is empty")
	}
	if err := scope.Validate(); err != nil {
		return externalprovider.Enrollment{}, fmt.Errorf("invalid external provider scope: %w", err)
	}
	ttlMicros := ttl.Microseconds()
	if ttlMicros <= 0 || ttl > externalprovider.MaxEnrollmentTTL {
		return externalprovider.Enrollment{}, errors.New("external provider enrollment TTL is outside the allowed range")
	}

	enrollment := externalprovider.Enrollment{UID: enrollmentUID, Scope: scope}
	err := p.pool.QueryRow(ctx, `
		INSERT INTO external_provider_enrollments (
			enrollment_uid, credential_digest, owner_atespace, worker_namespace, worker_pool,
			max_slots, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp() + $7::bigint * interval '1 microsecond')
		RETURNING expires_at`,
		enrollmentUID, digest[:], scope.OwnerAtespace, scope.WorkerNamespace, scope.WorkerPool,
		scope.MaxSlots, ttlMicros,
	).Scan(&enrollment.ExpiresAt)
	if isUniqueViolation(err) {
		return externalprovider.Enrollment{}, externalprovider.ErrCredentialCollision
	}
	if err != nil {
		return externalprovider.Enrollment{}, fmt.Errorf("inserting external provider enrollment: %w", err)
	}
	return enrollment, nil
}

// ConsumeExternalProviderEnrollment atomically claims a single-use enrollment
// and creates the immutable registration scope. A row lock serializes callers
// across ateapi replicas.
func (p *Persistence) ConsumeExternalProviderEnrollment(ctx context.Context, enrollmentDigest externalprovider.CredentialDigest, registrationUID string, refreshDigest externalprovider.CredentialDigest) (externalprovider.Registration, error) {
	if enrollmentDigest == (externalprovider.CredentialDigest{}) || refreshDigest == (externalprovider.CredentialDigest{}) || !externalprovider.IsValidIdentity(registrationUID) {
		return externalprovider.Registration{}, externalprovider.ErrAuthenticationFailed
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return externalprovider.Registration{}, fmt.Errorf("beginning external provider enrollment: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var registration externalprovider.Registration
	var usable bool
	err = tx.QueryRow(ctx, `
		SELECT enrollment_uid, owner_atespace, worker_namespace, worker_pool, max_slots,
		       consumed_at IS NULL AND revoked_at IS NULL AND expires_at > clock_timestamp()
		FROM external_provider_enrollments
		WHERE credential_digest = $1
		FOR UPDATE`, enrollmentDigest[:],
	).Scan(
		&registration.EnrollmentUID,
		&registration.Scope.OwnerAtespace,
		&registration.Scope.WorkerNamespace,
		&registration.Scope.WorkerPool,
		&registration.Scope.MaxSlots,
		&usable,
	)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !usable) {
		return externalprovider.Registration{}, externalprovider.ErrAuthenticationFailed
	}
	if err != nil {
		return externalprovider.Registration{}, fmt.Errorf("locking external provider enrollment: %w", err)
	}

	registration.UID = registrationUID
	err = tx.QueryRow(ctx, `
		INSERT INTO external_provider_registrations (
			registration_uid, enrollment_uid, owner_atespace, worker_namespace, worker_pool,
			max_slots, refresh_digest
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at`,
		registration.UID,
		registration.EnrollmentUID,
		registration.Scope.OwnerAtespace,
		registration.Scope.WorkerNamespace,
		registration.Scope.WorkerPool,
		registration.Scope.MaxSlots,
		refreshDigest[:],
	).Scan(&registration.CreatedAt)
	if isUniqueViolation(err) {
		return externalprovider.Registration{}, externalprovider.ErrCredentialCollision
	}
	if err != nil {
		return externalprovider.Registration{}, fmt.Errorf("inserting external provider registration: %w", err)
	}

	commandTag, err := tx.Exec(ctx, `
		UPDATE external_provider_enrollments
		SET consumed_at = clock_timestamp(), registration_uid = $2
		WHERE credential_digest = $1
		  AND consumed_at IS NULL
		  AND revoked_at IS NULL
		  AND expires_at > clock_timestamp()`,
		enrollmentDigest[:], registration.UID,
	)
	if err != nil {
		return externalprovider.Registration{}, fmt.Errorf("consuming external provider enrollment: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return externalprovider.Registration{}, externalprovider.ErrAuthenticationFailed
	}
	if err := tx.Commit(ctx); err != nil {
		return externalprovider.Registration{}, fmt.Errorf("committing external provider enrollment: %w", err)
	}
	return registration, nil
}

// RotateExternalProviderSession authenticates the refresh digest and replaces
// the registration's only current Connect credential under a row lock.
func (p *Persistence) RotateExternalProviderSession(ctx context.Context, registrationUID string, refreshDigest, sessionDigest externalprovider.CredentialDigest, ttl time.Duration) (externalprovider.SessionAuthorization, error) {
	if registrationUID == "" || refreshDigest == (externalprovider.CredentialDigest{}) || sessionDigest == (externalprovider.CredentialDigest{}) {
		return externalprovider.SessionAuthorization{}, externalprovider.ErrAuthenticationFailed
	}
	ttlMicros := ttl.Microseconds()
	if ttlMicros <= 0 || ttl > externalprovider.MaxSessionTTL {
		return externalprovider.SessionAuthorization{}, errors.New("external provider session TTL is outside the allowed range")
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return externalprovider.SessionAuthorization{}, fmt.Errorf("beginning external provider session rotation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var authorization externalprovider.SessionAuthorization
	var currentSessionDigest []byte
	err = tx.QueryRow(ctx, `
		SELECT registration.registration_uid, registration.enrollment_uid,
		       registration.owner_atespace, registration.worker_namespace,
		       registration.worker_pool, registration.max_slots,
		       registration.created_at, registration.current_session_digest
		FROM external_provider_registrations AS registration
		JOIN external_provider_enrollments AS enrollment
		  ON enrollment.enrollment_uid = registration.enrollment_uid
		WHERE registration.registration_uid = $1
		  AND registration.refresh_digest = $2
		  AND registration.revoked_at IS NULL
		  AND enrollment.revoked_at IS NULL
		FOR UPDATE OF registration`, registrationUID, refreshDigest[:],
	).Scan(
		&authorization.Registration.UID,
		&authorization.Registration.EnrollmentUID,
		&authorization.Registration.Scope.OwnerAtespace,
		&authorization.Registration.Scope.WorkerNamespace,
		&authorization.Registration.Scope.WorkerPool,
		&authorization.Registration.Scope.MaxSlots,
		&authorization.Registration.CreatedAt,
		&currentSessionDigest,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return externalprovider.SessionAuthorization{}, externalprovider.ErrAuthenticationFailed
	}
	if err != nil {
		return externalprovider.SessionAuthorization{}, fmt.Errorf("authenticating external provider refresh credential: %w", err)
	}
	if bytes.Equal(currentSessionDigest, sessionDigest[:]) {
		return externalprovider.SessionAuthorization{}, externalprovider.ErrCredentialCollision
	}

	err = tx.QueryRow(ctx, `
		UPDATE external_provider_registrations
		SET current_session_digest = $3,
		    current_session_expires_at = clock_timestamp() + $4::bigint * interval '1 microsecond',
		    session_consumed_at = NULL,
		    updated_at = clock_timestamp()
		WHERE registration_uid = $1 AND refresh_digest = $2
		RETURNING current_session_expires_at`,
		registrationUID, refreshDigest[:], sessionDigest[:], ttlMicros,
	).Scan(&authorization.ExpiresAt)
	if isUniqueViolation(err) {
		return externalprovider.SessionAuthorization{}, externalprovider.ErrCredentialCollision
	}
	if err != nil {
		return externalprovider.SessionAuthorization{}, fmt.Errorf("rotating external provider session: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return externalprovider.SessionAuthorization{}, fmt.Errorf("committing external provider session rotation: %w", err)
	}
	return authorization, nil
}

// RevokeExternalProviderEnrollment uses the database clock and atomically
// invalidates both the enrollment and any registration created from it.
func (p *Persistence) RevokeExternalProviderEnrollment(ctx context.Context, enrollmentUID string) error {
	if enrollmentUID == "" {
		return externalprovider.ErrNotFound
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning external provider enrollment revocation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	var found bool
	err = tx.QueryRow(ctx, `
		UPDATE external_provider_enrollments
		SET revoked_at = COALESCE(revoked_at, clock_timestamp())
		WHERE enrollment_uid = $1
		RETURNING true`, enrollmentUID).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return externalprovider.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("revoking external provider enrollment: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE external_provider_registrations
		SET revoked_at = COALESCE(revoked_at, clock_timestamp()),
		    current_session_digest = NULL,
		    current_session_expires_at = NULL,
		    session_consumed_at = NULL,
		    updated_at = clock_timestamp()
		WHERE enrollment_uid = $1`, enrollmentUID); err != nil {
		return fmt.Errorf("revoking registration for external provider enrollment: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing external provider enrollment revocation: %w", err)
	}
	return nil
}

// RevokeExternalProviderRegistration invalidates its refresh and current
// not-yet-claimed session credentials using the database clock.
func (p *Persistence) RevokeExternalProviderRegistration(ctx context.Context, registrationUID string) error {
	if registrationUID == "" {
		return externalprovider.ErrNotFound
	}
	var found bool
	err := p.pool.QueryRow(ctx, `
		UPDATE external_provider_registrations
		SET revoked_at = COALESCE(revoked_at, clock_timestamp()),
		    current_session_digest = NULL,
		    current_session_expires_at = NULL,
		    session_consumed_at = NULL,
		    updated_at = clock_timestamp()
		WHERE registration_uid = $1
		RETURNING true`, registrationUID).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return externalprovider.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("revoking external provider registration: %w", err)
	}
	return nil
}
