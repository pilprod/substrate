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
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schema is atepg's idempotent embedded schema.
//
// Resource fields are projected into columns only when PostgreSQL needs them
// for identity, relationships, queries, ordering, or atomic concurrency
// checks. All other resource state remains authoritative in the opaque proto.
const schema = `
CREATE TABLE IF NOT EXISTS atespaces (
    name   text PRIMARY KEY,
    uid    text NOT NULL,
    version bigint NOT NULL,
    proto  bytea NOT NULL
);

CREATE TABLE IF NOT EXISTS actors (
    atespace  text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_templates (
    atespace  text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_snapshots (
    atespace  text NOT NULL,
    name      text NOT NULL,
    uid       text NOT NULL,
    version   bigint NOT NULL,
    proto     bytea NOT NULL,
    PRIMARY KEY (atespace, name)
);

CREATE TABLE IF NOT EXISTS actor_snapshot_tags (
    atespace           text NOT NULL,
    name               text NOT NULL,
    snapshot_atespace  text NOT NULL,
    snapshot_name      text NOT NULL,
    uid                text NOT NULL,
    version            bigint NOT NULL,
    proto              bytea NOT NULL,
    PRIMARY KEY (atespace, name),
    CONSTRAINT actor_snapshot_tags_atespace_fk
        FOREIGN KEY (atespace) REFERENCES atespaces(name) ON DELETE RESTRICT,
    CONSTRAINT actor_snapshot_tags_snapshot_fk
        FOREIGN KEY (snapshot_atespace, snapshot_name)
        REFERENCES actor_snapshots(atespace, name) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS actor_snapshot_tags_snapshot_idx
    ON actor_snapshot_tags (snapshot_atespace, snapshot_name);

-- Workers are global-scoped and their opaque resource name alone is the
-- primary key. Provider-specific identity remains authoritative in the proto.
CREATE TABLE IF NOT EXISTS workers (
    name     text PRIMARY KEY,
    uid      text NOT NULL UNIQUE,
    version  bigint NOT NULL,
    proto    bytea NOT NULL
);

-- Transactional outbox backing WatchWorkers.
--
-- 1. Ordering (xid): writeAndAppendEvent guarantees exactly one row per tx,
--    ensuring distinct xids so polling batches never split a transaction.
-- 2. Retention (created_at partitions): outboxMaintenance drops expired
--    partitions to avoid VACUUM I/O debt. A DEFAULT partition catches overflow.
-- 3. Durability (UNLOGGED): Skips WAL overhead. Crash recoveries trigger
--    watchers to rebuild from the primary workers table. worker_outbox_trim
--    remains LOGGED to preserve the high-water mark across restarts.
CREATE TABLE IF NOT EXISTS worker_outbox (
    xid         xid8 NOT NULL DEFAULT pg_current_xact_id(),
    -- MUST use clock_timestamp() instead of now(). now() freezes at tx start,
    -- causing slow transactions to route into expired partitions.
    created_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload     bytea NOT NULL
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS worker_outbox_xid ON worker_outbox (xid);

CREATE UNLOGGED TABLE IF NOT EXISTS worker_outbox_default PARTITION OF worker_outbox DEFAULT WITH (autovacuum_enabled = off);

-- Single-row high-water mark of retention: the greatest xid ever discarded
-- from worker_outbox (dropped with an expired partition, or truncated
-- with the DEFAULT partition). Watchers compare it against their cursor to
-- detect exactly that unconsumed rows were discarded out from under them.
CREATE TABLE IF NOT EXISTS worker_outbox_trim (
    id   boolean PRIMARY KEY DEFAULT true CHECK (id),
    xid  xid8 NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    key         text PRIMARY KEY,
    token       text NOT NULL,
    expires_at  timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS leases_expires_at_idx ON leases (expires_at);

-- Enrollment and broker credentials are persisted only as domain-separated
-- SHA-256 digests. Scope and canonical slot-policy columns are copied once
-- into the registration and no adapter operation updates them afterwards.
CREATE TABLE IF NOT EXISTS external_provider_enrollments (
    enrollment_uid     text PRIMARY KEY
        CHECK (octet_length(enrollment_uid) BETWEEN 1 AND 253)
        CHECK (enrollment_uid ~ '^[A-Za-z0-9]$' OR enrollment_uid ~ '^[A-Za-z0-9][A-Za-z0-9._~-]*[A-Za-z0-9]$'),
    credential_digest bytea NOT NULL UNIQUE
        CHECK (octet_length(credential_digest) = 32),
    owner_atespace     text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    worker_namespace   text NOT NULL CHECK (worker_namespace <> ''),
    worker_pool        text NOT NULL CHECK (worker_pool <> ''),
    max_slots          integer NOT NULL CHECK (max_slots BETWEEN 1 AND 256),
    slot_policy_canonical bytea NOT NULL,
    slot_policy_digest bytea NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at         timestamptz NOT NULL,
    consumed_at        timestamptz,
    revoked_at         timestamptz,
    registration_uid   text UNIQUE,
    CHECK ((consumed_at IS NULL) = (registration_uid IS NULL)),
    CONSTRAINT external_provider_enrollments_slot_policy_check CHECK (
        octet_length(slot_policy_canonical) BETWEEN 1 AND 2097152
        AND octet_length(slot_policy_digest) = 32
    )
);

CREATE INDEX IF NOT EXISTS external_provider_enrollments_expires_at_idx
    ON external_provider_enrollments (expires_at);

CREATE TABLE IF NOT EXISTS external_provider_registrations (
    registration_uid          text PRIMARY KEY
        CHECK (octet_length(registration_uid) BETWEEN 1 AND 253)
        CHECK (registration_uid ~ '^[A-Za-z0-9]$' OR registration_uid ~ '^[A-Za-z0-9][A-Za-z0-9._~-]*[A-Za-z0-9]$'),
    enrollment_uid            text NOT NULL UNIQUE
        REFERENCES external_provider_enrollments(enrollment_uid) ON DELETE RESTRICT,
    owner_atespace            text NOT NULL
        REFERENCES atespaces(name) ON DELETE RESTRICT,
    worker_namespace          text NOT NULL CHECK (worker_namespace <> ''),
    worker_pool               text NOT NULL CHECK (worker_pool <> ''),
    max_slots                 integer NOT NULL CHECK (max_slots BETWEEN 1 AND 256),
    slot_policy_canonical     bytea NOT NULL,
    slot_policy_digest        bytea NOT NULL,
    refresh_digest            bytea NOT NULL UNIQUE
        CHECK (octet_length(refresh_digest) = 32),
    current_session_digest    bytea UNIQUE
        CHECK (current_session_digest IS NULL OR octet_length(current_session_digest) = 32),
    current_session_expires_at timestamptz,
    session_consumed_at       timestamptz,
    session_generation       bigint NOT NULL DEFAULT 0 CHECK (session_generation >= 0),
    revoked_at                timestamptz,
    created_at                timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at                timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((current_session_digest IS NULL) = (current_session_expires_at IS NULL)),
    CHECK (session_consumed_at IS NULL OR current_session_digest IS NOT NULL),
    CONSTRAINT external_provider_registrations_slot_policy_check CHECK (
        octet_length(slot_policy_canonical) BETWEEN 1 AND 2097152
        AND octet_length(slot_policy_digest) = 32
    )
);

CREATE INDEX IF NOT EXISTS external_provider_registrations_session_expiry_idx
    ON external_provider_registrations (current_session_expires_at)
    WHERE current_session_digest IS NOT NULL;

-- Existing development databases may predate registration capability policy.
-- Add nullable columns without inventing authority for those rows: loading a
-- NULL policy fails closed and the operator must issue a new enrollment. The
-- compatibility constraint keeps those rows revocable while rejecting partial
-- or malformed policy state; application issuance always writes both fields.
ALTER TABLE external_provider_enrollments
    ADD COLUMN IF NOT EXISTS slot_policy_canonical bytea;
ALTER TABLE external_provider_enrollments
    ADD COLUMN IF NOT EXISTS slot_policy_digest bytea;
ALTER TABLE external_provider_registrations
    ADD COLUMN IF NOT EXISTS slot_policy_canonical bytea;
ALTER TABLE external_provider_registrations
    ADD COLUMN IF NOT EXISTS slot_policy_digest bytea;

DO $migration$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'external_provider_enrollments_slot_policy_check'
          AND conrelid = 'external_provider_enrollments'::regclass
    ) THEN
        ALTER TABLE external_provider_enrollments
            ADD CONSTRAINT external_provider_enrollments_slot_policy_check CHECK (
                (slot_policy_canonical IS NULL AND slot_policy_digest IS NULL)
                OR (
                    slot_policy_canonical IS NOT NULL
                    AND octet_length(slot_policy_canonical) BETWEEN 1 AND 2097152
                    AND slot_policy_digest IS NOT NULL
                    AND octet_length(slot_policy_digest) = 32
                )
            ) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'external_provider_registrations_slot_policy_check'
          AND conrelid = 'external_provider_registrations'::regclass
    ) THEN
        ALTER TABLE external_provider_registrations
            ADD CONSTRAINT external_provider_registrations_slot_policy_check CHECK (
                (slot_policy_canonical IS NULL AND slot_policy_digest IS NULL)
                OR (
                    slot_policy_canonical IS NOT NULL
                    AND octet_length(slot_policy_canonical) BETWEEN 1 AND 2097152
                    AND slot_policy_digest IS NOT NULL
                    AND octet_length(slot_policy_digest) = 32
                )
            ) NOT VALID;
    END IF;
END
$migration$;
`

// applySchema idempotently creates atepg's tables.
func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning atepg schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	// The schema needs PostgreSQL 13+ (xid8, pg_current_xact_id,
	// pg_current_snapshot); fail with a clear message rather than an
	// opaque DDL or function error.
	var version int
	if err := tx.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		return fmt.Errorf("reading PostgreSQL version: %w", err)
	}
	if version < 130000 {
		return fmt.Errorf("atepg requires PostgreSQL 13 or newer (xid8 and pg_current_snapshot); server_version_num is %d", version)
	}

	// Multiple ateapi replicas can start against an empty database together.
	// PostgreSQL's IF NOT EXISTS does not eliminate every concurrent-DDL race,
	// so serialize schema application with a transaction-scoped advisory lock.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('agent-substrate-atepg-schema'))`); err != nil {
		return fmt.Errorf("locking atepg schema: %w", err)
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("applying atepg schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing atepg schema: %w", err)
	}
	return nil
}
