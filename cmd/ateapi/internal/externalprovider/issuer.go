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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
)

const credentialGenerationAttempts = 4

// SecretCredential prevents accidental logging or serialization of a plaintext
// credential while still allowing the issuer caller to explicitly copy it.
type SecretCredential struct {
	value []byte
}

// Bytes returns a caller-owned copy of the credential.
func (c SecretCredential) Bytes() []byte {
	return append([]byte(nil), c.value...)
}

// Destroy best-effort clears the shared backing bytes held by this wrapper and
// any value copies of it. It cannot clear copies previously returned by Bytes.
func (c *SecretCredential) Destroy() {
	if c == nil {
		return
	}
	clear(c.value)
	c.value = nil
}

// String always redacts the credential.
func (SecretCredential) String() string { return "[REDACTED]" }

// GoString always redacts the credential.
func (SecretCredential) GoString() string { return "externalprovider.SecretCredential([REDACTED])" }

// IssuedEnrollment is returned once by the in-process issuer. Its credential
// must be delivered through an out-of-band secret channel.
type IssuedEnrollment struct {
	UID        string
	Credential SecretCredential `json:"-"`
	ExpiresAt  time.Time
	Scope      Scope
}

// String omits the plaintext credential.
func (e IssuedEnrollment) String() string {
	return fmt.Sprintf("IssuedEnrollment{UID:%q ExpiresAt:%s Scope:%+v Credential:[REDACTED]}", e.UID, e.ExpiresAt, e.Scope)
}

// GoString omits the plaintext credential.
func (e IssuedEnrollment) GoString() string { return e.String() }

// Issuer creates scoped, single-use enrollment credentials. It is an
// in-process API under cmd/ateapi/internal and is not a network service.
type Issuer struct {
	store  ExternalProviderStore
	random io.Reader
}

// NewIssuer creates an in-process enrollment issuer backed by store.
func NewIssuer(store ExternalProviderStore) *Issuer {
	return &Issuer{store: store, random: rand.Reader}
}

func newIssuer(store ExternalProviderStore, random io.Reader) *Issuer {
	return &Issuer{store: store, random: random}
}

// IssueEnrollment creates a new single-use credential with a database-clock
// expiry. The store receives only its digest.
func (i *Issuer) IssueEnrollment(ctx context.Context, scope Scope, ttl time.Duration) (*IssuedEnrollment, error) {
	if i == nil || i.store == nil || i.random == nil {
		return nil, errors.New("external provider issuer is not configured")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := validateTTL(ttl, MaxEnrollmentTTL); err != nil {
		return nil, err
	}

	for range credentialGenerationAttempts {
		credential, err := generateCredential(i.random)
		if err != nil {
			return nil, err
		}
		digest := digestCredential(enrollmentDigestDomain, credential)
		enrollmentUUID, err := uuid.NewRandomFromReader(i.random)
		if err != nil {
			clear(credential)
			return nil, fmt.Errorf("generating enrollment identity: %w", err)
		}
		enrollment, err := i.store.CreateExternalProviderEnrollment(ctx, enrollmentUUID.String(), digest, scope, ttl)
		if err == nil {
			return &IssuedEnrollment{
				UID:        enrollment.UID,
				Credential: SecretCredential{value: credential},
				ExpiresAt:  enrollment.ExpiresAt,
				Scope:      enrollment.Scope,
			}, nil
		}
		clear(credential)
		if !errors.Is(err, ErrCredentialCollision) {
			return nil, fmt.Errorf("creating external provider enrollment: %w", err)
		}
	}
	return nil, errors.New("generating a unique external provider enrollment credential")
}

// RevokeEnrollment invalidates an enrollment and any registration created
// from it. The database clock records the revocation.
func (i *Issuer) RevokeEnrollment(ctx context.Context, enrollmentUID string) error {
	if i == nil || i.store == nil {
		return errors.New("external provider issuer is not configured")
	}
	if enrollmentUID == "" {
		return errors.New("enrollment UID is required")
	}
	return i.store.RevokeExternalProviderEnrollment(ctx, enrollmentUID)
}

// RevokeRegistration invalidates refresh and not-yet-claimed session
// credentials for one registration.
func (i *Issuer) RevokeRegistration(ctx context.Context, registrationUID string) error {
	if i == nil || i.store == nil {
		return errors.New("external provider issuer is not configured")
	}
	if registrationUID == "" {
		return errors.New("registration UID is required")
	}
	return i.store.RevokeExternalProviderRegistration(ctx, registrationUID)
}
