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

// Package externalprovider implements the private authentication core for the
// external provider broker.
package externalprovider

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/validate/content"
)

const maxSlots = 256

const maxIdentityBytes = 253

const (
	// MaxEnrollmentTTL bounds the out-of-band credential exposure window.
	MaxEnrollmentTTL = 24 * time.Hour
	// MaxSessionTTL bounds how long a not-yet-connected session token is useful.
	MaxSessionTTL = 15 * time.Minute
)

var (
	// ErrAuthenticationFailed intentionally covers missing, expired, consumed,
	// and mismatched credentials so callers cannot use the broker as an oracle.
	ErrAuthenticationFailed = errors.New("external provider authentication failed")

	// ErrCredentialCollision asks the credential-generating caller to retry
	// with fresh randomness. It is never returned over the public RPC boundary.
	ErrCredentialCollision = errors.New("external provider credential collision")

	// ErrNotFound indicates that an internal operator identity does not exist.
	ErrNotFound = errors.New("external provider identity not found")
)

// CredentialDigest is the only representation of a credential accepted by
// the persistence layer.
type CredentialDigest [32]byte

// Scope is immutable for the lifetime of an enrollment and its registration.
type Scope struct {
	OwnerAtespace   string
	WorkerNamespace string
	WorkerPool      string
	MaxSlots        uint32
}

// IsValidIdentity reports whether value satisfies the published opaque
// enrollment/registration identity grammar.
func IsValidIdentity(value string) bool {
	if len(value) == 0 || len(value) > maxIdentityBytes || !isASCIIAlphanumeric(value[0]) || !isASCIIAlphanumeric(value[len(value)-1]) {
		return false
	}
	for index := 1; index < len(value)-1; index++ {
		character := value[index]
		if !isASCIIAlphanumeric(character) && character != '.' && character != '_' && character != '~' && character != '-' {
			return false
		}
	}
	return true
}

func isASCIIAlphanumeric(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9'
}

// Validate rejects scopes which cannot identify Substrate scheduling
// resources or exceed the public ConnectHello slot bound.
func (s Scope) Validate() error {
	if !resources.IsValidResourceName(s.OwnerAtespace) {
		return fmt.Errorf("owner atespace %q is not a valid resource name", s.OwnerAtespace)
	}
	if errs := content.IsDNS1123Label(s.WorkerNamespace); len(errs) != 0 {
		return fmt.Errorf("worker namespace %q is not a valid DNS label", s.WorkerNamespace)
	}
	if errs := content.IsDNS1123Subdomain(s.WorkerPool); len(errs) != 0 {
		return fmt.Errorf("worker pool %q is not a valid DNS subdomain", s.WorkerPool)
	}
	if s.MaxSlots == 0 || s.MaxSlots > maxSlots {
		return fmt.Errorf("max slots %d must be between 1 and %d", s.MaxSlots, maxSlots)
	}
	return nil
}

// Registration is the non-secret, immutable identity and scope established by
// enrollment.
type Registration struct {
	UID           string
	EnrollmentUID string
	Scope         Scope
	CreatedAt     time.Time
}

// Enrollment is the stable, non-secret operator identity and server-clock
// expiry of an issued enrollment.
type Enrollment struct {
	UID       string
	Scope     Scope
	ExpiresAt time.Time
}

// SessionAuthorization describes the current short-lived Connect credential
// without exposing its digest.
type SessionAuthorization struct {
	Registration Registration
	ExpiresAt    time.Time
}

// SessionClaim is the non-secret authority established by atomically
// consuming one current session token. Generation fences older sessions for
// the same registration.
type SessionClaim struct {
	Registration Registration
	Generation   uint64
}

// ExternalProviderStore is deliberately separate from store.Interface. It is
// the minimal persistence boundary for enrollment and broker authentication.
// Every credential crossing this boundary is already a digest.
type ExternalProviderStore interface {
	CreateExternalProviderEnrollment(context.Context, string, CredentialDigest, Scope, time.Duration) (Enrollment, error)
	ConsumeExternalProviderEnrollment(context.Context, CredentialDigest, string, CredentialDigest) (Registration, error)
	RotateExternalProviderSession(context.Context, string, CredentialDigest, CredentialDigest, time.Duration) (SessionAuthorization, error)
	ClaimExternalProviderSession(context.Context, string, CredentialDigest) (SessionClaim, error)
	RevokeExternalProviderEnrollment(context.Context, string) error
	RevokeExternalProviderRegistration(context.Context, string) error
}

func validateTTL(ttl, maximum time.Duration) error {
	if ttl < time.Microsecond {
		return fmt.Errorf("credential TTL must be at least one microsecond")
	}
	if ttl > maximum {
		return fmt.Errorf("credential TTL %s exceeds maximum %s", ttl, maximum)
	}
	return nil
}
