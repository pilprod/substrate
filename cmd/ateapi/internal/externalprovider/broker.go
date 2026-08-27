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

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Broker implements the authentication unary RPCs. It is intentionally not
// registered by this slice, so it has no network exposure yet.
type Broker struct {
	externalproviderpb.UnimplementedExternalProviderBrokerServer
	store      ExternalProviderStore
	random     io.Reader
	sessionTTL time.Duration
}

var _ externalproviderpb.ExternalProviderBrokerServer = (*Broker)(nil)

// NewBroker creates an unregistered broker service.
func NewBroker(store ExternalProviderStore, sessionTTL time.Duration) (*Broker, error) {
	return newBroker(store, rand.Reader, sessionTTL)
}

func newBroker(store ExternalProviderStore, random io.Reader, sessionTTL time.Duration) (*Broker, error) {
	if store == nil || random == nil {
		return nil, errors.New("external provider broker is not configured")
	}
	if err := validateTTL(sessionTTL, MaxSessionTTL); err != nil {
		return nil, fmt.Errorf("session TTL: %w", err)
	}
	return &Broker{store: store, random: random, sessionTTL: sessionTTL}, nil
}

// Enroll consumes one out-of-band credential and returns one refresh
// credential. Neither credential is accepted in the request payload.
func (b *Broker) Enroll(ctx context.Context, req *externalproviderpb.EnrollRequest) (*externalproviderpb.EnrollResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	enrollmentCredential, err := bearerCredential(ctx)
	if err != nil {
		return nil, unauthenticated()
	}
	enrollmentDigest := digestCredential(enrollmentDigestDomain, enrollmentCredential)
	clear(enrollmentCredential)

	for range credentialGenerationAttempts {
		refreshCredential, err := generateCredential(b.random)
		if err != nil {
			return nil, status.Error(codes.Internal, "credential generation failed")
		}
		registrationUUID, err := uuid.NewRandomFromReader(b.random)
		if err != nil {
			clear(refreshCredential)
			return nil, status.Error(codes.Internal, "identity generation failed")
		}
		registration, err := b.store.ConsumeExternalProviderEnrollment(
			ctx,
			enrollmentDigest,
			registrationUUID.String(),
			digestCredential(refreshDigestDomain, refreshCredential),
		)
		if err == nil {
			return &externalproviderpb.EnrollResponse{
				RegistrationUid:   registration.UID,
				RefreshCredential: refreshCredential,
				SlotPolicy:        registration.Scope.SlotPolicy.Proto(),
			}, nil
		}
		clear(refreshCredential)
		if errors.Is(err, ErrAuthenticationFailed) {
			return nil, unauthenticated()
		}
		if !errors.Is(err, ErrCredentialCollision) {
			return nil, status.Error(codes.Internal, "enrollment failed")
		}
	}
	return nil, status.Error(codes.Internal, "credential generation failed")
}

// MintSessionToken authenticates the registration's durable refresh
// credential and atomically replaces its one current session token.
func (b *Broker) MintSessionToken(ctx context.Context, req *externalproviderpb.MintSessionTokenRequest) (*externalproviderpb.MintSessionTokenResponse, error) {
	if req == nil || !IsValidIdentity(req.GetRegistrationUid()) {
		return nil, status.Error(codes.InvalidArgument, "registration_uid is invalid")
	}
	refreshCredential, err := bearerCredential(ctx)
	if err != nil {
		return nil, unauthenticated()
	}
	refreshDigest := digestCredential(refreshDigestDomain, refreshCredential)
	clear(refreshCredential)

	for range credentialGenerationAttempts {
		sessionCredential, err := generateCredential(b.random)
		if err != nil {
			return nil, status.Error(codes.Internal, "credential generation failed")
		}
		authorization, err := b.store.RotateExternalProviderSession(
			ctx,
			req.GetRegistrationUid(),
			refreshDigest,
			digestCredential(sessionDigestDomain, sessionCredential),
			b.sessionTTL,
		)
		if err == nil {
			return &externalproviderpb.MintSessionTokenResponse{
				SessionToken: sessionCredential,
				ExpiresAt:    timestamppb.New(authorization.ExpiresAt),
				SlotPolicy:   authorization.Registration.Scope.SlotPolicy.Proto(),
			}, nil
		}
		clear(sessionCredential)
		if errors.Is(err, ErrAuthenticationFailed) {
			return nil, unauthenticated()
		}
		if !errors.Is(err, ErrCredentialCollision) {
			return nil, status.Error(codes.Internal, "session token issuance failed")
		}
	}
	return nil, status.Error(codes.Internal, "credential generation failed")
}

// Connect remains deliberately unavailable until the authenticated session
// router exists. It does not parse or authenticate the presented token.
func (b *Broker) Connect(grpc.BidiStreamingServer[externalproviderpb.ClientFrame, externalproviderpb.ServerFrame]) error {
	return status.Error(codes.Unimplemented, "external provider sessions are not implemented")
}

func unauthenticated() error {
	return status.Error(codes.Unauthenticated, "authentication failed")
}
