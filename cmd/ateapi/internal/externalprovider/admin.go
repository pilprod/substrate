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
	"fmt"
	"maps"
	"time"

	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EnrollmentAdminPrincipal identifies one exact JWT-authenticated operator.
// Provider configuration is resolved to an issuer before constructing the
// service so authorization never depends on request-controlled values.
type EnrollmentAdminPrincipal struct {
	Provider string
	Issuer   string
	Subject  string
}

type enrollmentAdminPrincipalKey struct {
	provider string
	issuer   string
	subject  string
}

// EnrollmentAdminServer issues external provider enrollments on the primary,
// authenticated ate-api listener. It is never registered on the Broker
// listener.
type EnrollmentAdminServer struct {
	externalproviderpb.UnimplementedExternalProviderAdminServer
	issuer     *Issuer
	authorized map[enrollmentAdminPrincipalKey]struct{}
}

var _ externalproviderpb.ExternalProviderAdminServer = (*EnrollmentAdminServer)(nil)

// NewEnrollmentAdminServer creates a fail-closed enrollment admin service.
// With no authorized principals, every call is denied and no store is needed.
func NewEnrollmentAdminServer(store ExternalProviderStore, authorized []EnrollmentAdminPrincipal) (*EnrollmentAdminServer, error) {
	server := &EnrollmentAdminServer{
		authorized: make(map[enrollmentAdminPrincipalKey]struct{}, len(authorized)),
	}
	for index, candidate := range authorized {
		if candidate.Provider == "" || candidate.Issuer == "" || candidate.Subject == "" {
			return nil, fmt.Errorf("external provider enrollment admin principal %d requires provider, issuer, and subject", index)
		}
		key := enrollmentAdminPrincipalKey{provider: candidate.Provider, issuer: candidate.Issuer, subject: candidate.Subject}
		if _, duplicate := server.authorized[key]; duplicate {
			return nil, fmt.Errorf("duplicate external provider enrollment admin principal")
		}
		server.authorized[key] = struct{}{}
	}
	if len(server.authorized) == 0 {
		return server, nil
	}
	if store == nil {
		return nil, fmt.Errorf("external provider enrollment admin store is required when principals are authorized")
	}
	server.issuer = NewIssuer(store)
	return server, nil
}

// CreateExternalProviderEnrollment authorizes before parsing any request
// authority, then returns the plaintext credential exactly once.
func (s *EnrollmentAdminServer) CreateExternalProviderEnrollment(ctx context.Context, request *externalproviderpb.CreateExternalProviderEnrollmentRequest) (*externalproviderpb.CreateExternalProviderEnrollmentResponse, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}

	scope, ttl, err := validateEnrollmentAdminRequest(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	issued, err := s.issuer.IssueEnrollment(ctx, scope, ttl)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to create external provider enrollment")
	}
	defer issued.Credential.Destroy()

	expiresAt := timestamppb.New(issued.ExpiresAt)
	if err := expiresAt.CheckValid(); err != nil {
		return nil, status.Error(codes.Internal, "external provider enrollment returned an invalid expiry")
	}
	return &externalproviderpb.CreateExternalProviderEnrollmentResponse{
		EnrollmentUid:        issued.UID,
		EnrollmentCredential: issued.Credential.Bytes(),
		ExpiresAt:            expiresAt,
		Scope:                enrollmentScopeProto(issued.Scope),
	}, nil
}

func (s *EnrollmentAdminServer) authorize(ctx context.Context) error {
	caller, ok := principal.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "authentication is required")
	}
	if caller.Kind != principal.KindJWT {
		return status.Error(codes.PermissionDenied, "caller is not permitted to issue external provider enrollments")
	}
	key := enrollmentAdminPrincipalKey{provider: caller.Provider, issuer: caller.Issuer, subject: caller.ID}
	if _, allowed := s.authorized[key]; !allowed {
		return status.Error(codes.PermissionDenied, "caller is not permitted to issue external provider enrollments")
	}
	if s.issuer == nil {
		return status.Error(codes.FailedPrecondition, "external provider enrollment issuer is not configured")
	}
	return nil
}

func validateEnrollmentAdminRequest(request *externalproviderpb.CreateExternalProviderEnrollmentRequest) (Scope, time.Duration, error) {
	if request == nil || request.GetScope() == nil {
		return Scope{}, 0, fmt.Errorf("scope is required")
	}
	if request.GetTtl() == nil {
		return Scope{}, 0, fmt.Errorf("ttl is required")
	}
	if err := request.GetTtl().CheckValid(); err != nil {
		return Scope{}, 0, fmt.Errorf("ttl is invalid")
	}
	ttl := request.GetTtl().AsDuration()
	if err := validateTTL(ttl, MaxEnrollmentTTL); err != nil {
		return Scope{}, 0, err
	}

	policy, err := slotCapabilityPolicyFromAdminRequest(request.GetScope().GetSlotPolicy())
	if err != nil {
		return Scope{}, 0, err
	}
	scope := Scope{
		OwnerAtespace:   request.GetScope().GetOwnerAtespace(),
		WorkerNamespace: request.GetScope().GetWorkerNamespace(),
		WorkerPool:      request.GetScope().GetWorkerPool(),
		MaxSlots:        request.GetScope().GetMaxSlots(),
		SlotPolicy:      policy,
	}
	if err := scope.Validate(); err != nil {
		return Scope{}, 0, err
	}
	return scope, ttl, nil
}

func slotCapabilityPolicyFromAdminRequest(policy *externalproviderpb.SlotCapabilityPolicy) (SlotCapabilityPolicy, error) {
	if policy == nil {
		return SlotCapabilityPolicy{}, fmt.Errorf("slot policy is required")
	}
	if policy.GetDigest() != "" {
		return SlotCapabilityPolicy{}, fmt.Errorf("slot policy digest must be empty on create")
	}
	profiles := make([]SlotProfile, len(policy.GetProfiles()))
	for index, profile := range policy.GetProfiles() {
		if profile == nil {
			return SlotCapabilityPolicy{}, fmt.Errorf("slot policy profile %d is required", index)
		}
		if profile.GetCapacity() == nil {
			return SlotCapabilityPolicy{}, fmt.Errorf("slot policy profile %d capacity is required", index)
		}
		profiles[index] = SlotProfile{
			ProfileID:    profile.GetProfileId(),
			SandboxClass: profile.GetSandboxClass(),
			Labels:       maps.Clone(profile.GetLabels()),
			MaxSlots:     profile.GetMaxSlots(),
			CPUMilli:     profile.GetCapacity().GetCpuMilli(),
			MemoryBytes:  profile.GetCapacity().GetMemoryBytes(),
		}
	}
	result, err := NewSlotCapabilityPolicy(policy.GetVersion(), profiles)
	if err != nil {
		return SlotCapabilityPolicy{}, err
	}
	return result, nil
}

func enrollmentScopeProto(scope Scope) *externalproviderpb.ExternalProviderEnrollmentScope {
	return &externalproviderpb.ExternalProviderEnrollmentScope{
		OwnerAtespace:   scope.OwnerAtespace,
		WorkerNamespace: scope.WorkerNamespace,
		WorkerPool:      scope.WorkerPool,
		MaxSlots:        scope.MaxSlots,
		SlotPolicy:      scope.SlotPolicy.Proto(),
	}
}
