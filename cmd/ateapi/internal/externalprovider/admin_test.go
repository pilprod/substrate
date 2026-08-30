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
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	testAdminIssuer   = "https://issuer.example"
	testAdminProvider = "operator-oidc"
	testAdminSubject  = "operator-a"
)

type fakeRegistrationSessionRevoker struct {
	calls int
	uid   string
	store RegistrationRevocationStore
	err   error
}

func (f *fakeRegistrationSessionRevoker) RevokeExternalProviderRegistration(_ context.Context, store RegistrationRevocationStore, registrationUID string) error {
	f.calls++
	f.uid = registrationUID
	f.store = store
	if f.err != nil {
		return f.err
	}
	return store.RevokeExternalProviderRegistration(context.Background(), registrationUID)
}

func TestEnrollmentAdminAuthorizesExactJWTPrincipalBeforeValidation(t *testing.T) {
	store := &fakeStore{create: func(_ context.Context, uid string, _ CredentialDigest, scope Scope, _ time.Duration) (Enrollment, error) {
		return Enrollment{UID: uid, Scope: scope, ExpiresAt: time.Now().Add(time.Hour)}, nil
	}}
	server, err := NewEnrollmentAdminServer(store, nil, []EnrollmentAdminPrincipal{{Provider: testAdminProvider, Issuer: testAdminIssuer, Subject: testAdminSubject}})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		ctx  context.Context
		code codes.Code
	}{
		{name: "unauthenticated", ctx: context.Background(), code: codes.Unauthenticated},
		{name: "wrong subject", ctx: adminContext("other"), code: codes.PermissionDenied},
		{name: "wrong provider", ctx: principal.InjectContext(context.Background(), principal.PrincipalInfo{Kind: principal.KindJWT, Provider: "other", Issuer: testAdminIssuer, ID: testAdminSubject}), code: codes.PermissionDenied},
		{name: "wrong issuer", ctx: principal.InjectContext(context.Background(), principal.PrincipalInfo{Kind: principal.KindJWT, Provider: testAdminProvider, Issuer: "https://other.example", ID: testAdminSubject}), code: codes.PermissionDenied},
		{name: "mTLS is not implicitly admin", ctx: principal.InjectContext(context.Background(), principal.PrincipalInfo{Kind: principal.KindMTLS, ID: testAdminSubject}), code: codes.PermissionDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := server.CreateExternalProviderEnrollment(test.ctx, nil)
			if got := status.Code(err); got != test.code {
				t.Fatalf("CreateExternalProviderEnrollment() code = %v, want %v (error %v)", got, test.code, err)
			}
		})
	}
	if store.createCalls != 0 {
		t.Fatalf("unauthorized calls reached store %d times", store.createCalls)
	}

	disabled, err := NewEnrollmentAdminServer(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := disabled.CreateExternalProviderEnrollment(adminContext(testAdminSubject), nil); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("disabled server error = %v, want PermissionDenied", err)
	}
}

func TestEnrollmentAdminIssuesExactScopedEnrollment(t *testing.T) {
	expiresAt := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	var storedScope Scope
	store := &fakeStore{create: func(_ context.Context, uid string, _ CredentialDigest, scope Scope, ttl time.Duration) (Enrollment, error) {
		if ttl != 30*time.Minute {
			t.Errorf("TTL = %v, want 30m", ttl)
		}
		storedScope = scope
		return Enrollment{UID: uid, Scope: scope, ExpiresAt: expiresAt}, nil
	}}
	server, err := NewEnrollmentAdminServer(store, nil, []EnrollmentAdminPrincipal{{Provider: testAdminProvider, Issuer: testAdminIssuer, Subject: testAdminSubject}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.CreateExternalProviderEnrollment(adminContext(testAdminSubject), validEnrollmentAdminRequest())
	if err != nil {
		t.Fatalf("CreateExternalProviderEnrollment() error = %v", err)
	}
	if response.GetEnrollmentUid() == "" || len(response.GetEnrollmentCredential()) != credentialEncodedBytes {
		t.Fatalf("issued identity or credential is invalid: uid=%q credential_bytes=%d", response.GetEnrollmentUid(), len(response.GetEnrollmentCredential()))
	}
	if got := response.GetExpiresAt().AsTime(); !got.Equal(expiresAt) {
		t.Fatalf("expiry = %v, want %v", got, expiresAt)
	}
	if response.GetScope().GetSlotPolicy().GetDigest() == "" {
		t.Fatal("response slot policy lacks server-computed digest")
	}
	if !proto.Equal(response.GetScope().GetSlotPolicy(), storedScope.SlotPolicy.Proto()) {
		t.Fatalf("response policy = %v, stored policy = %v", response.GetScope().GetSlotPolicy(), storedScope.SlotPolicy.Proto())
	}
	if got := response.GetScope(); got.GetOwnerAtespace() != "tenant-a" || got.GetWorkerNamespace() != "external-workers" || got.GetWorkerPool() != "local-agents" || got.GetMaxSlots() != 2 {
		t.Fatalf("response scope = %+v", got)
	}
}

func TestEnrollmentAdminRevocationAuthorizesBeforeTargetStoreAndFence(t *testing.T) {
	store := &fakeStore{revokeRegistration: func(context.Context, string) error { return nil }}
	revoker := &fakeRegistrationSessionRevoker{}
	server, err := NewEnrollmentAdminServer(store, revoker, []EnrollmentAdminPrincipal{{Provider: testAdminProvider, Issuer: testAdminIssuer, Subject: testAdminSubject}})
	if err != nil {
		t.Fatal(err)
	}
	request := &externalproviderpb.RevokeExternalProviderRegistrationRequest{RegistrationUid: "registration-a"}
	tests := []struct {
		name string
		ctx  context.Context
		code codes.Code
	}{
		{name: "unauthenticated", ctx: context.Background(), code: codes.Unauthenticated},
		{name: "wrong subject", ctx: adminContext("other"), code: codes.PermissionDenied},
		{name: "wrong provider", ctx: principal.InjectContext(context.Background(), principal.PrincipalInfo{Kind: principal.KindJWT, Provider: "other", Issuer: testAdminIssuer, ID: testAdminSubject}), code: codes.PermissionDenied},
		{name: "wrong issuer", ctx: principal.InjectContext(context.Background(), principal.PrincipalInfo{Kind: principal.KindJWT, Provider: testAdminProvider, Issuer: "https://other.example", ID: testAdminSubject}), code: codes.PermissionDenied},
		{name: "mTLS is not implicitly admin", ctx: principal.InjectContext(context.Background(), principal.PrincipalInfo{Kind: principal.KindMTLS, ID: testAdminSubject}), code: codes.PermissionDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := server.RevokeExternalProviderRegistration(test.ctx, request); status.Code(err) != test.code {
				t.Fatalf("RevokeExternalProviderRegistration() code = %v, want %v", status.Code(err), test.code)
			}
		})
	}
	if revoker.calls != 0 || store.revokeRegistrationCalls != 0 {
		t.Fatalf("unauthorized revocation reached revoker/store: revoker=%d store=%d", revoker.calls, store.revokeRegistrationCalls)
	}

	if _, err := server.RevokeExternalProviderRegistration(adminContext(testAdminSubject), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid target error = %v, want InvalidArgument", err)
	}
	if revoker.calls != 0 || store.revokeRegistrationCalls != 0 {
		t.Fatalf("invalid target reached revoker/store: revoker=%d store=%d", revoker.calls, store.revokeRegistrationCalls)
	}

	response, err := server.RevokeExternalProviderRegistration(adminContext(testAdminSubject), request)
	if err != nil || response == nil {
		t.Fatalf("authorized revoke = (%v, %v)", response, err)
	}
	if revoker.calls != 1 || store.revokeRegistrationCalls != 1 || revoker.uid != "registration-a" || revoker.store != store {
		t.Fatalf("authorized revoke calls = revoker:%d store:%d uid:%q authority-store:%T", revoker.calls, store.revokeRegistrationCalls, revoker.uid, revoker.store)
	}
}

func TestEnrollmentAdminRevocationMapsAuthorityFailuresWithoutLeakingCause(t *testing.T) {
	secretCause := errors.New("database included secret-refresh-credential")
	store := &fakeStore{revokeRegistration: func(context.Context, string) error { return nil }}
	revoker := &fakeRegistrationSessionRevoker{err: secretCause}
	server, err := NewEnrollmentAdminServer(store, revoker, []EnrollmentAdminPrincipal{{Provider: testAdminProvider, Issuer: testAdminIssuer, Subject: testAdminSubject}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.RevokeExternalProviderRegistration(adminContext(testAdminSubject), &externalproviderpb.RevokeExternalProviderRegistrationRequest{RegistrationUid: "registration-a"})
	if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "secret-refresh-credential") || strings.Contains(err.Error(), "database included") {
		t.Fatalf("revocation error was not sanitized: %v", err)
	}
}

func TestEnrollmentAdminRejectsInvalidScopeAndTTL(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*externalproviderpb.CreateExternalProviderEnrollmentRequest)
	}{
		{name: "missing scope", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentRequest) { r.Scope = nil }},
		{name: "invalid owner", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentRequest) {
			r.Scope.OwnerAtespace = "bad/name"
		}},
		{name: "missing policy", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentRequest) { r.Scope.SlotPolicy = nil }},
		{name: "client supplied digest", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentRequest) {
			r.Scope.SlotPolicy.Digest = strings.Repeat("a", 64)
		}},
		{name: "policy grants too few slots", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentRequest) { r.Scope.MaxSlots = 3 }},
		{name: "TTL above maximum", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentRequest) {
			r.Ttl = durationpb.New(MaxEnrollmentTTL + time.Microsecond)
		}},
		{name: "zero TTL", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentRequest) { r.Ttl = durationpb.New(0) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeStore{}
			server, err := NewEnrollmentAdminServer(store, nil, []EnrollmentAdminPrincipal{{Provider: testAdminProvider, Issuer: testAdminIssuer, Subject: testAdminSubject}})
			if err != nil {
				t.Fatal(err)
			}
			request := validEnrollmentAdminRequest()
			test.mutate(request)
			_, err = server.CreateExternalProviderEnrollment(adminContext(testAdminSubject), request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CreateExternalProviderEnrollment() error = %v, want InvalidArgument", err)
			}
			if store.createCalls != 0 {
				t.Fatalf("invalid request reached store %d times", store.createCalls)
			}
		})
	}
}

func TestEnrollmentAdminDoesNotExposeCredentialThroughErrors(t *testing.T) {
	credential := testCredential(0x53)
	store := &fakeStore{create: func(context.Context, string, CredentialDigest, Scope, time.Duration) (Enrollment, error) {
		return Enrollment{}, errors.New("store accidentally included " + string(credential))
	}}
	server, err := NewEnrollmentAdminServer(store, nil, []EnrollmentAdminPrincipal{{Provider: testAdminProvider, Issuer: testAdminIssuer, Subject: testAdminSubject}})
	if err != nil {
		t.Fatal(err)
	}
	server.issuer = newIssuer(store, bytes.NewReader(bytes.Repeat([]byte{0x53}, credentialEntropyBytes+16)))

	_, err = server.CreateExternalProviderEnrollment(adminContext(testAdminSubject), validEnrollmentAdminRequest())
	if status.Code(err) != codes.Internal {
		t.Fatalf("CreateExternalProviderEnrollment() error = %v, want Internal", err)
	}
	if strings.Contains(err.Error(), string(credential)) || strings.Contains(err.Error(), "store accidentally included") {
		t.Fatalf("admin error exposed secret-bearing cause: %v", err)
	}
}

func adminContext(subject string) context.Context {
	return principal.InjectContext(context.Background(), principal.PrincipalInfo{
		Kind:     principal.KindJWT,
		Provider: testAdminProvider,
		Issuer:   testAdminIssuer,
		ID:       subject,
	})
}

func validEnrollmentAdminRequest() *externalproviderpb.CreateExternalProviderEnrollmentRequest {
	return &externalproviderpb.CreateExternalProviderEnrollmentRequest{
		Scope: &externalproviderpb.ExternalProviderEnrollmentScope{
			OwnerAtespace:   "tenant-a",
			WorkerNamespace: "external-workers",
			WorkerPool:      "local-agents",
			MaxSlots:        2,
			SlotPolicy: &externalproviderpb.SlotCapabilityPolicy{
				Version: SlotCapabilityPolicyVersion,
				Profiles: []*externalproviderpb.SlotProfile{{
					ProfileId:    "codex-native",
					SandboxClass: "host-process-hardened",
					Labels:       map[string]string{"agent.example/provider": "codex"},
					MaxSlots:     2,
					Capacity:     &ateapipb.WorkerCapacity{CpuMilli: 4_000, MemoryBytes: 16 << 30},
				}},
			},
		},
		Ttl: durationpb.New(30 * time.Minute),
	}
}
