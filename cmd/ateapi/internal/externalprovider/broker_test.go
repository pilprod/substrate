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
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeStore struct {
	mu                      sync.Mutex
	createCalls             int
	consumeCalls            int
	rotateCalls             int
	claimCalls              int
	revokeEnrollmentCalls   int
	revokeRegistrationCalls int
	create                  func(context.Context, string, CredentialDigest, Scope, time.Duration) (Enrollment, error)
	consume                 func(context.Context, CredentialDigest, string, CredentialDigest) (Registration, error)
	rotate                  func(context.Context, string, CredentialDigest, CredentialDigest, time.Duration) (SessionAuthorization, error)
	claim                   func(context.Context, string, CredentialDigest) (SessionClaim, error)
	revokeEnrollment        func(context.Context, string) error
	revokeRegistration      func(context.Context, string) error
}

func testExternalProviderScope(maxSlots uint32) Scope {
	return validSessionClaim(maxSlots).Registration.Scope
}

func (f *fakeStore) CreateExternalProviderEnrollment(ctx context.Context, uid string, digest CredentialDigest, scope Scope, ttl time.Duration) (Enrollment, error) {
	f.mu.Lock()
	f.createCalls++
	f.mu.Unlock()
	return f.create(ctx, uid, digest, scope, ttl)
}

func (f *fakeStore) ConsumeExternalProviderEnrollment(ctx context.Context, digest CredentialDigest, uid string, refresh CredentialDigest) (Registration, error) {
	f.mu.Lock()
	f.consumeCalls++
	f.mu.Unlock()
	return f.consume(ctx, digest, uid, refresh)
}

func (f *fakeStore) RotateExternalProviderSession(ctx context.Context, uid string, refresh, session CredentialDigest, ttl time.Duration) (SessionAuthorization, error) {
	f.mu.Lock()
	f.rotateCalls++
	f.mu.Unlock()
	return f.rotate(ctx, uid, refresh, session, ttl)
}

func (f *fakeStore) ClaimExternalProviderSession(ctx context.Context, uid string, session CredentialDigest) (SessionClaim, error) {
	f.mu.Lock()
	f.claimCalls++
	f.mu.Unlock()
	return f.claim(ctx, uid, session)
}

func (f *fakeStore) RevokeExternalProviderEnrollment(ctx context.Context, uid string) error {
	f.mu.Lock()
	f.revokeEnrollmentCalls++
	f.mu.Unlock()
	return f.revokeEnrollment(ctx, uid)
}

func (f *fakeStore) RevokeExternalProviderRegistration(ctx context.Context, uid string) error {
	f.mu.Lock()
	f.revokeRegistrationCalls++
	f.mu.Unlock()
	return f.revokeRegistration(ctx, uid)
}

func TestIssuerStoresOnlyEnrollmentDigest(t *testing.T) {
	scope := testExternalProviderScope(4)
	expiresAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	credential := testCredential(0x21)
	store := &fakeStore{
		create: func(_ context.Context, uid string, got CredentialDigest, gotScope Scope, ttl time.Duration) (Enrollment, error) {
			if want := digestCredential(enrollmentDigestDomain, credential); got != want {
				t.Errorf("stored digest = %x, want %x", got, want)
			}
			if gotScope != scope {
				t.Errorf("stored scope = %+v, want %+v", gotScope, scope)
			}
			if ttl != time.Hour {
				t.Errorf("TTL = %v, want 1h", ttl)
			}
			return Enrollment{UID: uid, Scope: gotScope, ExpiresAt: expiresAt}, nil
		},
	}
	issuer := newIssuer(store, bytes.NewReader(bytes.Repeat([]byte{0x21}, credentialEntropyBytes+16)))
	issued, err := issuer.IssueEnrollment(context.Background(), scope, time.Hour)
	if err != nil {
		t.Fatalf("IssueEnrollment() error = %v", err)
	}
	defer issued.Credential.Destroy()
	if !bytes.Equal(issued.Credential.Bytes(), credential) {
		t.Errorf("credential = %q, want %q", issued.Credential.Bytes(), credential)
	}
	if issued.UID == "" {
		t.Error("enrollment UID is empty")
	}
	if !issued.ExpiresAt.Equal(expiresAt) {
		t.Errorf("expires_at = %v, want %v", issued.ExpiresAt, expiresAt)
	}
}

func TestIssuedEnrollmentRedactsCredential(t *testing.T) {
	credential := testCredential(0x27)
	issued := IssuedEnrollment{
		UID:        "enrollment-a",
		Credential: SecretCredential{value: credential},
		ExpiresAt:  time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC),
		Scope:      testExternalProviderScope(1),
	}
	encoded, err := json.Marshal(issued)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, rendered := range []string{string(encoded), fmt.Sprint(issued), fmt.Sprintf("%+v", issued), fmt.Sprintf("%#v", issued), fmt.Sprint(issued.Credential)} {
		if strings.Contains(rendered, string(credential)) {
			t.Fatalf("rendered enrollment leaks credential: %s", rendered)
		}
	}
	if strings.Contains(string(encoded), "Credential") {
		t.Errorf("JSON includes Credential field: %s", encoded)
	}
	credentialCopy := issued.Credential
	credentialCopy.Destroy()
	if got := issued.Credential.Bytes(); len(got) != len(credential) || !bytes.Equal(got, make([]byte, len(credential))) {
		t.Errorf("Destroy() left shared plaintext bytes: %x", got)
	}
	if got := credentialCopy.Bytes(); got != nil {
		t.Errorf("destroyed wrapper Bytes() = %x, want nil", got)
	}
}

func TestCredentialTTLsAreBounded(t *testing.T) {
	issuer := newIssuer(&fakeStore{}, bytes.NewReader(make([]byte, 64)))
	_, err := issuer.IssueEnrollment(context.Background(), testExternalProviderScope(1), MaxEnrollmentTTL+time.Microsecond)
	if err == nil {
		t.Fatal("IssueEnrollment() accepted TTL above MaxEnrollmentTTL")
	}
	if _, err := newBroker(&fakeStore{}, bytes.NewReader(make([]byte, 64)), MaxSessionTTL+time.Microsecond); err == nil {
		t.Fatal("newBroker() accepted TTL above MaxSessionTTL")
	}
}

func TestIssuerRevocationUsesStableOperatorUIDs(t *testing.T) {
	store := &fakeStore{
		revokeEnrollment: func(_ context.Context, uid string) error {
			if uid != "enrollment-a" {
				t.Errorf("enrollment UID = %q, want enrollment-a", uid)
			}
			return nil
		},
		revokeRegistration: func(_ context.Context, uid string) error {
			if uid != "registration-a" {
				t.Errorf("registration UID = %q, want registration-a", uid)
			}
			return nil
		},
	}
	issuer := NewIssuer(store)
	if err := issuer.RevokeEnrollment(context.Background(), "enrollment-a"); err != nil {
		t.Fatalf("RevokeEnrollment() error = %v", err)
	}
	if err := issuer.RevokeRegistration(context.Background(), "registration-a"); err != nil {
		t.Fatalf("RevokeRegistration() error = %v", err)
	}
}

func TestBrokerEnrollAndMint(t *testing.T) {
	enrollment := testCredential(0x31)
	refresh := testCredential(0x41)
	session := testCredential(0x51)
	expiresAt := time.Date(2026, 8, 26, 12, 5, 0, 0, time.UTC)
	scope := testExternalProviderScope(4)
	store := &fakeStore{
		consume: func(_ context.Context, digest CredentialDigest, uid string, refreshDigest CredentialDigest) (Registration, error) {
			if got, want := digest, digestCredential(enrollmentDigestDomain, enrollment); got != want {
				t.Errorf("enrollment digest = %x, want %x", got, want)
			}
			if got, want := refreshDigest, digestCredential(refreshDigestDomain, refresh); got != want {
				t.Errorf("refresh digest = %x, want %x", got, want)
			}
			return Registration{UID: uid, Scope: scope}, nil
		},
		rotate: func(_ context.Context, uid string, refreshDigest, sessionDigest CredentialDigest, ttl time.Duration) (SessionAuthorization, error) {
			if got, want := refreshDigest, digestCredential(refreshDigestDomain, refresh); got != want {
				t.Errorf("refresh digest = %x, want %x", got, want)
			}
			if got, want := sessionDigest, digestCredential(sessionDigestDomain, session); got != want {
				t.Errorf("session digest = %x, want %x", got, want)
			}
			if ttl != 5*time.Minute {
				t.Errorf("session TTL = %v, want 5m", ttl)
			}
			return SessionAuthorization{Registration: Registration{UID: uid, Scope: scope}, ExpiresAt: expiresAt}, nil
		},
	}
	// Enroll consumes 32 bytes for refresh and 16 for the UUID. Mint consumes
	// another 32 for its session credential.
	random := bytes.NewReader(append(append(bytes.Repeat([]byte{0x41}, 32), bytes.Repeat([]byte{0x61}, 16)...), bytes.Repeat([]byte{0x51}, 32)...))
	broker, err := newBroker(store, random, 5*time.Minute)
	if err != nil {
		t.Fatalf("newBroker() error = %v", err)
	}
	enrollResponse, err := broker.Enroll(bearerContext("authorization", "Bearer "+string(enrollment)), &externalproviderpb.EnrollRequest{})
	if err != nil {
		t.Fatalf("Enroll() error = %v", err)
	}
	if !bytes.Equal(enrollResponse.GetRefreshCredential(), refresh) {
		t.Errorf("refresh credential = %q, want %q", enrollResponse.GetRefreshCredential(), refresh)
	}
	if enrollResponse.GetRegistrationUid() == "" {
		t.Fatal("registration UID is empty")
	}
	if enrollResponse.GetSlotPolicy().GetDigest() != scope.SlotPolicy.DigestHex() {
		t.Fatalf("enrollment slot policy digest = %q, want %q", enrollResponse.GetSlotPolicy().GetDigest(), scope.SlotPolicy.DigestHex())
	}

	mintResponse, err := broker.MintSessionToken(
		bearerContext("authorization", "Bearer "+string(refresh)),
		&externalproviderpb.MintSessionTokenRequest{RegistrationUid: enrollResponse.GetRegistrationUid()},
	)
	if err != nil {
		t.Fatalf("MintSessionToken() error = %v", err)
	}
	if !bytes.Equal(mintResponse.GetSessionToken(), session) {
		t.Errorf("session token = %q, want %q", mintResponse.GetSessionToken(), session)
	}
	if got := mintResponse.GetExpiresAt().AsTime(); !got.Equal(expiresAt) {
		t.Errorf("expires_at = %v, want %v", got, expiresAt)
	}
	if mintResponse.GetSlotPolicy().GetDigest() != scope.SlotPolicy.DigestHex() {
		t.Fatalf("mint slot policy digest = %q, want %q", mintResponse.GetSlotPolicy().GetDigest(), scope.SlotPolicy.DigestHex())
	}
}

func TestBrokerRejectsCredentialsWithoutStoreAccess(t *testing.T) {
	store := &fakeStore{
		consume: func(context.Context, CredentialDigest, string, CredentialDigest) (Registration, error) {
			t.Fatal("ConsumeExternalProviderEnrollment called")
			return Registration{}, nil
		},
		rotate: func(context.Context, string, CredentialDigest, CredentialDigest, time.Duration) (SessionAuthorization, error) {
			t.Fatal("RotateExternalProviderSession called")
			return SessionAuthorization{}, nil
		},
	}
	broker, err := newBroker(store, bytes.NewReader(make([]byte, 128)), time.Minute)
	if err != nil {
		t.Fatalf("newBroker() error = %v", err)
	}
	if _, err := broker.Enroll(context.Background(), &externalproviderpb.EnrollRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("Enroll() code = %v, want Unauthenticated", status.Code(err))
	}
	if _, err := broker.MintSessionToken(context.Background(), &externalproviderpb.MintSessionTokenRequest{RegistrationUid: "valid-uid"}); status.Code(err) != codes.Unauthenticated {
		t.Errorf("MintSessionToken() code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestConnectFailsClosedWithoutRuntimeAndStoreAccess(t *testing.T) {
	store := &fakeStore{}
	broker, err := newBroker(store, bytes.NewReader(make([]byte, 32)), time.Minute)
	if err != nil {
		t.Fatalf("newBroker() error = %v", err)
	}
	if err := broker.Connect(nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Connect() code = %v, want FailedPrecondition", status.Code(err))
	}
	if store.createCalls != 0 || store.consumeCalls != 0 || store.rotateCalls != 0 || store.claimCalls != 0 || store.revokeEnrollmentCalls != 0 || store.revokeRegistrationCalls != 0 {
		t.Fatalf("Connect accessed store: %+v", store)
	}
}

func TestFakeStoreSessionClaimReturnsOnlyAuthority(t *testing.T) {
	digest := digestCredential(sessionDigestDomain, testCredential(0x66))
	want := SessionClaim{
		Registration: Registration{
			UID:           "registration-a",
			EnrollmentUID: "enrollment-a",
			Scope:         testExternalProviderScope(4),
		},
		Generation: 7,
	}
	store := &fakeStore{
		claim: func(_ context.Context, uid string, gotDigest CredentialDigest) (SessionClaim, error) {
			if uid != want.Registration.UID || gotDigest != digest {
				t.Errorf("claim arguments = %q/%x, want %q/%x", uid, gotDigest, want.Registration.UID, digest)
			}
			return want, nil
		},
	}
	got, err := store.ClaimExternalProviderSession(context.Background(), want.Registration.UID, digest)
	if err != nil {
		t.Fatalf("ClaimExternalProviderSession() error = %v", err)
	}
	if got != want {
		t.Errorf("ClaimExternalProviderSession() = %+v, want %+v", got, want)
	}
	if store.claimCalls != 1 {
		t.Errorf("claim calls = %d, want 1", store.claimCalls)
	}
}

func TestMetadataOnlyLoggingOmitsPayloadAndError(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	interceptor := MetadataOnlyUnaryLoggingInterceptor(logger)
	secret := string(testCredential(0x71))
	request := &externalproviderpb.MintSessionTokenRequest{RegistrationUid: secret}
	response, err := interceptor(
		context.Background(),
		request,
		&grpc.UnaryServerInfo{FullMethod: externalproviderpb.ExternalProviderBroker_MintSessionToken_FullMethodName},
		func(_ context.Context, got any) (any, error) {
			if got != request {
				t.Errorf("handler request = %T %v, want original request", got, got)
			}
			return &externalproviderpb.MintSessionTokenResponse{SessionToken: []byte(secret)}, status.Error(codes.PermissionDenied, secret)
		},
	)
	if response == nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("interceptor response/error = %v/%v", response, err)
	}
	logLine := output.String()
	if strings.Contains(logLine, secret) || strings.Contains(logLine, "request") || strings.Contains(logLine, "response") {
		t.Fatalf("metadata-only log contains payload: %s", logLine)
	}
	if !strings.Contains(logLine, "PermissionDenied") || !strings.Contains(logLine, externalproviderpb.ExternalProviderBroker_MintSessionToken_FullMethodName) {
		t.Errorf("metadata-only log is missing method/code: %s", logLine)
	}
}

func TestMetadataOnlyStreamLoggingOmitsCredentialAndError(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	interceptor := MetadataOnlyStreamLoggingInterceptor(logger)
	secret := string(testCredential(0x72))
	stream := &testServerStream{ctx: bearerContext("authorization", "Bearer "+secret)}
	err := interceptor(
		nil,
		stream,
		&grpc.StreamServerInfo{FullMethod: externalproviderpb.ExternalProviderBroker_Connect_FullMethodName},
		func(_ any, got grpc.ServerStream) error {
			if got != stream {
				t.Errorf("handler stream = %T, want original stream", got)
			}
			return status.Error(codes.PermissionDenied, secret)
		},
	)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("interceptor error = %v", err)
	}
	logLine := output.String()
	if strings.Contains(logLine, secret) || strings.Contains(logLine, "authorization") || strings.Contains(logLine, "error") {
		t.Fatalf("metadata-only stream log contains sensitive data: %s", logLine)
	}
	if !strings.Contains(logLine, "PermissionDenied") || !strings.Contains(logLine, externalproviderpb.ExternalProviderBroker_Connect_FullMethodName) {
		t.Errorf("metadata-only stream log is missing method/code: %s", logLine)
	}
}

func TestMetadataOnlyLoggingDoesNotExposeCredentialContextToHandler(t *testing.T) {
	handler := &credentialInspectingLogHandler{}
	logger := slog.New(handler)
	ctx := bearerContext("authorization", "Bearer "+string(testCredential(0x74)))

	unary := MetadataOnlyUnaryLoggingInterceptor(logger)
	if _, err := unary(
		ctx,
		&externalproviderpb.EnrollRequest{},
		&grpc.UnaryServerInfo{FullMethod: externalproviderpb.ExternalProviderBroker_Enroll_FullMethodName},
		func(context.Context, any) (any, error) { return &externalproviderpb.EnrollResponse{}, nil },
	); err != nil {
		t.Fatalf("unary logging interceptor error = %v", err)
	}

	stream := MetadataOnlyStreamLoggingInterceptor(logger)
	if err := stream(
		nil,
		&testServerStream{ctx: ctx},
		&grpc.StreamServerInfo{FullMethod: externalproviderpb.ExternalProviderBroker_Connect_FullMethodName},
		func(any, grpc.ServerStream) error { return nil },
	); err != nil {
		t.Fatalf("stream logging interceptor error = %v", err)
	}

	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.records != 2 {
		t.Fatalf("log records = %d, want 2", handler.records)
	}
	if handler.sawAuthorization {
		t.Fatal("metadata-only logger exposed credential-bearing RPC context to its handler")
	}
}

func TestOpaqueBearerInterceptorsRejectMalformedCredentials(t *testing.T) {
	unaryCalled := false
	_, err := OpaqueBearerUnaryInterceptor()(
		context.Background(),
		&externalproviderpb.EnrollRequest{},
		&grpc.UnaryServerInfo{FullMethod: externalproviderpb.ExternalProviderBroker_Enroll_FullMethodName},
		func(context.Context, any) (any, error) {
			unaryCalled = true
			return &externalproviderpb.EnrollResponse{}, nil
		},
	)
	if status.Code(err) != codes.Unauthenticated || unaryCalled {
		t.Fatalf("unary malformed credential result = called %v, error %v", unaryCalled, err)
	}

	streamCalled := false
	err = OpaqueBearerStreamInterceptor()(
		nil,
		&testServerStream{ctx: bearerContext("authorization", "Bearer not-an-opaque-credential")},
		&grpc.StreamServerInfo{FullMethod: externalproviderpb.ExternalProviderBroker_Connect_FullMethodName},
		func(any, grpc.ServerStream) error {
			streamCalled = true
			return nil
		},
	)
	if status.Code(err) != codes.Unauthenticated || streamCalled {
		t.Fatalf("stream malformed credential result = called %v, error %v", streamCalled, err)
	}
}

func TestOpaqueBearerInterceptorsPassValidCredentialEnvelope(t *testing.T) {
	ctx := bearerContext("authorization", "Bearer "+string(testCredential(0x73)))
	unaryCalled := false
	_, err := OpaqueBearerUnaryInterceptor()(
		ctx,
		&externalproviderpb.EnrollRequest{},
		&grpc.UnaryServerInfo{FullMethod: externalproviderpb.ExternalProviderBroker_Enroll_FullMethodName},
		func(context.Context, any) (any, error) {
			unaryCalled = true
			return &externalproviderpb.EnrollResponse{}, nil
		},
	)
	if err != nil || !unaryCalled {
		t.Fatalf("unary valid credential result = called %v, error %v", unaryCalled, err)
	}

	streamCalled := false
	err = OpaqueBearerStreamInterceptor()(
		nil,
		&testServerStream{ctx: ctx},
		&grpc.StreamServerInfo{FullMethod: externalproviderpb.ExternalProviderBroker_Connect_FullMethodName},
		func(any, grpc.ServerStream) error {
			streamCalled = true
			return nil
		},
	)
	if err != nil || !streamCalled {
		t.Fatalf("stream valid credential result = called %v, error %v", streamCalled, err)
	}
}

func TestBrokerMapsStoreAuthenticationFailure(t *testing.T) {
	store := &fakeStore{
		consume: func(context.Context, CredentialDigest, string, CredentialDigest) (Registration, error) {
			return Registration{}, ErrAuthenticationFailed
		},
	}
	broker, err := newBroker(store, bytes.NewReader(make([]byte, 64)), time.Minute)
	if err != nil {
		t.Fatalf("newBroker() error = %v", err)
	}
	_, err = broker.Enroll(bearerContext("authorization", "Bearer "+string(testCredential(0x11))), &externalproviderpb.EnrollRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Enroll() code = %v, want Unauthenticated", status.Code(err))
	}
}

type testServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *testServerStream) Context() context.Context { return s.ctx }

type credentialInspectingLogHandler struct {
	mu               sync.Mutex
	records          int
	sawAuthorization bool
}

func (*credentialInspectingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *credentialInspectingLogHandler) Handle(ctx context.Context, _ slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records++
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get("authorization")) != 0 {
		h.sawAuthorization = true
	}
	return nil
}

func (h *credentialInspectingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *credentialInspectingLogHandler) WithGroup(string) slog.Handler { return h }
