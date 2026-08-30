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
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"
)

func testCredential(fill byte) []byte {
	raw := bytes.Repeat([]byte{fill}, credentialEntropyBytes)
	return []byte(base64.RawURLEncoding.EncodeToString(raw))
}

func bearerContext(key string, values ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.MD{key: values})
}

func TestGenerateCredentialUsesCanonicalBase64URL(t *testing.T) {
	credential, err := generateCredential(bytes.NewReader(bytes.Repeat([]byte{0xff}, credentialEntropyBytes)))
	if err != nil {
		t.Fatalf("generateCredential() error = %v", err)
	}
	if got, want := len(credential), credentialEncodedBytes; got != want {
		t.Fatalf("credential length = %d, want %d", got, want)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(string(credential))
	if err != nil {
		t.Fatalf("decoding credential: %v", err)
	}
	if got, want := len(decoded), credentialEntropyBytes; got != want {
		t.Errorf("credential entropy length = %d, want %d", got, want)
	}
	if bytes.ContainsAny(credential, "+/=") {
		t.Errorf("credential %q is not unpadded base64url", credential)
	}
}

func TestCredentialDigestDomainsAreDistinct(t *testing.T) {
	credential := testCredential(0x42)
	digests := map[CredentialDigest]bool{
		digestCredential(enrollmentDigestDomain, credential): true,
		digestCredential(refreshDigestDomain, credential):    true,
		digestCredential(sessionDigestDomain, credential):    true,
	}
	if got, want := len(digests), 3; got != want {
		t.Fatalf("distinct credential digests = %d, want %d", got, want)
	}
}

func TestBearerCredentialStrictParsing(t *testing.T) {
	valid := string(testCredential(0x13))
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr bool
	}{
		{name: "valid", ctx: bearerContext("authorization", "Bearer "+valid)},
		{name: "missing metadata", ctx: context.Background(), wantErr: true},
		{name: "two values", ctx: bearerContext("authorization", "Bearer "+valid, "Bearer "+valid), wantErr: true},
		{name: "wrong scheme case", ctx: bearerContext("authorization", "bearer "+valid), wantErr: true},
		{name: "leading space", ctx: bearerContext("authorization", " Bearer "+valid), wantErr: true},
		{name: "trailing space", ctx: bearerContext("authorization", "Bearer "+valid+" "), wantErr: true},
		{name: "padded", ctx: bearerContext("authorization", "Bearer "+valid+"="), wantErr: true},
		{name: "too short", ctx: bearerContext("authorization", "Bearer "+valid[:len(valid)-1]), wantErr: true},
		{name: "invalid alphabet", ctx: bearerContext("authorization", "Bearer "+valid[:len(valid)-1]+"+"), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := bearerCredential(test.ctx)
			if test.wantErr {
				if !errors.Is(err, ErrAuthenticationFailed) {
					t.Fatalf("bearerCredential() error = %v, want ErrAuthenticationFailed", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("bearerCredential() error = %v", err)
			}
			if string(got) != valid {
				t.Errorf("bearerCredential() = %q, want %q", got, valid)
			}
		})
	}
}

func TestBearerCredentialRejectsNonLowercaseMetadataKey(t *testing.T) {
	valid := string(testCredential(0x13))
	_, err := bearerCredentialFromMetadata(metadata.MD{"Authorization": {"Bearer " + valid}})
	if !errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("bearerCredentialFromMetadata() error = %v, want ErrAuthenticationFailed", err)
	}
}

func TestIdentityGrammar(t *testing.T) {
	valid := []string{
		"a",
		"registration-a",
		"A._~-9",
		"a" + strings.Repeat("_", maxIdentityBytes-2) + "z",
	}
	for _, identity := range valid {
		if !IsValidIdentity(identity) {
			t.Errorf("IsValidIdentity(%q) = false, want true", identity)
		}
	}
	invalid := []string{
		"",
		"-starts-with-symbol",
		"ends-with-symbol_",
		"contains space",
		"contains/slash",
		"ü",
		"a" + strings.Repeat("_", maxIdentityBytes-1) + "z",
	}
	for _, identity := range invalid {
		if IsValidIdentity(identity) {
			t.Errorf("IsValidIdentity(%q) = true, want false", identity)
		}
	}
}

func TestSessionClaimContainsOnlyNonSecretAuthority(t *testing.T) {
	claimType := reflect.TypeFor[SessionClaim]()
	want := []struct {
		name   string
		typeOf reflect.Type
	}{
		{name: "Registration", typeOf: reflect.TypeFor[Registration]()},
		{name: "Generation", typeOf: reflect.TypeFor[uint64]()},
	}
	if claimType.NumField() != len(want) {
		t.Fatalf("SessionClaim fields = %d, want %d", claimType.NumField(), len(want))
	}
	for index, expected := range want {
		field := claimType.Field(index)
		if field.Name != expected.name || field.Type != expected.typeOf {
			t.Errorf("SessionClaim field %d = %s %v, want %s %v", index, field.Name, field.Type, expected.name, expected.typeOf)
		}
	}
}
