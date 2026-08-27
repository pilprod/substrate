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
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	credentialEntropyBytes = 32
	credentialEncodedBytes = 43

	enrollmentDigestDomain = "agent-substrate/external-provider/enrollment/v1\x00"
	refreshDigestDomain    = "agent-substrate/external-provider/refresh/v1\x00"
	sessionDigestDomain    = "agent-substrate/external-provider/session/v1\x00"
)

func generateCredential(random io.Reader) ([]byte, error) {
	raw := make([]byte, credentialEntropyBytes)
	if _, err := io.ReadFull(random, raw); err != nil {
		return nil, fmt.Errorf("reading credential entropy: %w", err)
	}
	encoded := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(encoded, raw)
	clear(raw)
	return encoded, nil
}

func digestCredential(domain string, credential []byte) CredentialDigest {
	h := sha256.New()
	_, _ = io.WriteString(h, domain)
	_, _ = h.Write(credential)
	var digest CredentialDigest
	copy(digest[:], h.Sum(nil))
	return digest
}

func bearerCredential(ctx context.Context) ([]byte, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, ErrAuthenticationFailed
	}
	return bearerCredentialFromMetadata(md)
}

func bearerCredentialFromMetadata(md metadata.MD) ([]byte, error) {
	var values []string
	for key, candidate := range md {
		if strings.EqualFold(key, "authorization") {
			if key != "authorization" {
				return nil, ErrAuthenticationFailed
			}
			values = append(values, candidate...)
		}
	}
	if len(values) != 1 {
		return nil, ErrAuthenticationFailed
	}

	const prefix = "Bearer "
	value := values[0]
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+credentialEncodedBytes {
		return nil, ErrAuthenticationFailed
	}
	credential := []byte(value[len(prefix):])
	decoded, err := base64.RawURLEncoding.DecodeString(string(credential))
	if err != nil || len(decoded) != credentialEntropyBytes || base64.RawURLEncoding.EncodeToString(decoded) != string(credential) {
		clear(decoded)
		clear(credential)
		return nil, ErrAuthenticationFailed
	}
	clear(decoded)
	return credential, nil
}
