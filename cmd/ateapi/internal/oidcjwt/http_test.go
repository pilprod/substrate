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

package oidcjwt

import (
	"io"
	"net/http"
	"os"
	"testing"
)

func TestIssuerScopedURL(t *testing.T) {
	for _, tt := range []struct {
		name   string
		url    string
		issuer string
		want   bool
	}{
		{
			name:   "same host root issuer discovery",
			url:    "https://kubernetes.default.svc/.well-known/openid-configuration",
			issuer: "https://kubernetes.default.svc",
			want:   true,
		},
		{
			name:   "same host path issuer jwks",
			url:    "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c/jwks",
			issuer: "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
			want:   true,
		},
		{
			name:   "same host sibling path",
			url:    "https://container.googleapis.com/v1/projects/p/locations/l/clusters/other/jwks",
			issuer: "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
			want:   false,
		},
		{
			name:   "prefix lookalike",
			url:    "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c-attacker/jwks",
			issuer: "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
			want:   false,
		},
		{
			name:   "different host",
			url:    "https://attacker.example/.well-known/openid-configuration",
			issuer: "https://kubernetes.default.svc",
			want:   false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := issuerScopedURL(tt.url, tt.issuer); got != tt.want {
				t.Fatalf("issuerScopedURL(%q, %q) = %v, want %v", tt.url, tt.issuer, got, tt.want)
			}
		})
	}
}

func TestK8sServiceAccountIssuerDiscoveryTransport(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	var gotAuth string
	transport := &issuerDiscoveryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotAuth = req.Header.Get("Authorization")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(nil),
				Header:     make(http.Header),
			}, nil
		}),
		tokenFile: tokenFile,
		issuer:    "https://kubernetes.default.svc",
	}

	req, err := http.NewRequest(http.MethodGet, "https://kubernetes.default.svc/.well-known/openid-configuration", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := transport.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("Authorization = %q, want Bearer test-token", gotAuth)
	}
}

func TestK8sServiceAccountIssuerDiscoveryTransportDoesNotSendTokenToAnyHostJWKSPath(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	var gotAuth string
	transport := &issuerDiscoveryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotAuth = req.Header.Get("Authorization")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(nil),
				Header:     make(http.Header),
			}, nil
		}),
		tokenFile: tokenFile,
		issuer:    "https://kubernetes.default.svc",
	}

	req, err := http.NewRequest(http.MethodGet, "https://172.18.0.2:6443/openid/v1/jwks", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := transport.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
}

func TestK8sServiceAccountIssuerDiscoveryTransportDoesNotSendTokenToArbitraryURL(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	var gotAuth string
	transport := &issuerDiscoveryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotAuth = req.Header.Get("Authorization")
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(nil),
				Header:     make(http.Header),
			}, nil
		}),
		tokenFile: tokenFile,
		issuer:    "https://kubernetes.default.svc",
	}

	req, err := http.NewRequest(http.MethodGet, "https://attacker.example/.well-known/openid-configuration", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	// Redirect handling can copy this sensitive header from an earlier request.
	// The transport must remove it when the new target is outside its allowlist.
	req.Header.Set("Authorization", "Bearer copied-by-redirect")
	if _, err := transport.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}
}

func TestK8sServiceAccountIssuerDiscoveryTransportSendsRotatingTokenOnlyToExactOverrides(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	writeToken := func(token string) {
		t.Helper()
		if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
			t.Fatalf("write token: %v", err)
		}
	}
	writeToken("token-one")

	var gotAuth []string
	transport := &issuerDiscoveryTransport{
		base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			gotAuth = append(gotAuth, req.Header.Get("Authorization"))
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(nil),
				Header:     make(http.Header),
			}, nil
		}),
		tokenFile: tokenFile,
		issuer:    "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c",
		overrideURLs: []string{
			"https://kubernetes.default.svc/.well-known/openid-configuration",
			"https://kubernetes.default.svc/openid/v1/jwks",
		},
	}

	request := func(rawURL string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, rawURL, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if _, err := transport.RoundTrip(req); err != nil {
			t.Fatalf("RoundTrip(%q) error = %v", rawURL, err)
		}
	}
	request("https://kubernetes.default.svc/.well-known/openid-configuration")
	writeToken("token-two")
	request("https://kubernetes.default.svc/openid/v1/jwks")
	request("https://kubernetes.default.svc/openid/v1/jwks?redirected=true")

	want := []string{"Bearer token-one", "Bearer token-two", ""}
	if len(gotAuth) != len(want) {
		t.Fatalf("captured Authorization headers = %q, want %q", gotAuth, want)
	}
	for i := range want {
		if gotAuth[i] != want[i] {
			t.Fatalf("Authorization[%d] = %q, want %q", i, gotAuth[i], want[i])
		}
	}
}

func TestBuildJWTIssuerDiscoveryClientUsesDefaultTransportWithoutDiscoveryToken(t *testing.T) {
	client, err := NewHTTPClient("https://accounts.google.com", EndpointOverrides{}, "", "")
	if err != nil {
		t.Fatalf("NewHTTPClient() error = %v", err)
	}
	if client.Timeout == 0 {
		t.Fatalf("client timeout = 0, want nonzero timeout")
	}
	if _, ok := client.Transport.(*issuerDiscoveryTransport); ok {
		t.Fatalf("provider without discovery token should not use token transport")
	}
}

func TestNewHTTPClientRequiresCAWithDiscoveryToken(t *testing.T) {
	if _, err := NewHTTPClient("https://kubernetes.default.svc", EndpointOverrides{}, "", "/token"); err == nil {
		t.Fatal("NewHTTPClient() with a discovery token and no CA succeeded")
	}
}

func TestNewHTTPClientRejectsInvalidEndpointOverrides(t *testing.T) {
	for _, tt := range []struct {
		name      string
		overrides EndpointOverrides
	}{
		{
			name:      "partial",
			overrides: EndpointOverrides{DiscoveryURL: "https://kubernetes.default.svc/.well-known/openid-configuration"},
		},
		{
			name: "insecure discovery",
			overrides: EndpointOverrides{
				DiscoveryURL: "http://kubernetes.default.svc/.well-known/openid-configuration",
				JWKSURL:      "https://kubernetes.default.svc/openid/v1/jwks",
			},
		},
		{
			name: "JWKS query",
			overrides: EndpointOverrides{
				DiscoveryURL: "https://kubernetes.default.svc/.well-known/openid-configuration",
				JWKSURL:      "https://kubernetes.default.svc/openid/v1/jwks?token=secret",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewHTTPClient("https://issuer.example", tt.overrides, "", ""); err == nil {
				t.Fatal("NewHTTPClient() succeeded, want error")
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
