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

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestVerifySuccess(t *testing.T) {
	t.Parallel()

	kagent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/health" {
			t.Errorf("kagent request = %s %s, want GET /health", req.Method, req.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(kagent.Close)

	const (
		substrateAudience = "api.ate-system.svc"
		kubernetesToken   = "projected-kubernetes-token"
		namespace         = "ate-system"
		gatewayName       = "external-provider-broker"
		routeName         = "external-provider-broker"
	)
	jwt := makeJWT(t, `{"alg":"RS256","typ":"JWT"}`, `{"aud":"`+substrateAudience+`","sub":"system:serviceaccount:ate-system:ate-enrollment-admin"}`)
	control := &fakeControlServer{expectedToken: jwt}
	grpcEndpoint, grpcCAFile := startFakeControlServer(t, "api.ate-system.svc", control)

	var kubeMu sync.Mutex
	kubeRequests := make(map[string]int)
	kubernetes := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if got := req.Header.Get("Authorization"); got != "Bearer "+kubernetesToken {
			t.Errorf("Kubernetes authorization = %q, want projected Kubernetes token", got)
		}
		kubeMu.Lock()
		kubeRequests[req.URL.Path]++
		kubeMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/apis/gateway.networking.k8s.io/v1/namespaces/ate-system/gateways/external-provider-broker":
			fmt.Fprint(w, `{"metadata":{"name":"external-provider-broker","namespace":"ate-system","generation":7},"status":{"conditions":[{"type":"Programmed","status":"True","observedGeneration":7}]}}`)
		case "/apis/gateway.networking.k8s.io/v1/namespaces/ate-system/tlsroutes/external-provider-broker":
			fmt.Fprint(w, `{"metadata":{"name":"external-provider-broker","namespace":"ate-system","generation":9},"status":{"parents":[{"parentRef":{"group":"gateway.networking.k8s.io","kind":"Gateway","namespace":"ate-system","name":"external-provider-broker"},"conditions":[{"type":"Accepted","status":"True","observedGeneration":9},{"type":"ResolvedRefs","status":"True","observedGeneration":9}]}]}}`)
		default:
			http.NotFound(w, req)
		}
	}))
	t.Cleanup(kubernetes.Close)
	kubernetesCAFile := writeServerCA(t, kubernetes)

	cfg := config{
		kagentHealthURL:          kagent.URL + "/health",
		substrateEndpoint:        grpcEndpoint,
		substrateCAFile:          grpcCAFile,
		substrateServerName:      "api.ate-system.svc",
		tokenFile:                writeFile(t, "substrate-token", jwt+"\n"),
		expectedAudience:         substrateAudience,
		kubernetesAPIURL:         kubernetes.URL,
		kubernetesTokenFile:      writeFile(t, "kubernetes-token", kubernetesToken+"\n"),
		kubernetesCAFile:         kubernetesCAFile,
		gatewayNamespace:         namespace,
		gatewayName:              gatewayName,
		tlsRouteName:             routeName,
		requireGatewayProgrammed: true,
		timeout:                  5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	if err := verify(ctx, cfg); err != nil {
		t.Fatalf("verify() error = %v", err)
	}

	control.mu.Lock()
	defer control.mu.Unlock()
	if control.calls != 1 {
		t.Errorf("ListActorTemplates calls = %d, want 1", control.calls)
	}
	if control.pageSize != 1 {
		t.Errorf("ListActorTemplates page_size = %d, want 1", control.pageSize)
	}
	if control.tlsVersion != tls.VersionTLS13 {
		t.Errorf("Substrate TLS version = %#x, want TLS 1.3", control.tlsVersion)
	}
	kubeMu.Lock()
	defer kubeMu.Unlock()
	if len(kubeRequests) != 2 {
		t.Errorf("Kubernetes paths = %v, want exactly Gateway and TLSRoute", kubeRequests)
	}
	for path, calls := range kubeRequests {
		if calls != 1 {
			t.Errorf("Kubernetes path %q calls = %d, want 1", path, calls)
		}
	}
}

func TestRequireExactJWTAudience(t *testing.T) {
	t.Parallel()

	const expected = "api.ate-system.svc"
	tests := []struct {
		name    string
		header  string
		payload string
		mutate  func(string) string
		wantErr bool
	}{
		{name: "string audience", header: `{"alg":"RS256"}`, payload: `{"aud":"api.ate-system.svc"}`},
		{name: "one item array", header: `{"alg":"RS256"}`, payload: `{"aud":["api.ate-system.svc"]}`},
		{name: "wrong audience", header: `{"alg":"RS256"}`, payload: `{"aud":"other"}`, wantErr: true},
		{name: "additional audience", header: `{"alg":"RS256"}`, payload: `{"aud":["api.ate-system.svc","other"]}`, wantErr: true},
		{name: "non-string audience", header: `{"alg":"RS256"}`, payload: `{"aud":7}`, wantErr: true},
		{name: "missing audience", header: `{"alg":"RS256"}`, payload: `{"sub":"subject"}`, wantErr: true},
		{name: "duplicate audience", header: `{"alg":"RS256"}`, payload: `{"aud":"api.ate-system.svc","aud":"api.ate-system.svc"}`, wantErr: true},
		{name: "unsigned algorithm", header: `{"alg":"none"}`, payload: `{"aud":"api.ate-system.svc"}`, wantErr: true},
		{name: "padded segment", header: `{"alg":"RS256"}`, payload: `{"aud":"api.ate-system.svc"}`, mutate: func(token string) string {
			parts := strings.Split(token, ".")
			parts[1] += "="
			return strings.Join(parts, ".")
		}, wantErr: true},
		{name: "extra segment", header: `{"alg":"RS256"}`, payload: `{"aud":"api.ate-system.svc"}`, mutate: func(token string) string {
			return token + ".extra"
		}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			token := makeJWT(t, test.header, test.payload)
			if test.mutate != nil {
				token = test.mutate(token)
			}
			err := requireExactJWTAudience(token, expected)
			if (err != nil) != test.wantErr {
				t.Fatalf("requireExactJWTAudience() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestReadinessRequiresCurrentConditions(t *testing.T) {
	t.Parallel()

	baseGateway := gatewayResource{Metadata: metadata{Name: "broker", Namespace: "ate-system", Generation: 3}}
	baseGateway.Status.Conditions = []condition{{Type: "Programmed", Status: "True", ObservedGeneration: 3}}
	if err := validateGateway(baseGateway, "ate-system", "broker"); err != nil {
		t.Fatalf("validateGateway(current) error = %v", err)
	}
	staleGateway := baseGateway
	staleGateway.Status.Conditions = []condition{{Type: "Programmed", Status: "True", ObservedGeneration: 2}}
	if err := validateGateway(staleGateway, "ate-system", "broker"); err == nil {
		t.Fatal("validateGateway(stale) unexpectedly succeeded")
	}

	baseRoute := tlsRouteResource{Metadata: metadata{Name: "broker", Namespace: "ate-system", Generation: 5}}
	baseRoute.Status.Parents = []routeParentStatus{{
		ParentRef: parentReference{Name: "broker"},
		Conditions: []condition{
			{Type: "Accepted", Status: "True", ObservedGeneration: 5},
			{Type: "ResolvedRefs", Status: "True", ObservedGeneration: 5},
		},
	}}
	if err := validateTLSRoute(baseRoute, "ate-system", "broker", "broker"); err != nil {
		t.Fatalf("validateTLSRoute(current) error = %v", err)
	}
	staleRoute := baseRoute
	staleRoute.Status.Parents = []routeParentStatus{{
		ParentRef: parentReference{Name: "broker"},
		Conditions: []condition{
			{Type: "Accepted", Status: "True", ObservedGeneration: 5},
			{Type: "ResolvedRefs", Status: "True", ObservedGeneration: 4},
		},
	}}
	if err := validateTLSRoute(staleRoute, "ate-system", "broker", "broker"); err == nil {
		t.Fatal("validateTLSRoute(stale) unexpectedly succeeded")
	}
	wrongParent := baseRoute
	wrongParent.Status.Parents = []routeParentStatus{{
		ParentRef:  parentReference{Name: "other"},
		Conditions: baseRoute.Status.Parents[0].Conditions,
	}}
	if err := validateTLSRoute(wrongParent, "ate-system", "broker", "broker"); err == nil {
		t.Fatal("validateTLSRoute(wrong parent) unexpectedly succeeded")
	}
}

func TestSensitiveResponseDataIsNotReturned(t *testing.T) {
	t.Parallel()

	const secret = "do-not-log-this-token"
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, secret, http.StatusServiceUnavailable)
	}))
	t.Cleanup(health.Close)
	if err := checkKagentHealth(context.Background(), health.URL); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("health error = %v; want failure without response body", err)
	}

	control := &fakeControlServer{expectedToken: secret, responseError: status.Error(codes.Internal, "server echoed "+secret)}
	endpoint, caFile := startFakeControlServer(t, "api.ate-system.svc", control)
	err := checkSubstrate(context.Background(), config{
		substrateEndpoint:   endpoint,
		substrateCAFile:     caFile,
		substrateServerName: "api.ate-system.svc",
	}, secret)
	if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "server echoed") {
		t.Fatalf("Substrate error = %v; want sanitized gRPC status", err)
	}
}

func TestParseConfigRequiresExplicitTrustAndReadiness(t *testing.T) {
	t.Parallel()

	args := []string{
		"--kagent-health-url", "http://kagent-controller.kagent-system.svc.cluster.local:8083/health",
		"--substrate-endpoint", "ate-api-server.ate-system.svc.cluster.local:443",
		"--substrate-ca-file", "/var/run/secrets/substrate/ca.crt",
		"--substrate-server-name", "api.ate-system.svc",
		"--token-file", "/var/run/secrets/tokens/substrate-token",
		"--expected-audience", "api.ate-system.svc",
		"--kubernetes-token-file", "/var/run/secrets/tokens/kubernetes-token",
		"--kubernetes-ca-file", "/var/run/secrets/tokens/kubernetes-ca.crt",
		"--gateway-namespace", "ate-system",
		"--gateway-name", "external-provider-broker",
		"--tls-route-name", "external-provider-broker",
		"--require-gateway-programmed",
	}
	if _, err := parseConfig(args); err != nil {
		t.Fatalf("parseConfig(valid) error = %v", err)
	}

	withoutCA := removeFlagAndValue(args, "--substrate-ca-file")
	if _, err := parseConfig(withoutCA); err == nil {
		t.Fatal("parseConfig() succeeded without --substrate-ca-file")
	}
	withoutProgrammed := removeFlag(args, "--require-gateway-programmed")
	if _, err := parseConfig(withoutProgrammed); err == nil {
		t.Fatal("parseConfig() succeeded without --require-gateway-programmed")
	}
}

type fakeControlServer struct {
	ateapipb.UnimplementedControlServer

	mu            sync.Mutex
	expectedToken string
	responseError error
	calls         int
	pageSize      int32
	tlsVersion    uint16
}

func (server *fakeControlServer) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	md, ok := grpcmetadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != "Bearer "+server.expectedToken {
		return nil, status.Error(codes.Unauthenticated, "missing or invalid authorization")
	}
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "peer information is missing")
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, status.Error(codes.Internal, "TLS information is missing")
	}

	server.mu.Lock()
	server.calls++
	server.pageSize = req.GetPageSize()
	server.tlsVersion = tlsInfo.State.Version
	responseError := server.responseError
	server.mu.Unlock()
	if responseError != nil {
		return nil, responseError
	}
	if req.GetPageSize() != 1 {
		return nil, status.Error(codes.InvalidArgument, "page_size must be one")
	}
	return &ateapipb.ListActorTemplatesResponse{}, nil
}

func startFakeControlServer(t *testing.T, serverName string, control ateapipb.ControlServer) (string, string) {
	t.Helper()
	certificate, caPEM := makeServerCertificate(t, serverName)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
	})))
	ateapipb.RegisterControlServer(server, control)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String(), writeBytes(t, "substrate-ca.pem", caPEM)
}

func makeServerCertificate(t *testing.T, serverName string) (tls.Certificate, []byte) {
	t.Helper()
	now := time.Now()
	rootPublic, rootPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate root key: %v", err)
	}
	rootTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test root"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, rootPublic, rootPrivate)
	if err != nil {
		t.Fatalf("create root certificate: %v", err)
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatalf("parse root certificate: %v", err)
	}
	leafPublic, leafPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: serverName},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		DNSNames:     []string{serverName},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, root, leafPublic, rootPrivate)
	if err != nil {
		t.Fatalf("create leaf certificate: %v", err)
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	rootPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
	privateKey, err := x509.MarshalPKCS8PrivateKey(leafPrivate)
	if err != nil {
		t.Fatalf("marshal leaf private key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKey})
	certificate, err := tls.X509KeyPair(append(leafPEM, rootPEM...), keyPEM)
	if err != nil {
		t.Fatalf("tls.X509KeyPair() error = %v", err)
	}
	return certificate, rootPEM
}

func writeServerCA(t *testing.T, server *httptest.Server) string {
	t.Helper()
	certificate := server.Certificate()
	if certificate == nil {
		t.Fatal("httptest TLS server has no certificate")
	}
	return writeBytes(t, "kubernetes-ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}))
}

func makeJWT(t *testing.T, header, payload string) string {
	t.Helper()
	for _, raw := range []string{header, payload} {
		var value any
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			t.Fatalf("invalid test JWT JSON %q: %v", raw, err)
		}
	}
	encode := base64.RawURLEncoding.EncodeToString
	return encode([]byte(header)) + "." + encode([]byte(payload)) + "." + encode([]byte("test-signature"))
}

func writeFile(t *testing.T, name, value string) string {
	t.Helper()
	return writeBytes(t, name, []byte(value))
}

func writeBytes(t *testing.T, name string, value []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, value, 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", path, err)
	}
	return path
}

func removeFlagAndValue(args []string, name string) []string {
	result := make([]string, 0, len(args)-2)
	for i := 0; i < len(args); i++ {
		if args[i] == name {
			i++
			continue
		}
		result = append(result, args[i])
	}
	return result
}

func removeFlag(args []string, name string) []string {
	result := make([]string, 0, len(args)-1)
	for _, arg := range args {
		if arg != name {
			result = append(result, arg)
		}
	}
	return result
}
