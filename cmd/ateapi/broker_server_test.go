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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestExternalProviderBrokerServerIsDedicatedTLSAndMetadataOnly(t *testing.T) {
	bundlePath, clientCredentials := writeBrokerServerTestCredentialBundle(t)
	serverCredentials, err := externalProviderBrokerServerCredentials(bundlePath)
	if err != nil {
		t.Fatalf("externalProviderBrokerServerCredentials() error = %v", err)
	}
	store := &brokerServerTestStore{}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	server, err := newExternalProviderBrokerGRPCServer(store, serverCredentials, time.Minute, logger)
	if err != nil {
		t.Fatalf("newExternalProviderBrokerGRPCServer() error = %v", err)
	}
	serviceInfo := server.GetServiceInfo()
	if len(serviceInfo) != 1 {
		t.Fatalf("registered services = %v, want only ExternalProviderBroker", serviceInfo)
	}
	if _, ok := serviceInfo[externalproviderpb.ExternalProviderBroker_ServiceDesc.ServiceName]; !ok {
		t.Fatalf("ExternalProviderBroker is not registered: %v", serviceInfo)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	conn, err := grpc.NewClient(
		"passthrough:///broker.test",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		}),
		grpc.WithTransportCredentials(clientCredentials),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	defer conn.Close()
	client := externalproviderpb.NewExternalProviderBrokerClient(conn)

	enrollmentCredential := brokerServerTestCredential(0x41)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+enrollmentCredential)
	response, err := client.Enroll(ctx, &externalproviderpb.EnrollRequest{})
	if err != nil {
		t.Fatalf("Enroll() with opaque non-JWT credential error = %v", err)
	}
	if response.GetRegistrationUid() != "registration-test" || len(response.GetRefreshCredential()) == 0 {
		t.Fatalf("Enroll() response = %+v", response)
	}
	if store.consumeCalls.Load() != 1 {
		t.Fatalf("ConsumeExternalProviderEnrollment calls = %d, want 1", store.consumeCalls.Load())
	}

	_, err = client.Enroll(context.Background(), &externalproviderpb.EnrollRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Enroll() without Broker credential code = %v, want Unauthenticated", status.Code(err))
	}
	sessionCredential := brokerServerTestCredential(0x42)
	connectCtx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+sessionCredential)
	stream, err := client.Connect(connectCtx)
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("Connect() code = %v, want Unimplemented", status.Code(err))
	}
	logOutput := logs.String()
	for _, secret := range []string{enrollmentCredential, string(response.GetRefreshCredential()), sessionCredential} {
		if strings.Contains(logOutput, secret) {
			t.Fatalf("Broker logs contain credential %q: %s", secret, logOutput)
		}
	}
	for _, forbidden := range []string{"authorization", "registration-test", "refresh_credential", "request", "response"} {
		if strings.Contains(logOutput, forbidden) {
			t.Fatalf("Broker logs contain payload field %q: %s", forbidden, logOutput)
		}
	}
	if !strings.Contains(logOutput, externalproviderpb.ExternalProviderBroker_Enroll_FullMethodName) ||
		!strings.Contains(logOutput, codes.Unauthenticated.String()) {
		t.Fatalf("Broker metadata-only logs lack method/status: %s", logOutput)
	}

	server.GracefulStop()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve() after GracefulStop error = %v", err)
		}
	case <-time.After(5 * time.Second):
		server.Stop()
		t.Fatal("Serve() did not return after GracefulStop")
	}
}

func TestExternalProviderBrokerServerRequiresExplicitEnablementAndTLS(t *testing.T) {
	runtime, err := startExternalProviderBroker(context.Background(), nil, externalProviderBrokerConfig{}, nil)
	if err != nil || runtime != nil {
		t.Fatalf("disabled Broker runtime = %v, error %v; want nil, nil", runtime, err)
	}
	if _, err := externalProviderBrokerServerCredentials(""); err == nil {
		t.Fatal("externalProviderBrokerServerCredentials() accepted an empty bundle")
	}
	if _, err := newExternalProviderBrokerGRPCServer(&brokerServerTestStore{}, nil, time.Minute, nil); err == nil {
		t.Fatal("newExternalProviderBrokerGRPCServer() accepted missing TLS credentials")
	}
}

func TestDrainOnShutdownStopsPrimaryAndBrokerServers(t *testing.T) {
	originalDelay, originalTimeout := *drainDelay, *drainTimeout
	*drainDelay, *drainTimeout = 0, time.Second
	defer func() { *drainDelay, *drainTimeout = originalDelay, originalTimeout }()

	servers := []*grpc.Server{grpc.NewServer(), grpc.NewServer()}
	serveResults := make(chan error, len(servers))
	for _, server := range servers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen() error = %v", err)
		}
		go func(server *grpc.Server, listener net.Listener) {
			serveResults <- server.Serve(listener)
		}(server, listener)
	}

	ctx, cancel := context.WithCancel(context.Background())
	readiness := &serverboot.Readiness{}
	done := drainOnShutdown(ctx, readiness, servers...)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		for _, server := range servers {
			server.Stop()
		}
		t.Fatal("drainOnShutdown() did not stop both servers")
	}
	if readiness.Ready() {
		t.Fatal("readiness remained ready after shutdown")
	}
	for range servers {
		select {
		case err := <-serveResults:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Fatalf("Serve() after drain error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a gRPC server did not return after drain")
		}
	}
}

type brokerServerTestStore struct {
	consumeCalls atomic.Int32
}

func (*brokerServerTestStore) CreateExternalProviderEnrollment(context.Context, string, externalprovider.CredentialDigest, externalprovider.Scope, time.Duration) (externalprovider.Enrollment, error) {
	return externalprovider.Enrollment{}, errors.New("unexpected CreateExternalProviderEnrollment call")
}

func (s *brokerServerTestStore) ConsumeExternalProviderEnrollment(context.Context, externalprovider.CredentialDigest, string, externalprovider.CredentialDigest) (externalprovider.Registration, error) {
	s.consumeCalls.Add(1)
	return externalprovider.Registration{UID: "registration-test"}, nil
}

func (*brokerServerTestStore) RotateExternalProviderSession(context.Context, string, externalprovider.CredentialDigest, externalprovider.CredentialDigest, time.Duration) (externalprovider.SessionAuthorization, error) {
	return externalprovider.SessionAuthorization{}, errors.New("unexpected RotateExternalProviderSession call")
}

func (*brokerServerTestStore) ClaimExternalProviderSession(context.Context, string, externalprovider.CredentialDigest) (externalprovider.SessionClaim, error) {
	return externalprovider.SessionClaim{}, errors.New("unexpected ClaimExternalProviderSession call")
}

func (*brokerServerTestStore) RevokeExternalProviderEnrollment(context.Context, string) error {
	return errors.New("unexpected RevokeExternalProviderEnrollment call")
}

func (*brokerServerTestStore) RevokeExternalProviderRegistration(context.Context, string) error {
	return errors.New("unexpected RevokeExternalProviderRegistration call")
}

func brokerServerTestCredential(fill byte) string {
	raw := bytes.Repeat([]byte{fill}, 32)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func writeBrokerServerTestCredentialBundle(t *testing.T) (string, credentials.TransportCredentials) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey() error = %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "broker.test"},
		DNSNames:     []string{"broker.test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("x509.CreateCertificate() error = %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("x509.MarshalPKCS8PrivateKey() error = %v", err)
	}
	bundle := append(
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})...,
	)
	bundlePath := filepath.Join(t.TempDir(), "broker-credential-bundle.pem")
	if err := os.WriteFile(bundlePath, bundle, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	roots := x509.NewCertPool()
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		t.Fatalf("x509.ParseCertificate() error = %v", err)
	}
	roots.AddCert(certificate)
	return bundlePath, credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: "broker.test",
	})
}
