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

package actoridentity

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/substratex509"
)

func TestExternalCertificateAuthorityIssuesBoundShortLivedCertificate(t *testing.T) {
	authority, now := testExternalCertificateAuthority(t)
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey() error = %v", err)
	}
	csr := externalActorCSR(t, &x509.CertificateRequest{}, privateKey)
	identity := &substratex509.ActorIdentity{
		Atespace:  "team-a",
		ActorName: "researcher",
		ActorUid:  "00000000-0000-4000-8000-000000000002",
		Purpose:   substratex509.ActorIdentityPurposeAtunnel,
	}
	binding := &substratex509.ExternalRouteBinding{
		Version:           substratex509.ExternalRouteBindingVersion,
		RegistrationUID:   "registration-a",
		SlotID:            "slot-a",
		WorkerUID:         "00000000-0000-4000-8000-000000000001",
		SessionGeneration: 7,
		ExecutionIdentity: "registration-a.slot-a.execution",
	}
	chain, err := authority.IssueExternalActorCertificate(context.Background(), csr, identity, binding)
	if err != nil || len(chain) == 0 {
		t.Fatalf("IssueExternalActorCertificate() = (%d certificates, %v)", len(chain), err)
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		t.Fatalf("x509.ParseCertificate() error = %v", err)
	}
	if lifetime := leaf.NotAfter.Sub(leaf.NotBefore); lifetime != ExternalActorCertificateLifetime {
		t.Fatalf("certificate lifetime = %v, want %v", lifetime, ExternalActorCertificateLifetime)
	}
	if remaining := leaf.NotAfter.Sub(now); remaining <= 0 || remaining > ExternalActorCertificateLifetime {
		t.Fatalf("certificate remaining lifetime = %v, want in (0, %v]", remaining, ExternalActorCertificateLifetime)
	}
	leafKey, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok || !leafKey.Equal(&privateKey.PublicKey) {
		t.Fatal("issued certificate does not contain the CSR public key")
	}
	gotIdentity, gotBinding, err := authority.VerifyExternalActorCertificate(context.Background(), chain)
	if err != nil || !reflect.DeepEqual(gotIdentity, identity) || !reflect.DeepEqual(gotBinding, binding) {
		t.Fatalf("VerifyExternalActorCertificate() = (%+v, %+v, %v)", gotIdentity, gotBinding, err)
	}
	authority.now = func() time.Time { return leaf.NotAfter.Add(time.Second) }
	if _, _, err := authority.VerifyExternalActorCertificate(context.Background(), chain); err == nil {
		t.Fatal("expired external Actor certificate was accepted")
	}
}

func TestExternalCertificateAuthorityRejectsClientIdentityAndUnsupportedKeys(t *testing.T) {
	authority, _ := testExternalCertificateAuthority(t)
	p256, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey() error = %v", err)
	}
	_, ed25519Key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey() error = %v", err)
	}
	tests := []struct {
		name string
		csr  []byte
	}{
		{name: "empty", csr: nil},
		{name: "oversized", csr: make([]byte, MaxExternalActorCSRBytes+1)},
		{name: "subject identity", csr: externalActorCSR(t, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "actor-chosen"}}, p256)},
		{name: "unsupported key", csr: externalActorCSR(t, &x509.CertificateRequest{}, ed25519Key)},
	}
	identity := &substratex509.ActorIdentity{Atespace: "team-a", ActorName: "actor-a", ActorUid: "actor-uid", Purpose: substratex509.ActorIdentityPurposeAtunnel}
	binding := &substratex509.ExternalRouteBinding{Version: 1, RegistrationUID: "registration-a", SlotID: "slot-a", WorkerUID: "00000000-0000-4000-8000-000000000001", SessionGeneration: 1, ExecutionIdentity: "execution-a"}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if chain, err := authority.IssueExternalActorCertificate(context.Background(), test.csr, identity, binding); err == nil || chain != nil {
				t.Fatalf("IssueExternalActorCertificate() = (%v, %v), want nil/error", chain, err)
			}
		})
	}
}

func TestExternalActorCSRAttributeEnvelope(t *testing.T) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey() error = %v", err)
	}
	withoutAttributes, err := x509.ParseCertificateRequest(externalActorCSR(t, &x509.CertificateRequest{}, privateKey))
	if err != nil {
		t.Fatalf("x509.ParseCertificateRequest() error = %v", err)
	}
	hasAttributes, err := externalActorCSRHasAttributes(withoutAttributes.RawTBSCertificateRequest)
	if err != nil || hasAttributes {
		t.Fatalf("externalActorCSRHasAttributes(empty) = (%v, %v), want false/nil", hasAttributes, err)
	}

	withExtension, err := x509.ParseCertificateRequest(externalActorCSR(t, &x509.CertificateRequest{
		ExtraExtensions: []pkix.Extension{{Id: []int{1, 2, 3, 4}, Value: []byte{5}}},
	}, privateKey))
	if err != nil {
		t.Fatalf("x509.ParseCertificateRequest(extension) error = %v", err)
	}
	hasAttributes, err = externalActorCSRHasAttributes(withExtension.RawTBSCertificateRequest)
	if err != nil || !hasAttributes {
		t.Fatalf("externalActorCSRHasAttributes(extension) = (%v, %v), want true/nil", hasAttributes, err)
	}
}

func testExternalCertificateAuthority(t *testing.T) (*ExternalCertificateAuthority, time.Time) {
	t.Helper()
	ca, err := localca.GenerateED25519CA("external-actor-test")
	if err != nil {
		t.Fatalf("localca.GenerateED25519CA() error = %v", err)
	}
	wire, err := localca.Marshal(&localca.Pool{CAs: []*localca.CA{ca}})
	if err != nil {
		t.Fatalf("localca.Marshal() error = %v", err)
	}
	poolFile := filepath.Join(t.TempDir(), "pool.json")
	if err := os.WriteFile(poolFile, wire, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	authority, err := NewExternalCertificateAuthority(poolFile)
	if err != nil {
		t.Fatalf("NewExternalCertificateAuthority() error = %v", err)
	}
	now := time.Now().UTC().Add(time.Minute).Truncate(time.Second)
	authority.now = func() time.Time { return now }
	return authority, now
}

func externalActorCSR(t *testing.T, template *x509.CertificateRequest, privateKey any) []byte {
	t.Helper()
	der, err := x509.CreateCertificateRequest(rand.Reader, template, privateKey)
	if err != nil {
		t.Fatalf("x509.CreateCertificateRequest() error = %v", err)
	}
	return der
}
