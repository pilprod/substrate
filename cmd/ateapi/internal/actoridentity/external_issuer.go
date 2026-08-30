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
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path"
	"slices"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/substratex509"
)

const (
	// MaxExternalActorCSRBytes bounds the credential material copied out of an
	// OpenChannel frame before any parsing or signing work occurs.
	MaxExternalActorCSRBytes = 16 << 10
	// ExternalActorCertificateLifetime is deliberately much shorter than the
	// in-cluster atelet certificate. Online route authorization remains the
	// actual generation fence; expiry only bounds residual credential lifetime.
	ExternalActorCertificateLifetime = 5 * time.Minute
	maxExternalActorChainBytes       = 256 << 10
	maxExternalActorChainLength      = 8
	externalActorClockSkew           = 30 * time.Second
)

// ExternalCertificateAuthority is a separate issuance and verification path
// for ExternalSlot actor certificates. It has no RPC surface and does not
// change or bypass MintCert's atelet authentication.
type ExternalCertificateAuthority struct {
	poolFile string
	now      func() time.Time
}

func NewExternalCertificateAuthority(poolFile string) (*ExternalCertificateAuthority, error) {
	if poolFile == "" {
		return nil, fmt.Errorf("external actor certificate authority pool is required")
	}
	return &ExternalCertificateAuthority{poolFile: poolFile, now: time.Now}, nil
}

// IssueExternalActorCertificate consumes only the CSR public key. Actor and
// route identity must already have been derived from current server state.
func (a *ExternalCertificateAuthority) IssueExternalActorCertificate(
	ctx context.Context,
	csrDER []byte,
	identity *substratex509.ActorIdentity,
	binding *substratex509.ExternalRouteBinding,
) ([][]byte, error) {
	if a == nil || a.now == nil || ctx == nil {
		return nil, fmt.Errorf("external actor certificate authority is unavailable")
	}
	if identity == nil || binding == nil {
		return nil, fmt.Errorf("external actor identity and route binding are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	csr, err := parseExternalActorCSR(csrDER)
	if err != nil {
		return nil, err
	}
	ca, err := a.loadCA()
	if err != nil {
		return nil, err
	}

	now := a.now()
	notBefore := now.Add(-externalActorClockSkew)
	notAfter := notBefore.Add(ExternalActorCertificateLifetime)
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, fmt.Errorf("generating actor certificate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		URIs: []*url.URL{{
			Scheme: "spiffe",
			Host:   "substrate-actor.local",
			Path:   path.Join("atespace", identity.Atespace, "actor", identity.ActorName),
		}},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		Issuer: pkix.Name{
			CommonName: "api.ate-system.svc.cluster.local",
		},
	}
	if err := substratex509.AddActorIdentityToCertificate(identity, template); err != nil {
		return nil, fmt.Errorf("building external actor identity: %w", err)
	}
	if err := substratex509.AddExternalRouteBindingToCertificate(binding, template); err != nil {
		return nil, fmt.Errorf("building external route binding: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, template, ca.RootCertificate, csr.PublicKey, ca.SigningKey)
	if err != nil {
		return nil, fmt.Errorf("signing external actor certificate: %w", err)
	}
	chain := make([][]byte, 1, 1+len(ca.IntermediateCertificates))
	chain[0] = leafDER
	for _, certificate := range ca.IntermediateCertificates {
		chain = append(chain, slices.Clone(certificate.Raw))
	}
	return chain, nil
}

// VerifyExternalActorCertificate independently verifies the exact chain sent
// by atenet and returns only signed extension state.
func (a *ExternalCertificateAuthority) VerifyExternalActorCertificate(
	ctx context.Context,
	chainDER [][]byte,
) (*substratex509.ActorIdentity, *substratex509.ExternalRouteBinding, error) {
	if a == nil || a.now == nil || ctx == nil {
		return nil, nil, fmt.Errorf("external actor certificate authority is unavailable")
	}
	if len(chainDER) == 0 || len(chainDER) > maxExternalActorChainLength {
		return nil, nil, fmt.Errorf("external actor certificate chain length is invalid")
	}
	var total int
	chain := make([]*x509.Certificate, len(chainDER))
	for index, der := range chainDER {
		if len(der) == 0 || total > maxExternalActorChainBytes-len(der) {
			return nil, nil, fmt.Errorf("external actor certificate chain is too large")
		}
		total += len(der)
		certificate, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, nil, fmt.Errorf("parsing external actor certificate: %w", err)
		}
		chain[index] = certificate
	}
	ca, err := a.loadCA()
	if err != nil {
		return nil, nil, err
	}
	leaf := chain[0]
	if leaf.IsCA || !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		return nil, nil, fmt.Errorf("external actor leaf is not a client-auth end-entity certificate")
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca.RootCertificate)
	intermediates := x509.NewCertPool()
	for _, certificate := range chain[1:] {
		intermediates.AddCert(certificate)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		CurrentTime:   a.now(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return nil, nil, fmt.Errorf("verifying external actor certificate: %w", err)
	}
	identity, err := substratex509.ActorIdentityFromCertificate(leaf)
	if err != nil || identity == nil || identity.Purpose != substratex509.ActorIdentityPurposeAtunnel {
		return nil, nil, fmt.Errorf("external actor certificate has no valid atunnel identity")
	}
	binding, err := substratex509.ExternalRouteBindingFromCertificate(leaf)
	if err != nil || binding == nil {
		return nil, nil, fmt.Errorf("external actor certificate has no valid route binding")
	}
	return identity, binding, nil
}

func (a *ExternalCertificateAuthority) loadCA() (*localca.CA, error) {
	poolBytes, err := os.ReadFile(a.poolFile)
	if err != nil {
		return nil, fmt.Errorf("reading actor CA pool: %w", err)
	}
	pool, err := localca.Unmarshal(poolBytes)
	if err != nil || len(pool.CAs) == 0 {
		return nil, fmt.Errorf("loading actor CA pool")
	}
	ca := pool.CAs[0]
	if err := ca.Validate(); err != nil {
		return nil, fmt.Errorf("validating actor CA: %w", err)
	}
	return ca, nil
}

func parseExternalActorCSR(der []byte) (*x509.CertificateRequest, error) {
	if len(der) == 0 || len(der) > MaxExternalActorCSRBytes {
		return nil, fmt.Errorf("external actor CSR length is invalid")
	}
	csr, err := x509.ParseCertificateRequest(der)
	if err != nil {
		return nil, fmt.Errorf("parsing external actor CSR: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("verifying external actor CSR signature: %w", err)
	}
	hasAttributes, err := externalActorCSRHasAttributes(csr.RawTBSCertificateRequest)
	if err != nil {
		return nil, err
	}
	// The provider is permitted to choose a key and nothing else. Rejecting
	// rather than silently copying request attributes keeps that boundary
	// obvious during future signer refactors.
	if csr.Subject.String() != "" || len(csr.DNSNames) != 0 || len(csr.EmailAddresses) != 0 ||
		len(csr.IPAddresses) != 0 || len(csr.URIs) != 0 || len(csr.Extensions) != 0 ||
		len(csr.ExtraExtensions) != 0 || hasAttributes {
		return nil, fmt.Errorf("external actor CSR may contain only a public key")
	}
	publicKey, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok || publicKey.Curve != elliptic.P256() {
		return nil, fmt.Errorf("external actor CSR must use ECDSA P-256")
	}
	if _, err := publicKey.ECDH(); err != nil {
		return nil, fmt.Errorf("external actor CSR must use ECDSA P-256")
	}
	return csr, nil
}

// externalActorCSRHasAttributes parses only the bounded request-info envelope
// so arbitrary PKCS#10 attributes remain forbidden without depending on
// x509.CertificateRequest.Attributes, whose typed view is deprecated.
func externalActorCSRHasAttributes(raw []byte) (bool, error) {
	var requestInfo struct {
		Version    int
		Subject    asn1.RawValue
		PublicKey  asn1.RawValue
		Attributes []asn1.RawValue `asn1:"tag:0"`
	}
	rest, err := asn1.Unmarshal(raw, &requestInfo)
	if err != nil || len(rest) != 0 {
		return false, fmt.Errorf("parsing external actor CSR request info")
	}
	return len(requestInfo.Attributes) != 0, nil
}
