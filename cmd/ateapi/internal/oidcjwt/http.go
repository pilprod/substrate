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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// EndpointOverrides routes discovery and JWKS requests independently of the
// issuer URL. Both fields are either set together or left empty by validated
// authentication configuration.
type EndpointOverrides struct {
	DiscoveryURL string
	JWKSURL      string
}

// NewHTTPClient returns a client for OIDC discovery and JWKS requests.
func NewHTTPClient(issuer string, overrides EndpointOverrides, certificateAuthorityFile, discoveryTokenFile string) (*http.Client, error) {
	if err := overrides.validate(); err != nil {
		return nil, err
	}
	if discoveryTokenFile != "" && certificateAuthorityFile == "" {
		return nil, fmt.Errorf("discovery token file requires a certificate authority file")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if certificateAuthorityFile != "" {
		ca, err := os.ReadFile(certificateAuthorityFile)
		if err != nil {
			return nil, fmt.Errorf("read certificate authority file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("certificate authority file %q contains no certificates", certificateAuthorityFile)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	}
	var roundTripper http.RoundTripper = transport
	if discoveryTokenFile != "" {
		roundTripper = &issuerDiscoveryTransport{
			base:         transport,
			tokenFile:    discoveryTokenFile,
			issuer:       issuer,
			overrideURLs: []string{overrides.DiscoveryURL, overrides.JWKSURL},
		}
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: roundTripper}, nil
}

func (o EndpointOverrides) validate() error {
	if (o.DiscoveryURL == "") != (o.JWKSURL == "") {
		return fmt.Errorf("discovery URL and JWKS URL overrides must be configured together")
	}
	if o.DiscoveryURL == "" {
		return nil
	}
	if err := validateOverrideURL("discovery URL", o.DiscoveryURL); err != nil {
		return err
	}
	if err := validateOverrideURL("JWKS URL", o.JWKSURL); err != nil {
		return err
	}
	return nil
}

func validateOverrideURL(name, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil || !u.IsAbs() || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("%s override must be an absolute HTTPS URL without userinfo, query, or fragment", name)
	}
	return nil
}

// issuerDiscoveryTransport injects a bearer token for requests within the
// configured issuer or at an exact configured override endpoint. It reads the
// token file on every request so rotation is handled automatically.
type issuerDiscoveryTransport struct {
	base         http.RoundTripper
	tokenFile    string
	issuer       string
	overrideURLs []string
}

func (t *issuerDiscoveryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// A redirect can copy an Authorization header from the prior request. Clone
	// every request and remove it before applying the endpoint allowlist.
	req = req.Clone(req.Context())
	req.Header.Del("Authorization")
	if issuerScopedURL(req.URL.String(), t.issuer) || exactConfiguredURL(req.URL.String(), t.overrideURLs) {
		token, err := os.ReadFile(t.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("read discovery token file: %w", err)
		}
		trimmed := strings.TrimSpace(string(token))
		if trimmed == "" {
			return nil, fmt.Errorf("discovery token file %q is empty", t.tokenFile)
		}
		req.Header.Set("Authorization", "Bearer "+trimmed)
	}
	return t.base.RoundTrip(req)
}

func issuerScopedURL(rawURL, issuer string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	issuerURL, err := url.Parse(issuer)
	if err != nil {
		return false
	}
	if !strings.EqualFold(u.Scheme, issuerURL.Scheme) || !strings.EqualFold(u.Host, issuerURL.Host) {
		return false
	}
	issuerPath := strings.TrimRight(issuerURL.EscapedPath(), "/")
	if issuerPath == "" {
		issuerPath = "/"
	}
	requestPath := u.EscapedPath()
	if issuerPath == "/" {
		return strings.HasPrefix(requestPath, "/")
	}
	return requestPath == issuerPath || strings.HasPrefix(requestPath, issuerPath+"/")
}

func exactConfiguredURL(rawURL string, configuredURLs []string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	for _, configuredURL := range configuredURLs {
		if configuredURL == "" {
			continue
		}
		expected, err := url.Parse(configuredURL)
		if err != nil {
			continue
		}
		if strings.EqualFold(u.Scheme, expected.Scheme) &&
			strings.EqualFold(u.Host, expected.Host) &&
			u.EscapedPath() == expected.EscapedPath() &&
			u.RawQuery == expected.RawQuery &&
			u.ForceQuery == expected.ForceQuery {
			return true
		}
	}
	return false
}
