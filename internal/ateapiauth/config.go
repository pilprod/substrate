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

package ateapiauth

import (
	"fmt"
	"net/url"
	"os"

	"sigs.k8s.io/yaml"
)

// AuthenticationConfig configures JWT authentication for ateapi.
type AuthenticationConfig struct {
	ActorIdentityJWTProvider         string               `json:"actorIdentityJWTProvider"`
	ExternalProviderEnrollmentAdmins []JWTPrincipalConfig `json:"externalProviderEnrollmentAdmins,omitempty"`
	JWTProviders                     []JWTProviderConfig  `json:"jwtProviders"`
}

// JWTPrincipalConfig grants one narrowly scoped operation to exact subjects
// authenticated by a named JWT provider. An empty list grants nobody.
type JWTPrincipalConfig struct {
	Provider string   `json:"provider"`
	Subjects []string `json:"subjects"`
}

// JWTProviderConfig configures one trusted OIDC issuer.
type JWTProviderConfig struct {
	Name                     string   `json:"name"`
	Issuer                   string   `json:"issuer"`
	Audiences                []string `json:"audiences"`
	DiscoveryURL             string   `json:"discoveryURL,omitempty"`
	JWKSURL                  string   `json:"jwksURL,omitempty"`
	CertificateAuthorityFile string   `json:"certificateAuthorityFile,omitempty"`
	DiscoveryTokenFile       string   `json:"discoveryTokenFile,omitempty"`
}

// LoadAuthenticationConfig strictly parses and validates a YAML or JSON file.
func LoadAuthenticationConfig(path string) (*AuthenticationConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read authentication config: %w", err)
	}
	var cfg AuthenticationConfig
	if err := yaml.UnmarshalStrict(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse authentication config: %w", err)
	}
	if err := ValidateAuthenticationConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ValidateAuthenticationConfig validates fields that do not require I/O.
func ValidateAuthenticationConfig(cfg *AuthenticationConfig) error {
	if cfg == nil {
		return fmt.Errorf("authentication config is required")
	}
	if len(cfg.JWTProviders) == 0 {
		return fmt.Errorf("at least one JWT provider is required")
	}

	names := make(map[string]bool, len(cfg.JWTProviders))
	issuers := make(map[string]bool, len(cfg.JWTProviders))
	for i, p := range cfg.JWTProviders {
		field := fmt.Sprintf("jwtProviders[%d]", i)
		if p.Name == "" {
			return fmt.Errorf("%s.name is required", field)
		}
		if names[p.Name] {
			return fmt.Errorf("duplicate JWT provider name %q", p.Name)
		}
		names[p.Name] = true
		if err := validateHTTPSURL(p.Issuer); err != nil {
			return fmt.Errorf("%s.issuer must be an absolute HTTPS URL without userinfo, query, or fragment", field)
		}
		if issuers[p.Issuer] {
			return fmt.Errorf("duplicate JWT provider issuer %q", p.Issuer)
		}
		issuers[p.Issuer] = true
		if len(p.Audiences) == 0 {
			return fmt.Errorf("%s.audiences must contain at least one audience", field)
		}
		for _, audience := range p.Audiences {
			if audience == "" {
				return fmt.Errorf("%s.audiences must not contain an empty audience", field)
			}
		}
		if (p.DiscoveryURL == "") != (p.JWKSURL == "") {
			return fmt.Errorf("%s.discoveryURL and %s.jwksURL must be configured together", field, field)
		}
		if p.DiscoveryURL != "" {
			if err := validateHTTPSURL(p.DiscoveryURL); err != nil {
				return fmt.Errorf("%s.discoveryURL must be an absolute HTTPS URL without userinfo, query, or fragment", field)
			}
			if err := validateHTTPSURL(p.JWKSURL); err != nil {
				return fmt.Errorf("%s.jwksURL must be an absolute HTTPS URL without userinfo, query, or fragment", field)
			}
		}
	}
	if cfg.ActorIdentityJWTProvider == "" {
		return fmt.Errorf("actorIdentityJWTProvider is required")
	}
	if !names[cfg.ActorIdentityJWTProvider] {
		return fmt.Errorf("actorIdentityJWTProvider %q does not name a JWT provider", cfg.ActorIdentityJWTProvider)
	}

	principals := make(map[string]bool)
	for i, admin := range cfg.ExternalProviderEnrollmentAdmins {
		field := fmt.Sprintf("externalProviderEnrollmentAdmins[%d]", i)
		if !names[admin.Provider] {
			return fmt.Errorf("%s.provider %q does not name a JWT provider", field, admin.Provider)
		}
		if len(admin.Subjects) == 0 {
			return fmt.Errorf("%s.subjects must contain at least one subject", field)
		}
		for j, subject := range admin.Subjects {
			if subject == "" {
				return fmt.Errorf("%s.subjects[%d] must not be empty", field, j)
			}
			principal := admin.Provider + "\x00" + subject
			if principals[principal] {
				return fmt.Errorf("duplicate external provider enrollment admin subject %q for provider %q", subject, admin.Provider)
			}
			principals[principal] = true
		}
	}
	return nil
}

func validateHTTPSURL(value string) error {
	u, err := url.Parse(value)
	if err != nil || !u.IsAbs() || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("invalid HTTPS URL")
	}
	return nil
}
