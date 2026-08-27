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
	"log/slog"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateapiauth"
)

func TestConnectStoreRequiresPostgresConnectionString(t *testing.T) {
	oldDSN := *postgresConnectionString
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
	})
	*postgresConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-connection-string is required") {
		t.Fatalf("connectStore() error = %v, want missing-connection-string error", err)
	}
}

func TestExternalProviderEnrollmentAdminPrincipalsResolveExactIssuerAndSubject(t *testing.T) {
	cfg := &ateapiauth.AuthenticationConfig{
		JWTProviders: []ateapiauth.JWTProviderConfig{
			{Name: "kubernetes", Issuer: "https://kubernetes.example"},
			{Name: "google", Issuer: "https://accounts.google.com"},
		},
		ExternalProviderEnrollmentAdmins: []ateapiauth.JWTPrincipalConfig{
			{Provider: "kubernetes", Subjects: []string{"system:serviceaccount:ate-system:ate-client"}},
			{Provider: "google", Subjects: []string{"operator-a", "operator-b"}},
		},
	}
	principals := externalProviderEnrollmentAdminPrincipals(cfg)
	if got, want := len(principals), 3; got != want {
		t.Fatalf("principal count = %d, want %d", got, want)
	}
	if got := principals[0]; got.Provider != "kubernetes" || got.Issuer != "https://kubernetes.example" || got.Subject != "system:serviceaccount:ate-system:ate-client" {
		t.Fatalf("first principal = %+v", got)
	}
	if got := principals[2]; got.Provider != "google" || got.Issuer != "https://accounts.google.com" || got.Subject != "operator-b" {
		t.Fatalf("last principal = %+v", got)
	}
}

func TestLogFlagValuesRedactsPostgresConnectionString(t *testing.T) {
	oldDSN := *postgresConnectionString
	oldLogger := slog.Default()
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
		slog.SetDefault(oldLogger)
	})

	const secretDSN = "postgresql://audit-user:startup-log-secret@database.example/substrate"
	*postgresConnectionString = secretDSN
	var output bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))

	logFlagValues(context.Background())
	logged := output.String()
	if strings.Contains(logged, secretDSN) || strings.Contains(logged, "startup-log-secret") {
		t.Fatalf("flag log contains PostgreSQL credentials: %s", logged)
	}
	if !strings.Contains(logged, `"postgres-connection-string-configured":true`) {
		t.Fatalf("flag log lacks non-secret PostgreSQL configuration state: %s", logged)
	}
}
