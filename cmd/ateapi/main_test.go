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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateapiauth"
)

func TestConnectStoreRequiresPostgresConnectionString(t *testing.T) {
	oldDSN := *postgresConnectionString
	oldDSNFile := *postgresConnectionStringFile
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
		*postgresConnectionStringFile = oldDSNFile
	})
	*postgresConnectionString = ""
	*postgresConnectionStringFile = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-connection-string or --postgres-connection-string-file is required") {
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

func TestResolvePostgresConnectionStringFromFile(t *testing.T) {
	oldDSN := *postgresConnectionString
	oldDSNFile := *postgresConnectionStringFile
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
		*postgresConnectionStringFile = oldDSNFile
	})

	const want = "postgresql://file-user:file-secret@database.example/substrate"
	path := filepath.Join(t.TempDir(), "connection-string")
	if err := os.WriteFile(path, []byte(want+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	*postgresConnectionString = ""
	*postgresConnectionStringFile = path

	got, err := resolvePostgresConnectionString()
	if err != nil {
		t.Fatalf("resolvePostgresConnectionString() error = %v", err)
	}
	if got != want {
		t.Fatalf("resolvePostgresConnectionString() = %q, want %q", got, want)
	}
}

func TestResolvePostgresConnectionStringFileFailsClosed(t *testing.T) {
	oldDSN := *postgresConnectionString
	oldDSNFile := *postgresConnectionStringFile
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
		*postgresConnectionStringFile = oldDSNFile
	})

	tests := []struct {
		name     string
		direct   string
		contents []byte
		missing  bool
		want     string
	}{
		{name: "both sources", direct: "postgresql://direct.example/db", contents: []byte("postgresql://file.example/db"), want: "mutually exclusive"},
		{name: "empty file", contents: nil, want: "is empty"},
		{name: "NUL byte", contents: []byte("postgresql://database.example/db\x00ignored"), want: "NUL byte"},
		{name: "oversized file", contents: bytes.Repeat([]byte("x"), maxPostgresConnectionStringBytes+1), want: "exceeds"},
		{name: "missing file", missing: true, want: "open PostgreSQL connection string file"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "connection-string")
			if !test.missing {
				if err := os.WriteFile(path, test.contents, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			*postgresConnectionString = test.direct
			*postgresConnectionStringFile = path
			if _, err := resolvePostgresConnectionString(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("resolvePostgresConnectionString() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLogFlagValuesRedactsPostgresConnectionString(t *testing.T) {
	oldDSN := *postgresConnectionString
	oldDSNFile := *postgresConnectionStringFile
	oldLogger := slog.Default()
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
		*postgresConnectionStringFile = oldDSNFile
		slog.SetDefault(oldLogger)
	})

	const secretDSN = "postgresql://audit-user:startup-log-secret@database.example/substrate"
	*postgresConnectionString = secretDSN
	*postgresConnectionStringFile = ""
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
