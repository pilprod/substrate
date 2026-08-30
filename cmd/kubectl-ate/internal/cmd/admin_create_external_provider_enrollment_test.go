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

package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeExternalProviderEnrollmentAdminClient struct {
	response *externalproviderpb.CreateExternalProviderEnrollmentResponse
	err      error
}

func (f fakeExternalProviderEnrollmentAdminClient) CreateExternalProviderEnrollment(context.Context, *externalproviderpb.CreateExternalProviderEnrollmentRequest, ...grpc.CallOption) (*externalproviderpb.CreateExternalProviderEnrollmentResponse, error) {
	return f.response, f.err
}

func TestRunCreateExternalProviderEnrollmentSeparatesOneTimeCredentialFromMetadata(t *testing.T) {
	const credential = "opaque-single-use-credential"
	response := &externalproviderpb.CreateExternalProviderEnrollmentResponse{
		EnrollmentUid:        "enrollment-uid",
		EnrollmentCredential: []byte(credential),
		ExpiresAt:            timestamppb.New(time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)),
		Scope: &externalproviderpb.ExternalProviderEnrollmentScope{
			OwnerAtespace:   "tenant-a",
			WorkerNamespace: "external-workers",
			WorkerPool:      "local-agents",
			MaxSlots:        2,
			SlotPolicy:      &externalproviderpb.SlotCapabilityPolicy{Digest: strings.Repeat("a", 64)},
		},
	}
	var stdout, stderr bytes.Buffer
	output := &stdoutCredentialOutput{writer: &stdout}
	err := runCreateExternalProviderEnrollment(context.Background(), fakeExternalProviderEnrollmentAdminClient{response: response}, &externalproviderpb.CreateExternalProviderEnrollmentRequest{}, output, &stderr)
	if err != nil {
		t.Fatalf("runCreateExternalProviderEnrollment() error = %v", err)
	}
	if got, want := stdout.String(), credential+"\n"; got != want {
		t.Fatalf("stdout = %q, want raw credential only", got)
	}
	if strings.Contains(stderr.String(), credential) || !strings.Contains(stderr.String(), "enrollment_uid=enrollment-uid") || !strings.Contains(stderr.String(), "slot_policy_digest=") {
		t.Fatalf("stderr metadata is unsafe or incomplete: %s", stderr.String())
	}
	if got := response.GetEnrollmentCredential(); !bytes.Equal(got, make([]byte, len(credential))) {
		t.Fatalf("credential response bytes were not cleared: %x", got)
	}
}

func TestRunCreateExternalProviderEnrollmentWritesNoCredentialOnRPCError(t *testing.T) {
	var stdout bytes.Buffer
	output := &stdoutCredentialOutput{writer: &stdout}
	err := runCreateExternalProviderEnrollment(context.Background(), fakeExternalProviderEnrollmentAdminClient{err: errors.New("permission denied")}, &externalproviderpb.CreateExternalProviderEnrollmentRequest{}, output, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "failed to create external provider enrollment") {
		t.Fatalf("error = %v, want RPC error", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("RPC failure wrote stdout: %q", stdout.String())
	}
}

func TestRunCreateExternalProviderEnrollmentMarksAllPostRPCValidationFailuresDoNotRetry(t *testing.T) {
	const credential = "opaque-single-use-credential"
	tests := []struct {
		name   string
		mutate func(*externalproviderpb.CreateExternalProviderEnrollmentResponse)
		want   string
	}{
		{name: "missing UID", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentResponse) { r.EnrollmentUid = "" }, want: "missing enrollment UID"},
		{name: "malformed credential", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentResponse) {
			r.EnrollmentCredential = []byte("bad\ncredential")
		}, want: "malformed enrollment credential"},
		{name: "missing expiry", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentResponse) { r.ExpiresAt = nil }, want: "missing enrollment expiry"},
		{name: "invalid expiry", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentResponse) {
			r.ExpiresAt = &timestamppb.Timestamp{Seconds: 253402300800}
		}, want: "invalid enrollment expiry"},
		{name: "missing scope", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentResponse) { r.Scope = nil }, want: "missing enrollment scope"},
		{name: "missing policy", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentResponse) { r.Scope.SlotPolicy = nil }, want: "missing enrollment slot policy"},
		{name: "invalid policy digest", mutate: func(r *externalproviderpb.CreateExternalProviderEnrollmentResponse) {
			r.Scope.SlotPolicy.Digest = strings.Repeat("A", 64)
		}, want: "invalid enrollment slot policy digest"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := validCLIEnrollmentResponse(credential)
			test.mutate(response)
			originalCredential := append([]byte(nil), response.GetEnrollmentCredential()...)
			var stdout bytes.Buffer
			output := &stdoutCredentialOutput{writer: &stdout}
			err := runCreateExternalProviderEnrollment(context.Background(), fakeExternalProviderEnrollmentAdminClient{response: response}, &externalproviderpb.CreateExternalProviderEnrollmentRequest{}, output, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "DO NOT RETRY") || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q and DO NOT RETRY", err, test.want)
			}
			if stdout.Len() != 0 || strings.Contains(err.Error(), string(originalCredential)) {
				t.Fatalf("validation failure exposed credential: stdout=%q error=%v", stdout.String(), err)
			}
			if got := response.GetEnrollmentCredential(); !bytes.Equal(got, make([]byte, len(originalCredential))) {
				t.Fatalf("validation failure did not clear credential: %x", got)
			}
		})
	}
}

func TestRunCreateExternalProviderEnrollmentMarksEveryPostRPCOutputFailureDoNotRetry(t *testing.T) {
	const credential = "opaque-single-use-credential"
	tests := []struct {
		name   string
		output credentialOutput
		stderr io.Writer
	}{
		{
			name:   "partial credential write",
			output: &stdoutCredentialOutput{writer: &partialWriter{limit: 5}},
			stderr: io.Discard,
		},
		{
			name:   "metadata write after delivery",
			output: &stdoutCredentialOutput{writer: io.Discard},
			stderr: &failingWriter{fail: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := validCLIEnrollmentResponse(credential)
			err := runCreateExternalProviderEnrollment(context.Background(), fakeExternalProviderEnrollmentAdminClient{response: response}, &externalproviderpb.CreateExternalProviderEnrollmentRequest{}, test.output, test.stderr)
			if err == nil || !strings.Contains(err.Error(), "DO NOT RETRY") {
				t.Fatalf("error = %v, want explicit DO NOT RETRY", err)
			}
			if strings.Contains(err.Error(), credential) {
				t.Fatalf("error exposed credential: %v", err)
			}
		})
	}
}

func TestCredentialFileOutputPublishesAtomicallyOwnerOnlyAndNeverOverwrites(t *testing.T) {
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(parent, "enrollment-token")
	output, err := prepareCredentialOutput(target, io.Discard)
	if err != nil {
		t.Fatalf("prepareCredentialOutput() error = %v", err)
	}
	defer output.Abort()
	response := validCLIEnrollmentResponse("opaque-single-use-credential")
	if err := runCreateExternalProviderEnrollment(context.Background(), fakeExternalProviderEnrollmentAdminClient{response: response}, &externalproviderpb.CreateExternalProviderEnrollmentRequest{}, output, io.Discard); err != nil {
		t.Fatalf("runCreateExternalProviderEnrollment() error = %v", err)
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(contents), "opaque-single-use-credential\n"; got != want {
		t.Fatalf("credential file = %q, want %q", got, want)
	}
	info, err := os.Lstat(target)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("credential mode = %v, want regular 0600", info.Mode())
	}

	existing := filepath.Join(parent, "existing-token")
	if err := os.WriteFile(existing, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCredentialOutput(existing, io.Discard); err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
		t.Fatalf("existing credential output error = %v", err)
	}
	if got, err := os.ReadFile(existing); err != nil || string(got) != "keep" {
		t.Fatalf("existing credential changed: %q, %v", got, err)
	}
}

func TestCredentialFileOutputRejectsUnsafePathsAndPublishRace(t *testing.T) {
	privateParent := t.TempDir()
	if err := os.Chmod(privateParent, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(privateParent, "token")
	output, err := prepareCredentialOutput(target, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Abort()
	if err := os.WriteFile(target, []byte("racing-owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	published, err := output.Publish([]byte("new-credential"))
	if err == nil || published {
		t.Fatalf("Publish() = (%t, %v), want false/error when target appears", published, err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "racing-owner" {
		t.Fatalf("racing target was overwritten: %q, %v", got, err)
	}

	unsafeParent := t.TempDir()
	if err := os.Chmod(unsafeParent, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCredentialOutput(filepath.Join(unsafeParent, "token"), io.Discard); err == nil || !strings.Contains(err.Error(), "exclude group and other") {
		t.Fatalf("unsafe parent error = %v", err)
	}
	if _, err := prepareCredentialOutput("relative-token", io.Discard); err == nil || !strings.Contains(err.Error(), "absolute path") {
		t.Fatalf("relative target error = %v", err)
	}
	symlinkParent := filepath.Join(privateParent, "linked-parent")
	if err := os.Symlink(unsafeParent, symlinkParent); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCredentialOutput(filepath.Join(symlinkParent, "token"), io.Discard); err == nil || !strings.Contains(err.Error(), "not a symlink") {
		t.Fatalf("symlink parent error = %v", err)
	}
}

func TestCredentialFileOutputRechecksParentPermissionsBeforePublish(t *testing.T) {
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	output, err := prepareCredentialOutput(filepath.Join(parent, "token"), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Abort()
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	published, err := output.Publish([]byte("opaque-single-use-credential"))
	if err == nil || published || !strings.Contains(err.Error(), "permissions changed") {
		t.Fatalf("Publish() = (%t, %v), want false/permissions-changed error", published, err)
	}
}

func TestReadExternalProviderSlotPolicyIsStrictAndNeverAcceptsDigest(t *testing.T) {
	valid := `
version: 1
profiles:
- profileId: codex-native
  sandboxClass: host-process-hardened
  labels:
    agent.example/provider: codex
  maxSlots: 2
  capacity:
    cpuMilli: 4000
    memoryBytes: 17179869184
`
	path := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := readExternalProviderSlotPolicy(path)
	if err != nil {
		t.Fatalf("readExternalProviderSlotPolicy() error = %v", err)
	}
	if policy.GetVersion() != 1 || policy.GetDigest() != "" || len(policy.GetProfiles()) != 1 || policy.GetProfiles()[0].GetCapacity().GetCpuMilli() != 4_000 {
		t.Fatalf("policy = %+v", policy)
	}

	if err := os.WriteFile(path, []byte(valid+"digest: forged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readExternalProviderSlotPolicy(path); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("policy with digest error = %v, want strict unknown-field error", err)
	}

	linked := filepath.Join(t.TempDir(), "policy-link.yaml")
	if err := os.Symlink(path, linked); err != nil {
		t.Fatal(err)
	}
	if _, err := readExternalProviderSlotPolicy(linked); err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("symlink policy error = %v", err)
	}
}

type failingWriter struct {
	fail bool
}

type partialWriter struct {
	limit int
}

func (w *partialWriter) Write(p []byte) (int, error) {
	if w.limit > len(p) {
		w.limit = len(p)
	}
	return w.limit, nil
}

func validCLIEnrollmentResponse(credential string) *externalproviderpb.CreateExternalProviderEnrollmentResponse {
	return &externalproviderpb.CreateExternalProviderEnrollmentResponse{
		EnrollmentUid:        "enrollment-uid",
		EnrollmentCredential: []byte(credential),
		ExpiresAt:            timestamppb.New(time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)),
		Scope: &externalproviderpb.ExternalProviderEnrollmentScope{
			OwnerAtespace:   "tenant-a",
			WorkerNamespace: "external-workers",
			WorkerPool:      "local-agents",
			MaxSlots:        2,
			SlotPolicy:      &externalproviderpb.SlotCapabilityPolicy{Digest: strings.Repeat("a", 64)},
		},
	}
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.fail {
		return 0, errors.New("write failed")
	}
	return len(p), nil
}
