// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc"
)

type fakeExternalProviderRegistrationAdminClient struct {
	request *externalproviderpb.RevokeExternalProviderRegistrationRequest
	err     error
}

func (f *fakeExternalProviderRegistrationAdminClient) RevokeExternalProviderRegistration(_ context.Context, request *externalproviderpb.RevokeExternalProviderRegistrationRequest, _ ...grpc.CallOption) (*externalproviderpb.RevokeExternalProviderRegistrationResponse, error) {
	f.request = request
	return &externalproviderpb.RevokeExternalProviderRegistrationResponse{}, f.err
}

func TestRunRevokeExternalProviderRegistration(t *testing.T) {
	client := &fakeExternalProviderRegistrationAdminClient{}
	var output bytes.Buffer
	if err := runRevokeExternalProviderRegistration(context.Background(), client, "registration-a", &output); err != nil {
		t.Fatalf("runRevokeExternalProviderRegistration() error = %v", err)
	}
	if client.request.GetRegistrationUid() != "registration-a" {
		t.Fatalf("registration UID = %q, want registration-a", client.request.GetRegistrationUid())
	}
	if got, want := output.String(), "external provider registration registration-a revoked\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestRunRevokeExternalProviderRegistrationReportsRPCFailureWithoutSuccess(t *testing.T) {
	client := &fakeExternalProviderRegistrationAdminClient{err: errors.New("permission denied")}
	var output bytes.Buffer
	err := runRevokeExternalProviderRegistration(context.Background(), client, "registration-a", &output)
	if err == nil || !strings.Contains(err.Error(), "failed to revoke external provider registration") {
		t.Fatalf("error = %v, want RPC failure", err)
	}
	if output.Len() != 0 {
		t.Fatalf("RPC failure wrote success output %q", output.String())
	}
}
