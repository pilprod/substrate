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
	"context"
	"fmt"
	"io"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var revokeExternalProviderRegistrationCmd = &cobra.Command{
	Use:   "external-provider-registration REGISTRATION_UID",
	Short: "Revoke one external provider registration and its live session",
	Long: `Durably revoke one external provider registration through the authenticated
ate-api admin boundary. The command succeeds only after any current provider
route and forwarding channels are fenced and its Workers are OFFLINE.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if outputFmt != "table" {
			return fmt.Errorf("external provider registration revocation does not support --output")
		}
		apiClient, err := ateclient.NewClient(cmd.Context(), kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled, serverCAFile, serverName)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()
		return runRevokeExternalProviderRegistration(cmd.Context(), apiClient, args[0], cmd.OutOrStdout())
	},
}

type externalProviderRegistrationAdminClient interface {
	RevokeExternalProviderRegistration(context.Context, *externalproviderpb.RevokeExternalProviderRegistrationRequest, ...grpc.CallOption) (*externalproviderpb.RevokeExternalProviderRegistrationResponse, error)
}

func runRevokeExternalProviderRegistration(ctx context.Context, client externalProviderRegistrationAdminClient, registrationUID string, output io.Writer) error {
	if _, err := client.RevokeExternalProviderRegistration(ctx, &externalproviderpb.RevokeExternalProviderRegistrationRequest{RegistrationUid: registrationUID}); err != nil {
		return fmt.Errorf("failed to revoke external provider registration: %w", err)
	}
	if _, err := fmt.Fprintf(output, "external provider registration %s revoked\n", registrationUID); err != nil {
		return fmt.Errorf("external provider registration was revoked but confirmation output failed: %w", err)
	}
	return nil
}

func init() {
	adminRevokeCmd.AddCommand(revokeExternalProviderRegistrationCmd)
}
