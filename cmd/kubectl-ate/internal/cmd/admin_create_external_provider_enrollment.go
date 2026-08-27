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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"sigs.k8s.io/yaml"
)

var (
	externalProviderOwnerAtespace   string
	externalProviderWorkerNamespace string
	externalProviderWorkerPool      string
	externalProviderMaxSlots        uint32
	externalProviderSlotPolicyFile  string
	externalProviderEnrollmentTTL   time.Duration
	externalProviderCredentialFile  string
)

var createExternalProviderEnrollmentCmd = &cobra.Command{
	Use:   "external-provider-enrollment",
	Short: "Issue a scoped, single-use external provider enrollment",
	Long: `Issue one enrollment credential through the authenticated ate-api admin boundary.

The credential is atomically published to the owner-only --credential-file.
Use --credential-file=- only when raw stdout delivery is explicitly required.
Non-secret enrollment metadata is written to stderr. This command never
renders the credential through JSON or YAML output.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		if outputFmt != "table" {
			return fmt.Errorf("external provider enrollment does not support --output; the credential is emitted only through --credential-file")
		}
		policy, err := readExternalProviderSlotPolicy(externalProviderSlotPolicyFile)
		if err != nil {
			return err
		}
		credentialOutput, err := prepareCredentialOutput(externalProviderCredentialFile, cmd.OutOrStdout())
		if err != nil {
			return err
		}
		defer credentialOutput.Abort()
		apiClient, err := ateclient.NewClient(cmd.Context(), kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		request := &externalproviderpb.CreateExternalProviderEnrollmentRequest{
			Scope: &externalproviderpb.ExternalProviderEnrollmentScope{
				OwnerAtespace:   externalProviderOwnerAtespace,
				WorkerNamespace: externalProviderWorkerNamespace,
				WorkerPool:      externalProviderWorkerPool,
				MaxSlots:        externalProviderMaxSlots,
				SlotPolicy:      policy,
			},
			Ttl: durationpb.New(externalProviderEnrollmentTTL),
		}
		return runCreateExternalProviderEnrollment(cmd.Context(), apiClient, request, credentialOutput, cmd.ErrOrStderr())
	},
}

type externalProviderEnrollmentAdminClient interface {
	CreateExternalProviderEnrollment(context.Context, *externalproviderpb.CreateExternalProviderEnrollmentRequest, ...grpc.CallOption) (*externalproviderpb.CreateExternalProviderEnrollmentResponse, error)
}

func runCreateExternalProviderEnrollment(ctx context.Context, client externalProviderEnrollmentAdminClient, request *externalproviderpb.CreateExternalProviderEnrollmentRequest, output credentialOutput, stderr io.Writer) error {
	response, err := client.CreateExternalProviderEnrollment(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to create external provider enrollment: %w", err)
	}
	credential := response.GetEnrollmentCredential()
	defer clear(credential)
	if response.GetEnrollmentUid() == "" {
		return invalidIssuedEnrollmentResponse("missing enrollment UID")
	}
	if !isOpaqueEnrollmentCredential(credential) {
		return invalidIssuedEnrollmentResponse("malformed enrollment credential")
	}
	if response.GetExpiresAt() == nil {
		return invalidIssuedEnrollmentResponse("missing enrollment expiry")
	}
	if err := response.GetExpiresAt().CheckValid(); err != nil {
		return invalidIssuedEnrollmentResponse("invalid enrollment expiry")
	}
	if response.GetScope() == nil {
		return invalidIssuedEnrollmentResponse("missing enrollment scope")
	}
	if response.GetScope().GetSlotPolicy() == nil {
		return invalidIssuedEnrollmentResponse("missing enrollment slot policy")
	}
	if !isCanonicalSlotPolicyDigest(response.GetScope().GetSlotPolicy().GetDigest()) {
		return invalidIssuedEnrollmentResponse("invalid enrollment slot policy digest")
	}

	published, err := output.Publish(credential)
	if err != nil {
		if published {
			return fmt.Errorf("external provider enrollment credential was delivered to %s but finalization failed; DO NOT RETRY issuance: %w", output.Destination(), err)
		}
		return fmt.Errorf("external provider enrollment was issued but credential delivery to %s failed or is ambiguous; DO NOT RETRY automatically; issue a new enrollment after discarding partial output: %w", output.Destination(), err)
	}
	if _, err := fmt.Fprintf(stderr,
		"enrollment_uid=%s\nexpires_at=%s\nowner_atespace=%s\nworker_namespace=%s\nworker_pool=%s\nmax_slots=%d\nslot_policy_digest=%s\n",
		response.GetEnrollmentUid(),
		response.GetExpiresAt().AsTime().UTC().Format(time.RFC3339Nano),
		response.GetScope().GetOwnerAtespace(),
		response.GetScope().GetWorkerNamespace(),
		response.GetScope().GetWorkerPool(),
		response.GetScope().GetMaxSlots(),
		response.GetScope().GetSlotPolicy().GetDigest(),
	); err != nil {
		return fmt.Errorf("external provider enrollment credential was delivered to %s but metadata output failed; DO NOT RETRY issuance", output.Destination())
	}
	return nil
}

func invalidIssuedEnrollmentResponse(reason string) error {
	return fmt.Errorf("external provider enrollment may have been issued but ate-api-server returned an invalid response (%s); DO NOT RETRY automatically; issue a new enrollment after operator review", reason)
}

func isOpaqueEnrollmentCredential(credential []byte) bool {
	if len(credential) == 0 || len(credential) > 4096 {
		return false
	}
	for _, character := range credential {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func isCanonicalSlotPolicyDigest(digest string) bool {
	if len(digest) != 64 {
		return false
	}
	for _, character := range digest {
		if character >= '0' && character <= '9' || character >= 'a' && character <= 'f' {
			continue
		}
		return false
	}
	return true
}

type credentialOutput interface {
	Publish([]byte) (bool, error)
	Destination() string
	Abort()
}

type stdoutCredentialOutput struct {
	writer io.Writer
}

func (o *stdoutCredentialOutput) Publish(credential []byte) (bool, error) {
	written, expected, err := writeCredential(o.writer, credential)
	return written == expected, err
}

func (*stdoutCredentialOutput) Destination() string { return "stdout" }
func (*stdoutCredentialOutput) Abort()              {}

type fileCredentialOutput struct {
	file       *os.File
	temporary  string
	target     string
	parent     string
	parentInfo os.FileInfo
}

func prepareCredentialOutput(destination string, stdout io.Writer) (credentialOutput, error) {
	if destination == "-" {
		return &stdoutCredentialOutput{writer: stdout}, nil
	}
	if !filepath.IsAbs(destination) {
		return nil, fmt.Errorf("--credential-file must be an absolute path or -")
	}
	destination = filepath.Clean(destination)
	parent := filepath.Dir(destination)
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return nil, fmt.Errorf("inspect credential parent directory: %w", err)
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return nil, fmt.Errorf("credential parent must be a real directory, not a symlink")
	}
	if parentInfo.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("credential parent directory permissions must exclude group and other access")
	}
	if err := requireCurrentUserOwner(parentInfo); err != nil {
		return nil, fmt.Errorf("credential parent directory: %w", err)
	}
	if _, err := os.Lstat(destination); err == nil {
		return nil, fmt.Errorf("credential file already exists; refusing to overwrite")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect credential file: %w", err)
	}

	prefix := "." + filepath.Base(destination) + ".tmp-"
	file, err := os.CreateTemp(parent, prefix)
	if err != nil {
		return nil, fmt.Errorf("prepare credential file: %w", err)
	}
	output := &fileCredentialOutput{
		file:       file,
		temporary:  file.Name(),
		target:     destination,
		parent:     parent,
		parentInfo: parentInfo,
	}
	if err := file.Chmod(0o600); err != nil {
		output.Abort()
		return nil, fmt.Errorf("set credential file permissions: %w", err)
	}
	currentParent, err := os.Lstat(parent)
	if err != nil || !os.SameFile(parentInfo, currentParent) {
		output.Abort()
		return nil, fmt.Errorf("credential parent directory changed while preparing output")
	}
	return output, nil
}

func (o *fileCredentialOutput) Publish(credential []byte) (bool, error) {
	if o == nil || o.file == nil {
		return false, fmt.Errorf("credential output is not prepared")
	}
	written, expected, err := writeCredential(o.file, credential)
	if err != nil {
		return false, err
	}
	if written != expected {
		return false, io.ErrShortWrite
	}
	if err := o.file.Sync(); err != nil {
		return false, err
	}
	if err := o.file.Close(); err != nil {
		o.file = nil
		return false, err
	}
	o.file = nil

	currentParent, err := os.Lstat(o.parent)
	if err != nil || !os.SameFile(o.parentInfo, currentParent) || currentParent.Mode()&os.ModeSymlink != 0 || !currentParent.IsDir() {
		return false, fmt.Errorf("credential parent directory changed before publish")
	}
	if currentParent.Mode().Perm()&0o077 != 0 {
		return false, fmt.Errorf("credential parent directory permissions changed before publish")
	}
	if err := requireCurrentUserOwner(currentParent); err != nil {
		return false, fmt.Errorf("credential parent directory ownership changed before publish: %w", err)
	}
	if err := os.Link(o.temporary, o.target); err != nil {
		return false, fmt.Errorf("publish credential file without overwrite: %w", err)
	}
	if err := os.Remove(o.temporary); err != nil {
		return true, fmt.Errorf("remove credential staging link: %w", err)
	}
	o.temporary = ""
	parent, err := os.Open(o.parent)
	if err != nil {
		return true, fmt.Errorf("open credential parent for sync: %w", err)
	}
	defer parent.Close()
	if err := parent.Sync(); err != nil {
		return true, fmt.Errorf("sync credential parent: %w", err)
	}
	return true, nil
}

func (o *fileCredentialOutput) Destination() string {
	if o == nil {
		return "credential file"
	}
	return o.target
}

func (o *fileCredentialOutput) Abort() {
	if o == nil {
		return
	}
	if o.file != nil {
		_ = o.file.Close()
		o.file = nil
	}
	if o.temporary != "" {
		_ = os.Remove(o.temporary)
		o.temporary = ""
	}
}

func writeCredential(output io.Writer, credential []byte) (int, int, error) {
	framed := make([]byte, len(credential)+1)
	copy(framed, credential)
	framed[len(credential)] = '\n'
	defer clear(framed)
	written, err := output.Write(framed)
	if err != nil {
		return written, len(framed), err
	}
	if written != len(framed) {
		return written, len(framed), io.ErrShortWrite
	}
	return written, len(framed), nil
}

type slotPolicyDocument struct {
	Version  uint32                `json:"version"`
	Profiles []slotProfileDocument `json:"profiles"`
}

type slotProfileDocument struct {
	ProfileID    string            `json:"profileId"`
	SandboxClass string            `json:"sandboxClass"`
	Labels       map[string]string `json:"labels,omitempty"`
	MaxSlots     uint32            `json:"maxSlots"`
	Capacity     capacityDocument  `json:"capacity"`
}

type capacityDocument struct {
	CPUMilli    int64 `json:"cpuMilli"`
	MemoryBytes int64 `json:"memoryBytes"`
}

const maxSlotPolicyFileBytes = 2 << 20

func readExternalProviderSlotPolicy(path string) (*externalproviderpb.SlotCapabilityPolicy, error) {
	contents, err := readStableRegularFile(path, maxSlotPolicyFileBytes)
	if err != nil {
		return nil, fmt.Errorf("read external provider slot policy: %w", err)
	}
	var document slotPolicyDocument
	if err := yaml.UnmarshalStrict(contents, &document); err != nil {
		return nil, fmt.Errorf("parse external provider slot policy: %w", err)
	}
	policy := &externalproviderpb.SlotCapabilityPolicy{
		Version:  document.Version,
		Profiles: make([]*externalproviderpb.SlotProfile, len(document.Profiles)),
	}
	for index, profile := range document.Profiles {
		policy.Profiles[index] = &externalproviderpb.SlotProfile{
			ProfileId:    profile.ProfileID,
			SandboxClass: profile.SandboxClass,
			Labels:       profile.Labels,
			MaxSlots:     profile.MaxSlots,
			Capacity: &ateapipb.WorkerCapacity{
				CpuMilli:    profile.Capacity.CPUMilli,
				MemoryBytes: profile.Capacity.MemoryBytes,
			},
		}
	}
	return policy, nil
}

func readStableRegularFile(path string, maximum int64) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, fmt.Errorf("file must be regular and must not be a symlink")
	}
	if before.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("file must not be writable by group or other")
	}
	if before.Size() < 0 || before.Size() > maximum {
		return nil, fmt.Errorf("file exceeds %d bytes", maximum)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("file changed while opening")
	}
	contents, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(contents)) > maximum {
		return nil, fmt.Errorf("file exceeds %d bytes", maximum)
	}
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(opened, after) || opened.Size() != after.Size() || !opened.ModTime().Equal(after.ModTime()) {
		return nil, fmt.Errorf("file changed while reading")
	}
	return contents, nil
}

func init() {
	createExternalProviderEnrollmentCmd.Flags().StringVar(&externalProviderOwnerAtespace, "owner-atespace", "", "Atespace which owns all Actors scheduled to this provider (required)")
	createExternalProviderEnrollmentCmd.Flags().StringVar(&externalProviderWorkerNamespace, "worker-namespace", "", "Kubernetes namespace containing the pinned WorkerPool (required)")
	createExternalProviderEnrollmentCmd.Flags().StringVar(&externalProviderWorkerPool, "worker-pool", "", "Pinned WorkerPool name (required)")
	createExternalProviderEnrollmentCmd.Flags().Uint32Var(&externalProviderMaxSlots, "max-slots", 0, "Maximum slots this registration may advertise (required)")
	createExternalProviderEnrollmentCmd.Flags().StringVar(&externalProviderSlotPolicyFile, "slot-policy", "", "Path to the exact slot capability policy YAML or JSON document (required)")
	createExternalProviderEnrollmentCmd.Flags().DurationVar(&externalProviderEnrollmentTTL, "ttl", time.Hour, "Enrollment credential lifetime, at most 24h")
	createExternalProviderEnrollmentCmd.Flags().StringVar(&externalProviderCredentialFile, "credential-file", "", "Absolute new owner-only credential file, or - for explicit raw stdout delivery (required)")
	_ = createExternalProviderEnrollmentCmd.MarkFlagRequired("owner-atespace")
	_ = createExternalProviderEnrollmentCmd.MarkFlagRequired("worker-namespace")
	_ = createExternalProviderEnrollmentCmd.MarkFlagRequired("worker-pool")
	_ = createExternalProviderEnrollmentCmd.MarkFlagRequired("max-slots")
	_ = createExternalProviderEnrollmentCmd.MarkFlagRequired("slot-policy")
	_ = createExternalProviderEnrollmentCmd.MarkFlagRequired("credential-file")
	adminCreateCmd.AddCommand(createExternalProviderEnrollmentCmd)
}
