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

package externalprovider

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

const (
	workerNameDomain        = "agent-substrate/external-provider/worker-name/v1\x00"
	executionIdentityDomain = "agent-substrate/external-provider/execution-identity/v1\x00"
	localityIdentityDomain  = "agent-substrate/external-provider/locality-identity/v1\x00"
)

var (
	// ErrInvalidWorkerPlan reports an incomplete or inconsistent admission.
	ErrInvalidWorkerPlan = errors.New("invalid external provider Worker plan")

	// ErrWorkerIdentityCollision reports that an existing Worker has the name
	// of a planned slot but different immutable provider identity.
	ErrWorkerIdentityCollision = errors.New("external provider Worker identity collision")
)

// WorkerPlan is an immutable, non-secret set of desired ExternalSlot Workers.
// It contains no session generation or live route: those belong to the session
// registry, while Worker identities remain stable across reconnects.
type WorkerPlan struct {
	registration Registration
	workers      []*ateapipb.Worker
}

// WorkerPlanReconciler persists the durable Worker resources for an admitted
// provider session. Reconciliation does not make the Workers available: the
// live session owner activates them only after its route is installed.
type WorkerPlanReconciler interface {
	ReconcileExternalWorkers(context.Context, *WorkerPlan) ([]*ateapipb.Worker, error)
}

// Registration returns the immutable authenticated registration and scope.
func (p *WorkerPlan) Registration() Registration {
	if p == nil {
		return Registration{}
	}
	return p.registration
}

// Workers returns deep copies sorted by Worker resource name.
func (p *WorkerPlan) Workers() []*ateapipb.Worker {
	if p == nil {
		return nil
	}
	workers := make([]*ateapipb.Worker, len(p.workers))
	for index, worker := range p.workers {
		workers[index] = proto.Clone(worker).(*ateapipb.Worker)
	}
	return workers
}

// ValidateExisting verifies the immutable provider fields of an existing
// Worker whose name is present in this plan. Mutable sandbox_class and labels,
// server-owned metadata/status, and liveness are intentionally ignored.
func (p *WorkerPlan) ValidateExisting(existing *ateapipb.Worker) error {
	if p == nil || existing == nil || existing.GetMetadata().GetName() == "" {
		return fmt.Errorf("%w: existing Worker and name are required", ErrInvalidWorkerPlan)
	}
	name := existing.GetMetadata().GetName()
	index, found := slices.BinarySearchFunc(p.workers, name, func(worker *ateapipb.Worker, candidate string) int {
		switch {
		case worker.GetMetadata().GetName() < candidate:
			return -1
		case worker.GetMetadata().GetName() > candidate:
			return 1
		default:
			return 0
		}
	})
	if !found {
		return fmt.Errorf("%w: Worker name is not present in the admission", ErrInvalidWorkerPlan)
	}
	desired := p.workers[index]
	return validatePlannedWorkerIdentity(desired, existing)
}

// validatePlannedWorkerIdentity compares only the durable, immutable provider
// identity. Mutable scheduling hints, server metadata/status, and liveness are
// deliberately outside this check.
func validatePlannedWorkerIdentity(desired, existing *ateapipb.Worker) error {
	if desired == nil || existing == nil || desired.GetMetadata().GetName() == "" ||
		existing.GetMetadata().GetName() != desired.GetMetadata().GetName() ||
		existing.GetMetadata().GetAtespace() != "" ||
		existing.GetProvider() != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT ||
		existing.GetWorkerNamespace() != desired.GetWorkerNamespace() ||
		existing.GetWorkerPool() != desired.GetWorkerPool() ||
		existing.GetWorkerPod() != "" || existing.GetWorkerPodUid() != "" || existing.GetNodeName() != "" || existing.GetIp() != "" ||
		!proto.Equal(existing.GetExternalSlot(), desired.GetExternalSlot()) ||
		!proto.Equal(existing.GetCapacity(), desired.GetCapacity()) {
		return fmt.Errorf("%w: immutable fields differ for Worker %q", ErrWorkerIdentityCollision, desired.GetMetadata().GetName())
	}
	return nil
}

// PlanExternalWorkers derives every persistent Worker identity on the server.
// Caller-provided registration and slot strings are hashed with separate
// domains and length framing; none is used directly as a resource or routing
// identity. The returned Workers omit status so CreateWorker can initialize
// them OFFLINE.
func PlanExternalWorkers(admission *ConnectAdmission) (*WorkerPlan, error) {
	if admission == nil || admission.generation == 0 || len(admission.slots) == 0 {
		return nil, fmt.Errorf("%w: a nonempty authenticated admission is required", ErrInvalidWorkerPlan)
	}
	registration := admission.registration
	if !IsValidIdentity(registration.UID) {
		return nil, fmt.Errorf("%w: registration identity is invalid", ErrInvalidWorkerPlan)
	}
	if err := registration.Scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: registration scope is invalid", ErrInvalidWorkerPlan)
	}

	localityIdentity := deriveOpaqueIdentity("loc-", localityIdentityDomain, registration.UID)
	workers := make([]*ateapipb.Worker, 0, len(admission.slots))
	seenNames := make(map[string]struct{}, len(admission.slots))
	seenExecutions := make(map[string]struct{}, len(admission.slots))
	for _, slot := range admission.slots {
		if !IsValidIdentity(slot.slotID) {
			return nil, fmt.Errorf("%w: admitted slot identity is invalid", ErrInvalidWorkerPlan)
		}
		name := deriveOpaqueIdentity("ext-", workerNameDomain, registration.UID, slot.slotID)
		executionIdentity := deriveOpaqueIdentity("exec-", executionIdentityDomain, registration.UID, slot.slotID)
		if _, duplicate := seenNames[name]; duplicate {
			return nil, fmt.Errorf("%w: derived Worker name collision", ErrWorkerIdentityCollision)
		}
		if _, duplicate := seenExecutions[executionIdentity]; duplicate {
			return nil, fmt.Errorf("%w: derived execution identity collision", ErrWorkerIdentityCollision)
		}
		seenNames[name] = struct{}{}
		seenExecutions[executionIdentity] = struct{}{}

		worker := &ateapipb.Worker{
			Metadata:        &ateapipb.ResourceMetadata{Name: name},
			WorkerNamespace: registration.Scope.WorkerNamespace,
			WorkerPool:      registration.Scope.WorkerPool,
			SandboxClass:    slot.sandboxClass,
			Labels:          slot.Labels(),
			Capacity:        slot.Capacity(),
			Provider:        ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
			ExternalSlot: &ateapipb.ExternalSlotIdentity{
				ExecutionIdentity: executionIdentity,
				LocalityIdentity:  localityIdentity,
			},
		}
		if !resources.IsValidResourceName(name) || !IsValidIdentity(executionIdentity) || !IsValidIdentity(localityIdentity) {
			return nil, fmt.Errorf("%w: derived identity violates the Worker contract", ErrInvalidWorkerPlan)
		}
		workers = append(workers, worker)
	}
	slices.SortFunc(workers, func(left, right *ateapipb.Worker) int {
		switch {
		case left.GetMetadata().GetName() < right.GetMetadata().GetName():
			return -1
		case left.GetMetadata().GetName() > right.GetMetadata().GetName():
			return 1
		default:
			return 0
		}
	})
	return &WorkerPlan{registration: registration, workers: workers}, nil
}

func deriveOpaqueIdentity(prefix, domain string, parts ...string) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(domain))
	var length [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(length[:], uint32(len(part)))
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(part))
	}
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hasher.Sum(nil))
	return prefix + string(bytesToLowerASCII([]byte(encoded)))
}

func bytesToLowerASCII(value []byte) []byte {
	for index, character := range value {
		if character >= 'A' && character <= 'Z' {
			value[index] = character + ('a' - 'A')
		}
	}
	return value
}
