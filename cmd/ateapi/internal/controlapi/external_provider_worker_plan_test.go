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

package controlapi

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
)

// This test pins the pure external-provider plan to the authoritative
// CreateWorker validator without making the planning package depend on the RPC
// implementation.
func TestExternalProviderWorkerPlanSatisfiesCreateWorkerContract(t *testing.T) {
	claim := externalprovider.SessionClaim{
		Registration: externalprovider.Registration{
			UID:           "registration-a",
			EnrollmentUID: "enrollment-a",
			Scope: externalprovider.Scope{
				OwnerAtespace:   "tenant-a",
				WorkerNamespace: "workers",
				WorkerPool:      "pool-a",
				MaxSlots:        2,
			},
		},
		Generation: 1,
	}
	frame := &externalproviderpb.ClientFrame{Frame: &externalproviderpb.ClientFrame_Hello{Hello: &externalproviderpb.ConnectHello{
		RegistrationUid: claim.Registration.UID,
		ProtocolVersion: 1,
		Slots: []*externalproviderpb.ExternalSlot{
			{SlotId: "slot-a", SandboxClass: "gvisor", Labels: map[string]string{"region": "south"}, Capacity: &ateapipb.WorkerCapacity{CpuMilli: 1_000}},
			{SlotId: "slot-b", SandboxClass: "gvisor", Capacity: &ateapipb.WorkerCapacity{MemoryBytes: 1 << 30}},
		},
	}}}
	admission, err := externalprovider.ValidateConnectAdmission(claim, frame)
	if err != nil {
		t.Fatalf("ValidateConnectAdmission() error = %v", err)
	}
	plan, err := externalprovider.PlanExternalWorkers(admission)
	if err != nil {
		t.Fatalf("PlanExternalWorkers() error = %v", err)
	}
	for _, worker := range plan.Workers() {
		if got := worker.GetExternalSlot().GetOwnerAtespace(); got != claim.Registration.Scope.OwnerAtespace {
			t.Errorf("planned Worker %q owner atespace = %q, want %q", worker.GetMetadata().GetName(), got, claim.Registration.Scope.OwnerAtespace)
		}
		if errs := validateCreateWorkerRequest(&ateapipb.CreateWorkerRequest{Worker: worker}); len(errs) != 0 {
			t.Errorf("planned Worker %q failed CreateWorker validation: %v", worker.GetMetadata().GetName(), errs)
		}
	}
}
