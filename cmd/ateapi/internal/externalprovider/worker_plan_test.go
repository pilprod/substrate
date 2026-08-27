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
	"errors"
	"maps"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/proto"
)

func workerPlanAdmission(t *testing.T, registrationUID string, slotIDs ...string) *ConnectAdmission {
	t.Helper()
	claim := validSessionClaim(uint32(len(slotIDs)))
	claim.Registration.UID = registrationUID
	frame := validClientFrame()
	frame.GetHello().RegistrationUid = registrationUID
	frame.GetHello().Slots = make([]*externalproviderpb.ExternalSlot, len(slotIDs))
	for index, slotID := range slotIDs {
		frame.GetHello().Slots[index] = validExternalSlot(slotID)
	}
	admission, err := ValidateConnectAdmission(claim, frame)
	if err != nil {
		t.Fatalf("ValidateConnectAdmission() error = %v", err)
	}
	return admission
}

func TestPlanExternalWorkersDerivesStableOpaqueProviderIdentity(t *testing.T) {
	admission := workerPlanAdmission(t, "registration-a", "slot-a", "slot-b")
	first, err := PlanExternalWorkers(admission)
	if err != nil {
		t.Fatalf("PlanExternalWorkers() error = %v", err)
	}
	second, err := PlanExternalWorkers(admission)
	if err != nil {
		t.Fatalf("second PlanExternalWorkers() error = %v", err)
	}
	if first.Registration() != admission.Registration() {
		t.Fatalf("Registration() = %+v, want %+v", first.Registration(), admission.Registration())
	}
	firstWorkers := first.Workers()
	secondWorkers := second.Workers()
	if !proto.Equal(&ateapipb.ListWorkersResponse{Workers: firstWorkers}, &ateapipb.ListWorkersResponse{Workers: secondWorkers}) {
		t.Fatal("same admission produced different Worker plans")
	}
	if len(firstWorkers) != 2 {
		t.Fatalf("Workers() length = %d, want 2", len(firstWorkers))
	}

	var locality string
	executions := make(map[string]struct{}, len(firstWorkers))
	for _, worker := range firstWorkers {
		name := worker.GetMetadata().GetName()
		execution := worker.GetExternalSlot().GetExecutionIdentity()
		candidateLocality := worker.GetExternalSlot().GetLocalityIdentity()
		if !resources.IsValidResourceName(name) || !strings.HasPrefix(name, "ext-") || len(name) != 56 {
			t.Errorf("derived Worker name = %q", name)
		}
		if !IsValidIdentity(execution) || !strings.HasPrefix(execution, "exec-") || len(execution) != 57 {
			t.Errorf("derived execution identity = %q", execution)
		}
		if !IsValidIdentity(candidateLocality) || !strings.HasPrefix(candidateLocality, "loc-") || len(candidateLocality) != 56 {
			t.Errorf("derived locality identity = %q", candidateLocality)
		}
		for _, raw := range []string{"registration-a", "slot-a", "slot-b"} {
			if strings.Contains(name, raw) || strings.Contains(execution, raw) || strings.Contains(candidateLocality, raw) {
				t.Errorf("derived identity exposed caller string %q", raw)
			}
		}
		if locality == "" {
			locality = candidateLocality
		} else if candidateLocality != locality {
			t.Errorf("one registration produced locality identities %q and %q", locality, candidateLocality)
		}
		if _, duplicate := executions[execution]; duplicate {
			t.Errorf("duplicate execution identity %q", execution)
		}
		executions[execution] = struct{}{}
		if worker.GetProvider() != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT || worker.GetStatus() != nil {
			t.Errorf("Worker provider/status = %v/%v", worker.GetProvider(), worker.GetStatus())
		}
		if worker.GetWorkerNamespace() != "workers" || worker.GetWorkerPool() != "pool-a" ||
			worker.GetWorkerPod() != "" || worker.GetWorkerPodUid() != "" || worker.GetNodeName() != "" || worker.GetIp() != "" {
			t.Errorf("Worker provider coordinates = %+v", worker)
		}
	}
	if firstWorkers[0].GetMetadata().GetName() >= firstWorkers[1].GetMetadata().GetName() {
		t.Fatal("Workers are not sorted by derived resource name")
	}
}

func TestDerivedIdentitiesAreDomainSeparatedAndLengthFramed(t *testing.T) {
	workerName := deriveOpaqueIdentity("ext-", workerNameDomain, "registration-a", "slot-a")
	execution := deriveOpaqueIdentity("exec-", executionIdentityDomain, "registration-a", "slot-a")
	locality := deriveOpaqueIdentity("loc-", localityIdentityDomain, "registration-a")
	if workerName != "ext-jbwjozioobozinex6ri6tjypj5qvhqin6ptkzshbkm22fxxrejta" ||
		execution != "exec-cgekxj4bfqw5fsuso32jvnv52pzqsem6z2tk3qq6m7co43zsf3bq" ||
		locality != "loc-tcfc7blrzhzlnop7zdc6swnazlcq6jmbx4qg77hffrhmbkrcikpq" {
		t.Fatalf("stable identity derivation changed: %q %q %q", workerName, execution, locality)
	}
	if strings.TrimPrefix(workerName, "ext-") == strings.TrimPrefix(execution, "exec-") ||
		strings.TrimPrefix(workerName, "ext-") == strings.TrimPrefix(locality, "loc-") ||
		strings.TrimPrefix(execution, "exec-") == strings.TrimPrefix(locality, "loc-") {
		t.Fatal("identity domains produced the same digest")
	}
	left := deriveOpaqueIdentity("exec-", executionIdentityDomain, "a", "bc")
	right := deriveOpaqueIdentity("exec-", executionIdentityDomain, "ab", "c")
	if left == right {
		t.Fatal("length framing did not distinguish ambiguous concatenations")
	}
	if got := deriveOpaqueIdentity("exec-", executionIdentityDomain, "registration-a", "slot-a"); got != execution {
		t.Fatal("identity derivation is not deterministic")
	}
}

func TestWorkerIdentityChangesOnlyWithItsAuthority(t *testing.T) {
	base, err := PlanExternalWorkers(workerPlanAdmission(t, "registration-a", "slot-a"))
	if err != nil {
		t.Fatal(err)
	}
	otherSlot, err := PlanExternalWorkers(workerPlanAdmission(t, "registration-a", "slot-b"))
	if err != nil {
		t.Fatal(err)
	}
	otherRegistration, err := PlanExternalWorkers(workerPlanAdmission(t, "registration-b", "slot-a"))
	if err != nil {
		t.Fatal(err)
	}
	baseWorker := base.Workers()[0]
	otherSlotWorker := otherSlot.Workers()[0]
	otherRegistrationWorker := otherRegistration.Workers()[0]
	if baseWorker.GetMetadata().GetName() == otherSlotWorker.GetMetadata().GetName() ||
		baseWorker.GetExternalSlot().GetExecutionIdentity() == otherSlotWorker.GetExternalSlot().GetExecutionIdentity() {
		t.Fatal("different slots shared a Worker or execution identity")
	}
	if baseWorker.GetExternalSlot().GetLocalityIdentity() != otherSlotWorker.GetExternalSlot().GetLocalityIdentity() {
		t.Fatal("slots in one registration did not share locality")
	}
	if baseWorker.GetMetadata().GetName() == otherRegistrationWorker.GetMetadata().GetName() ||
		baseWorker.GetExternalSlot().GetExecutionIdentity() == otherRegistrationWorker.GetExternalSlot().GetExecutionIdentity() ||
		baseWorker.GetExternalSlot().GetLocalityIdentity() == otherRegistrationWorker.GetExternalSlot().GetLocalityIdentity() {
		t.Fatal("different registrations shared derived authority")
	}
}

func TestWorkerPlanDoesNotAliasAdmissionOrAccessors(t *testing.T) {
	admission := workerPlanAdmission(t, "registration-a", "slot-a")
	plan, err := PlanExternalWorkers(admission)
	if err != nil {
		t.Fatal(err)
	}
	admission.registration.Scope.WorkerPool = "mutated-pool"
	admission.slots[0].sandboxClass = "mutated-sandbox"
	admission.slots[0].labels[0].value = "mutated-label"
	admission.slots[0].cpuMilli = 1

	first := plan.Workers()
	first[0].WorkerPool = "accessor-mutated"
	first[0].SandboxClass = "accessor-mutated"
	first[0].Labels["region"] = "accessor-mutated"
	first[0].Capacity.CpuMilli = 2
	first[0].ExternalSlot.ExecutionIdentity = "accessor-mutated"

	second := plan.Workers()[0]
	if second.GetWorkerPool() != "pool-a" || second.GetSandboxClass() != "gvisor" ||
		second.GetLabels()["region"] != "south" || second.GetCapacity().GetCpuMilli() != 2_000 ||
		!strings.HasPrefix(second.GetExternalSlot().GetExecutionIdentity(), "exec-") {
		t.Fatalf("immutable Worker plan was mutated: %+v", second)
	}
	if plan.Registration().Scope.WorkerPool != "pool-a" {
		t.Fatal("Worker plan registration aliased admission")
	}
}

func TestWorkerPlanValidateExistingProtectsImmutableIdentity(t *testing.T) {
	plan, err := PlanExternalWorkers(workerPlanAdmission(t, "registration-a", "slot-a"))
	if err != nil {
		t.Fatal(err)
	}
	desired := plan.Workers()[0]
	existing := proto.Clone(desired).(*ateapipb.Worker)
	existing.Metadata.Uid = "server-uid"
	existing.Metadata.Version = 9
	existing.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_OFFLINE}
	existing.SandboxClass = "new-sandbox"
	existing.Labels = map[string]string{"mutable": "label"}
	if err := plan.ValidateExisting(existing); err != nil {
		t.Fatalf("ValidateExisting(mutable/server fields) error = %v", err)
	}

	mutations := []struct {
		name string
		edit func(*ateapipb.Worker)
	}{
		{name: "atespace", edit: func(worker *ateapipb.Worker) { worker.Metadata.Atespace = "tenant-a" }},
		{name: "provider", edit: func(worker *ateapipb.Worker) {
			worker.Provider = ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD
		}},
		{name: "namespace", edit: func(worker *ateapipb.Worker) { worker.WorkerNamespace = "other" }},
		{name: "pool", edit: func(worker *ateapipb.Worker) { worker.WorkerPool = "other" }},
		{name: "pod coordinate", edit: func(worker *ateapipb.Worker) { worker.WorkerPod = "pod" }},
		{name: "execution", edit: func(worker *ateapipb.Worker) { worker.ExternalSlot.ExecutionIdentity = "other" }},
		{name: "locality", edit: func(worker *ateapipb.Worker) { worker.ExternalSlot.LocalityIdentity = "other" }},
		{name: "capacity", edit: func(worker *ateapipb.Worker) { worker.Capacity.CpuMilli++ }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			candidate := proto.Clone(desired).(*ateapipb.Worker)
			test.edit(candidate)
			if err := plan.ValidateExisting(candidate); !errors.Is(err, ErrWorkerIdentityCollision) {
				t.Fatalf("ValidateExisting() error = %v, want ErrWorkerIdentityCollision", err)
			}
		})
	}
	unknown := proto.Clone(desired).(*ateapipb.Worker)
	unknown.Metadata.Name = "ext-" + strings.Repeat("a", 52)
	if err := plan.ValidateExisting(unknown); !errors.Is(err, ErrInvalidWorkerPlan) {
		t.Fatalf("ValidateExisting(unknown) error = %v", err)
	}
	if err := plan.ValidateExisting(nil); !errors.Is(err, ErrInvalidWorkerPlan) {
		t.Fatalf("ValidateExisting(nil) error = %v", err)
	}
}

func TestPlanExternalWorkersRejectsIncompleteAdmission(t *testing.T) {
	for _, admission := range []*ConnectAdmission{
		nil,
		{},
		{generation: 1, registration: Registration{UID: "registration-a"}},
		{generation: 1, registration: validSessionClaim(1).Registration, slots: []AdmittedSlot{{slotID: "/invalid"}}},
	} {
		if _, err := PlanExternalWorkers(admission); !errors.Is(err, ErrInvalidWorkerPlan) {
			t.Fatalf("PlanExternalWorkers(%+v) error = %v", admission, err)
		}
	}
}

func TestWorkerPlanAccessorsAreRaceSafe(t *testing.T) {
	plan, err := PlanExternalWorkers(workerPlanAdmission(t, "registration-a", "slot-a", "slot-b"))
	if err != nil {
		t.Fatal(err)
	}
	wantLabels := map[string]string{"region": "south", "topology.example/zone": "zone-a"}
	var wait sync.WaitGroup
	for worker := range 32 {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for range 100 {
				workers := plan.Workers()
				if len(workers) != 2 || !maps.Equal(workers[0].GetLabels(), wantLabels) {
					t.Errorf("Worker plan changed under concurrent access")
					return
				}
				workers[0].Labels["region"] = string(rune('a' + worker%26))
				workers[0].Capacity.CpuMilli = int64(worker)
			}
		}(worker)
	}
	wait.Wait()
}
