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
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/proto"
)

type reconcileWorkerStore struct {
	mu              sync.Mutex
	workers         map[string]*ateapipb.Worker
	createCalls     int
	updateCalls     int
	createRace      bool
	updateConflicts int
	nextUID         int
}

func newReconcileWorkerStore() *reconcileWorkerStore {
	return &reconcileWorkerStore{workers: make(map[string]*ateapipb.Worker)}
}

func (s *reconcileWorkerStore) GetWorker(_ context.Context, name string) (*ateapipb.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, ok := s.workers[name]
	if !ok {
		return nil, store.ErrNotFound
	}
	return proto.Clone(worker).(*ateapipb.Worker), nil
}

func (s *reconcileWorkerStore) CreateWorker(_ context.Context, worker *ateapipb.Worker) (*ateapipb.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createCalls++
	if _, exists := s.workers[worker.GetMetadata().GetName()]; exists {
		return nil, store.ErrAlreadyExists
	}
	created := proto.Clone(worker).(*ateapipb.Worker)
	s.nextUID++
	created.Metadata = &ateapipb.ResourceMetadata{
		Name:    worker.GetMetadata().GetName(),
		Uid:     fmt.Sprintf("00000000-0000-4000-8000-%012d", s.nextUID),
		Version: 1,
	}
	s.workers[created.GetMetadata().GetName()] = created
	if s.createRace {
		s.createRace = false
		return nil, store.ErrAlreadyExists
	}
	return proto.Clone(created).(*ateapipb.Worker), nil
}

func (s *reconcileWorkerStore) UpdateWorker(
	_ context.Context,
	name string,
	precondition store.Precondition,
	mutate func(*ateapipb.Worker) error,
) (*ateapipb.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCalls++
	if s.updateConflicts > 0 {
		s.updateConflicts--
		return nil, store.ErrVersionConflict
	}
	worker, ok := s.workers[name]
	if !ok {
		return nil, store.ErrNotFound
	}
	if err := precondition.Check(worker.GetMetadata()); err != nil {
		return nil, err
	}
	updated := proto.Clone(worker).(*ateapipb.Worker)
	if err := mutate(updated); err != nil {
		return nil, err
	}
	updated.Metadata = proto.Clone(worker.GetMetadata()).(*ateapipb.ResourceMetadata)
	updated.Metadata.Version++
	s.workers[name] = updated
	return proto.Clone(updated).(*ateapipb.Worker), nil
}

func (s *reconcileWorkerStore) mutate(name string, mutate func(*ateapipb.Worker)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mutate(s.workers[name])
}

func reconcileWorkerPlan(t *testing.T, registrationUID string, slots ...*externalproviderpb.ExternalSlot) *externalprovider.WorkerPlan {
	t.Helper()
	claim := externalprovider.SessionClaim{
		Registration: externalprovider.Registration{
			UID:           registrationUID,
			EnrollmentUID: "enrollment-a",
			Scope: externalprovider.Scope{
				OwnerAtespace:   "tenant-a",
				WorkerNamespace: "workers",
				WorkerPool:      "pool-a",
				MaxSlots:        uint32(len(slots)),
			},
		},
		Generation: 1,
	}
	admission, err := externalprovider.ValidateConnectAdmission(claim, &externalproviderpb.ClientFrame{
		Frame: &externalproviderpb.ClientFrame_Hello{Hello: &externalproviderpb.ConnectHello{
			RegistrationUid: registrationUID,
			ProtocolVersion: 1,
			Slots:           slots,
		}},
	})
	if err != nil {
		t.Fatalf("ValidateConnectAdmission() error = %v", err)
	}
	plan, err := externalprovider.PlanExternalWorkers(admission)
	if err != nil {
		t.Fatalf("PlanExternalWorkers() error = %v", err)
	}
	return plan
}

func reconcileSlot(id, sandbox string, labels map[string]string) *externalproviderpb.ExternalSlot {
	return &externalproviderpb.ExternalSlot{
		SlotId:       id,
		SandboxClass: sandbox,
		Labels:       labels,
		Capacity:     &ateapipb.WorkerCapacity{CpuMilli: 2_000, MemoryBytes: 4 << 30},
	}
}

func TestReconcileExternalWorkersCreatesOfflineAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	plan := reconcileWorkerPlan(t, "registration-a",
		reconcileSlot("slot-a", "native", map[string]string{"runtime": "codex"}),
		reconcileSlot("slot-b", "docker", map[string]string{"runtime": "claude"}),
	)

	first, err := reconcileExternalWorkers(ctx, persistence, plan)
	if err != nil {
		t.Fatalf("reconcileExternalWorkers() error = %v", err)
	}
	if len(first) != 2 || persistence.createCalls != 2 || persistence.updateCalls != 0 {
		t.Fatalf("first reconcile returned %d Workers after %d creates/%d updates", len(first), persistence.createCalls, persistence.updateCalls)
	}
	for _, worker := range first {
		if worker.GetMetadata().GetUid() == "" || worker.GetMetadata().GetVersion() != 1 ||
			worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			t.Errorf("created Worker is not a durable OFFLINE incarnation: %+v", worker)
		}
	}

	second, err := reconcileExternalWorkers(ctx, persistence, plan)
	if err != nil {
		t.Fatalf("second reconcileExternalWorkers() error = %v", err)
	}
	if persistence.createCalls != 2 || persistence.updateCalls != 0 {
		t.Fatalf("idempotent reconcile wrote again: %d creates/%d updates", persistence.createCalls, persistence.updateCalls)
	}
	for index := range first {
		if !proto.Equal(first[index], second[index]) {
			t.Errorf("idempotent reconcile changed Worker %d", index)
		}
	}
}

func TestReconcileExternalWorkersRefreshesOnlyMutableProviderFields(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	originalPlan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "native", map[string]string{"runtime": "codex"}))
	created, err := reconcileExternalWorkers(ctx, persistence, originalPlan)
	if err != nil {
		t.Fatal(err)
	}
	name := created[0].GetMetadata().GetName()
	persistence.mutate(name, func(worker *ateapipb.Worker) {
		worker.Status.State = ateapipb.WorkerState_WORKER_STATE_ACTIVE
		worker.Status.Assignment = newAPIAssignment("actor-uid-1")
	})

	changedPlan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "docker", map[string]string{"runtime": "codex", "accelerator": "none"}))
	updated, err := reconcileExternalWorkers(ctx, persistence, changedPlan)
	if err != nil {
		t.Fatalf("reconcileExternalWorkers(changed) error = %v", err)
	}
	worker := updated[0]
	if worker.GetSandboxClass() != "docker" || !maps.Equal(worker.GetLabels(), map[string]string{"runtime": "codex", "accelerator": "none"}) {
		t.Errorf("mutable fields were not refreshed: %+v", worker)
	}
	if worker.GetMetadata().GetUid() != created[0].GetMetadata().GetUid() || worker.GetMetadata().GetVersion() != 2 ||
		worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE ||
		worker.GetStatus().GetAssignment().GetActorUid() != "actor-uid-1" {
		t.Errorf("reconcile disturbed server-owned state: %+v", worker)
	}
}

func TestReconcileExternalWorkersRejectsIdentityCollision(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	plan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "native", nil))
	desired := plan.Workers()[0]
	collision := proto.Clone(desired).(*ateapipb.Worker)
	collision.ExternalSlot.ExecutionIdentity = "different-execution"
	collision.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_OFFLINE}
	if _, err := persistence.CreateWorker(ctx, collision); err != nil {
		t.Fatal(err)
	}

	if _, err := reconcileExternalWorkers(ctx, persistence, plan); !errors.Is(err, externalprovider.ErrWorkerIdentityCollision) {
		t.Fatalf("reconcileExternalWorkers() error = %v, want ErrWorkerIdentityCollision", err)
	}
}

func TestReconcileExternalWorkersRetriesCreateAndUpdateRaces(t *testing.T) {
	ctx := context.Background()
	t.Run("create", func(t *testing.T) {
		persistence := newReconcileWorkerStore()
		persistence.createRace = true
		plan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "native", nil))
		workers, err := reconcileExternalWorkers(ctx, persistence, plan)
		if err != nil {
			t.Fatalf("reconcileExternalWorkers() error = %v", err)
		}
		if len(workers) != 1 || persistence.createCalls != 1 {
			t.Fatalf("create race returned %d Workers after %d creates", len(workers), persistence.createCalls)
		}
	})

	t.Run("update", func(t *testing.T) {
		persistence := newReconcileWorkerStore()
		original := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "native", nil))
		if _, err := reconcileExternalWorkers(ctx, persistence, original); err != nil {
			t.Fatal(err)
		}
		persistence.updateConflicts = 2
		changed := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "docker", nil))
		workers, err := reconcileExternalWorkers(ctx, persistence, changed)
		if err != nil {
			t.Fatalf("reconcileExternalWorkers() error = %v", err)
		}
		if workers[0].GetSandboxClass() != "docker" || persistence.updateCalls != 3 {
			t.Fatalf("update race ended with sandbox %q after %d updates", workers[0].GetSandboxClass(), persistence.updateCalls)
		}
	})
}

func TestReconcileExternalWorkersDoesNotOwnOmittedSlots(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	fullPlan := reconcileWorkerPlan(t, "registration-a",
		reconcileSlot("slot-a", "native", nil),
		reconcileSlot("slot-b", "native", nil),
	)
	created, err := reconcileExternalWorkers(ctx, persistence, fullPlan)
	if err != nil {
		t.Fatal(err)
	}
	omitted := created[1]
	persistence.mutate(omitted.GetMetadata().GetName(), func(worker *ateapipb.Worker) {
		worker.Status.State = ateapipb.WorkerState_WORKER_STATE_ACTIVE
	})

	narrowPlan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "native", nil))
	if _, err := reconcileExternalWorkers(ctx, persistence, narrowPlan); err != nil {
		t.Fatal(err)
	}
	stillThere, err := persistence.GetWorker(ctx, omitted.GetMetadata().GetName())
	if err != nil || stillThere.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Fatalf("omitted slot was modified or removed: %+v, %v", stillThere, err)
	}
}

func TestReconcileExternalWorkersBoundsRetriesAndHonorsContext(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	original := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "native", nil))
	if _, err := reconcileExternalWorkers(ctx, persistence, original); err != nil {
		t.Fatal(err)
	}
	persistence.updateConflicts = maxExternalWorkerReconcileAttempts + 1
	changed := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "docker", nil))
	if _, err := reconcileExternalWorkers(ctx, persistence, changed); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("reconcileExternalWorkers() error = %v, want ErrVersionConflict", err)
	}
	if persistence.updateCalls != maxExternalWorkerReconcileAttempts {
		t.Fatalf("UpdateWorker calls = %d, want %d", persistence.updateCalls, maxExternalWorkerReconcileAttempts)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reconcileExternalWorkers(cancelled, persistence, changed); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled reconcile error = %v, want context.Canceled", err)
	}
	if _, err := reconcileExternalWorkers(context.Background(), persistence, nil); !errors.Is(err, externalprovider.ErrInvalidWorkerPlan) {
		t.Fatalf("nil plan error = %v, want ErrInvalidWorkerPlan", err)
	}
}
