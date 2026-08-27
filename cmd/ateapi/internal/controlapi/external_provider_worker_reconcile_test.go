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
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
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
	profiles := make([]externalprovider.SlotProfile, len(slots))
	declared := make([]*externalproviderpb.ExternalSlot, len(slots))
	for index, slot := range slots {
		profileID := fmt.Sprintf("profile-%03d", index)
		profiles[index] = externalprovider.SlotProfile{
			ProfileID:    profileID,
			SandboxClass: slot.GetSandboxClass(),
			Labels:       maps.Clone(slot.GetLabels()),
			MaxSlots:     1,
			CPUMilli:     slot.GetCapacity().GetCpuMilli(),
			MemoryBytes:  slot.GetCapacity().GetMemoryBytes(),
		}
		declared[index] = proto.Clone(slot).(*externalproviderpb.ExternalSlot)
		declared[index].ProfileId = profileID
		declared[index].SandboxClass = ""
		declared[index].Labels = nil
		declared[index].Capacity = nil
	}
	policy, err := externalprovider.NewSlotCapabilityPolicy(externalprovider.SlotCapabilityPolicyVersion, profiles)
	if err != nil {
		t.Fatalf("NewSlotCapabilityPolicy() error = %v", err)
	}
	claim := externalprovider.SessionClaim{
		Registration: externalprovider.Registration{
			UID:           registrationUID,
			EnrollmentUID: "enrollment-a",
			Scope: externalprovider.Scope{
				OwnerAtespace:   "tenant-a",
				WorkerNamespace: "workers",
				WorkerPool:      "pool-a",
				MaxSlots:        uint32(len(slots)),
				SlotPolicy:      policy,
			},
		},
		Generation: 1,
	}
	admission, err := externalprovider.ValidateConnectAdmission(claim, &externalproviderpb.ClientFrame{
		Frame: &externalproviderpb.ClientFrame_Hello{Hello: &externalproviderpb.ConnectHello{
			RegistrationUid:  registrationUID,
			ProtocolVersion:  2,
			SlotPolicyDigest: policy.DigestHex(),
			Slots:            declared,
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

func reconcileWorkerPoolLister(t *testing.T, labels map[string]string) listersv1alpha1.WorkerPoolLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	pool := &atev1alpha1.WorkerPool{ObjectMeta: metav1.ObjectMeta{
		Namespace: "workers",
		Name:      "pool-a",
		Labels:    maps.Clone(labels),
	}}
	if err := indexer.Add(pool); err != nil {
		t.Fatalf("adding WorkerPool to indexer: %v", err)
	}
	return listersv1alpha1.NewWorkerPoolLister(indexer)
}

func emptyReconcileWorkerPoolLister() listersv1alpha1.WorkerPoolLister {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	return listersv1alpha1.NewWorkerPoolLister(indexer)
}

func reconcileWorkers(
	t *testing.T,
	ctx context.Context,
	persistence externalWorkerPlanStore,
	plan *externalprovider.WorkerPlan,
) ([]*ateapipb.Worker, error) {
	t.Helper()
	return reconcileExternalWorkers(ctx, persistence, reconcileWorkerPoolLister(t, nil), plan)
}

func TestReconcileExternalWorkersCreatesOfflineAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	plan := reconcileWorkerPlan(t, "registration-a",
		reconcileSlot("slot-a", "gvisor", map[string]string{"runtime": "codex"}),
		reconcileSlot("slot-b", "microvm", map[string]string{"runtime": "claude"}),
	)

	first, err := reconcileWorkers(t, ctx, persistence, plan)
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

	second, err := reconcileWorkers(t, ctx, persistence, plan)
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

func TestReconcileExternalWorkersPropagatesPinnedPoolLabels(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	plan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", map[string]string{
		"runtime": "codex",
	}))
	poolLister := reconcileWorkerPoolLister(t, map[string]string{
		"kagent.dev/worker-pool": "coding-local",
		"placement":              "external",
	})

	workers, err := reconcileExternalWorkers(ctx, persistence, poolLister, plan)
	if err != nil {
		t.Fatalf("reconcileExternalWorkers() error = %v", err)
	}
	want := map[string]string{
		"runtime":                "codex",
		"kagent.dev/worker-pool": "coding-local",
		"placement":              "external",
	}
	if len(workers) != 1 || !maps.Equal(workers[0].GetLabels(), want) {
		t.Fatalf("effective Worker labels = %v, want %v", workers[0].GetLabels(), want)
	}
}

func TestReconcileExternalWorkersRejectsPoolLabelSpoofBeforeWrites(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	plan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", map[string]string{
		"kagent.dev/worker-pool": "coding-local",
	}))
	poolLister := reconcileWorkerPoolLister(t, map[string]string{
		"kagent.dev/worker-pool": "coding-local",
	})

	_, err := reconcileExternalWorkers(ctx, persistence, poolLister, plan)
	if !errors.Is(err, externalprovider.ErrInvalidWorkerPlan) || !strings.Contains(err.Error(), "collides with server-owned WorkerPool label") {
		t.Fatalf("reconcileExternalWorkers() error = %v, want explicit server-label collision", err)
	}
	if persistence.createCalls != 0 || persistence.updateCalls != 0 {
		t.Fatalf("spoof rejection wrote Workers: %d creates/%d updates", persistence.createCalls, persistence.updateCalls)
	}
}

func TestReconcileExternalWorkersRejectsUnavailableOrInvalidPoolBeforeWrites(t *testing.T) {
	ctx := context.Background()
	plan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", nil))

	t.Run("missing", func(t *testing.T) {
		persistence := newReconcileWorkerStore()
		_, err := reconcileExternalWorkers(ctx, persistence, emptyReconcileWorkerPoolLister(), plan)
		if !errors.Is(err, externalprovider.ErrInvalidWorkerPlan) || !strings.Contains(err.Error(), "resolving pinned WorkerPool") {
			t.Fatalf("reconcileExternalWorkers() error = %v, want missing pinned pool", err)
		}
		if persistence.createCalls != 0 || persistence.updateCalls != 0 {
			t.Fatalf("missing-pool rejection wrote Workers: %d creates/%d updates", persistence.createCalls, persistence.updateCalls)
		}
	})

	t.Run("invalid labels", func(t *testing.T) {
		persistence := newReconcileWorkerStore()
		poolLister := reconcileWorkerPoolLister(t, map[string]string{"not a label": "value"})
		_, err := reconcileExternalWorkers(ctx, persistence, poolLister, plan)
		if !errors.Is(err, externalprovider.ErrInvalidWorkerPlan) || !strings.Contains(err.Error(), "label key") {
			t.Fatalf("reconcileExternalWorkers() error = %v, want invalid pool labels", err)
		}
		if persistence.createCalls != 0 || persistence.updateCalls != 0 {
			t.Fatalf("invalid-pool rejection wrote Workers: %d creates/%d updates", persistence.createCalls, persistence.updateCalls)
		}
	})

	t.Run("lister unavailable", func(t *testing.T) {
		persistence := newReconcileWorkerStore()
		_, err := reconcileExternalWorkers(ctx, persistence, nil, plan)
		if !errors.Is(err, externalprovider.ErrInvalidWorkerPlan) {
			t.Fatalf("reconcileExternalWorkers() error = %v, want unavailable lister rejection", err)
		}
		if persistence.createCalls != 0 || persistence.updateCalls != 0 {
			t.Fatalf("unavailable-lister rejection wrote Workers: %d creates/%d updates", persistence.createCalls, persistence.updateCalls)
		}
	})
}

func TestReconcileExternalWorkersRefreshesPoolLabelsAndPreservesServerState(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	plan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", map[string]string{"runtime": "codex"}))

	created, err := reconcileExternalWorkers(ctx, persistence, reconcileWorkerPoolLister(t, map[string]string{
		"kagent.dev/worker-pool": "coding-local",
		"policy":                 "v1",
	}), plan)
	if err != nil {
		t.Fatal(err)
	}
	name := created[0].GetMetadata().GetName()
	persistence.mutate(name, func(worker *ateapipb.Worker) {
		worker.Status.State = ateapipb.WorkerState_WORKER_STATE_ACTIVE
		worker.Status.Assignment = newAPIAssignment("actor-uid-1")
	})

	updated, err := reconcileExternalWorkers(ctx, persistence, reconcileWorkerPoolLister(t, map[string]string{
		"kagent.dev/worker-pool": "coding-local",
		"policy":                 "v2",
	}), plan)
	if err != nil {
		t.Fatalf("reconcileExternalWorkers(updated pool) error = %v", err)
	}
	worker := updated[0]
	wantLabels := map[string]string{
		"runtime":                "codex",
		"kagent.dev/worker-pool": "coding-local",
		"policy":                 "v2",
	}
	if !maps.Equal(worker.GetLabels(), wantLabels) {
		t.Errorf("refreshed labels = %v, want %v", worker.GetLabels(), wantLabels)
	}
	if worker.GetMetadata().GetUid() != created[0].GetMetadata().GetUid() || worker.GetMetadata().GetVersion() != 2 ||
		worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE ||
		worker.GetStatus().GetAssignment().GetActorUid() != "actor-uid-1" {
		t.Errorf("pool label refresh disturbed Worker incarnation or server state: %+v", worker)
	}
	if persistence.createCalls != 1 || persistence.updateCalls != 1 {
		t.Errorf("pool label refresh used %d creates/%d updates, want 1/1", persistence.createCalls, persistence.updateCalls)
	}
}

func TestReconcileExternalWorkersPreflightsAllMergedLabelsBeforeWrites(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	tooMany := make(map[string]string, maxExternalWorkerLabels)
	for index := range maxExternalWorkerLabels {
		tooMany[fmt.Sprintf("capacity.example/label-%02d", index)] = "available"
	}
	plan := reconcileWorkerPlan(t, "registration-a",
		reconcileSlot("slot-a", "gvisor", map[string]string{"runtime": "codex"}),
		reconcileSlot("slot-b", "gvisor", tooMany),
	)
	poolLister := reconcileWorkerPoolLister(t, map[string]string{"placement": "external"})

	_, err := reconcileExternalWorkers(ctx, persistence, poolLister, plan)
	if !errors.Is(err, externalprovider.ErrInvalidWorkerPlan) || !strings.Contains(err.Error(), "merged labels exceed") {
		t.Fatalf("reconcileExternalWorkers() error = %v, want merged label bound", err)
	}
	if persistence.createCalls != 0 || persistence.updateCalls != 0 {
		t.Fatalf("multi-slot preflight left partial writes: %d creates/%d updates", persistence.createCalls, persistence.updateCalls)
	}
}

func TestReconcileExternalWorkersRefreshesOnlyMutableProviderFields(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	originalPlan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", map[string]string{"runtime": "codex"}))
	created, err := reconcileWorkers(t, ctx, persistence, originalPlan)
	if err != nil {
		t.Fatal(err)
	}
	name := created[0].GetMetadata().GetName()
	persistence.mutate(name, func(worker *ateapipb.Worker) {
		worker.Status.State = ateapipb.WorkerState_WORKER_STATE_ACTIVE
		worker.Status.Assignment = newAPIAssignment("actor-uid-1")
	})

	changedPlan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "microvm", map[string]string{"runtime": "codex", "accelerator": "none"}))
	updated, err := reconcileWorkers(t, ctx, persistence, changedPlan)
	if err != nil {
		t.Fatalf("reconcileExternalWorkers(changed) error = %v", err)
	}
	worker := updated[0]
	if worker.GetSandboxClass() != "microvm" || !maps.Equal(worker.GetLabels(), map[string]string{"runtime": "codex", "accelerator": "none"}) {
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
	plan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", nil))
	desired := plan.Workers()[0]
	collision := proto.Clone(desired).(*ateapipb.Worker)
	collision.ExternalSlot.ExecutionIdentity = "different-execution"
	collision.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_OFFLINE}
	if _, err := persistence.CreateWorker(ctx, collision); err != nil {
		t.Fatal(err)
	}

	if _, err := reconcileWorkers(t, ctx, persistence, plan); !errors.Is(err, externalprovider.ErrWorkerIdentityCollision) {
		t.Fatalf("reconcileExternalWorkers() error = %v, want ErrWorkerIdentityCollision", err)
	}
}

func TestReconcileExternalWorkersRetriesCreateAndUpdateRaces(t *testing.T) {
	ctx := context.Background()
	t.Run("create", func(t *testing.T) {
		persistence := newReconcileWorkerStore()
		persistence.createRace = true
		plan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", nil))
		workers, err := reconcileWorkers(t, ctx, persistence, plan)
		if err != nil {
			t.Fatalf("reconcileExternalWorkers() error = %v", err)
		}
		if len(workers) != 1 || persistence.createCalls != 1 {
			t.Fatalf("create race returned %d Workers after %d creates", len(workers), persistence.createCalls)
		}
	})

	t.Run("update", func(t *testing.T) {
		persistence := newReconcileWorkerStore()
		original := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", nil))
		if _, err := reconcileWorkers(t, ctx, persistence, original); err != nil {
			t.Fatal(err)
		}
		persistence.updateConflicts = 2
		changed := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "microvm", nil))
		workers, err := reconcileWorkers(t, ctx, persistence, changed)
		if err != nil {
			t.Fatalf("reconcileExternalWorkers() error = %v", err)
		}
		if workers[0].GetSandboxClass() != "microvm" || persistence.updateCalls != 3 {
			t.Fatalf("update race ended with sandbox %q after %d updates", workers[0].GetSandboxClass(), persistence.updateCalls)
		}
	})
}

func TestReconcileExternalWorkersDoesNotOwnOmittedSlots(t *testing.T) {
	ctx := context.Background()
	persistence := newReconcileWorkerStore()
	fullPlan := reconcileWorkerPlan(t, "registration-a",
		reconcileSlot("slot-a", "gvisor", nil),
		reconcileSlot("slot-b", "gvisor", nil),
	)
	created, err := reconcileWorkers(t, ctx, persistence, fullPlan)
	if err != nil {
		t.Fatal(err)
	}
	omitted := created[1]
	persistence.mutate(omitted.GetMetadata().GetName(), func(worker *ateapipb.Worker) {
		worker.Status.State = ateapipb.WorkerState_WORKER_STATE_ACTIVE
	})

	narrowPlan := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", nil))
	if _, err := reconcileWorkers(t, ctx, persistence, narrowPlan); err != nil {
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
	original := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "gvisor", nil))
	if _, err := reconcileWorkers(t, ctx, persistence, original); err != nil {
		t.Fatal(err)
	}
	persistence.updateConflicts = maxExternalWorkerReconcileAttempts + 1
	changed := reconcileWorkerPlan(t, "registration-a", reconcileSlot("slot-a", "microvm", nil))
	if _, err := reconcileWorkers(t, ctx, persistence, changed); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("reconcileExternalWorkers() error = %v, want ErrVersionConflict", err)
	}
	if persistence.updateCalls != maxExternalWorkerReconcileAttempts {
		t.Fatalf("UpdateWorker calls = %d, want %d", persistence.updateCalls, maxExternalWorkerReconcileAttempts)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reconcileWorkers(t, cancelled, persistence, changed); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled reconcile error = %v, want context.Canceled", err)
	}
	if _, err := reconcileExternalWorkers(context.Background(), persistence, reconcileWorkerPoolLister(t, nil), nil); !errors.Is(err, externalprovider.ErrInvalidWorkerPlan) {
		t.Fatalf("nil plan error = %v, want ErrInvalidWorkerPlan", err)
	}
}
