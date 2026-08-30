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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

type conflictingWorkerStore struct {
	store.Interface
	once    sync.Once
	compete func(*ateapipb.Worker)
}

func (s *conflictingWorkerStore) UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapipb.Worker) error) (*ateapipb.Worker, error) {
	var injected bool
	var injectionErr error
	s.once.Do(func() {
		injected = true
		observed, err := s.Interface.GetWorker(ctx, name)
		if err != nil {
			injectionErr = err
			return
		}
		_, injectionErr = s.Interface.UpdateWorker(ctx, name, store.PreconditionFrom(observed), func(toUpdate *ateapipb.Worker) error {
			s.compete(toUpdate)
			return nil
		})
	})
	if injected && injectionErr != nil {
		return nil, injectionErr
	}
	return s.Interface.UpdateWorker(ctx, name, precondition, mutate)
}

type alwaysConflictingWorkerStore struct {
	store.Interface
	calls atomic.Int32
}

func (s *alwaysConflictingWorkerStore) UpdateWorker(context.Context, string, store.Precondition, func(*ateapipb.Worker) error) (*ateapipb.Worker, error) {
	s.calls.Add(1)
	return nil, store.ErrVersionConflict
}

func createAPIExternalWorker(t *testing.T, ctx context.Context, svc *RPCService) *ateapipb.Worker {
	t.Helper()
	worker := newAPIWorker(apiWorkerName)
	makeAPIWorkerExternal(worker)
	created, err := svc.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: worker})
	if err != nil {
		t.Fatalf("CreateWorker() failed: %v", err)
	}
	return created
}

func TestSetExternalWorkerAvailability(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	created := createAPIExternalWorker(t, ctx, svc)
	if created.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		t.Fatalf("initial state = %v, want OFFLINE", created.GetStatus().GetState())
	}

	active, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, created.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if err != nil {
		t.Fatalf("SetExternalWorkerAvailability(ACTIVE) failed: %v", err)
	}
	if active.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("state = %v, want ACTIVE", active.GetStatus().GetState())
	}
	if active.GetMetadata().GetVersion() != created.GetMetadata().GetVersion()+1 {
		t.Errorf("version = %d, want %d", active.GetMetadata().GetVersion(), created.GetMetadata().GetVersion()+1)
	}

	// Re-announcing the current state is idempotent and emits no store update.
	again, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, active.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if err != nil {
		t.Fatalf("second SetExternalWorkerAvailability(ACTIVE) failed: %v", err)
	}
	if diff := cmp.Diff(active, again, protocmp.Transform()); diff != "" {
		t.Errorf("idempotent transition changed Worker (-first +second):\n%s", diff)
	}

	assigned := assignAPIWorker(t, ctx, persistence, apiWorkerName, "actor-uid-1")
	offline, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, assigned.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_OFFLINE)
	if err != nil {
		t.Fatalf("SetExternalWorkerAvailability(OFFLINE) failed: %v", err)
	}
	if offline.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		t.Errorf("state = %v, want OFFLINE", offline.GetStatus().GetState())
	}
	if diff := cmp.Diff(assigned.GetStatus().GetAssignment(), offline.GetStatus().GetAssignment(), protocmp.Transform()); diff != "" {
		t.Errorf("going offline changed assignment (-want +got):\n%s", diff)
	}
	if offline.GetMetadata().GetVersion() != assigned.GetMetadata().GetVersion()+1 {
		t.Errorf("version = %d, want %d", offline.GetMetadata().GetVersion(), assigned.GetMetadata().GetVersion()+1)
	}

	stored, err := persistence.GetWorker(ctx, apiWorkerName)
	if err != nil {
		t.Fatalf("GetWorker() failed: %v", err)
	}
	if diff := cmp.Diff(offline, stored, protocmp.Transform()); diff != "" {
		t.Errorf("availability API returned something other than what it stored (-returned +stored):\n%s", diff)
	}
}

func TestSetExternalWorkerAvailability_RejectsNonExternalWorker(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	created, err := svc.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: newAPIWorker(apiWorkerName)})
	if err != nil {
		t.Fatalf("CreateWorker() failed: %v", err)
	}

	_, err = svc.SetExternalWorkerAvailability(ctx, apiWorkerName, created.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_OFFLINE)
	if !errors.Is(err, ErrExternalWorkerProvider) {
		t.Fatalf("SetExternalWorkerAvailability() error = %v, want ErrExternalWorkerProvider", err)
	}
	got, getErr := persistence.GetWorker(ctx, apiWorkerName)
	if getErr != nil {
		t.Fatalf("GetWorker() failed: %v", getErr)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("rejected transition changed Kubernetes Worker (-want +got):\n%s", diff)
	}
}

func TestSetExternalWorkerAvailability_DrainIsOneWay(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	createAPIExternalWorker(t, ctx, svc)
	draining, err := svc.DrainWorker(ctx, &ateapipb.DrainWorkerRequest{Worker: workerRef(apiWorkerName)})
	if err != nil {
		t.Fatalf("DrainWorker() failed: %v", err)
	}

	for _, desired := range []ateapipb.WorkerState{
		ateapipb.WorkerState_WORKER_STATE_ACTIVE,
		ateapipb.WorkerState_WORKER_STATE_OFFLINE,
	} {
		t.Run(desired.String(), func(t *testing.T) {
			_, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, draining.GetMetadata().GetUid(), desired)
			if !errors.Is(err, ErrExternalWorkerDraining) {
				t.Fatalf("SetExternalWorkerAvailability(%s) error = %v, want ErrExternalWorkerDraining", desired, err)
			}
		})
	}

	got, err := persistence.GetWorker(ctx, apiWorkerName)
	if err != nil {
		t.Fatalf("GetWorker() failed: %v", err)
	}
	if diff := cmp.Diff(draining, got, protocmp.Transform()); diff != "" {
		t.Errorf("availability transition changed draining Worker (-want +got):\n%s", diff)
	}
}

func TestSetExternalWorkerAvailability_RejectsInvalidStates(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	created := createAPIExternalWorker(t, ctx, svc)

	for _, desired := range []ateapipb.WorkerState{
		ateapipb.WorkerState_WORKER_STATE_UNSPECIFIED,
		ateapipb.WorkerState_WORKER_STATE_DRAINING,
		ateapipb.WorkerState(99),
	} {
		t.Run(desired.String(), func(t *testing.T) {
			_, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, created.GetMetadata().GetUid(), desired)
			if !errors.Is(err, ErrExternalWorkerState) {
				t.Fatalf("SetExternalWorkerAvailability(%s) error = %v, want ErrExternalWorkerState", desired, err)
			}
		})
	}

	got, err := persistence.GetWorker(ctx, apiWorkerName)
	if err != nil {
		t.Fatalf("GetWorker() failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("invalid desired state changed Worker (-want +got):\n%s", diff)
	}
}

func TestSetExternalWorkerAvailability_RejectsInvalidCurrentState(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	worker := newAPIWorker(apiWorkerName)
	makeAPIWorkerExternal(worker)
	worker.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_UNSPECIFIED}
	created := seedAPIWorker(t, ctx, persistence, worker)

	_, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, created.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if !errors.Is(err, ErrExternalWorkerState) {
		t.Fatalf("SetExternalWorkerAvailability() error = %v, want ErrExternalWorkerState", err)
	}
	got, getErr := persistence.GetWorker(ctx, apiWorkerName)
	if getErr != nil {
		t.Fatalf("GetWorker() failed: %v", getErr)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("invalid current state changed Worker (-want +got):\n%s", diff)
	}
}

func TestSetExternalWorkerAvailability_NotFound(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)

	_, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, apiOtherWorkerName, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("SetExternalWorkerAvailability() error = %v, want store.ErrNotFound", err)
	}
}

func TestSetExternalWorkerAvailability_StaleUIDCannotChangeRecreatedWorker(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	original := createAPIExternalWorker(t, ctx, svc)
	if _, err := persistence.DeleteWorker(ctx, apiWorkerName, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}
	recreated := createAPIExternalWorker(t, ctx, svc)
	if recreated.GetMetadata().GetUid() == original.GetMetadata().GetUid() {
		t.Fatalf("recreated Worker reused uid %q", recreated.GetMetadata().GetUid())
	}
	active, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, recreated.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if err != nil {
		t.Fatalf("activating recreated Worker failed: %v", err)
	}

	_, err = svc.SetExternalWorkerAvailability(ctx, apiWorkerName, original.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_OFFLINE)
	if !errors.Is(err, store.ErrUIDConflict) {
		t.Fatalf("stale SetExternalWorkerAvailability() error = %v, want store.ErrUIDConflict", err)
	}
	got, getErr := persistence.GetWorker(ctx, apiWorkerName)
	if getErr != nil {
		t.Fatalf("GetWorker() failed: %v", getErr)
	}
	if diff := cmp.Diff(active, got, protocmp.Transform()); diff != "" {
		t.Errorf("stale session changed recreated Worker (-want +got):\n%s", diff)
	}
}

func TestSetExternalWorkerAvailability_RequiresUID(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)
	createAPIExternalWorker(t, ctx, svc)

	_, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, "", ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if !errors.Is(err, store.ErrPreconditionRequired) {
		t.Fatalf("SetExternalWorkerAvailability() error = %v, want store.ErrPreconditionRequired", err)
	}
}

func TestSetExternalWorkerAvailability_RetriesConcurrentAssignment(t *testing.T) {
	ctx := context.Background()
	createService, persistence := newWorkerAPIService(t)
	created := createAPIExternalWorker(t, ctx, createService)
	conflicting := &conflictingWorkerStore{
		Interface: persistence,
		compete: func(toUpdate *ateapipb.Worker) {
			toUpdate.Status.Assignment = newAPIAssignment("actor-uid-1")
		},
	}
	svc := &RPCService{impl: conflicting}

	got, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, created.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if err != nil {
		t.Fatalf("SetExternalWorkerAvailability() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("state = %v, want ACTIVE", got.GetStatus().GetState())
	}
	if got.GetStatus().GetAssignment().GetActorUid() != "actor-uid-1" {
		t.Errorf("assignment = %v, want concurrent assignment preserved", got.GetStatus().GetAssignment())
	}
	if got.GetMetadata().GetVersion() != created.GetMetadata().GetVersion()+2 {
		t.Errorf("version = %d, want %d", got.GetMetadata().GetVersion(), created.GetMetadata().GetVersion()+2)
	}
}

func TestSetExternalWorkerAvailability_RacingDrainWins(t *testing.T) {
	ctx := context.Background()
	createService, persistence := newWorkerAPIService(t)
	created := createAPIExternalWorker(t, ctx, createService)
	conflicting := &conflictingWorkerStore{
		Interface: persistence,
		compete: func(toUpdate *ateapipb.Worker) {
			toUpdate.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
		},
	}
	svc := &RPCService{impl: conflicting}

	_, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, created.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if !errors.Is(err, ErrExternalWorkerDraining) {
		t.Fatalf("SetExternalWorkerAvailability() error = %v, want ErrExternalWorkerDraining", err)
	}
	got, getErr := persistence.GetWorker(ctx, apiWorkerName)
	if getErr != nil {
		t.Fatalf("GetWorker() failed: %v", getErr)
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("state = %v, want DRAINING", got.GetStatus().GetState())
	}
	if got.GetMetadata().GetVersion() != created.GetMetadata().GetVersion()+1 {
		t.Errorf("version = %d, want %d", got.GetMetadata().GetVersion(), created.GetMetadata().GetVersion()+1)
	}
}

func TestSetExternalWorkerAvailability_RetryIsBoundedAndContextAware(t *testing.T) {
	ctx := context.Background()
	createService, persistence := newWorkerAPIService(t)
	created := createAPIExternalWorker(t, ctx, createService)
	conflicting := &alwaysConflictingWorkerStore{Interface: persistence}
	svc := &RPCService{impl: conflicting}

	_, err := svc.SetExternalWorkerAvailability(ctx, apiWorkerName, created.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("SetExternalWorkerAvailability() error = %v, want store.ErrVersionConflict", err)
	}
	if got := conflicting.calls.Load(); got != maxExternalWorkerAvailabilityAttempts {
		t.Errorf("UpdateWorker calls = %d, want bounded %d", got, maxExternalWorkerAvailabilityAttempts)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	before := conflicting.calls.Load()
	_, err = svc.SetExternalWorkerAvailability(cancelled, apiWorkerName, created.GetMetadata().GetUid(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled SetExternalWorkerAvailability() error = %v, want context.Canceled", err)
	}
	if got := conflicting.calls.Load(); got != before {
		t.Errorf("cancelled call attempted %d updates, want none", got-before)
	}
}
