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
	"errors"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

type startupAvailabilityCall struct {
	name  string
	uid   string
	state ateapipb.WorkerState
}

type fakeStartupRecoveryStore struct {
	pages        map[string]*ateapipb.ListWorkersResponse
	listErr      map[string]error
	availability map[string]*ateapipb.Worker
	setErr       map[string]error
	listTokens   []string
	setCalls     []startupAvailabilityCall
}

func (f *fakeStartupRecoveryStore) ListWorkers(_ context.Context, request *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	f.listTokens = append(f.listTokens, request.GetPageToken())
	if request.GetPageSize() != startupSweepPageSize {
		return nil, fmt.Errorf("unexpected page size %d", request.GetPageSize())
	}
	if err := f.listErr[request.GetPageToken()]; err != nil {
		return nil, err
	}
	page := f.pages[request.GetPageToken()]
	if page == nil {
		return nil, nil
	}
	return proto.Clone(page).(*ateapipb.ListWorkersResponse), nil
}

func (f *fakeStartupRecoveryStore) SetExternalWorkerAvailability(_ context.Context, name, uid string, state ateapipb.WorkerState) (*ateapipb.Worker, error) {
	f.setCalls = append(f.setCalls, startupAvailabilityCall{name: name, uid: uid, state: state})
	if err := f.setErr[name]; err != nil {
		return nil, err
	}
	worker := f.availability[name]
	if worker == nil {
		return nil, fmt.Errorf("missing availability result")
	}
	return proto.Clone(worker).(*ateapipb.Worker), nil
}

func TestRecoverExternalWorkersOfflinePreflightsThenPinsActiveIncarnations(t *testing.T) {
	activeA := startupWorker(1, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	offline := startupWorker(2, ateapipb.WorkerState_WORKER_STATE_OFFLINE)
	draining := startupWorker(3, ateapipb.WorkerState_WORKER_STATE_DRAINING)
	activeB := startupWorker(4, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	kubernetes := startupWorker(5, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	kubernetes.Provider = ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD
	kubernetes.ExternalSlot = nil

	store := &fakeStartupRecoveryStore{
		pages: map[string]*ateapipb.ListWorkersResponse{
			"":       {Workers: []*ateapipb.Worker{activeA, offline, kubernetes}, NextPageToken: "page-2"},
			"page-2": {Workers: []*ateapipb.Worker{draining, activeB}},
		},
		availability: map[string]*ateapipb.Worker{
			activeA.GetMetadata().GetName(): startupWorker(1, ateapipb.WorkerState_WORKER_STATE_OFFLINE),
			activeB.GetMetadata().GetName(): startupWorker(4, ateapipb.WorkerState_WORKER_STATE_OFFLINE),
		},
	}
	result, err := RecoverExternalWorkersOffline(context.Background(), store, StartupSweepConfig{MaxScannedWorkers: 10})
	if err != nil {
		t.Fatalf("RecoverExternalWorkersOffline() error = %v", err)
	}
	want := StartupSweepResult{Scanned: 5, External: 4, Offlined: 2, AlreadyOffline: 1, AlreadyDraining: 1}
	if result != want {
		t.Fatalf("result = %+v, want %+v", result, want)
	}
	if len(store.listTokens) != 2 || store.listTokens[0] != "" || store.listTokens[1] != "page-2" {
		t.Fatalf("list tokens = %v, want initial and page-2", store.listTokens)
	}
	wantCalls := []startupAvailabilityCall{
		{name: activeA.GetMetadata().GetName(), uid: activeA.GetMetadata().GetUid(), state: ateapipb.WorkerState_WORKER_STATE_OFFLINE},
		{name: activeB.GetMetadata().GetName(), uid: activeB.GetMetadata().GetUid(), state: ateapipb.WorkerState_WORKER_STATE_OFFLINE},
	}
	if !equalStartupCalls(store.setCalls, wantCalls) {
		t.Fatalf("availability calls = %+v, want %+v", store.setCalls, wantCalls)
	}
}

func TestRecoverExternalWorkersOfflineRejectsInventoryBeforeWrites(t *testing.T) {
	valid := startupWorker(1, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	tests := []struct {
		name   string
		store  *fakeStartupRecoveryStore
		config StartupSweepConfig
	}{
		{
			name:  "nil page",
			store: &fakeStartupRecoveryStore{pages: map[string]*ateapipb.ListWorkersResponse{}, listErr: map[string]error{}},
		},
		{
			name:  "list error",
			store: &fakeStartupRecoveryStore{pages: map[string]*ateapipb.ListWorkersResponse{}, listErr: map[string]error{"": errors.New("database unavailable")}},
		},
		{
			name:   "limit",
			store:  &fakeStartupRecoveryStore{pages: map[string]*ateapipb.ListWorkersResponse{"": {Workers: []*ateapipb.Worker{valid, startupWorker(2, ateapipb.WorkerState_WORKER_STATE_ACTIVE)}}}},
			config: StartupSweepConfig{MaxScannedWorkers: 1},
		},
		{
			name:  "token cycle",
			store: &fakeStartupRecoveryStore{pages: map[string]*ateapipb.ListWorkersResponse{"": {Workers: []*ateapipb.Worker{valid}, NextPageToken: "cycle"}, "cycle": {NextPageToken: "cycle"}}},
		},
		{
			name:  "duplicate Worker",
			store: &fakeStartupRecoveryStore{pages: map[string]*ateapipb.ListWorkersResponse{"": {Workers: []*ateapipb.Worker{valid, proto.Clone(valid).(*ateapipb.Worker)}}}},
		},
		{
			name:  "unknown state",
			store: &fakeStartupRecoveryStore{pages: map[string]*ateapipb.ListWorkersResponse{"": {Workers: []*ateapipb.Worker{startupWorker(1, ateapipb.WorkerState_WORKER_STATE_UNSPECIFIED)}}}},
		},
		{
			name:  "incomplete identity",
			store: &fakeStartupRecoveryStore{pages: map[string]*ateapipb.ListWorkersResponse{"": {Workers: []*ateapipb.Worker{{Provider: ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT}}}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.store.listErr == nil {
				test.store.listErr = map[string]error{}
			}
			if test.store.availability == nil {
				test.store.availability = map[string]*ateapipb.Worker{}
			}
			_, err := RecoverExternalWorkersOffline(context.Background(), test.store, test.config)
			if err == nil {
				t.Fatal("RecoverExternalWorkersOffline() succeeded, want error")
			}
			if len(test.store.setCalls) != 0 {
				t.Fatalf("preflight failure performed availability writes: %+v", test.store.setCalls)
			}
		})
	}
}

func TestRecoverExternalWorkersOfflineFailsClosedOnTransitionMismatch(t *testing.T) {
	active := startupWorker(1, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	wrong := startupWorker(1, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	store := &fakeStartupRecoveryStore{
		pages:        map[string]*ateapipb.ListWorkersResponse{"": {Workers: []*ateapipb.Worker{active}}},
		availability: map[string]*ateapipb.Worker{active.GetMetadata().GetName(): wrong},
		listErr:      map[string]error{},
	}
	result, err := RecoverExternalWorkersOffline(context.Background(), store, StartupSweepConfig{})
	if !errors.Is(err, errInvalidStartupSweep) {
		t.Fatalf("error = %v, want errInvalidStartupSweep", err)
	}
	if result.Offlined != 0 || len(store.setCalls) != 1 {
		t.Fatalf("result/calls = %+v/%+v, want one failed validation", result, store.setCalls)
	}
}

func TestRecoverExternalWorkersOfflineValidatesArgumentsAndContext(t *testing.T) {
	if _, err := RecoverExternalWorkersOffline(nil, &fakeStartupRecoveryStore{}, StartupSweepConfig{}); !errors.Is(err, errInvalidStartupSweep) {
		t.Fatalf("nil context error = %v, want errInvalidStartupSweep", err)
	}
	if _, err := RecoverExternalWorkersOffline(context.Background(), nil, StartupSweepConfig{}); !errors.Is(err, errInvalidStartupSweep) {
		t.Fatalf("nil store error = %v, want errInvalidStartupSweep", err)
	}
	if _, err := RecoverExternalWorkersOffline(context.Background(), &fakeStartupRecoveryStore{}, StartupSweepConfig{MaxScannedWorkers: maximumStartupSweepMaxWorkers + 1}); !errors.Is(err, errInvalidStartupSweep) {
		t.Fatalf("large bound error = %v, want errInvalidStartupSweep", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &fakeStartupRecoveryStore{pages: map[string]*ateapipb.ListWorkersResponse{}, listErr: map[string]error{}}
	if _, err := RecoverExternalWorkersOffline(ctx, store, StartupSweepConfig{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context error = %v, want context.Canceled", err)
	}
}

func startupWorker(id int, state ateapipb.WorkerState) *ateapipb.Worker {
	return &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{
			Name: fmt.Sprintf("worker-%d", id),
			Uid:  fmt.Sprintf("uid-%d", id),
		},
		Provider: ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
		ExternalSlot: &ateapipb.ExternalSlotIdentity{
			ExecutionIdentity: fmt.Sprintf("execution-%d", id),
			LocalityIdentity:  fmt.Sprintf("locality-%d", id),
		},
		Status: &ateapipb.WorkerStatus{State: state},
	}
}

func equalStartupCalls(left, right []startupAvailabilityCall) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
