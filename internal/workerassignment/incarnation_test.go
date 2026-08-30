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

package workerassignment

import (
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestValidateIncarnation(t *testing.T) {
	const (
		workerName = "worker-a"
		workerUID  = "11111111-1111-4111-8111-111111111111"
	)
	validAssignment := func() *ateapipb.WorkerAssignment {
		return &ateapipb.WorkerAssignment{
			Worker:            &ateapipb.ObjectRef{Name: workerName},
			WorkerResourceUid: workerUID,
		}
	}
	validWorker := func() *ateapipb.Worker {
		return &ateapipb.Worker{
			Metadata: &ateapipb.ResourceMetadata{Name: workerName, Uid: workerUID},
		}
	}

	tests := []struct {
		name       string
		assignment *ateapipb.WorkerAssignment
		worker     *ateapipb.Worker
		wantErr    bool
	}{
		{name: "exact incarnation", assignment: validAssignment(), worker: validWorker()},
		{name: "missing assignment", worker: validWorker(), wantErr: true},
		{name: "missing worker reference", assignment: &ateapipb.WorkerAssignment{WorkerResourceUid: workerUID}, worker: validWorker(), wantErr: true},
		{name: "legacy assignment is unpinned", assignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: workerName}}, worker: validWorker(), wantErr: true},
		{name: "missing resolved worker", assignment: validAssignment(), wantErr: true},
		{name: "resolved worker has no UID", assignment: validAssignment(), worker: &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: workerName}}, wantErr: true},
		{name: "different resource name", assignment: validAssignment(), worker: &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "worker-b", Uid: workerUID}}, wantErr: true},
		{name: "different resource UID", assignment: validAssignment(), worker: &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: workerName, Uid: "22222222-2222-4222-8222-222222222222"}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIncarnation(tt.assignment, tt.worker)
			if tt.wantErr {
				if !errors.Is(err, ErrIncarnationMismatch) {
					t.Fatalf("ValidateIncarnation() error = %v, want ErrIncarnationMismatch", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateIncarnation() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateIncarnationRejectsRecreatedWorkerWithSameName(t *testing.T) {
	const workerName = "stable-worker-name"
	original := &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{
			Name: workerName,
			Uid:  "11111111-1111-4111-8111-111111111111",
		},
	}
	assignment := &ateapipb.WorkerAssignment{
		Worker:            &ateapipb.ObjectRef{Name: original.GetMetadata().GetName()},
		WorkerResourceUid: original.GetMetadata().GetUid(),
	}

	// Deleting and recreating a Worker may reuse its stable resource name, but
	// the server assigns a new UID to the new durable resource incarnation.
	recreated := &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{
			Name: workerName,
			Uid:  "22222222-2222-4222-8222-222222222222",
		},
	}
	if err := ValidateIncarnation(assignment, recreated); !errors.Is(err, ErrIncarnationMismatch) {
		t.Fatalf("ValidateIncarnation(old assignment, recreated Worker) error = %v, want ErrIncarnationMismatch", err)
	}
}
