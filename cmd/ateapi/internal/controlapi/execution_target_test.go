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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestWorkerExecutionTargetUIDIsProviderSpecific(t *testing.T) {
	tests := []struct {
		name       string
		assignment *ateapipb.WorkerAssignment
		want       string
		wantError  string
	}{
		{
			name: "legacy Kubernetes assignment uses pod UID",
			assignment: &ateapipb.WorkerAssignment{
				WorkerPodUid:      "pod-uid",
				WorkerResourceUid: "resource-uid",
			},
			want: "pod-uid",
		},
		{
			name: "explicit Kubernetes assignment uses pod UID",
			assignment: &ateapipb.WorkerAssignment{
				Provider:          ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD,
				WorkerPodUid:      "pod-uid",
				WorkerResourceUid: "resource-uid",
			},
			want: "pod-uid",
		},
		{
			name: "ExternalSlot assignment uses Worker resource UID",
			assignment: &ateapipb.WorkerAssignment{
				Provider:          ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
				WorkerResourceUid: "11111111-1111-4111-8111-111111111111",
				ExternalSlot:      &ateapipb.ExternalSlotIdentity{ExecutionIdentity: "exec-slot-a"},
			},
			want: "11111111-1111-4111-8111-111111111111",
		},
		{name: "missing assignment", wantError: "assignment is required"},
		{
			name:       "Kubernetes assignment missing pod UID",
			assignment: &ateapipb.WorkerAssignment{Provider: ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD},
			wantError:  "no pod UID",
		},
		{
			name: "ExternalSlot assignment missing Worker resource UID",
			assignment: &ateapipb.WorkerAssignment{
				Provider:     ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
				ExternalSlot: &ateapipb.ExternalSlotIdentity{ExecutionIdentity: "exec-slot-a"},
			},
			wantError: "no Worker resource UID",
		},
		{
			name: "ExternalSlot assignment missing execution identity",
			assignment: &ateapipb.WorkerAssignment{
				Provider:          ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
				WorkerResourceUid: "11111111-1111-4111-8111-111111111111",
			},
			wantError: "no execution identity",
		},
		{
			name:       "unknown provider",
			assignment: &ateapipb.WorkerAssignment{Provider: ateapipb.WorkerProvider(99)},
			wantError:  "unsupported worker execution provider",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := workerExecutionTargetUID(test.assignment)
			if test.wantError == "" {
				if err != nil || got != test.want {
					t.Fatalf("workerExecutionTargetUID() = %q, %v, want %q, nil", got, err, test.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) || got != "" {
				t.Fatalf("workerExecutionTargetUID() = %q, %v, want error containing %q", got, err, test.wantError)
			}
		})
	}
}
