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
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// workerExecutionTargetUID returns the provider-specific sandbox identity sent
// to AteomHerder. Kubernetes workloads retain their pod UID identity, while an
// ExternalSlot is pinned to the exact durable Worker resource incarnation.
func workerExecutionTargetUID(assignment *ateapipb.WorkerAssignment) (string, error) {
	if assignment == nil {
		return "", fmt.Errorf("worker execution assignment is required")
	}
	switch effectiveWorkerProvider(assignment.GetProvider()) {
	case ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD:
		if assignment.GetWorkerPodUid() == "" {
			return "", fmt.Errorf("KubernetesPod worker execution assignment has no pod UID")
		}
		return assignment.GetWorkerPodUid(), nil
	case ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT:
		if assignment.GetWorkerResourceUid() == "" {
			return "", fmt.Errorf("ExternalSlot worker execution assignment has no Worker resource UID")
		}
		if assignment.GetExternalSlot().GetExecutionIdentity() == "" {
			return "", fmt.Errorf("ExternalSlot worker execution assignment has no execution identity")
		}
		return assignment.GetWorkerResourceUid(), nil
	default:
		return "", fmt.Errorf("unsupported worker execution provider %q", assignment.GetProvider())
	}
}
