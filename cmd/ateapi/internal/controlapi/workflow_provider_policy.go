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

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rejectUnsupportedSnapshotLifecycle keeps ExternalSlot actors in their
// current durable state. The first external-provider protocol supports only
// cold Run and Terminate; it has no checkpoint, restore, or node-local
// snapshot contract. Callers must invoke this after loading the immutable
// ActorTemplate and before persisting any lifecycle transition.
func rejectUnsupportedSnapshotLifecycle(template *atev1alpha1.ActorTemplate, operation string) error {
	if template == nil || template.Spec.WorkerProvider != atev1alpha1.WorkerProviderExternalSlot {
		return nil
	}
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("%s is not supported for ExternalSlot actors; delete and cold-start a new actor instead", operation))
}

// rejectUnsupportedSnapshotSource prevents a legacy or cloned ExternalSlot
// actor from reaching Restore. A cold resume has no snapshot source and is
// deliberately allowed to continue to AteomHerder.Run.
func rejectUnsupportedSnapshotSource(template *atev1alpha1.ActorTemplate, hasSnapshotSource bool, operation string) error {
	if template == nil || template.Spec.WorkerProvider != atev1alpha1.WorkerProviderExternalSlot || !hasSnapshotSource {
		return nil
	}
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("%s is not supported for ExternalSlot actors; only a cold start is supported", operation))
}
