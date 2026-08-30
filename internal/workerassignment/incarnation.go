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

// Package workerassignment validates durable Worker assignment identity.
package workerassignment

import (
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ErrIncarnationMismatch marks an assignment which cannot be proven to refer
// to the supplied Worker resource incarnation.
var ErrIncarnationMismatch = errors.New("worker assignment incarnation mismatch")

// ValidateIncarnation verifies that assignment pins the exact supplied Worker
// resource by both global resource name and server-assigned resource UID. It is
// deliberately strict: legacy assignments with no resource UID fail closed.
func ValidateIncarnation(assignment *ateapipb.WorkerAssignment, worker *ateapipb.Worker) error {
	if assignment == nil {
		return fmt.Errorf("%w: assignment is missing", ErrIncarnationMismatch)
	}
	assignedName := assignment.GetWorker().GetName()
	if assignedName == "" {
		return fmt.Errorf("%w: assignment worker name is missing", ErrIncarnationMismatch)
	}
	assignedUID := assignment.GetWorkerResourceUid()
	if assignedUID == "" {
		return fmt.Errorf("%w: assignment worker resource UID is missing", ErrIncarnationMismatch)
	}
	if worker == nil {
		return fmt.Errorf("%w: resolved Worker is missing", ErrIncarnationMismatch)
	}
	resolvedName := worker.GetMetadata().GetName()
	resolvedUID := worker.GetMetadata().GetUid()
	if resolvedName == "" || resolvedUID == "" {
		return fmt.Errorf("%w: resolved Worker resource identity is incomplete", ErrIncarnationMismatch)
	}
	if assignedName != resolvedName {
		return fmt.Errorf("%w: assignment names Worker %q, resolved %q", ErrIncarnationMismatch, assignedName, resolvedName)
	}
	if assignedUID != resolvedUID {
		return fmt.Errorf("%w: assignment pins Worker UID %q, resolved %q", ErrIncarnationMismatch, assignedUID, resolvedUID)
	}
	return nil
}
