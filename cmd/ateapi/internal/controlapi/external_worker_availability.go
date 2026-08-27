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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

const maxExternalWorkerAvailabilityAttempts = 5

// ErrExternalWorkerProvider indicates that an availability transition targeted
// a Worker that is not backed by an ExternalSlot provider.
var ErrExternalWorkerProvider = errors.New("worker is not backed by an ExternalSlot provider")

// ErrExternalWorkerDraining indicates that an availability transition targeted
// a Worker whose operator-controlled drain has already begun.
var ErrExternalWorkerDraining = errors.New("external worker is draining")

// ErrExternalWorkerState indicates that either the requested availability or
// the Worker's current state is outside the ACTIVE/OFFLINE availability domain.
var ErrExternalWorkerState = errors.New("invalid external worker availability state")

// ExternalWorkerAvailabilityService is the in-process availability API for an
// external provider broker. It is intentionally not part of the public Control
// gRPC service.
type ExternalWorkerAvailabilityService interface {
	SetExternalWorkerAvailability(ctx context.Context, name, uid string, state ateapipb.WorkerState) (*ateapipb.Worker, error)
}

var _ ExternalWorkerAvailabilityService = (*RPCService)(nil)

// SetExternalWorkerAvailability moves an ExternalSlot Worker between ACTIVE
// and OFFLINE. The mutation preserves any Actor assignment. DRAINING is an
// operator-controlled terminal state for this API and can never be reactivated.
// uid pins the durable Worker incarnation so a stale provider session cannot
// change a new Worker that later reused the same name.
func (s *RPCService) SetExternalWorkerAvailability(ctx context.Context, name, uid string, state ateapipb.WorkerState) (*ateapipb.Worker, error) {
	if state != ateapipb.WorkerState_WORKER_STATE_ACTIVE && state != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
		return nil, fmt.Errorf("%w: requested %s", ErrExternalWorkerState, state)
	}
	if uid == "" {
		return nil, fmt.Errorf("%w: worker uid is required", store.ErrPreconditionRequired)
	}

	for attempt := 0; attempt < maxExternalWorkerAvailabilityAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		observed, err := s.impl.GetWorker(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("getting external worker %q: %w", name, err)
		}
		if observed.GetMetadata().GetUid() != uid {
			return nil, fmt.Errorf("setting external worker %q availability: %w", name, store.ErrUIDConflict)
		}

		updated, err := s.impl.UpdateWorker(ctx, name, store.Precondition{UID: uid, Version: observed.GetMetadata().GetVersion()}, func(toUpdate *ateapipb.Worker) error {
			if effectiveWorkerProvider(toUpdate.GetProvider()) != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT {
				return fmt.Errorf("%w: worker %q uses %s", ErrExternalWorkerProvider, name, effectiveWorkerProvider(toUpdate.GetProvider()))
			}

			current := toUpdate.GetStatus().GetState()
			switch current {
			case ateapipb.WorkerState_WORKER_STATE_DRAINING:
				return fmt.Errorf("%w: worker %q", ErrExternalWorkerDraining, name)
			case ateapipb.WorkerState_WORKER_STATE_ACTIVE, ateapipb.WorkerState_WORKER_STATE_OFFLINE:
			case ateapipb.WorkerState_WORKER_STATE_UNSPECIFIED:
				return fmt.Errorf("%w: worker %q is %s", ErrExternalWorkerState, name, current)
			default:
				return fmt.Errorf("%w: worker %q is %d", ErrExternalWorkerState, name, current)
			}

			if current == state {
				return &workerUnchanged{worker: proto.Clone(toUpdate).(*ateapipb.Worker)}
			}
			toUpdate.Status.State = state
			// Assignment belongs to actor lifecycle workflows. Going offline only
			// removes capacity from scheduling; it does not reassign or release.
			return nil
		})
		if err == nil {
			return updated, nil
		}

		var unchanged *workerUnchanged
		if errors.As(err, &unchanged) {
			return unchanged.worker, nil
		}
		if errors.Is(err, store.ErrVersionConflict) && attempt+1 < maxExternalWorkerAvailabilityAttempts {
			continue
		}
		return nil, fmt.Errorf("setting external worker %q availability to %s: %w", name, state, err)
	}
	return nil, fmt.Errorf("setting external worker %q availability to %s: %w", name, state, store.ErrVersionConflict)
}
