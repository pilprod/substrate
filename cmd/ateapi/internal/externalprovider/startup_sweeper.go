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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	startupSweepPageSize          int32  = 1000
	defaultStartupSweepMaxWorkers uint64 = 1 << 20
	maximumStartupSweepMaxWorkers uint64 = 1 << 24
)

var errInvalidStartupSweep = errors.New("invalid external provider startup sweep")

// ExternalWorkerRecoveryStore is the narrow in-process boundary needed to
// make persisted external capacity unavailable before a restarted broker is
// marked ready. Implementations must pin availability writes by Worker UID.
type ExternalWorkerRecoveryStore interface {
	ListWorkers(context.Context, *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error)
	SetExternalWorkerAvailability(context.Context, string, string, ateapipb.WorkerState) (*ateapipb.Worker, error)
}

// StartupSweepConfig bounds the total Worker inventory inspected before any
// availability write. Zero selects a conservative default.
type StartupSweepConfig struct {
	MaxScannedWorkers uint64
}

// StartupSweepResult is a non-secret summary suitable for readiness logs and
// metrics. Draining Workers are already unavailable and are not mutated.
type StartupSweepResult struct {
	Scanned         uint64
	External        uint64
	Offlined        uint64
	AlreadyOffline  uint64
	AlreadyDraining uint64
}

type startupWorkerRef struct {
	name string
	uid  string
}

// RecoverExternalWorkersOffline removes every persisted ExternalSlot Worker
// from scheduling before a new in-memory route directory can accept sessions.
// It first validates and snapshots the complete bounded inventory, so a bad
// page token, malformed Worker, or unsupported persisted state causes zero
// availability writes. A later write failure leaves the broker unready; the
// operation is safe to retry because OFFLINE is idempotent and writes pin UID.
func RecoverExternalWorkersOffline(
	ctx context.Context,
	store ExternalWorkerRecoveryStore,
	config StartupSweepConfig,
) (StartupSweepResult, error) {
	if ctx == nil || store == nil {
		return StartupSweepResult{}, fmt.Errorf("%w: context and recovery store are required", errInvalidStartupSweep)
	}
	limit := config.MaxScannedWorkers
	if limit == 0 {
		limit = defaultStartupSweepMaxWorkers
	}
	if limit > maximumStartupSweepMaxWorkers {
		return StartupSweepResult{}, fmt.Errorf("%w: max scanned Workers must be at most %d", errInvalidStartupSweep, maximumStartupSweepMaxWorkers)
	}

	result := StartupSweepResult{}
	active := make([]startupWorkerRef, 0)
	seenTokens := map[string]struct{}{"": {}}
	seenNames := make(map[string]struct{})
	seenUIDs := make(map[string]struct{})
	seenExecutions := make(map[string]struct{})
	pageToken := ""
	for {
		if err := ctx.Err(); err != nil {
			return StartupSweepResult{}, err
		}
		page, err := store.ListWorkers(ctx, &ateapipb.ListWorkersRequest{
			PageSize:  startupSweepPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return StartupSweepResult{}, fmt.Errorf("listing Workers for external provider recovery: %w", err)
		}
		if page == nil {
			return StartupSweepResult{}, fmt.Errorf("%w: Worker inventory returned a nil page", errInvalidStartupSweep)
		}
		if uint64(len(page.GetWorkers())) > limit-result.Scanned {
			return StartupSweepResult{}, fmt.Errorf("%w: Worker inventory exceeds the configured bound of %d", errInvalidStartupSweep, limit)
		}
		result.Scanned += uint64(len(page.GetWorkers()))
		for _, worker := range page.GetWorkers() {
			if worker == nil {
				return StartupSweepResult{}, fmt.Errorf("%w: Worker inventory contains a nil resource", errInvalidStartupSweep)
			}
			if worker.GetProvider() != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT {
				continue
			}
			metadata := worker.GetMetadata()
			identity := worker.GetExternalSlot()
			if metadata == nil || !IsValidIdentity(metadata.GetName()) || !IsValidIdentity(metadata.GetUid()) || identity == nil ||
				!IsValidIdentity(identity.GetExecutionIdentity()) || !IsValidIdentity(identity.GetLocalityIdentity()) {
				return StartupSweepResult{}, fmt.Errorf("%w: external Worker identity is incomplete", errInvalidStartupSweep)
			}
			if _, duplicate := seenNames[metadata.GetName()]; duplicate {
				return StartupSweepResult{}, fmt.Errorf("%w: external Worker name is duplicated", errInvalidStartupSweep)
			}
			if _, duplicate := seenUIDs[metadata.GetUid()]; duplicate {
				return StartupSweepResult{}, fmt.Errorf("%w: external Worker UID is duplicated", errInvalidStartupSweep)
			}
			if _, duplicate := seenExecutions[identity.GetExecutionIdentity()]; duplicate {
				return StartupSweepResult{}, fmt.Errorf("%w: external Worker execution identity is duplicated", errInvalidStartupSweep)
			}
			seenNames[metadata.GetName()] = struct{}{}
			seenUIDs[metadata.GetUid()] = struct{}{}
			seenExecutions[identity.GetExecutionIdentity()] = struct{}{}
			result.External++

			switch worker.GetStatus().GetState() {
			case ateapipb.WorkerState_WORKER_STATE_ACTIVE:
				active = append(active, startupWorkerRef{name: metadata.GetName(), uid: metadata.GetUid()})
			case ateapipb.WorkerState_WORKER_STATE_OFFLINE:
				result.AlreadyOffline++
			case ateapipb.WorkerState_WORKER_STATE_DRAINING:
				result.AlreadyDraining++
			default:
				return StartupSweepResult{}, fmt.Errorf("%w: external Worker %q has unsupported state %s", errInvalidStartupSweep, metadata.GetName(), worker.GetStatus().GetState())
			}
		}

		next := page.GetNextPageToken()
		if next == "" {
			break
		}
		if _, duplicate := seenTokens[next]; duplicate {
			return StartupSweepResult{}, fmt.Errorf("%w: Worker inventory page token repeated", errInvalidStartupSweep)
		}
		seenTokens[next] = struct{}{}
		pageToken = next
	}

	for _, worker := range active {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		updated, err := store.SetExternalWorkerAvailability(
			ctx,
			worker.name,
			worker.uid,
			ateapipb.WorkerState_WORKER_STATE_OFFLINE,
		)
		if err != nil {
			return result, fmt.Errorf("recovering external Worker %q OFFLINE: %w", worker.name, err)
		}
		if updated == nil || updated.GetMetadata().GetName() != worker.name || updated.GetMetadata().GetUid() != worker.uid ||
			updated.GetProvider() != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT ||
			updated.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_OFFLINE {
			return result, fmt.Errorf("%w: external Worker %q recovery returned a different incarnation or state", errInvalidStartupSweep, worker.name)
		}
		result.Offlined++
	}
	return result, nil
}
