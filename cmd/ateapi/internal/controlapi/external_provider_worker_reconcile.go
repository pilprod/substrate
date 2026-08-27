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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

const maxExternalWorkerReconcileAttempts = 5

type externalWorkerPlanStore interface {
	GetWorker(context.Context, string) (*ateapipb.Worker, error)
	CreateWorker(context.Context, *ateapipb.Worker) (*ateapipb.Worker, error)
	UpdateWorker(context.Context, string, store.Precondition, func(*ateapipb.Worker) error) (*ateapipb.Worker, error)
}

var _ externalprovider.WorkerPlanReconciler = (*RPCService)(nil)

// ReconcileExternalWorkers idempotently creates or refreshes the durable
// ExternalSlot Workers in plan. New Workers start OFFLINE. Existing status,
// assignment, and server metadata are preserved; only the provider-owned
// sandbox class and labels are refreshed after immutable identity validation.
//
// Slots missing from plan are deliberately untouched. Session teardown owns
// the OFFLINE transition, while operator drain and deletion remain separate
// control-plane actions.
func (s *RPCService) ReconcileExternalWorkers(ctx context.Context, plan *externalprovider.WorkerPlan) ([]*ateapipb.Worker, error) {
	return reconcileExternalWorkers(ctx, s.impl, plan)
}

func reconcileExternalWorkers(ctx context.Context, persistence externalWorkerPlanStore, plan *externalprovider.WorkerPlan) ([]*ateapipb.Worker, error) {
	if persistence == nil || plan == nil {
		return nil, fmt.Errorf("%w: reconciler store and plan are required", externalprovider.ErrInvalidWorkerPlan)
	}
	desiredWorkers := plan.Workers()
	if len(desiredWorkers) == 0 {
		return nil, fmt.Errorf("%w: plan contains no Workers", externalprovider.ErrInvalidWorkerPlan)
	}

	reconciled := make([]*ateapipb.Worker, 0, len(desiredWorkers))
	for _, desired := range desiredWorkers {
		worker, err := reconcileExternalWorker(ctx, persistence, plan, desired)
		if err != nil {
			return nil, err
		}
		reconciled = append(reconciled, worker)
	}
	return reconciled, nil
}

func reconcileExternalWorker(
	ctx context.Context,
	persistence externalWorkerPlanStore,
	plan *externalprovider.WorkerPlan,
	desired *ateapipb.Worker,
) (*ateapipb.Worker, error) {
	name := desired.GetMetadata().GetName()
	if errs := validateCreateWorkerRequest(&ateapipb.CreateWorkerRequest{Worker: desired}); len(errs) != 0 {
		return nil, fmt.Errorf("%w: planned Worker %q violates the control API contract: %v", externalprovider.ErrInvalidWorkerPlan, name, errs)
	}

	for attempt := 0; attempt < maxExternalWorkerReconcileAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		existing, err := persistence.GetWorker(ctx, name)
		if errors.Is(err, store.ErrNotFound) {
			candidate := proto.Clone(desired).(*ateapipb.Worker)
			candidate.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_OFFLINE}
			created, createErr := persistence.CreateWorker(ctx, candidate)
			switch {
			case createErr == nil:
				return created, nil
			case errors.Is(createErr, store.ErrAlreadyExists):
				continue
			default:
				return nil, fmt.Errorf("creating external Worker %q: %w", name, createErr)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("getting external Worker %q: %w", name, err)
		}
		if err := plan.ValidateExisting(existing); err != nil {
			return nil, err
		}
		if existing.GetSandboxClass() == desired.GetSandboxClass() && maps.Equal(existing.GetLabels(), desired.GetLabels()) {
			return existing, nil
		}

		updated, updateErr := persistence.UpdateWorker(ctx, name, store.PreconditionFrom(existing), func(toUpdate *ateapipb.Worker) error {
			if err := plan.ValidateExisting(toUpdate); err != nil {
				return err
			}
			toUpdate.SandboxClass = desired.GetSandboxClass()
			toUpdate.Labels = maps.Clone(desired.GetLabels())
			return nil
		})
		if updateErr == nil {
			return updated, nil
		}
		if (errors.Is(updateErr, store.ErrVersionConflict) || errors.Is(updateErr, store.ErrNotFound)) && attempt+1 < maxExternalWorkerReconcileAttempts {
			continue
		}
		return nil, fmt.Errorf("refreshing external Worker %q: %w", name, updateErr)
	}
	return nil, fmt.Errorf("reconciling external Worker %q: %w", name, store.ErrVersionConflict)
}
