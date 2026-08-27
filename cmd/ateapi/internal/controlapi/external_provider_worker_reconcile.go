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
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	maxExternalWorkerLabels            = 64
	maxExternalWorkerReconcileAttempts = 5
)

type externalWorkerPlanStore interface {
	GetWorker(context.Context, string) (*ateapipb.Worker, error)
	CreateWorker(context.Context, *ateapipb.Worker) (*ateapipb.Worker, error)
	UpdateWorker(context.Context, string, store.Precondition, func(*ateapipb.Worker) error) (*ateapipb.Worker, error)
}

type externalWorkerPoolLister interface {
	WorkerPools(namespace string) listersv1alpha1.WorkerPoolNamespaceLister
}

var _ externalprovider.WorkerPlanReconciler = (*RPCService)(nil)

// ReconcileExternalWorkers idempotently creates or refreshes the durable
// ExternalSlot Workers in plan. New Workers start OFFLINE. Existing status,
// assignment, and server metadata are preserved; only the provider-owned
// sandbox class and effective policy-plus-pool labels are refreshed after
// immutable identity validation.
//
// Slots missing from plan are deliberately untouched. Session teardown owns
// the OFFLINE transition, while operator drain and deletion remain separate
// control-plane actions.
func (s *RPCService) ReconcileExternalWorkers(ctx context.Context, plan *externalprovider.WorkerPlan) ([]*ateapipb.Worker, error) {
	return reconcileExternalWorkers(ctx, s.impl, s.workerPoolLister, plan)
}

func reconcileExternalWorkers(
	ctx context.Context,
	persistence externalWorkerPlanStore,
	poolLister externalWorkerPoolLister,
	plan *externalprovider.WorkerPlan,
) ([]*ateapipb.Worker, error) {
	if persistence == nil || poolLister == nil || plan == nil {
		return nil, fmt.Errorf("%w: reconciler store, WorkerPool lister, and plan are required", externalprovider.ErrInvalidWorkerPlan)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	registration := plan.Registration()
	if err := registration.Scope.Validate(); err != nil {
		return nil, fmt.Errorf("%w: plan scope is invalid", externalprovider.ErrInvalidWorkerPlan)
	}
	pool, err := poolLister.WorkerPools(registration.Scope.WorkerNamespace).Get(registration.Scope.WorkerPool)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: resolving pinned WorkerPool %s/%s: %v",
			externalprovider.ErrInvalidWorkerPlan,
			registration.Scope.WorkerNamespace,
			registration.Scope.WorkerPool,
			err,
		)
	}
	if pool == nil || pool.GetNamespace() != registration.Scope.WorkerNamespace || pool.GetName() != registration.Scope.WorkerPool {
		return nil, fmt.Errorf(
			"%w: lister returned an invalid pinned WorkerPool for %s/%s",
			externalprovider.ErrInvalidWorkerPlan,
			registration.Scope.WorkerNamespace,
			registration.Scope.WorkerPool,
		)
	}

	desiredWorkers, err := effectiveExternalWorkers(plan.Workers(), pool.GetLabels())
	if err != nil {
		return nil, err
	}
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

// effectiveExternalWorkers overlays the authenticated pool's server-owned
// labels on registration-policy labels. Every candidate is validated before the
// caller performs a Worker read or write, so one malformed slot cannot leave a
// partially reconciled plan.
func effectiveExternalWorkers(planned []*ateapipb.Worker, poolLabels map[string]string) ([]*ateapipb.Worker, error) {
	if len(planned) == 0 {
		return nil, fmt.Errorf("%w: plan contains no Workers", externalprovider.ErrInvalidWorkerPlan)
	}

	effective := make([]*ateapipb.Worker, 0, len(planned))
	for _, worker := range planned {
		if worker == nil {
			return nil, fmt.Errorf("%w: plan contains a nil Worker", externalprovider.ErrInvalidWorkerPlan)
		}
		labels, err := mergeExternalWorkerLabels(worker.GetLabels(), poolLabels)
		if err != nil {
			return nil, fmt.Errorf("%w: planned Worker %q labels: %v", externalprovider.ErrInvalidWorkerPlan, worker.GetMetadata().GetName(), err)
		}
		candidate := proto.Clone(worker).(*ateapipb.Worker)
		candidate.Labels = labels
		if errs := validateCreateWorkerRequest(&ateapipb.CreateWorkerRequest{Worker: candidate}); len(errs) != 0 {
			return nil, fmt.Errorf(
				"%w: planned Worker %q violates the control API contract: %v",
				externalprovider.ErrInvalidWorkerPlan,
				candidate.GetMetadata().GetName(),
				errs,
			)
		}
		effective = append(effective, candidate)
	}
	return effective, nil
}

func mergeExternalWorkerLabels(profileLabels, poolLabels map[string]string) (map[string]string, error) {
	poolKeys := make([]string, 0, len(poolLabels))
	for key := range poolLabels {
		poolKeys = append(poolKeys, key)
	}
	slices.Sort(poolKeys)
	for _, key := range poolKeys {
		if _, collision := profileLabels[key]; collision {
			return nil, fmt.Errorf("slot profile label collides with server-owned WorkerPool label %q", key)
		}
	}
	if len(profileLabels)+len(poolLabels) > maxExternalWorkerLabels {
		return nil, fmt.Errorf("merged labels exceed %d entries", maxExternalWorkerLabels)
	}

	labels := maps.Clone(profileLabels)
	if labels == nil && len(poolLabels) != 0 {
		labels = make(map[string]string, len(poolLabels))
	}
	for _, key := range poolKeys {
		labels[key] = poolLabels[key]
	}

	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if len(k8svalidation.IsQualifiedName(key)) != 0 {
			return nil, fmt.Errorf("label key %q is invalid", key)
		}
		if len(k8svalidation.IsValidLabelValue(labels[key])) != 0 {
			return nil, fmt.Errorf("label %q has an invalid value", key)
		}
	}
	return labels, nil
}

func reconcileExternalWorker(
	ctx context.Context,
	persistence externalWorkerPlanStore,
	plan *externalprovider.WorkerPlan,
	desired *ateapipb.Worker,
) (*ateapipb.Worker, error) {
	name := desired.GetMetadata().GetName()

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
