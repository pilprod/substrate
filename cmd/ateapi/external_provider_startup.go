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

package main

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
)

type externalProviderStartupControl interface {
	externalprovider.ExternalWorkerRecoveryStore
	externalprovider.WorkerPlanReconciler
	externalprovider.ExternalWorkerAvailabilityController
}

// recoverAndBindExternalProviderDataPlanes makes persisted external capacity
// unavailable before publishing the in-memory execution, Actor ingress, and
// server-owned Actor egress authorities. The caller must complete this
// function before opening either the Control or Broker listener.
func recoverAndBindExternalProviderDataPlanes(
	ctx context.Context,
	control externalProviderStartupControl,
	authority *externalprovider.SessionAuthority,
	actorEgress externalprovider.ActorEgressGateway,
	bindExecution func(*externalprovider.ExternalExecutionDialer) error,
	bindActorIngress func(*externalprovider.ExternalActorIngressDialer) error,
) (externalprovider.StartupSweepResult, *externalprovider.SessionRuntime, error) {
	if authority == nil || actorEgress == nil || bindExecution == nil || bindActorIngress == nil {
		return externalprovider.StartupSweepResult{}, nil, fmt.Errorf("external provider session authority and data-plane binders are required")
	}
	recovery, err := externalprovider.RecoverExternalWorkersOffline(ctx, control, externalprovider.StartupSweepConfig{})
	if err != nil {
		return externalprovider.StartupSweepResult{}, nil, fmt.Errorf("recovering external provider Workers: %w", err)
	}
	sessionRuntime, executionDialer, ingressDialer, err := authority.BindProviderForwarding(control, control, actorEgress)
	if err != nil {
		return externalprovider.StartupSweepResult{}, nil, fmt.Errorf("binding external provider session data planes: %w", err)
	}
	if err := bindExecution(executionDialer); err != nil {
		return externalprovider.StartupSweepResult{}, nil, fmt.Errorf("binding provider execution dialer: %w", err)
	}
	if err := bindActorIngress(ingressDialer); err != nil {
		return externalprovider.StartupSweepResult{}, nil, fmt.Errorf("binding provider Actor ingress dialer: %w", err)
	}
	return recovery, sessionRuntime, nil
}
