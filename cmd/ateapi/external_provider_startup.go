// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
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

// recoverAndBindExternalProviderExecution makes persisted external capacity
// unavailable before publishing the in-memory execution authority. The caller
// must complete this function before opening the Broker listener.
func recoverAndBindExternalProviderExecution(
	ctx context.Context,
	control externalProviderStartupControl,
	authority *externalprovider.SessionAuthority,
	bindExternal func(*externalprovider.ExternalExecutionDialer) error,
) (externalprovider.StartupSweepResult, *externalprovider.SessionRuntime, error) {
	if authority == nil || bindExternal == nil {
		return externalprovider.StartupSweepResult{}, nil, fmt.Errorf("external provider session authority and execution binder are required")
	}
	recovery, err := externalprovider.RecoverExternalWorkersOffline(ctx, control, externalprovider.StartupSweepConfig{})
	if err != nil {
		return externalprovider.StartupSweepResult{}, nil, fmt.Errorf("recovering external provider Workers: %w", err)
	}
	sessionRuntime, executionDialer, err := authority.BindExecutionForwarding(control, control)
	if err != nil {
		return externalprovider.StartupSweepResult{}, nil, fmt.Errorf("binding external provider session execution: %w", err)
	}
	if err := bindExternal(executionDialer); err != nil {
		return externalprovider.StartupSweepResult{}, nil, fmt.Errorf("binding provider execution dialer: %w", err)
	}
	return recovery, sessionRuntime, nil
}
