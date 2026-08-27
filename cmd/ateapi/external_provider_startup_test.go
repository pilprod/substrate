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
	"errors"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestRecoverAndBindExternalProviderExecutionOrdersRecoveryBeforeBinding(t *testing.T) {
	events := make([]string, 0, 2)
	control := &startupControlRecorder{events: &events}
	authority, err := externalprovider.NewSessionAuthority(externalprovider.DefaultSessionRuntimeConfig())
	if err != nil {
		t.Fatalf("NewSessionAuthority() error = %v", err)
	}
	binder := &startupExecutionBinderRecorder{events: &events}

	recovery, runtime, err := recoverAndBindExternalProviderExecution(context.Background(), control, authority, binder.BindExternal)
	if err != nil {
		t.Fatalf("recoverAndBindExternalProviderExecution() error = %v", err)
	}
	if runtime == nil || binder.dialer == nil {
		t.Fatal("startup did not return the runtime and bind its external execution dialer")
	}
	if recovery != (externalprovider.StartupSweepResult{}) {
		t.Fatalf("recovery result = %+v, want empty inventory", recovery)
	}
	if !slices.Equal(events, []string{"recover", "bind-external"}) {
		t.Fatalf("startup events = %v, want recovery before external binding", events)
	}
	if second, err := authority.Bind(control, control); err == nil || second != nil {
		t.Fatalf("second authority Bind() = (%v, %v), want nil/error", second, err)
	}
}

func TestRecoverAndBindExternalProviderExecutionDoesNotPublishAfterRecoveryFailure(t *testing.T) {
	wantErr := errors.New("recovery unavailable")
	events := make([]string, 0, 1)
	control := &startupControlRecorder{events: &events, recoveryErr: wantErr}
	authority, err := externalprovider.NewSessionAuthority(externalprovider.DefaultSessionRuntimeConfig())
	if err != nil {
		t.Fatalf("NewSessionAuthority() error = %v", err)
	}
	binder := &startupExecutionBinderRecorder{events: &events}

	recovery, runtime, err := recoverAndBindExternalProviderExecution(context.Background(), control, authority, binder.BindExternal)
	if !errors.Is(err, wantErr) || runtime != nil || recovery != (externalprovider.StartupSweepResult{}) {
		t.Fatalf("failed recovery startup = (%+v, %v, %v), want empty/nil/recovery error", recovery, runtime, err)
	}
	if !slices.Equal(events, []string{"recover"}) || binder.dialer != nil {
		t.Fatalf("failed recovery published execution authority: events=%v dialer=%v", events, binder.dialer)
	}
	if sessionRuntime, executionDialer, err := authority.BindExecutionForwarding(control, control); err != nil || sessionRuntime == nil || executionDialer == nil {
		t.Fatalf("authority was consumed before successful recovery: (%v, %v, %v)", sessionRuntime, executionDialer, err)
	}
}

type startupControlRecorder struct {
	events      *[]string
	recoveryErr error
}

func (c *startupControlRecorder) ListWorkers(context.Context, *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	*c.events = append(*c.events, "recover")
	if c.recoveryErr != nil {
		return nil, c.recoveryErr
	}
	return &ateapipb.ListWorkersResponse{}, nil
}

func (*startupControlRecorder) ReconcileExternalWorkers(context.Context, *externalprovider.WorkerPlan) ([]*ateapipb.Worker, error) {
	return nil, errors.New("unexpected reconcile during startup binding")
}

func (*startupControlRecorder) SetExternalWorkerAvailability(context.Context, string, string, ateapipb.WorkerState) (*ateapipb.Worker, error) {
	return nil, errors.New("unexpected availability mutation for empty startup inventory")
}

type startupExecutionBinderRecorder struct {
	events *[]string
	dialer *externalprovider.ExternalExecutionDialer
}

func (b *startupExecutionBinderRecorder) BindExternal(dialer *externalprovider.ExternalExecutionDialer) error {
	*b.events = append(*b.events, "bind-external")
	b.dialer = dialer
	return nil
}
