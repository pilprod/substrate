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

func TestRecoverAndBindExternalProviderDataPlanesOrdersRecoveryBeforeBinding(t *testing.T) {
	events := make([]string, 0, 3)
	control := &startupControlRecorder{events: &events}
	authority, err := externalprovider.NewSessionAuthority(externalprovider.DefaultSessionRuntimeConfig())
	if err != nil {
		t.Fatalf("NewSessionAuthority() error = %v", err)
	}
	binder := &startupExecutionBinderRecorder{events: &events}

	recovery, runtime, err := recoverAndBindExternalProviderDataPlanes(context.Background(), control, authority, binder.BindExternal, binder.BindActorIngress)
	if err != nil {
		t.Fatalf("recoverAndBindExternalProviderDataPlanes() error = %v", err)
	}
	if runtime == nil || binder.dialer == nil || binder.ingress == nil {
		t.Fatal("startup did not return the runtime and bind both external data-plane dialers")
	}
	if recovery != (externalprovider.StartupSweepResult{}) {
		t.Fatalf("recovery result = %+v, want empty inventory", recovery)
	}
	if !slices.Equal(events, []string{"recover", "bind-execution", "bind-ingress"}) {
		t.Fatalf("startup events = %v, want recovery before both data-plane bindings", events)
	}
	if second, err := authority.Bind(control, control); err == nil || second != nil {
		t.Fatalf("second authority Bind() = (%v, %v), want nil/error", second, err)
	}
}

func TestRecoverAndBindExternalProviderDataPlanesDoesNotPublishAfterRecoveryFailure(t *testing.T) {
	wantErr := errors.New("recovery unavailable")
	events := make([]string, 0, 1)
	control := &startupControlRecorder{events: &events, recoveryErr: wantErr}
	authority, err := externalprovider.NewSessionAuthority(externalprovider.DefaultSessionRuntimeConfig())
	if err != nil {
		t.Fatalf("NewSessionAuthority() error = %v", err)
	}
	binder := &startupExecutionBinderRecorder{events: &events}

	recovery, runtime, err := recoverAndBindExternalProviderDataPlanes(context.Background(), control, authority, binder.BindExternal, binder.BindActorIngress)
	if !errors.Is(err, wantErr) || runtime != nil || recovery != (externalprovider.StartupSweepResult{}) {
		t.Fatalf("failed recovery startup = (%+v, %v, %v), want empty/nil/recovery error", recovery, runtime, err)
	}
	if !slices.Equal(events, []string{"recover"}) || binder.dialer != nil || binder.ingress != nil {
		t.Fatalf("failed recovery published data-plane authority: events=%v execution=%v ingress=%v", events, binder.dialer, binder.ingress)
	}
	if sessionRuntime, executionDialer, ingressDialer, err := authority.BindProviderForwarding(control, control); err != nil || sessionRuntime == nil || executionDialer == nil || ingressDialer == nil {
		t.Fatalf("authority was consumed before successful recovery: (%v, %v, %v, %v)", sessionRuntime, executionDialer, ingressDialer, err)
	}
}

func TestRecoverAndBindExternalProviderDataPlanesFailsClosedOnIngressBind(t *testing.T) {
	events := make([]string, 0, 3)
	control := &startupControlRecorder{events: &events}
	authority, err := externalprovider.NewSessionAuthority(externalprovider.DefaultSessionRuntimeConfig())
	if err != nil {
		t.Fatalf("NewSessionAuthority() error = %v", err)
	}
	binder := &startupExecutionBinderRecorder{events: &events, ingressErr: errors.New("ingress unavailable")}

	recovery, runtime, err := recoverAndBindExternalProviderDataPlanes(context.Background(), control, authority, binder.BindExternal, binder.BindActorIngress)
	if err == nil || runtime != nil || recovery != (externalprovider.StartupSweepResult{}) {
		t.Fatalf("failed ingress bind startup = (%+v, %v, %v), want empty/nil/error", recovery, runtime, err)
	}
	if !slices.Equal(events, []string{"recover", "bind-execution", "bind-ingress"}) || binder.dialer == nil || binder.ingress == nil {
		t.Fatalf("ingress bind failure order/authorities = events:%v execution:%v ingress:%v", events, binder.dialer, binder.ingress)
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
	events     *[]string
	dialer     *externalprovider.ExternalExecutionDialer
	ingress    *externalprovider.ExternalActorIngressDialer
	ingressErr error
}

func (b *startupExecutionBinderRecorder) BindExternal(dialer *externalprovider.ExternalExecutionDialer) error {
	*b.events = append(*b.events, "bind-execution")
	b.dialer = dialer
	return nil
}

func (b *startupExecutionBinderRecorder) BindActorIngress(dialer *externalprovider.ExternalActorIngressDialer) error {
	*b.events = append(*b.events, "bind-ingress")
	b.ingress = dialer
	return b.ingressErr
}
