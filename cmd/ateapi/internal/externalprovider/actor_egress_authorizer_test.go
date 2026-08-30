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
	"testing"

	"github.com/agent-substrate/substrate/internal/substratex509"
)

func TestExternalActorEgressRouteAuthorizerFencesClaimsAndReplay(t *testing.T) {
	registry := mustSessionRegistry(t, 1)
	routes := mustRouteDirectory(t, registry, 1, 1)
	bindings := mustRouteBindings(t, workerPlanAdmission(t, "registration-a", "slot-a"), 1)
	firstRoute := mustPublishRoute(t, routes, mustInstallSession(t, registry, "registration-a", 1), bindings)
	authorizer := &externalActorEgressRouteAuthorizer{registry: registry, routes: routes}
	claim := routeBindingClaim(firstRoute, bindings[0])

	resolved, ok := authorizer.ResolveExternalActorEgress(claim)
	if !ok || resolved != bindings[0] {
		t.Fatalf("ResolveExternalActorEgress() = (%+v, %v), want current binding", resolved, ok)
	}
	callbackCalls := 0
	if err := authorizer.GuardExternalActorEgress(context.Background(), claim, func(current SessionWorkerBinding) error {
		callbackCalls++
		if current != bindings[0] {
			t.Fatalf("guard binding = %+v, want %+v", current, bindings[0])
		}
		return nil
	}); err != nil || callbackCalls != 1 {
		t.Fatalf("GuardExternalActorEgress() = %v, callback calls = %d", err, callbackCalls)
	}

	mutations := []struct {
		name   string
		mutate func(*substratex509.ExternalRouteBinding)
	}{
		{name: "registration", mutate: func(value *substratex509.ExternalRouteBinding) { value.RegistrationUID = "registration-b" }},
		{name: "slot", mutate: func(value *substratex509.ExternalRouteBinding) { value.SlotID = "slot-b" }},
		{name: "Worker UID", mutate: func(value *substratex509.ExternalRouteBinding) {
			value.WorkerUID = "00000000-0000-4000-8000-000000000999"
		}},
		{name: "generation", mutate: func(value *substratex509.ExternalRouteBinding) { value.SessionGeneration++ }},
		{name: "execution identity", mutate: func(value *substratex509.ExternalRouteBinding) { value.ExecutionIdentity = "other.execution" }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			candidate := *claim
			test.mutate(&candidate)
			if _, ok := authorizer.ResolveExternalActorEgress(&candidate); ok {
				t.Fatal("mutated route claim resolved")
			}
			if err := authorizer.GuardExternalActorEgress(context.Background(), &candidate, func(SessionWorkerBinding) error {
				t.Fatal("mutated route claim reached callback")
				return nil
			}); !errors.Is(err, ErrExternalActorEgressRouteUnauthorized) {
				t.Fatalf("GuardExternalActorEgress() error = %v, want unauthorized", err)
			}
		})
	}

	secondRoute := mustPublishRoute(t, routes, mustInstallSession(t, registry, "registration-a", 2), bindings)
	if _, ok := authorizer.ResolveExternalActorEgress(claim); ok {
		t.Fatal("generation-1 replay resolved after generation replacement")
	}
	if err := authorizer.GuardExternalActorEgress(context.Background(), claim, func(SessionWorkerBinding) error { return nil }); !errors.Is(err, ErrExternalActorEgressRouteUnauthorized) {
		t.Fatalf("generation-1 replay error = %v, want unauthorized", err)
	}
	secondClaim := routeBindingClaim(secondRoute, bindings[0])
	if _, ok := authorizer.ResolveExternalActorEgress(secondClaim); !ok {
		t.Fatal("current generation did not resolve")
	}
	if !routes.Close(secondRoute) {
		t.Fatal("Close(current route) = false")
	}
	if _, ok := authorizer.ResolveExternalActorEgress(secondClaim); ok {
		t.Fatal("disconnected route still resolved")
	}
	if err := authorizer.GuardExternalActorEgress(context.Background(), secondClaim, func(SessionWorkerBinding) error { return nil }); !errors.Is(err, ErrExternalActorEgressRouteUnauthorized) {
		t.Fatalf("disconnected route error = %v, want unauthorized", err)
	}
}

func routeBindingClaim(route *SessionRoute, binding SessionWorkerBinding) *substratex509.ExternalRouteBinding {
	return &substratex509.ExternalRouteBinding{
		Version:           substratex509.ExternalRouteBindingVersion,
		RegistrationUID:   binding.RegistrationUID(),
		SlotID:            binding.SlotID(),
		WorkerUID:         binding.WorkerUID(),
		SessionGeneration: route.Generation(),
		ExecutionIdentity: binding.ExecutionIdentity(),
	}
}
