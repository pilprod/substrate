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

	"github.com/agent-substrate/substrate/internal/substratex509"
)

var ErrExternalActorEgressRouteUnauthorized = errors.New("external Actor egress route is unauthorized")

// ExternalActorEgressRouteAuthorizer linearizes one gateway CONNECT
// authorization with route close/replacement. The callback receives only the
// exact current server-derived Worker binding and runs while that route cannot
// begin closing.
type ExternalActorEgressRouteAuthorizer interface {
	ResolveExternalActorEgress(*substratex509.ExternalRouteBinding) (SessionWorkerBinding, bool)
	GuardExternalActorEgress(
		context.Context,
		*substratex509.ExternalRouteBinding,
		func(SessionWorkerBinding) error,
	) error
}

func (a *externalActorEgressRouteAuthorizer) ResolveExternalActorEgress(claim *substratex509.ExternalRouteBinding) (SessionWorkerBinding, bool) {
	if a == nil || a.routes == nil || claim == nil {
		return SessionWorkerBinding{}, false
	}
	route, binding, ok := a.routes.LookupExecutionIdentity(claim.ExecutionIdentity)
	if !ok || !externalRouteClaimMatches(route, binding, claim) {
		return SessionWorkerBinding{}, false
	}
	return binding, true
}

type externalActorEgressRouteAuthorizer struct {
	registry *sessionRegistry
	routes   *SessionRouteDirectory
}

// ActorEgressAuthorizer returns a read-only authorization boundary sharing
// this authority's exact process-local generation state.
func (a *SessionAuthority) ActorEgressAuthorizer() ExternalActorEgressRouteAuthorizer {
	if a == nil || a.registry == nil || a.routes == nil {
		return nil
	}
	return &externalActorEgressRouteAuthorizer{registry: a.registry, routes: a.routes}
}

func (a *externalActorEgressRouteAuthorizer) GuardExternalActorEgress(
	ctx context.Context,
	claim *substratex509.ExternalRouteBinding,
	validate func(SessionWorkerBinding) error,
) error {
	if a == nil || a.registry == nil || a.routes == nil || ctx == nil || claim == nil || validate == nil {
		return ErrExternalActorEgressRouteUnauthorized
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	route, binding, ok := a.routes.LookupExecutionIdentity(claim.ExecutionIdentity)
	if !ok || !externalRouteClaimMatches(route, binding, claim) {
		return ErrExternalActorEgressRouteUnauthorized
	}
	err := a.registry.withCurrentLease(route.lifecycleLease(), func(_ *sessionLifecycleState, _ sessionEntry) error {
		route.assignmentGate.RLock()
		defer route.assignmentGate.RUnlock()
		if err := ctx.Err(); err != nil {
			return err
		}
		if routeLiveError(route) != nil || !a.routes.AuthorizesBinding(route, binding) || !externalRouteClaimMatches(route, binding, claim) {
			return ErrExternalActorEgressRouteUnauthorized
		}
		return validate(binding)
	})
	if err != nil {
		if errors.Is(err, errSessionFenced) || errors.Is(err, errSessionNotCurrent) ||
			errors.Is(err, ErrSessionRouteClosing) || errors.Is(err, ErrSessionRouteWithdrawn) ||
			errors.Is(err, ErrSessionRouteReplaced) {
			return fmt.Errorf("%w: route is no longer current", ErrExternalActorEgressRouteUnauthorized)
		}
		return err
	}
	return nil
}

func externalRouteClaimMatches(route *SessionRoute, binding SessionWorkerBinding, claim *substratex509.ExternalRouteBinding) bool {
	return route != nil && claim != nil &&
		route.RegistrationUID() == claim.RegistrationUID && route.Generation() == claim.SessionGeneration &&
		binding.RegistrationUID() == claim.RegistrationUID && binding.SlotID() == claim.SlotID &&
		binding.WorkerUID() == claim.WorkerUID && binding.ExecutionIdentity() == claim.ExecutionIdentity
}
