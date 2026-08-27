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
	"errors"
	"fmt"
	"sync"
)

const (
	defaultSessionRegistrations uint32 = 4096
	defaultSessionRoutes        uint32 = 4096
	defaultSessionBindings      uint32 = 1 << 20
	defaultSessionOpenChannels  uint32 = 256
	defaultSessionDataBytes     uint32 = 64 << 10
)

var errInvalidSessionRuntime = errors.New("invalid external provider session runtime")

// SessionRuntimeConfig explicitly bounds every in-memory authority used by
// live external provider sessions.
type SessionRuntimeConfig struct {
	MaxTrackedRegistrations uint32
	ClaimInstallGateLimits  ClaimInstallGateLimits
	RouteLimits             SessionRouteDirectoryLimits
	ChannelLimits           ChannelSessionLimits
}

// DefaultSessionRuntimeConfig returns conservative process-wide bounds. The
// route binding bound permits at most 256 slots for each default route.
func DefaultSessionRuntimeConfig() SessionRuntimeConfig {
	return SessionRuntimeConfig{
		MaxTrackedRegistrations: defaultSessionRegistrations,
		ClaimInstallGateLimits: ClaimInstallGateLimits{
			MaxInFlight:     defaultMaxClaimInstallInFlight,
			MaxDistinctKeys: defaultMaxClaimInstallKeys,
		},
		RouteLimits: SessionRouteDirectoryLimits{
			MaxRoutes:   defaultSessionRoutes,
			MaxBindings: defaultSessionBindings,
		},
		ChannelLimits: ChannelSessionLimits{
			MaxOpenChannels: defaultSessionOpenChannels,
			MaxDataBytes:    defaultSessionDataBytes,
		},
	}
}

// SessionAuthority owns the route and generation authorities which must be
// shared by scheduling and Connect. It is created before the control service,
// then bound exactly once to that service's Worker persistence boundaries.
type SessionAuthority struct {
	registry         *sessionRegistry
	routes           *SessionRouteDirectory
	claimInstallGate *claimInstallGate
	channelLimits    ChannelSessionLimits

	mu    sync.Mutex
	bound bool
}

// SessionRuntime is the Connect-side view of one bound SessionAuthority. Every
// Broker using this runtime shares its claim-install gate and route authority.
type SessionRuntime struct {
	coordinator      *sessionCoordinator
	claimInstallGate *claimInstallGate
}

// NewSessionAuthority constructs an empty bounded authority. It publishes no
// routes and therefore cannot make a Worker eligible before Bind succeeds.
func NewSessionAuthority(config SessionRuntimeConfig) (*SessionAuthority, error) {
	registry, err := newSessionRegistry(config.MaxTrackedRegistrations)
	if err != nil {
		return nil, fmt.Errorf("%w: registry: %w", errInvalidSessionRuntime, err)
	}
	claimInstallGate, err := newClaimInstallGate(
		config.ClaimInstallGateLimits.MaxInFlight,
		config.ClaimInstallGateLimits.MaxDistinctKeys,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: claim-install gate: %w", errInvalidSessionRuntime, err)
	}
	routes, err := newSessionRouteDirectory(registry, config.RouteLimits)
	if err != nil {
		return nil, fmt.Errorf("%w: routes: %w", errInvalidSessionRuntime, err)
	}
	channelLimits, err := normalizeChannelSessionLimits(config.ChannelLimits)
	if err != nil {
		return nil, fmt.Errorf("%w: channels: %w", errInvalidSessionRuntime, err)
	}
	return &SessionAuthority{
		registry:         registry,
		routes:           routes,
		claimInstallGate: claimInstallGate,
		channelLimits:    channelLimits,
	}, nil
}

// AssignmentGuard returns the scheduling guard sharing this authority's exact
// registry and route directory.
func (a *SessionAuthority) AssignmentGuard() RouteAssignmentGuard {
	if a == nil || a.routes == nil {
		return nil
	}
	return a.routes.AssignmentGuard()
}

// Bind attaches the durable Worker operations and returns the sole Connect
// runtime for this authority. Multiple broker runtimes must never share one
// route authority, so a second call fails closed.
func (a *SessionAuthority) Bind(
	reconciler WorkerPlanReconciler,
	availability ExternalWorkerAvailabilityController,
) (*SessionRuntime, error) {
	if a == nil || a.registry == nil || a.routes == nil || a.claimInstallGate == nil || reconciler == nil || availability == nil {
		return nil, fmt.Errorf("%w: authority and Worker boundaries are required", errInvalidSessionRuntime)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.bound {
		return nil, fmt.Errorf("%w: authority is already bound", errInvalidSessionRuntime)
	}
	lifecycle, err := newWorkerSessionLifecycle(a.registry, availability, a.routes)
	if err != nil {
		return nil, fmt.Errorf("%w: lifecycle: %w", errInvalidSessionRuntime, err)
	}
	// This runtime intentionally has no execution-channel forwarder yet. Keep
	// every reconciled Worker OFFLINE until a later constructor can bind both
	// the coordinator and that forwarding authority atomically.
	coordinator, err := newSessionCoordinatorWithActivation(a.registry, reconciler, a.routes, lifecycle, a.channelLimits, false)
	if err != nil {
		return nil, fmt.Errorf("%w: coordinator: %w", errInvalidSessionRuntime, err)
	}
	a.bound = true
	return &SessionRuntime{coordinator: coordinator, claimInstallGate: a.claimInstallGate}, nil
}
