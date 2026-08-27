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
	"sync"

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/proto"
)

var errInvalidSessionCoordinator = errors.New("invalid external provider session coordinator")

// sessionReadyCallback is the transport boundary for the first server frame.
// It must return nil only after making the supplied ConnectReady frame visible
// to the peer. The frame is an independent value containing no credential.
type sessionReadyCallback func(context.Context, *externalproviderpb.ServerFrame) error

// sessionCoordinator sequences one already-authenticated provider generation.
// It owns no transport, credential, Worker persistence, or route index.
type sessionCoordinator struct {
	registry   *sessionRegistry
	reconciler WorkerPlanReconciler
	routes     *SessionRouteDirectory
	lifecycle  *workerSessionLifecycle
	limits     ChannelSessionLimits
	// activateWorkers is true only when the caller also owns a complete
	// execution-channel forwarding path. Passive transport runtimes leave every
	// reconciled Worker OFFLINE while still exercising route/session lifecycle.
	activateWorkers bool
}

// coordinatedSession is returned only after Ready, route publication, and any
// enabled Worker activation succeed. close is retryable after an OFFLINE
// failure.
type coordinatedSession struct {
	coordinator *sessionCoordinator
	lease       *sessionLease
	route       *SessionRoute
	channels    *ChannelSessionState

	closeMu sync.Mutex
	closed  bool
}

func newSessionCoordinator(
	registry *sessionRegistry,
	reconciler WorkerPlanReconciler,
	routes *SessionRouteDirectory,
	lifecycle *workerSessionLifecycle,
	limits ChannelSessionLimits,
) (*sessionCoordinator, error) {
	return newSessionCoordinatorWithActivation(registry, reconciler, routes, lifecycle, limits, true)
}

func newSessionCoordinatorWithActivation(
	registry *sessionRegistry,
	reconciler WorkerPlanReconciler,
	routes *SessionRouteDirectory,
	lifecycle *workerSessionLifecycle,
	limits ChannelSessionLimits,
	activateWorkers bool,
) (*sessionCoordinator, error) {
	if registry == nil || reconciler == nil || routes == nil || lifecycle == nil ||
		routes.registry != registry || lifecycle.registry != registry || lifecycle.routes != routes {
		return nil, fmt.Errorf("%w: registry, reconciler, route directory, and lifecycle authority must agree", errInvalidSessionCoordinator)
	}
	normalized, err := normalizeChannelSessionLimits(limits)
	if err != nil {
		return nil, fmt.Errorf("%w: channel limits: %w", errInvalidSessionCoordinator, err)
	}
	return &sessionCoordinator{
		registry:        registry,
		reconciler:      reconciler,
		routes:          routes,
		lifecycle:       lifecycle,
		limits:          normalized,
		activateWorkers: activateWorkers,
	}, nil
}

// establish validates the authenticated admission and executes the only
// permitted startup order. The caller owns first-frame receive and the atomic
// credential claim which produced claim and hello.
func (c *sessionCoordinator) establish(
	ctx context.Context,
	claim SessionClaim,
	hello *prevalidatedConnectHello,
	ready sessionReadyCallback,
) (_ *coordinatedSession, returnedErr error) {
	if c == nil || ctx == nil || ready == nil {
		return nil, fmt.Errorf("%w: context and Ready callback are required", errInvalidSessionCoordinator)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	admission, err := validatePrevalidatedConnectAdmission(claim, hello)
	if err != nil {
		return nil, err
	}

	lease, err := c.lifecycle.install(ctx, admission.Registration().UID, admission.Generation())
	if err != nil {
		return nil, fmt.Errorf("installing external provider session: %w", err)
	}
	var route *SessionRoute
	established := false
	defer func() {
		if established {
			return
		}
		cleanupErr := c.cleanupFailedEstablishment(ctx, lease, route)
		returnedErr = errors.Join(returnedErr, cleanupErr)
	}()

	plan, err := PlanExternalWorkers(admission)
	if err != nil {
		return nil, err
	}
	reconciled, err := c.reconciler.ReconcileExternalWorkers(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("reconciling external provider Workers: %w", err)
	}
	bindings, err := BuildSessionWorkerBindings(admission, reconciled)
	if err != nil {
		return nil, err
	}
	channels, err := NewChannelSessionState(admission, c.limits)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	readyFrame := &externalproviderpb.ServerFrame{
		SessionGeneration: admission.Generation(),
		Frame: &externalproviderpb.ServerFrame_Ready{Ready: &externalproviderpb.ConnectReady{
			MaxOpenChannels: channels.limits.MaxOpenChannels,
			MaxDataBytes:    channels.limits.MaxDataBytes,
		}},
	}
	err = c.registry.withCurrentLease(lease, func(_ *sessionLifecycleState, _ sessionEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ready(ctx, proto.Clone(readyFrame).(*externalproviderpb.ServerFrame)); err != nil {
			return fmt.Errorf("crossing external provider Ready boundary: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var publishErr error
		route, publishErr = c.routes.publish(lease, bindings)
		if publishErr != nil {
			return fmt.Errorf("publishing external provider session route: %w", publishErr)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if c.activateWorkers {
		if _, err := c.lifecycle.activate(ctx, route, plan, reconciled); err != nil {
			return nil, fmt.Errorf("activating external provider Workers: %w", err)
		}
	}

	session := &coordinatedSession{
		coordinator: c,
		lease:       lease,
		route:       route,
		channels:    channels,
	}
	established = true
	return session, nil
}

func (c *sessionCoordinator) cleanupFailedEstablishment(ctx context.Context, lease *sessionLease, route *SessionRoute) error {
	var cleanupErr error
	if route == nil {
		_, cleanupErr = c.lifecycle.cleanupUnpublished(ctx, lease)
	} else {
		_, cleanupErr = c.lifecycle.cleanup(ctx, route)
	}
	// No handle is returned on failure. Always exact-remove this lease after
	// the best-effort OFFLINE pass; conservative pending ownership remains in
	// the bounded lifecycle tombstone for the next generation.
	c.registry.remove(lease.registration(), lease.sessionGeneration(), lease)
	if cleanupErr != nil {
		return fmt.Errorf("cleaning failed external provider session establishment: %w", cleanupErr)
	}
	return nil
}

func (s *coordinatedSession) channelState() *ChannelSessionState {
	if s == nil {
		return nil
	}
	return s.channels
}

func (s *coordinatedSession) done() <-chan struct{} {
	if s == nil || s.route == nil {
		return closedRouteDone
	}
	return s.route.Done()
}

// close first withdraws the exact route, then offlines owned Workers, then
// exact-removes the lease. An OFFLINE failure leaves the lease available for a
// bounded caller-controlled retry while the route remains unavailable.
func (s *coordinatedSession) close(ctx context.Context) error {
	if s == nil || s.coordinator == nil || s.lease == nil || s.route == nil || ctx == nil {
		return fmt.Errorf("%w: coordinated session and context are required", errInvalidSessionCoordinator)
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	if _, err := s.coordinator.lifecycle.cleanup(ctx, s.route); err != nil {
		return fmt.Errorf("cleaning external provider Workers: %w", err)
	}
	s.coordinator.registry.remove(s.lease.registration(), s.lease.sessionGeneration(), s.lease)
	s.closed = true
	return nil
}
