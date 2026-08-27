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

// sessionReadyCallback is the sole transport boundary for server frames. It
// must return nil only after making the supplied frame visible to the peer.
// Every frame is an independent value containing no credential.
type sessionReadyCallback func(context.Context, *externalproviderpb.ServerFrame) error

// sessionCoordinator sequences one already-authenticated provider generation.
// It owns no transport, credential, Worker persistence, or route index.
type sessionCoordinator struct {
	registry       *sessionRegistry
	reconciler     WorkerPlanReconciler
	routes         *SessionRouteDirectory
	lifecycle      *workerSessionLifecycle
	limits         ChannelSessionLimits
	forwarder      *executionForwarder
	sendQueueDepth uint32
	// activateWorkers is derived from forwarder presence and retained as an
	// inspectable invariant; it is never an independently configurable switch.
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
	wire        *sessionWire
	forwarding  *executionSession
	ctx         context.Context
	cancel      context.CancelCauseFunc

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
	forwarder, err := newExecutionForwarder(routes, DefaultExecutionForwardingLimits())
	if err != nil {
		return nil, fmt.Errorf("%w: forwarder: %w", errInvalidSessionCoordinator, err)
	}
	return newSessionCoordinatorWithForwarder(registry, reconciler, routes, lifecycle, limits, forwarder)
}

func newPassiveSessionCoordinator(
	registry *sessionRegistry,
	reconciler WorkerPlanReconciler,
	routes *SessionRouteDirectory,
	lifecycle *workerSessionLifecycle,
	limits ChannelSessionLimits,
) (*sessionCoordinator, error) {
	return newSessionCoordinatorWithForwarder(registry, reconciler, routes, lifecycle, limits, nil)
}

func newSessionCoordinatorWithForwarder(
	registry *sessionRegistry,
	reconciler WorkerPlanReconciler,
	routes *SessionRouteDirectory,
	lifecycle *workerSessionLifecycle,
	limits ChannelSessionLimits,
	forwarder *executionForwarder,
) (*sessionCoordinator, error) {
	if registry == nil || reconciler == nil || routes == nil || lifecycle == nil ||
		routes.registry != registry || lifecycle.registry != registry || lifecycle.routes != routes ||
		(forwarder != nil && forwarder.routes != routes) {
		return nil, fmt.Errorf("%w: registry, reconciler, route directory, and lifecycle authority must agree", errInvalidSessionCoordinator)
	}
	normalized, err := normalizeChannelSessionLimits(limits)
	if err != nil {
		return nil, fmt.Errorf("%w: channel limits: %w", errInvalidSessionCoordinator, err)
	}
	sendLimits := DefaultExecutionForwardingLimits()
	if forwarder != nil {
		sendLimits = forwarder.limits
	}
	return &sessionCoordinator{
		registry:        registry,
		reconciler:      reconciler,
		routes:          routes,
		lifecycle:       lifecycle,
		limits:          normalized,
		forwarder:       forwarder,
		sendQueueDepth:  sendLimits.SendQueueDepth,
		activateWorkers: forwarder != nil,
	}, nil
}

// establish validates the gated authenticated admission and executes the only
// permitted startup order. The claim-install gate is released immediately
// after the registry install, before reconciliation or transport publication.
func (c *sessionCoordinator) establish(
	ctx context.Context,
	gatedClaim *gatedSessionClaim,
	hello *prevalidatedConnectHello,
	ready sessionReadyCallback,
) (_ *coordinatedSession, returnedErr error) {
	if c == nil || ctx == nil || ready == nil {
		return nil, fmt.Errorf("%w: context and Ready callback are required", errInvalidSessionCoordinator)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	claim, claimGate, err := gatedClaim.beginInstall()
	if err != nil {
		return nil, err
	}
	releaseClaimGate := true
	defer func() {
		if releaseClaimGate {
			claimGate.release()
		}
	}()
	admission, err := validatePrevalidatedConnectAdmission(claim, hello)
	if err != nil {
		return nil, err
	}

	lease, err := c.lifecycle.install(ctx, admission.Registration().UID, admission.Generation())
	if err != nil {
		return nil, fmt.Errorf("installing external provider session: %w", err)
	}
	var route *SessionRoute
	var wire *sessionWire
	var forwarding *executionSession
	established := false
	defer func() {
		if established {
			return
		}
		cleanupErr := c.cleanupFailedEstablishment(ctx, lease, route, forwarding, wire)
		returnedErr = errors.Join(returnedErr, cleanupErr)
	}()
	claimGate.release()
	releaseClaimGate = false

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
	wire, err = newSessionWire(ctx, ready, c.sendQueueDepth)
	if err != nil {
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
		if err := wire.sendFrame(ctx, proto.Clone(readyFrame).(*externalproviderpb.ServerFrame)); err != nil {
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
	if c.forwarder != nil {
		forwarding, err = c.forwarder.bind(route, channels, wire)
		if err != nil {
			return nil, fmt.Errorf("binding external provider execution forwarding: %w", err)
		}
		if _, err := c.lifecycle.activate(ctx, route, plan, reconciled); err != nil {
			return nil, fmt.Errorf("activating external provider Workers: %w", err)
		}
	}
	sessionCtx, cancelSession := context.WithCancelCause(ctx)

	session := &coordinatedSession{
		coordinator: c,
		lease:       lease,
		route:       route,
		channels:    channels,
		wire:        wire,
		forwarding:  forwarding,
		ctx:         sessionCtx,
		cancel:      cancelSession,
	}
	go session.observeTransport()
	established = true
	return session, nil
}

func (c *sessionCoordinator) cleanupFailedEstablishment(
	ctx context.Context,
	lease *sessionLease,
	route *SessionRoute,
	forwarding *executionSession,
	wire *sessionWire,
) error {
	if forwarding != nil {
		forwarding.close(ErrExternalExecutionUnavailable)
	}
	if wire != nil {
		wire.close(ErrExternalExecutionUnavailable)
	}
	var cleanupErr error
	if route == nil {
		_, cleanupErr = c.lifecycle.cleanupUnpublished(ctx, lease)
	} else {
		_, cleanupErr = c.lifecycle.cleanup(ctx, route)
	}
	// No handle is returned on failure. Always exact-remove this lease after
	// the best-effort OFFLINE pass; conservative pending ownership remains in
	// the bounded lifecycle tombstone for the next generation.
	c.registry.remove(lease.registration(), lease.sessionGeneration(), lease, cleanupErr == nil)
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
	if s == nil || s.ctx == nil {
		return closedRouteDone
	}
	if routeLiveError(s.route) != nil {
		return s.route.Done()
	}
	if s.wire == nil || s.wire.ctx.Err() != nil {
		return s.wire.done()
	}
	return s.ctx.Done()
}

func (s *coordinatedSession) observeTransport() {
	select {
	case <-s.route.Done():
		s.cancel(s.route.CancellationCause())
	case <-s.wire.done():
		s.cancel(context.Cause(s.wire.ctx))
	case <-s.ctx.Done():
	}
}

func (s *coordinatedSession) applyClientFrame(frame *externalproviderpb.ClientFrame) error {
	if s == nil || s.wire == nil || s.channels == nil {
		return ErrExternalExecutionUnavailable
	}
	if s.forwarding != nil {
		return s.forwarding.applyClientFrame(frame)
	}
	if err := s.wire.lockOperation(s.ctx); err != nil {
		return err
	}
	defer s.wire.unlockOperation()
	effect, err := s.channels.ApplyClientFrame(frame)
	if err != nil {
		return err
	}
	switch effect := effect.(type) {
	case *SendServerFrameEffect:
		return s.wire.sendFrame(s.ctx, effect.Frame())
	case *ServerHeartbeatAckEffect:
		return nil
	default:
		return ErrExternalExecutionUnavailable
	}
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
	if s.forwarding != nil {
		s.forwarding.close(ErrExternalExecutionUnavailable)
	}
	s.wire.close(ErrExternalExecutionUnavailable)
	s.cancel(ErrExternalExecutionUnavailable)
	if _, err := s.coordinator.lifecycle.cleanup(ctx, s.route); err != nil {
		return fmt.Errorf("cleaning external provider Workers: %w", err)
	}
	s.coordinator.registry.remove(s.lease.registration(), s.lease.sessionGeneration(), s.lease, true)
	s.closed = true
	return nil
}
