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
	"io"
	"math"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/validate/content"
)

const (
	defaultExecutionSendQueueDepth    uint32 = 256
	maximumExecutionSendQueueDepth    uint32 = 65535
	defaultExecutionReceiveQueueDepth uint32 = 16
	maximumExecutionReceiveQueueDepth uint32 = 1024
	defaultExecutionCloseTimeout             = 5 * time.Second
	maximumExecutionCloseTimeout             = 30 * time.Second
)

var (
	// ErrInvalidExecutionForwardingConfig reports an unsafe forwarding bound.
	ErrInvalidExecutionForwardingConfig = errors.New("invalid external execution forwarding configuration")

	// ErrInvalidExternalExecutionAssignment reports an assignment which does
	// not carry the exact ExternalSlot Worker incarnation identities.
	ErrInvalidExternalExecutionAssignment = errors.New("invalid external execution assignment")

	// ErrExternalExecutionUnavailable reports that no exact live transport can
	// currently serve an otherwise valid ExternalSlot assignment.
	ErrExternalExecutionUnavailable = errors.New("external execution transport is unavailable")

	// ErrExternalExecutionRejected reports a generic client-side Open rejection.
	// The untrusted peer message is deliberately not included in this error.
	ErrExternalExecutionRejected = errors.New("external execution channel was rejected")

	// ErrExternalExecutionBackpressure reports that a bounded channel queue was
	// exhausted. The affected channel is reset instead of stalling Connect.
	ErrExternalExecutionBackpressure = errors.New("external execution channel backpressure limit was exceeded")

	// ErrExternalExecutionChannelIDsExhausted reports that this generation has
	// no unused even channel IDs left. A newer generation is required.
	ErrExternalExecutionChannelIDsExhausted = errors.New("external execution channel IDs are exhausted")
)

// ExecutionForwardingLimits bounds every queue owned by the execution data
// plane. MaxServerChannelID is an even, nonzero testable ceiling; zero selects
// the largest even uint64. Channel IDs are never reused within a generation.
type ExecutionForwardingLimits struct {
	SendQueueDepth     uint32
	ReceiveQueueDepth  uint32
	MaxServerChannelID uint64
	CloseTimeout       time.Duration
}

// DefaultExecutionForwardingLimits returns conservative in-process bounds.
func DefaultExecutionForwardingLimits() ExecutionForwardingLimits {
	return ExecutionForwardingLimits{
		SendQueueDepth:     defaultExecutionSendQueueDepth,
		ReceiveQueueDepth:  defaultExecutionReceiveQueueDepth,
		MaxServerChannelID: math.MaxUint64 - 1,
		CloseTimeout:       defaultExecutionCloseTimeout,
	}
}

func normalizeExecutionForwardingLimits(limits ExecutionForwardingLimits) (ExecutionForwardingLimits, error) {
	if limits.SendQueueDepth == 0 {
		limits.SendQueueDepth = defaultExecutionSendQueueDepth
	}
	if limits.ReceiveQueueDepth == 0 {
		limits.ReceiveQueueDepth = defaultExecutionReceiveQueueDepth
	}
	if limits.MaxServerChannelID == 0 {
		limits.MaxServerChannelID = math.MaxUint64 - 1
	}
	if limits.CloseTimeout == 0 {
		limits.CloseTimeout = defaultExecutionCloseTimeout
	}
	if limits.SendQueueDepth > maximumExecutionSendQueueDepth {
		return ExecutionForwardingLimits{}, fmt.Errorf("%w: send queue depth must be at most %d", ErrInvalidExecutionForwardingConfig, maximumExecutionSendQueueDepth)
	}
	if limits.ReceiveQueueDepth > maximumExecutionReceiveQueueDepth {
		return ExecutionForwardingLimits{}, fmt.Errorf("%w: receive queue depth must be at most %d", ErrInvalidExecutionForwardingConfig, maximumExecutionReceiveQueueDepth)
	}
	if limits.MaxServerChannelID < 2 || limits.MaxServerChannelID%2 != 0 {
		return ExecutionForwardingLimits{}, fmt.Errorf("%w: maximum server channel ID must be nonzero and even", ErrInvalidExecutionForwardingConfig)
	}
	if limits.CloseTimeout < time.Millisecond || limits.CloseTimeout > maximumExecutionCloseTimeout {
		return ExecutionForwardingLimits{}, fmt.Errorf("%w: close timeout must be in 1ms..30s", ErrInvalidExecutionForwardingConfig)
	}
	return limits, nil
}

type wireSendRequest struct {
	ctx    context.Context
	frame  *externalproviderpb.ServerFrame
	result chan error
}

// sessionWire is the sole caller of a Connect stream's Send method. Its
// context-aware operation gate also orders state-machine transitions with
// their emitted frames, so concurrent net.Conn writes cannot overtake effects.
type sessionWire struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	send   sessionReadyCallback
	queue  chan wireSendRequest

	opGate chan struct{}
	// sendMu is held across the production transport callback itself. Unlike
	// the operation gate, it is not released when a request context wakes the
	// caller while stream.Send is still running. A route fence takes this lock
	// after canceling the wire, thereby joining an already-started Send and
	// preventing a queued Send from crossing the fence afterwards.
	sendMu     sync.Mutex
	sendActive atomic.Bool
}

func newSessionWire(parent context.Context, send sessionReadyCallback, queueDepth uint32) (*sessionWire, error) {
	if parent == nil || send == nil || queueDepth == 0 || queueDepth > maximumExecutionSendQueueDepth {
		return nil, fmt.Errorf("%w: session sender and bounded queue are required", ErrInvalidExecutionForwardingConfig)
	}
	ctx, cancel := context.WithCancelCause(parent)
	wire := &sessionWire{
		ctx:    ctx,
		cancel: cancel,
		send:   send,
		queue:  make(chan wireSendRequest, queueDepth),
		opGate: make(chan struct{}, 1),
	}
	wire.opGate <- struct{}{}
	go wire.run()
	return wire, nil
}

func (w *sessionWire) lockOperation(ctx context.Context) error {
	if w == nil || ctx == nil {
		return ErrExternalExecutionUnavailable
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if err := w.ctx.Err(); err != nil {
		return context.Cause(w.ctx)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-w.ctx.Done():
		return context.Cause(w.ctx)
	case <-w.opGate:
		if err := ctx.Err(); err != nil {
			w.unlockOperation()
			return context.Cause(ctx)
		}
		if err := w.ctx.Err(); err != nil {
			w.unlockOperation()
			return context.Cause(w.ctx)
		}
		return nil
	}
}

func (w *sessionWire) unlockOperation() {
	if w != nil {
		w.opGate <- struct{}{}
	}
}

// fenceOperations joins the one operation which may already have crossed the
// route/context checks and entered the transport sender. Route cancellation is
// performed before this call, so later operations fail when they acquire the
// gate. Waiting here is the safety boundary which prevents an old generation's
// Send from completing after Close/replacement returns.
func (w *sessionWire) fenceOperations(cause error, fence func()) {
	if w == nil {
		if fence != nil {
			fence()
		}
		return
	}
	w.close(cause)
	<-w.opGate
	defer w.unlockOperation()
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if fence != nil {
		fence()
	}
}

func (w *sessionWire) run() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case request := <-w.queue:
			if w.ctx.Err() != nil {
				select {
				case request.result <- context.Cause(w.ctx):
				default:
				}
				return
			}
			if request.ctx == nil || request.ctx.Err() != nil {
				cause := ErrExternalExecutionUnavailable
				if request.ctx != nil {
					cause = context.Cause(request.ctx)
				}
				select {
				case request.result <- cause:
				default:
				}
				continue
			}
			// The cancellation check and transport call share sendMu with the
			// route barrier. Thus either this Send was already in flight and the
			// barrier joins it, or cancellation wins and this queued frame is
			// rejected without crossing the old generation boundary.
			w.sendMu.Lock()
			// Publish immediately after taking sendMu, before the cancellation
			// check. If shutdown observes false, a preempted sender can only
			// resume after wire cancellation and will reject the frame; if it
			// observes true, the RPC handler returns to abort a callback which
			// may already depend on stream.Context.
			w.sendActive.Store(true)
			var err error
			if w.ctx.Err() != nil {
				err = context.Cause(w.ctx)
			} else if request.ctx == nil || request.ctx.Err() != nil {
				err = ErrExternalExecutionUnavailable
				if request.ctx != nil {
					err = context.Cause(request.ctx)
				}
			} else {
				err = w.send(request.ctx, proto.Clone(request.frame).(*externalproviderpb.ServerFrame))
			}
			w.sendActive.Store(false)
			w.sendMu.Unlock()
			select {
			case request.result <- err:
			default:
			}
			if err != nil {
				w.cancel(err)
				return
			}
		}
	}
}

func (w *sessionWire) sendFrame(ctx context.Context, frame *externalproviderpb.ServerFrame) error {
	if w == nil || ctx == nil || frame == nil {
		return ErrExternalExecutionUnavailable
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if err := w.ctx.Err(); err != nil {
		return context.Cause(w.ctx)
	}
	request := wireSendRequest{
		ctx:    ctx,
		frame:  proto.Clone(frame).(*externalproviderpb.ServerFrame),
		result: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-w.ctx.Done():
		return context.Cause(w.ctx)
	case w.queue <- request:
	}
	select {
	case err := <-request.result:
		return err
	case <-w.ctx.Done():
		return context.Cause(w.ctx)
	case <-ctx.Done():
		// Once queued, a timeout makes the frame outcome ambiguous. Fence the
		// whole generation instead of permitting later effects to overtake it.
		w.cancel(context.Cause(ctx))
		return context.Cause(ctx)
	}
}

func (w *sessionWire) close(cause error) {
	if w == nil {
		return
	}
	if cause == nil {
		cause = ErrExternalExecutionUnavailable
	}
	w.cancel(cause)
}

func (w *sessionWire) done() <-chan struct{} {
	if w == nil || w.ctx == nil {
		return closedRouteDone
	}
	return w.ctx.Done()
}

// transportIdle is a non-blocking proof that no sender can cross the production
// transport boundary. The caller must cancel the wire first. sendActive is
// published immediately after sendMu acquisition, before the cancellation
// recheck, which closes the preemption window without confusing a route fence
// merely holding sendMu with a blocked production callback.
func (w *sessionWire) transportIdle() bool {
	if w == nil {
		return true
	}
	return !w.sendActive.Load()
}

// ExternalExecutionDialer opens generation-fenced EXECUTION_GRPC byte streams.
// It is returned only by SessionAuthority.BindExecutionForwarding.
type ExternalExecutionDialer struct {
	forwarder *executionForwarder
}

// DialContext validates the complete assignment snapshot, opens a server-owned
// even channel, and returns only after the external provider accepts Open.
func (d *ExternalExecutionDialer) DialContext(ctx context.Context, assignment *ateapipb.WorkerAssignment) (net.Conn, error) {
	if d == nil || d.forwarder == nil {
		return nil, ErrExternalExecutionUnavailable
	}
	return d.forwarder.dial(ctx, assignment, externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC)
}

// ExternalActorIngressDialer opens generation-fenced ACTOR_INGRESS byte
// streams. It shares the same route/session fence as ExternalExecutionDialer,
// so neither channel kind can cross a provider generation replacement.
type ExternalActorIngressDialer struct {
	forwarder *executionForwarder
}

// DialContext validates the complete assignment snapshot, opens a server-owned
// ACTOR_INGRESS channel, and returns only after the external provider accepts
// Open. It never consumes an endpoint or provider credential from the caller.
func (d *ExternalActorIngressDialer) DialContext(ctx context.Context, assignment *ateapipb.WorkerAssignment) (net.Conn, error) {
	if d == nil || d.forwarder == nil {
		return nil, ErrExternalExecutionUnavailable
	}
	return d.forwarder.dial(ctx, assignment, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_INGRESS)
}

type executionForwarder struct {
	routes *SessionRouteDirectory
	limits ExecutionForwardingLimits

	mu       sync.RWMutex
	sessions map[*SessionRoute]*executionSession
}

func newExecutionForwarder(routes *SessionRouteDirectory, limits ExecutionForwardingLimits) (*executionForwarder, error) {
	if routes == nil || routes.registry == nil {
		return nil, fmt.Errorf("%w: route authority is required", ErrInvalidExecutionForwardingConfig)
	}
	normalized, err := normalizeExecutionForwardingLimits(limits)
	if err != nil {
		return nil, err
	}
	return &executionForwarder{
		routes:   routes,
		limits:   normalized,
		sessions: make(map[*SessionRoute]*executionSession),
	}, nil
}

func (f *executionForwarder) dial(ctx context.Context, assignment *ateapipb.WorkerAssignment, kind externalproviderpb.ChannelKind) (net.Conn, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalidExternalExecutionAssignment)
	}
	if kind != externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC &&
		kind != externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_INGRESS {
		return nil, fmt.Errorf("%w: unsupported server channel kind", ErrInvalidExternalExecutionAssignment)
	}
	snapshot, err := validateExecutionAssignment(assignment)
	if err != nil {
		return nil, err
	}
	route, binding, found := f.routes.LookupExecutionIdentity(snapshot.executionIdentity)
	if !found || !snapshot.matches(binding) {
		return nil, ErrExternalExecutionUnavailable
	}
	f.mu.RLock()
	session := f.sessions[route]
	f.mu.RUnlock()
	if session == nil || !f.routes.AuthorizesBinding(route, binding) {
		return nil, ErrExternalExecutionUnavailable
	}
	return session.open(ctx, binding, kind)
}

type executionAssignmentSnapshot struct {
	workerName        string
	workerUID         string
	workerNamespace   string
	workerPool        string
	executionIdentity string
	localityIdentity  string
	ownerAtespace     string
}

func (s executionAssignmentSnapshot) matches(binding SessionWorkerBinding) bool {
	return s.workerName == binding.WorkerName() && s.workerUID == binding.WorkerUID() &&
		s.workerNamespace == binding.WorkerNamespace() && s.workerPool == binding.WorkerPool() &&
		s.executionIdentity == binding.ExecutionIdentity() && s.localityIdentity == binding.LocalityIdentity() &&
		s.ownerAtespace == binding.OwnerAtespace()
}

func validateExecutionAssignment(assignment *ateapipb.WorkerAssignment) (executionAssignmentSnapshot, error) {
	if assignment == nil || assignment.GetProvider() != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT ||
		assignment.GetWorker() == nil || assignment.GetWorker().GetAtespace() != "" ||
		!resources.IsValidResourceName(assignment.GetWorker().GetName()) ||
		len(content.IsDNS1123Label(assignment.GetWorkerNamespace())) != 0 ||
		len(content.IsDNS1123Subdomain(assignment.GetWorkerPool())) != 0 ||
		assignment.GetWorkerPod() != "" || assignment.GetWorkerPodUid() != "" || assignment.GetWorkerPodIp() != "" ||
		assignment.GetExternalSlot() == nil || !IsValidIdentity(assignment.GetExternalSlot().GetExecutionIdentity()) ||
		!IsValidIdentity(assignment.GetExternalSlot().GetLocalityIdentity()) ||
		!resources.IsValidResourceName(assignment.GetExternalSlot().GetOwnerAtespace()) {
		return executionAssignmentSnapshot{}, ErrInvalidExternalExecutionAssignment
	}
	parsedUID, err := uuid.Parse(assignment.GetWorkerResourceUid())
	if err != nil || parsedUID.String() != assignment.GetWorkerResourceUid() {
		return executionAssignmentSnapshot{}, ErrInvalidExternalExecutionAssignment
	}
	return executionAssignmentSnapshot{
		workerName:        assignment.GetWorker().GetName(),
		workerUID:         assignment.GetWorkerResourceUid(),
		workerNamespace:   assignment.GetWorkerNamespace(),
		workerPool:        assignment.GetWorkerPool(),
		executionIdentity: assignment.GetExternalSlot().GetExecutionIdentity(),
		localityIdentity:  assignment.GetExternalSlot().GetLocalityIdentity(),
		ownerAtespace:     assignment.GetExternalSlot().GetOwnerAtespace(),
	}, nil
}

func (f *executionForwarder) bind(route *SessionRoute, channels *ChannelSessionState, wire *sessionWire) (*executionSession, error) {
	if f == nil || route == nil || channels == nil || wire == nil || routeLiveError(route) != nil ||
		channels.Generation() != route.Generation() {
		return nil, ErrExternalExecutionUnavailable
	}
	wantSlots := make([]string, 0, len(route.bindings))
	for _, binding := range route.bindings {
		wantSlots = append(wantSlots, binding.SlotID())
	}
	slices.Sort(wantSlots)
	if !slices.Equal(wantSlots, channels.SlotIDs()) {
		return nil, ErrExternalExecutionUnavailable
	}
	// Deriving directly from the route eliminates the observer window: route
	// Close/replacement synchronously fences every operation context before the
	// directory mutation returns. observeRoute still handles wire failure and
	// deterministic map/connection cleanup.
	sessionCtx, cancel := context.WithCancelCause(route.ctx)
	session := &executionSession{
		forwarder: f,
		route:     route,
		channels:  channels,
		wire:      wire,
		ctx:       sessionCtx,
		cancel:    cancel,
		nextID:    2,
		conns:     make(map[uint64]*executionConn),
	}
	var replaced []*executionSession
	f.mu.Lock()
	if _, exists := f.sessions[route]; exists || routeLiveError(route) != nil {
		f.mu.Unlock()
		cancel(ErrExternalExecutionUnavailable)
		return nil, ErrExternalExecutionUnavailable
	}
	if !route.installExecutionFence(session.fenceRoute) {
		f.mu.Unlock()
		cancel(ErrExternalExecutionUnavailable)
		return nil, ErrExternalExecutionUnavailable
	}
	for published, existing := range f.sessions {
		if published.RegistrationUID() == route.RegistrationUID() {
			delete(f.sessions, published)
			replaced = append(replaced, existing)
		}
	}
	f.sessions[route] = session
	f.mu.Unlock()
	for _, existing := range replaced {
		existing.close(ErrSessionRouteReplaced)
	}
	go session.observeRoute()
	return session, nil
}

func (f *executionForwarder) remove(session *executionSession) {
	if f == nil || session == nil {
		return
	}
	f.mu.Lock()
	if f.sessions[session.route] == session {
		delete(f.sessions, session.route)
	}
	f.mu.Unlock()
}

type executionSession struct {
	forwarder *executionForwarder
	route     *SessionRoute
	channels  *ChannelSessionState
	wire      *sessionWire
	ctx       context.Context
	cancel    context.CancelCauseFunc

	closeOnce sync.Once
	mu        sync.Mutex
	nextID    uint64
	conns     map[uint64]*executionConn
}

func (s *executionSession) observeRoute() {
	select {
	case <-s.route.Done():
		s.close(s.route.CancellationCause())
	case <-s.wire.done():
		s.close(context.Cause(s.wire.ctx))
	case <-s.ctx.Done():
		// s.ctx is a direct child of route.ctx, so route cancellation can make
		// both select cases ready. Recheck the parents to ensure choosing this
		// case never skips deterministic map/connection cleanup. A direct
		// s.close cancellation has already performed that cleanup.
		if cause := s.route.CancellationCause(); cause != nil {
			s.close(cause)
		} else if cause := context.Cause(s.wire.ctx); cause != nil {
			s.close(cause)
		}
	}
}

func (s *executionSession) fenceRoute(cause error) {
	if s == nil {
		return
	}
	if cause == nil {
		cause = ErrExternalExecutionUnavailable
	}
	s.cancel(cause)
	s.wire.fenceOperations(cause, func() {
		s.close(cause)
	})
}

func (s *executionSession) close(cause error) {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		if cause == nil {
			cause = ErrExternalExecutionUnavailable
		}
		s.cancel(cause)
		s.forwarder.remove(s)
		s.mu.Lock()
		connections := make([]*executionConn, 0, len(s.conns))
		for _, connection := range s.conns {
			connections = append(connections, connection)
		}
		clear(s.conns)
		s.mu.Unlock()
		for _, connection := range connections {
			connection.fail(cause)
		}
	})
}

func (s *executionSession) done() <-chan struct{} {
	if s == nil || s.ctx == nil {
		return closedRouteDone
	}
	return s.ctx.Done()
}

func (s *executionSession) open(ctx context.Context, binding SessionWorkerBinding, kind externalproviderpb.ChannelKind) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if kind != externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC &&
		kind != externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_INGRESS {
		return nil, ErrInvalidExternalExecutionAssignment
	}
	if err := s.wire.lockOperation(ctx); err != nil {
		return nil, err
	}
	if err := s.liveBindingError(binding); err != nil {
		s.wire.unlockOperation()
		return nil, err
	}
	connection, err := s.allocateConnection(binding)
	if err != nil {
		s.wire.unlockOperation()
		return nil, err
	}
	effect, err := s.channels.OpenServerChannel(connection.channelID, kind, binding.SlotID())
	if err == nil {
		err = s.wire.sendFrame(ctx, effect.Frame())
	}
	s.wire.unlockOperation()
	if err != nil {
		s.removeConnection(connection.channelID, connection)
		connection.fail(err)
		if s.wire.ctx.Err() != nil {
			s.close(context.Cause(s.wire.ctx))
		}
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, fmt.Errorf("%w: opening channel", ErrExternalExecutionUnavailable)
	}

	accepted, err := connection.waitForAck(ctx)
	if err != nil {
		// The channel state machine cannot retract an Open while it is awaiting
		// the peer's ack. Once the caller stops waiting, the outcome is ambiguous;
		// fence the entire generation so the pending channel can never consume
		// capacity in a reusable live session.
		s.wire.close(err)
		s.close(err)
		return nil, err
	}
	if !accepted {
		return nil, ErrExternalExecutionRejected
	}
	if err := ctx.Err(); err != nil {
		if connection.abandon() {
			_ = s.resetBestEffort(connection)
		}
		return nil, err
	}
	if err := s.liveBindingError(binding); err != nil {
		_ = connection.Close()
		return nil, err
	}
	return connection, nil
}

func (s *executionSession) liveBindingError(binding SessionWorkerBinding) error {
	if s == nil || s.ctx.Err() != nil || routeLiveError(s.route) != nil ||
		!s.forwarder.routes.AuthorizesBinding(s.route, binding) {
		return ErrExternalExecutionUnavailable
	}
	return nil
}

func (s *executionSession) allocateConnection(binding SessionWorkerBinding) (*executionConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextID == 0 || s.nextID > s.forwarder.limits.MaxServerChannelID {
		return nil, ErrExternalExecutionChannelIDsExhausted
	}
	channelID := s.nextID
	if s.forwarder.limits.MaxServerChannelID-channelID < 2 {
		s.nextID = 0
	} else {
		s.nextID += 2
	}
	connection := newExecutionConn(s, channelID, binding, s.forwarder.limits.ReceiveQueueDepth)
	s.conns[channelID] = connection
	return connection, nil
}

func (s *executionSession) connection(channelID uint64) *executionConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[channelID]
}

func (s *executionSession) removeConnection(channelID uint64, expected *executionConn) {
	s.mu.Lock()
	if s.conns[channelID] == expected {
		delete(s.conns, channelID)
	}
	s.mu.Unlock()
}

func (s *executionSession) applyClientFrame(frame *externalproviderpb.ClientFrame) error {
	if s == nil {
		return ErrExternalExecutionUnavailable
	}
	if err := s.wire.lockOperation(s.ctx); err != nil {
		return err
	}
	defer s.wire.unlockOperation()
	if s.ctx.Err() != nil || routeLiveError(s.route) != nil {
		return ErrExternalExecutionUnavailable
	}
	effect, err := s.channels.ApplyClientFrame(frame)
	if err != nil {
		return err
	}
	return s.applyEffectLocked(effect)
}

func (s *executionSession) applyEffectLocked(effect SessionEffect) error {
	switch effect := effect.(type) {
	case *SendServerFrameEffect:
		return s.wire.sendFrame(s.ctx, effect.Frame())
	case *ServerHeartbeatAckEffect:
		return nil
	case *ClientOpenEffect:
		ack, err := s.channels.AcknowledgeClientOpen(effect.ChannelID(), false, "channel kind is unavailable")
		if err != nil {
			return err
		}
		return s.wire.sendFrame(s.ctx, ack.Frame())
	case *ServerOpenAckEffect:
		connection := s.connection(effect.ChannelID())
		if connection == nil {
			return ErrExternalExecutionUnavailable
		}
		if !effect.Accepted() {
			s.removeConnection(effect.ChannelID(), connection)
			connection.resolveAck(false)
			connection.fail(ErrExternalExecutionRejected)
			return nil
		}
		if connection.resolveAck(true) {
			return s.resetLocked(connection, codes.Canceled, "server abandoned execution channel", context.Canceled)
		}
		return nil
	case *ClientDataEffect:
		connection := s.connection(effect.ChannelID())
		if connection == nil {
			return ErrExternalExecutionUnavailable
		}
		if !connection.enqueue(effect.Data()) {
			return s.resetLocked(connection, codes.ResourceExhausted, "server receive queue exhausted", ErrExternalExecutionBackpressure)
		}
		return nil
	case *ClientHalfCloseEffect:
		connection := s.connection(effect.ChannelID())
		if connection == nil {
			return ErrExternalExecutionUnavailable
		}
		if connection.peerHalfClose() {
			s.removeConnection(effect.ChannelID(), connection)
		}
		return nil
	case *ClientResetEffect:
		connection := s.connection(effect.ChannelID())
		if connection == nil {
			return ErrExternalExecutionUnavailable
		}
		s.removeConnection(effect.ChannelID(), connection)
		connection.fail(ErrExternalExecutionUnavailable)
		return nil
	default:
		return ErrExternalExecutionUnavailable
	}
}

func (s *executionSession) resetLocked(connection *executionConn, code codes.Code, reason string, cause error) error {
	effect, err := s.channels.ResetServerChannel(connection.channelID, uint32(code), reason)
	if err != nil {
		return err
	}
	s.removeConnection(connection.channelID, connection)
	connection.fail(cause)
	return s.wire.sendFrame(s.ctx, effect.Frame())
}

func (s *executionSession) write(ctx context.Context, connection *executionConn, data []byte) error {
	if err := s.wire.lockOperation(ctx); err != nil {
		return err
	}
	defer s.wire.unlockOperation()
	if s.connection(connection.channelID) != connection || connection.localWriteIsClosed() ||
		s.liveBindingError(connection.binding) != nil {
		return net.ErrClosed
	}
	effect, err := s.channels.SendServerData(connection.channelID, data)
	if err != nil {
		return err
	}
	if err := s.wire.sendFrame(ctx, effect.Frame()); err != nil {
		if s.wire.ctx.Err() != nil {
			s.close(context.Cause(s.wire.ctx))
		}
		return err
	}
	return nil
}

func (s *executionSession) halfClose(ctx context.Context, connection *executionConn) error {
	if err := s.wire.lockOperation(ctx); err != nil {
		return err
	}
	defer s.wire.unlockOperation()
	if s.connection(connection.channelID) != connection || s.liveBindingError(connection.binding) != nil {
		return net.ErrClosed
	}
	effect, err := s.channels.HalfCloseServerChannel(connection.channelID)
	if err != nil {
		return err
	}
	if err := s.wire.sendFrame(ctx, effect.Frame()); err != nil {
		return err
	}
	if connection.markLocalWriteClosed() {
		s.removeConnection(connection.channelID, connection)
	}
	return nil
}

func (s *executionSession) reset(ctx context.Context, connection *executionConn) error {
	if err := s.wire.lockOperation(ctx); err != nil {
		return err
	}
	defer s.wire.unlockOperation()
	if s.connection(connection.channelID) != connection {
		return nil
	}
	if s.liveBindingError(connection.binding) != nil {
		s.removeConnection(connection.channelID, connection)
		return ErrExternalExecutionUnavailable
	}
	effect, err := s.channels.ResetServerChannel(connection.channelID, uint32(codes.Canceled), "server closed execution channel")
	if err != nil {
		return err
	}
	s.removeConnection(connection.channelID, connection)
	return s.wire.sendFrame(ctx, effect.Frame())
}

func (s *executionSession) resetBestEffort(connection *executionConn) error {
	ctx, cancel := context.WithTimeout(s.ctx, s.forwarder.limits.CloseTimeout)
	defer cancel()
	err := s.reset(ctx, connection)
	if err != nil && (ctx.Err() != nil || s.wire.ctx.Err() != nil) {
		s.wire.close(err)
		s.close(err)
	}
	return err
}

type executionConn struct {
	session   *executionSession
	channelID uint64
	binding   SessionWorkerBinding
	ack       chan bool

	stateMu        sync.Mutex
	ackResolved    bool
	ackAccepted    bool
	abandoned      bool
	localWriteDone bool
	peerReadDone   bool
	terminalErr    error
	chunks         [][]byte
	readBuffer     []byte
	receiveLimit   uint32
	readDeadline   time.Time
	writeDeadline  time.Time
	writeOperation uint64
	writeCancel    context.CancelCauseFunc
	writeTimer     *time.Timer
	writeTimerGen  uint64
	notify         chan struct{}

	readMu  sync.Mutex
	writeMu sync.Mutex
	close   sync.Once
}

func newExecutionConn(session *executionSession, channelID uint64, binding SessionWorkerBinding, receiveLimit uint32) *executionConn {
	return &executionConn{
		session:      session,
		channelID:    channelID,
		binding:      binding,
		ack:          make(chan bool, 1),
		receiveLimit: receiveLimit,
		notify:       make(chan struct{}, 1),
	}
}

func (c *executionConn) signal() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *executionConn) resolveAck(accepted bool) bool {
	c.stateMu.Lock()
	if c.ackResolved {
		abandoned := c.abandoned && c.ackAccepted
		c.stateMu.Unlock()
		return abandoned
	}
	c.ackResolved = true
	c.ackAccepted = accepted
	abandoned := c.abandoned && accepted
	c.stateMu.Unlock()
	c.ack <- accepted
	return abandoned
}

func (c *executionConn) waitForAck(ctx context.Context) (bool, error) {
	select {
	case accepted := <-c.ack:
		return accepted, nil
	case <-ctx.Done():
		return false, context.Cause(ctx)
	case <-c.session.done():
		return false, ErrExternalExecutionUnavailable
	}
}

func (c *executionConn) abandon() bool {
	c.stateMu.Lock()
	c.abandoned = true
	accepted := c.ackResolved && c.ackAccepted
	c.stateMu.Unlock()
	return accepted
}

func (c *executionConn) enqueue(data []byte) bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.terminalErr != nil || c.peerReadDone || uint32(len(c.chunks)) >= c.receiveLimit {
		return false
	}
	c.chunks = append(c.chunks, slices.Clone(data))
	c.signal()
	return true
}

func (c *executionConn) peerHalfClose() bool {
	c.stateMu.Lock()
	c.peerReadDone = true
	bothClosed := c.localWriteDone
	c.stateMu.Unlock()
	c.signal()
	return bothClosed
}

func (c *executionConn) markLocalWriteClosed() bool {
	c.stateMu.Lock()
	c.localWriteDone = true
	bothClosed := c.peerReadDone
	c.stateMu.Unlock()
	return bothClosed
}

func (c *executionConn) localWriteIsClosed() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.localWriteDone || c.terminalErr != nil
}

func (c *executionConn) fail(err error) {
	if err == nil {
		err = ErrExternalExecutionUnavailable
	}
	c.stateMu.Lock()
	writeCancel := c.writeCancel
	if c.terminalErr == nil {
		c.terminalErr = err
		c.chunks = nil
		c.readBuffer = nil
	}
	c.stateMu.Unlock()
	if writeCancel != nil {
		writeCancel(err)
	}
	c.signal()
}

func (c *executionConn) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		c.stateMu.Lock()
		if c.terminalErr != nil {
			err := c.terminalErr
			c.stateMu.Unlock()
			return 0, err
		}
		if len(c.readBuffer) == 0 && len(c.chunks) > 0 {
			c.readBuffer = c.chunks[0]
			c.chunks = c.chunks[1:]
		}
		if len(c.readBuffer) > 0 {
			count := copy(data, c.readBuffer)
			c.readBuffer = c.readBuffer[count:]
			c.stateMu.Unlock()
			return count, nil
		}
		if c.peerReadDone {
			c.stateMu.Unlock()
			return 0, io.EOF
		}
		deadline := c.readDeadline
		c.stateMu.Unlock()
		if err := c.waitForNotification(deadline); err != nil {
			return 0, err
		}
	}
}

func (c *executionConn) waitForNotification(deadline time.Time) error {
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return os.ErrDeadlineExceeded
	}
	var timer *time.Timer
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		timer = time.NewTimer(time.Until(deadline))
		timeout = timer.C
		defer timer.Stop()
	}
	select {
	case <-c.notify:
		return nil
	case <-c.session.done():
		return ErrExternalExecutionUnavailable
	case <-timeout:
		return os.ErrDeadlineExceeded
	}
}

func (c *executionConn) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	written := 0
	maxData := int(c.session.channels.Limits().MaxDataBytes)
	for written < len(data) {
		end := min(written+maxData, len(data))
		ctx, cancel, err := c.writeContext()
		if err != nil {
			return written, err
		}
		err = c.session.write(ctx, c, data[written:end])
		cancel()
		if err != nil {
			return written, err
		}
		written = end
	}
	return written, nil
}

func (c *executionConn) writeContext() (context.Context, func(), error) {
	c.stateMu.Lock()
	closed := c.localWriteDone || c.terminalErr != nil
	if closed {
		c.stateMu.Unlock()
		return nil, func() {}, net.ErrClosed
	}
	if !c.writeDeadline.IsZero() && !time.Now().Before(c.writeDeadline) {
		c.stateMu.Unlock()
		return nil, func() {}, os.ErrDeadlineExceeded
	}
	ctx, cancel := context.WithCancelCause(c.session.ctx)
	c.writeOperation++
	operation := c.writeOperation
	c.writeCancel = cancel
	c.installWriteTimerLocked(cancel)
	c.stateMu.Unlock()
	cleanup := func() {
		c.stateMu.Lock()
		if c.writeOperation == operation {
			c.stopWriteTimerLocked()
			c.writeCancel = nil
		}
		c.stateMu.Unlock()
		cancel(context.Canceled)
	}
	return ctx, cleanup, nil
}

func (c *executionConn) installWriteTimerLocked(cancel context.CancelCauseFunc) {
	c.stopWriteTimerLocked()
	if c.writeDeadline.IsZero() || cancel == nil {
		return
	}
	remaining := time.Until(c.writeDeadline)
	if remaining <= 0 {
		cancel(os.ErrDeadlineExceeded)
		return
	}
	c.writeTimerGen++
	generation := c.writeTimerGen
	operation := c.writeOperation
	c.writeTimer = time.AfterFunc(remaining, func() {
		c.expireWriteDeadline(operation, generation, cancel)
	})
}

func (c *executionConn) stopWriteTimerLocked() {
	// Increment even when Timer.Stop reports success: a callback can already be
	// runnable, and its generation must not cancel a rescheduled/current write.
	c.writeTimerGen++
	if c.writeTimer != nil {
		c.writeTimer.Stop()
		c.writeTimer = nil
	}
}

func (c *executionConn) expireWriteDeadline(operation, generation uint64, cancel context.CancelCauseFunc) {
	c.stateMu.Lock()
	if c.writeOperation != operation || c.writeTimerGen != generation || c.writeCancel == nil {
		c.stateMu.Unlock()
		return
	}
	c.writeTimer = nil
	// Invalidate duplicate/runnable callbacks before invoking cancellation.
	c.writeTimerGen++
	// Cancel under stateMu so a concurrent deadline extension either wins the
	// generation check first or observes an already-expired operation. Unlocking
	// between the check and cancel would let a stopped old timer cancel the newly
	// rescheduled write.
	cancel(os.ErrDeadlineExceeded)
	c.stateMu.Unlock()
}

// CloseWrite sends the byte-stream half-close used by gRPC transports.
func (c *executionConn) CloseWrite() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	ctx, cancel, err := c.writeContext()
	if err != nil {
		return err
	}
	defer cancel()
	return c.session.halfClose(ctx, c)
}

func (c *executionConn) Close() error {
	var closeErr error
	c.close.Do(func() {
		c.fail(net.ErrClosed)
		closeErr = c.session.resetBestEffort(c)
	})
	return closeErr
}

func (c *executionConn) LocalAddr() net.Addr  { return executionAddr("ateapi") }
func (c *executionConn) RemoteAddr() net.Addr { return executionAddr("external-slot") }

func (c *executionConn) SetDeadline(deadline time.Time) error {
	c.stateMu.Lock()
	c.readDeadline = deadline
	c.writeDeadline = deadline
	c.installWriteTimerLocked(c.writeCancel)
	c.stateMu.Unlock()
	c.signal()
	return nil
}

func (c *executionConn) SetReadDeadline(deadline time.Time) error {
	c.stateMu.Lock()
	c.readDeadline = deadline
	c.stateMu.Unlock()
	c.signal()
	return nil
}

func (c *executionConn) SetWriteDeadline(deadline time.Time) error {
	c.stateMu.Lock()
	c.writeDeadline = deadline
	c.installWriteTimerLocked(c.writeCancel)
	c.stateMu.Unlock()
	return nil
}

type executionAddr string

func (executionAddr) Network() string  { return "external-execution" }
func (a executionAddr) String() string { return string(a) }
