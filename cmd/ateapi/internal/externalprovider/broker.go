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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMaxPendingConnectHandshakes uint32 = 64
	maximumPendingConnectHandshakes    uint32 = 1024
	defaultConnectHandshakeTimeout            = 15 * time.Second
	maximumConnectHandshakeTimeout            = time.Minute
)

// Broker implements authentication and the external session stream. The ateapi
// binary registers it only on the dedicated, explicitly enabled TLS listener.
type Broker struct {
	externalproviderpb.UnimplementedExternalProviderBrokerServer
	store          ExternalProviderStore
	random         io.Reader
	sessionTTL     time.Duration
	sessionRuntime *SessionRuntime

	handshakeTimeout    time.Duration
	pendingHandshakes   chan struct{}
	handshakeConfigured bool
}

var _ externalproviderpb.ExternalProviderBrokerServer = (*Broker)(nil)

// BrokerOption configures an optional Broker authority.
type BrokerOption func(*Broker) error

// ConnectHandshakeLimits bounds streams which have not yet presented and
// atomically claimed a valid first Hello.
type ConnectHandshakeLimits struct {
	MaxPending uint32
	Timeout    time.Duration
}

// WithConnectHandshakeLimits overrides the conservative pending-handshake
// bounds. It may be supplied at most once.
func WithConnectHandshakeLimits(limits ConnectHandshakeLimits) BrokerOption {
	return func(broker *Broker) error {
		if broker.handshakeConfigured {
			return errors.New("external provider Connect handshake limits are already configured")
		}
		if limits.MaxPending == 0 || limits.MaxPending > maximumPendingConnectHandshakes ||
			limits.Timeout < time.Millisecond || limits.Timeout > maximumConnectHandshakeTimeout {
			return errors.New("external provider Connect handshake limits are invalid")
		}
		broker.pendingHandshakes = make(chan struct{}, limits.MaxPending)
		broker.handshakeTimeout = limits.Timeout
		broker.handshakeConfigured = true
		return nil
	}
}

// WithSessionRuntime enables authenticated Connect handling with the supplied
// process-local route and Worker lifecycle authority.
func WithSessionRuntime(runtime *SessionRuntime) BrokerOption {
	return func(broker *Broker) error {
		if runtime == nil || runtime.coordinator == nil || runtime.claimInstallGate == nil {
			return errors.New("external provider session runtime is not configured")
		}
		if broker.sessionRuntime != nil {
			return errors.New("external provider session runtime is already configured")
		}
		broker.sessionRuntime = runtime
		return nil
	}
}

// NewBroker creates a broker service. Connect fails closed unless exactly one
// SessionRuntime is supplied.
func NewBroker(store ExternalProviderStore, sessionTTL time.Duration, opts ...BrokerOption) (*Broker, error) {
	return newBroker(store, rand.Reader, sessionTTL, opts...)
}

func newBroker(store ExternalProviderStore, random io.Reader, sessionTTL time.Duration, opts ...BrokerOption) (*Broker, error) {
	if store == nil || random == nil {
		return nil, errors.New("external provider broker is not configured")
	}
	if err := validateTTL(sessionTTL, MaxSessionTTL); err != nil {
		return nil, fmt.Errorf("session TTL: %w", err)
	}
	broker := &Broker{
		store:             store,
		random:            random,
		sessionTTL:        sessionTTL,
		handshakeTimeout:  defaultConnectHandshakeTimeout,
		pendingHandshakes: make(chan struct{}, defaultMaxPendingConnectHandshakes),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(broker); err != nil {
			return nil, fmt.Errorf("external provider broker option: %w", err)
		}
	}
	return broker, nil
}

// Enroll consumes one out-of-band credential and returns one refresh
// credential. Neither credential is accepted in the request payload.
func (b *Broker) Enroll(ctx context.Context, req *externalproviderpb.EnrollRequest) (*externalproviderpb.EnrollResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	enrollmentCredential, err := bearerCredential(ctx)
	if err != nil {
		return nil, unauthenticated()
	}
	enrollmentDigest := digestCredential(enrollmentDigestDomain, enrollmentCredential)
	clear(enrollmentCredential)

	for range credentialGenerationAttempts {
		refreshCredential, err := generateCredential(b.random)
		if err != nil {
			return nil, status.Error(codes.Internal, "credential generation failed")
		}
		registrationUUID, err := uuid.NewRandomFromReader(b.random)
		if err != nil {
			clear(refreshCredential)
			return nil, status.Error(codes.Internal, "identity generation failed")
		}
		registration, err := b.store.ConsumeExternalProviderEnrollment(
			ctx,
			enrollmentDigest,
			registrationUUID.String(),
			digestCredential(refreshDigestDomain, refreshCredential),
		)
		if err == nil {
			return &externalproviderpb.EnrollResponse{
				RegistrationUid:   registration.UID,
				RefreshCredential: refreshCredential,
				SlotPolicy:        registration.Scope.SlotPolicy.Proto(),
			}, nil
		}
		clear(refreshCredential)
		if errors.Is(err, ErrAuthenticationFailed) {
			return nil, unauthenticated()
		}
		if !errors.Is(err, ErrCredentialCollision) {
			return nil, status.Error(codes.Internal, "enrollment failed")
		}
	}
	return nil, status.Error(codes.Internal, "credential generation failed")
}

// MintSessionToken authenticates the registration's durable refresh
// credential and atomically replaces its one current session token.
func (b *Broker) MintSessionToken(ctx context.Context, req *externalproviderpb.MintSessionTokenRequest) (*externalproviderpb.MintSessionTokenResponse, error) {
	if req == nil || !IsValidIdentity(req.GetRegistrationUid()) {
		return nil, status.Error(codes.InvalidArgument, "registration_uid is invalid")
	}
	refreshCredential, err := bearerCredential(ctx)
	if err != nil {
		return nil, unauthenticated()
	}
	refreshDigest := digestCredential(refreshDigestDomain, refreshCredential)
	clear(refreshCredential)

	for range credentialGenerationAttempts {
		sessionCredential, err := generateCredential(b.random)
		if err != nil {
			return nil, status.Error(codes.Internal, "credential generation failed")
		}
		authorization, err := b.store.RotateExternalProviderSession(
			ctx,
			req.GetRegistrationUid(),
			refreshDigest,
			digestCredential(sessionDigestDomain, sessionCredential),
			b.sessionTTL,
		)
		if err == nil {
			return &externalproviderpb.MintSessionTokenResponse{
				SessionToken: sessionCredential,
				ExpiresAt:    timestamppb.New(authorization.ExpiresAt),
				SlotPolicy:   authorization.Registration.Scope.SlotPolicy.Proto(),
			}, nil
		}
		clear(sessionCredential)
		if errors.Is(err, ErrAuthenticationFailed) {
			return nil, unauthenticated()
		}
		if !errors.Is(err, ErrCredentialCollision) {
			return nil, status.Error(codes.Internal, "session token issuance failed")
		}
	}
	return nil, status.Error(codes.Internal, "credential generation failed")
}

// Connect receives and prevalidates Hello before atomically consuming the
// session credential. Ready is sent synchronously by the coordinator before
// the route becomes visible and, for a forwarding-bound coordinator, Workers
// become ACTIVE.
func (b *Broker) Connect(stream grpc.BidiStreamingServer[externalproviderpb.ClientFrame, externalproviderpb.ServerFrame]) error {
	if b == nil || b.store == nil || b.sessionRuntime == nil || b.sessionRuntime.coordinator == nil || b.sessionRuntime.claimInstallGate == nil || stream == nil || stream.Context() == nil {
		return status.Error(codes.FailedPrecondition, "session runtime is unavailable")
	}
	ctx := stream.Context()
	handshake, acquired := b.acquireConnectHandshake()
	if !acquired {
		return status.Error(codes.ResourceExhausted, "too many pending Connect handshakes")
	}
	releaseHandshakeOnReturn := true
	defer func() {
		if releaseHandshakeOnReturn {
			handshake.release()
		}
	}()
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, b.handshakeTimeout)
	defer cancelHandshake()
	firstResult := make(chan connectReceiveResult, 1)
	firstReceiveDone := make(chan struct{})
	go func() {
		defer close(firstReceiveDone)
		frame, err := stream.Recv()
		firstResult <- connectReceiveResult{frame: frame, err: err}
	}()
	var first *externalproviderpb.ClientFrame
	select {
	case result := <-firstResult:
		if result.err != nil {
			return connectReceiveError(ctx, result.err, true)
		}
		first = result.frame
	case <-handshakeCtx.Done():
		// A timed-out Recv may unblock only after this handler returns and gRPC
		// cancels its stream. Retain its admission slot until that goroutine exits,
		// so stalled peers can never create more than the configured bound.
		select {
		case <-firstReceiveDone:
		default:
			releaseHandshakeOnReturn = false
			go func() {
				<-firstReceiveDone
				handshake.release()
			}()
		}
		if ctx.Err() != nil {
			return connectContextError(ctx)
		}
		return status.Error(codes.DeadlineExceeded, "Connect handshake deadline exceeded")
	}
	hello, err := prevalidateConnectHello(first)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid Connect hello")
	}

	sessionCredential, err := bearerCredential(ctx)
	if err != nil {
		return unauthenticated()
	}
	sessionDigest := digestCredential(sessionDigestDomain, sessionCredential)
	clear(sessionCredential)
	claimGate, err := b.sessionRuntime.claimInstallGate.acquire(handshakeCtx, hello.registrationUID)
	if err != nil {
		if ctx.Err() != nil {
			return connectContextError(ctx)
		}
		if handshakeCtx.Err() != nil {
			return status.Error(codes.DeadlineExceeded, "Connect handshake deadline exceeded")
		}
		return status.Error(codes.ResourceExhausted, "session admission is busy")
	}
	defer claimGate.release()
	gatedClaim, err := claimGate.claimSession(handshakeCtx, b.store, sessionDigest)
	if err != nil {
		if ctx.Err() != nil {
			return connectContextError(ctx)
		}
		if handshakeCtx.Err() != nil {
			return status.Error(codes.DeadlineExceeded, "Connect handshake deadline exceeded")
		}
		if errors.Is(err, ErrAuthenticationFailed) {
			return unauthenticated()
		}
		return status.Error(codes.Unavailable, "session authentication is unavailable")
	}
	handshake.release()
	releaseHandshakeOnReturn = false
	cancelHandshake()

	session, err := b.sessionRuntime.coordinator.establish(ctx, gatedClaim, hello, func(_ context.Context, frame *externalproviderpb.ServerFrame) error {
		return stream.Send(frame)
	})
	if err != nil {
		if ctx.Err() != nil {
			return connectContextError(ctx)
		}
		return status.Error(codes.Unavailable, "session establishment failed")
	}
	return runConnectedSession(stream, session)
}

type connectReceiveResult struct {
	frame *externalproviderpb.ClientFrame
	err   error
}

// runConnectedSession keeps at most one received frame waiting for ordered
// state-machine application. The receiver is isolated so route replacement
// can terminate a stream even while the peer is idle.
func runConnectedSession(
	stream grpc.BidiStreamingServer[externalproviderpb.ClientFrame, externalproviderpb.ServerFrame],
	session *coordinatedSession,
) (returnedErr error) {
	ctx := stream.Context()
	pumpCtx, stopPump := context.WithCancel(ctx)
	received := make(chan connectReceiveResult, 1)
	go func() {
		defer close(received)
		for {
			frame, err := stream.Recv()
			select {
			case received <- connectReceiveResult{frame: frame, err: err}:
			case <-pumpCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer stopPump()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := session.close(cleanupCtx); err != nil && returnedErr == nil {
			returnedErr = status.Error(codes.Unavailable, "session cleanup failed")
		}
	}()

	if session.channelState() == nil {
		return status.Error(codes.Internal, "session channel state is unavailable")
	}
	for {
		select {
		case <-ctx.Done():
			return connectContextError(ctx)
		case <-session.done():
			return status.Error(codes.Aborted, "session was replaced or closed")
		case result, open := <-received:
			if !open {
				return status.Error(codes.Unavailable, "session transport failed")
			}
			if result.err != nil {
				return connectReceiveError(ctx, result.err, false)
			}
			if err := session.applyClientFrame(result.frame); err != nil {
				if ctx.Err() != nil {
					return connectContextError(ctx)
				}
				if errors.Is(err, ErrChannelProtocolViolation) {
					return status.Error(codes.InvalidArgument, "invalid session frame")
				}
				if session.wire != nil && session.wire.ctx.Err() != nil {
					return status.Error(codes.Unavailable, "session transport failed")
				}
				return status.Error(codes.InvalidArgument, "invalid session frame")
			}
		}
	}
}

type connectHandshakeLease struct {
	slots chan struct{}
	once  sync.Once
}

func (b *Broker) acquireConnectHandshake() (*connectHandshakeLease, bool) {
	if b == nil || b.pendingHandshakes == nil {
		return nil, false
	}
	select {
	case b.pendingHandshakes <- struct{}{}:
		return &connectHandshakeLease{slots: b.pendingHandshakes}, true
	default:
		return nil, false
	}
}

func (l *connectHandshakeLease) release() {
	if l == nil || l.slots == nil {
		return
	}
	l.once.Do(func() { <-l.slots })
}

func connectReceiveError(ctx context.Context, err error, first bool) error {
	if ctx != nil && ctx.Err() != nil {
		return connectContextError(ctx)
	}
	if errors.Is(err, io.EOF) {
		if first {
			return status.Error(codes.InvalidArgument, "Connect hello is required")
		}
		return nil
	}
	return status.Error(codes.Unavailable, "session transport failed")
}

func connectContextError(ctx context.Context) error {
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "session deadline exceeded")
	}
	return status.Error(codes.Canceled, "session canceled")
}

func unauthenticated() error {
	return status.Error(codes.Unauthenticated, "authentication failed")
}
