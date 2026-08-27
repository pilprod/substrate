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
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Broker implements authentication and the external session stream. The ateapi
// binary registers it only on the dedicated, explicitly enabled TLS listener.
type Broker struct {
	externalproviderpb.UnimplementedExternalProviderBrokerServer
	store          ExternalProviderStore
	random         io.Reader
	sessionTTL     time.Duration
	sessionRuntime *SessionRuntime
}

var _ externalproviderpb.ExternalProviderBrokerServer = (*Broker)(nil)

// BrokerOption configures an optional Broker authority.
type BrokerOption func(*Broker) error

// WithSessionRuntime enables authenticated Connect handling with the supplied
// process-local route and Worker lifecycle authority.
func WithSessionRuntime(runtime *SessionRuntime) BrokerOption {
	return func(broker *Broker) error {
		if runtime == nil || runtime.coordinator == nil {
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
	broker := &Broker{store: store, random: random, sessionTTL: sessionTTL}
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
// the route becomes visible and Workers become ACTIVE.
func (b *Broker) Connect(stream grpc.BidiStreamingServer[externalproviderpb.ClientFrame, externalproviderpb.ServerFrame]) error {
	if b == nil || b.store == nil || b.sessionRuntime == nil || b.sessionRuntime.coordinator == nil || stream == nil || stream.Context() == nil {
		return status.Error(codes.FailedPrecondition, "session runtime is unavailable")
	}
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return connectReceiveError(ctx, err, true)
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
	claim, err := b.store.ClaimExternalProviderSession(ctx, hello.registrationUID, sessionDigest)
	if err != nil {
		if errors.Is(err, ErrAuthenticationFailed) {
			return unauthenticated()
		}
		return status.Error(codes.Unavailable, "session authentication is unavailable")
	}

	session, err := b.sessionRuntime.coordinator.establish(ctx, claim, hello, func(_ context.Context, frame *externalproviderpb.ServerFrame) error {
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
		if err := session.close(cleanupCtx); err != nil {
			returnedErr = status.Error(codes.Unavailable, "session cleanup failed")
		}
	}()

	channels := session.channelState()
	if channels == nil {
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
			effect, err := channels.ApplyClientFrame(result.frame)
			if err != nil {
				return status.Error(codes.InvalidArgument, "invalid session frame")
			}
			if err := applyConnectEffect(stream, effect); err != nil {
				return err
			}
		}
	}
}

func applyConnectEffect(
	stream grpc.BidiStreamingServer[externalproviderpb.ClientFrame, externalproviderpb.ServerFrame],
	effect SessionEffect,
) error {
	switch effect := effect.(type) {
	case *SendServerFrameEffect:
		frame := effect.Frame()
		if frame == nil {
			return status.Error(codes.Internal, "session effect is invalid")
		}
		if err := stream.Send(frame); err != nil {
			return status.Error(codes.Unavailable, "session transport failed")
		}
		return nil
	case *ServerHeartbeatAckEffect:
		return nil
	case *ClientOpenEffect, *ServerOpenAckEffect, *ClientDataEffect, *ClientHalfCloseEffect, *ClientResetEffect:
		return status.Error(codes.FailedPrecondition, "session channel forwarding is unavailable")
	default:
		return status.Error(codes.Internal, "session effect is unsupported")
	}
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
