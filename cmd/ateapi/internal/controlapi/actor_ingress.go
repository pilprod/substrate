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

package controlapi

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	maxActorIngressDataBytes      = 64 << 10
	actorIngressLeaseWait         = 30 * time.Second
	actorIngressLeaseRetryMinimum = 10 * time.Millisecond
	actorIngressLeaseRetryMaximum = 500 * time.Millisecond
)

var (
	// ErrActorIngressUnavailable reports that the provider-aware data plane is
	// not bound or cannot prove an exact live ExternalSlot route.
	ErrActorIngressUnavailable = errors.New("actor ingress is unavailable")

	// ErrActorIngressAlreadyBound reports an unsafe second authority binding.
	ErrActorIngressAlreadyBound = errors.New("actor ingress is already bound")
)

// ActorIngressByteDialer opens one authenticated, generation-fenced provider
// stream for an exact ExternalSlot Worker assignment.
type ActorIngressByteDialer interface {
	DialContext(context.Context, *ateapipb.WorkerAssignment) (net.Conn, error)
}

type actorIngressStore interface {
	GetActor(context.Context, resources.ActorRef) (*ateapipb.Actor, error)
	GetWorker(context.Context, string) (*ateapipb.Worker, error)
	AcquireLease(context.Context, string) (*store.Lease, error)
}

type actorIngressStream interface {
	Context() context.Context
	Recv() (*ateapipb.ActorIngressFrame, error)
	Send(*ateapipb.ActorIngressFrame) error
}

type actorIngressCloseWriter interface {
	CloseWrite() error
}

type actorIngressReceiveResult struct {
	frame *ateapipb.ActorIngressFrame
	err   error
}

type actorIngressReadResult struct {
	data []byte
	err  error
}

// BindActorIngress installs the sole provider-aware Actor ingress authority.
// It must complete before the Control listener starts accepting requests.
func (s *RPCService) BindActorIngress(dialer ActorIngressByteDialer) error {
	if s == nil || dialer == nil {
		return ErrActorIngressUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actorIngressDialer != nil {
		return ErrActorIngressAlreadyBound
	}
	s.actorIngressDialer = dialer
	return nil
}

// OpenActorIngress implements the authenticated Control stream. Authentication
// is performed by the Control server's stream interceptor before this handler.
func (s *RPCService) OpenActorIngress(stream grpc.BidiStreamingServer[ateapipb.ActorIngressFrame, ateapipb.ActorIngressFrame]) error {
	return s.serveActorIngress(stream)
}

func (s *RPCService) serveActorIngress(stream actorIngressStream) error {
	if s == nil || stream == nil || stream.Context() == nil {
		return status.Error(codes.FailedPrecondition, "Actor ingress is unavailable")
	}
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return actorIngressReceiveStatus(ctx, err, true)
	}
	open, err := validateActorIngressOpen(first)
	if err != nil {
		return err
	}
	actorRef := resources.ActorRefFromObjectRef(open.GetActor())
	lease, err := s.acquireActorIngressLease(ctx, actorRef)
	if err != nil {
		return err
	}
	defer func() {
		if lease != nil {
			lease.Close()
		}
	}()

	assignment, err := s.resolveActorIngressAssignment(lease.Context(), actorRef, open.GetActorUid())
	if err != nil {
		return err
	}
	dialer := s.boundActorIngressDialer()
	if dialer == nil {
		return status.Error(codes.FailedPrecondition, "Actor ingress is unavailable")
	}
	connection, err := dialer.DialContext(lease.Context(), proto.Clone(assignment).(*ateapipb.WorkerAssignment))
	if err != nil {
		return status.Error(codes.Unavailable, "Actor ingress route is unavailable")
	}
	defer connection.Close()
	// The lifecycle lease fences target resolution and the one-shot provider
	// channel open. Once DialContext returns, the byte stream is permanently
	// bound to that Worker assignment and provider generation; it cannot
	// reconnect to a later Actor. Release here so concurrent public RPCs can
	// open independent streams while lifecycle operations remain serialized
	// against every not-yet-bound ingress.
	lease.Close()
	lease = nil

	if err := stream.Send(&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_Opened{Opened: &ateapipb.ActorIngressOpened{}}}); err != nil {
		return actorIngressSendStatus(ctx)
	}
	return bridgeActorIngress(ctx, stream, connection)
}

func (s *RPCService) boundActorIngressDialer() ActorIngressByteDialer {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.actorIngressDialer
}

func (s *RPCService) acquireActorIngressLease(ctx context.Context, actorRef resources.ActorRef) (*store.Lease, error) {
	if s.actorIngressStore == nil {
		return nil, status.Error(codes.FailedPrecondition, "Actor ingress is unavailable")
	}
	deadline := time.Now().Add(actorIngressLeaseWait)
	retryDelay := actorIngressLeaseRetryMinimum
	leaseKey := "lease:actor:" + actorRef.Atespace + ":" + actorRef.Name
	for {
		lease, err := s.actorIngressStore.AcquireLease(ctx, leaseKey)
		if err == nil {
			if lease == nil || lease.Context() == nil {
				return nil, status.Error(codes.Unavailable, "Actor ingress authorization is unavailable")
			}
			return lease, nil
		}
		if !errors.Is(err, store.ErrLeaseConflict) {
			return nil, status.Error(codes.Unavailable, "Actor ingress authorization is unavailable")
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, status.Error(codes.Aborted, "Another operation is in progress for this Actor")
		}
		wait := min(retryDelay, remaining)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, actorIngressContextStatus(ctx)
		case <-timer.C:
		}
		retryDelay = min(retryDelay*2, actorIngressLeaseRetryMaximum)
	}
}

func validateActorIngressOpen(frame *ateapipb.ActorIngressFrame) (*ateapipb.ActorIngressOpen, error) {
	if frame == nil || len(validateNoUnknownFields(frame, field.NewPath("frame"))) != 0 {
		return nil, status.Error(codes.InvalidArgument, "Invalid Actor ingress open frame")
	}
	member, ok := frame.GetFrame().(*ateapipb.ActorIngressFrame_Open)
	if !ok || member.Open == nil || member.Open.GetActor() == nil {
		return nil, status.Error(codes.InvalidArgument, "First Actor ingress frame must contain open")
	}
	actor := member.Open.GetActor()
	if !resources.IsValidResourceName(actor.GetAtespace()) || !resources.IsValidResourceName(actor.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "Actor ingress reference is invalid")
	}
	parsedUID, err := uuid.Parse(member.Open.GetActorUid())
	if err != nil || parsedUID.String() != member.Open.GetActorUid() {
		return nil, status.Error(codes.InvalidArgument, "Actor ingress UID is invalid")
	}
	return member.Open, nil
}

func (s *RPCService) resolveActorIngressAssignment(ctx context.Context, actorRef resources.ActorRef, expectedUID string) (*ateapipb.WorkerAssignment, error) {
	actor, err := s.actorIngressStore.GetActor(ctx, actorRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "Actor not found")
	}
	if err != nil || actor == nil {
		return nil, status.Error(codes.Unavailable, "Actor ingress authorization is unavailable")
	}
	if actor.GetMetadata().GetUid() != expectedUID {
		return nil, status.Error(codes.FailedPrecondition, "Actor incarnation changed")
	}
	if actor.GetMetadata().GetAtespace() != actorRef.Atespace || actor.GetMetadata().GetName() != actorRef.Name {
		return nil, status.Error(codes.FailedPrecondition, "Actor identity changed")
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return nil, status.Error(codes.FailedPrecondition, "Actor is not running")
	}
	assignment := actor.GetStatus().GetWorkerAssignment()
	if !isExternalActorIngressAssignment(assignment) {
		return nil, status.Error(codes.FailedPrecondition, "Actor is not assigned to an ExternalSlot Worker")
	}

	worker, err := s.actorIngressStore.GetWorker(ctx, assignment.GetWorker().GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.FailedPrecondition, "Actor Worker is unavailable")
	}
	if err != nil || worker == nil {
		return nil, status.Error(codes.Unavailable, "Actor ingress authorization is unavailable")
	}
	if !workerMatchesActorIngress(actor, worker, assignment) {
		return nil, status.Error(codes.FailedPrecondition, "Actor Worker assignment changed")
	}
	return proto.Clone(assignment).(*ateapipb.WorkerAssignment), nil
}

func isExternalActorIngressAssignment(assignment *ateapipb.WorkerAssignment) bool {
	if assignment == nil || assignment.GetProvider() != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT ||
		assignment.GetWorker() == nil || assignment.GetWorker().GetAtespace() != "" ||
		!resources.IsValidResourceName(assignment.GetWorker().GetName()) || assignment.GetWorkerResourceUid() == "" ||
		len(content.IsDNS1123Label(assignment.GetWorkerNamespace())) != 0 ||
		len(content.IsDNS1123Subdomain(assignment.GetWorkerPool())) != 0 ||
		assignment.GetWorkerPod() != "" || assignment.GetWorkerPodUid() != "" || assignment.GetWorkerPodIp() != "" ||
		assignment.GetExternalSlot() == nil ||
		!externalprovider.IsValidIdentity(assignment.GetExternalSlot().GetExecutionIdentity()) ||
		!externalprovider.IsValidIdentity(assignment.GetExternalSlot().GetLocalityIdentity()) ||
		!resources.IsValidResourceName(assignment.GetExternalSlot().GetOwnerAtespace()) {
		return false
	}
	parsedUID, err := uuid.Parse(assignment.GetWorkerResourceUid())
	return err == nil && parsedUID.String() == assignment.GetWorkerResourceUid()
}

func workerMatchesActorIngress(actor *ateapipb.Actor, worker *ateapipb.Worker, assignment *ateapipb.WorkerAssignment) bool {
	boundActor := worker.GetStatus().GetAssignment()
	return actor != nil && worker != nil && assignment != nil &&
		worker.GetProvider() == ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT &&
		worker.GetMetadata().GetAtespace() == "" &&
		worker.GetMetadata().GetName() == assignment.GetWorker().GetName() &&
		worker.GetMetadata().GetUid() == assignment.GetWorkerResourceUid() &&
		worker.GetWorkerNamespace() == assignment.GetWorkerNamespace() &&
		worker.GetWorkerPool() == assignment.GetWorkerPool() &&
		worker.GetWorkerPod() == "" && worker.GetWorkerPodUid() == "" && worker.GetNodeName() == "" && worker.GetIp() == "" &&
		proto.Equal(worker.GetExternalSlot(), assignment.GetExternalSlot()) &&
		worker.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_ACTIVE &&
		boundActor != nil && boundActor.GetActor() != nil &&
		boundActor.GetActor().GetAtespace() == actor.GetMetadata().GetAtespace() &&
		boundActor.GetActor().GetName() == actor.GetMetadata().GetName() &&
		boundActor.GetActorUid() == actor.GetMetadata().GetUid()
}

func bridgeActorIngress(ctx context.Context, stream actorIngressStream, connection net.Conn) error {
	if ctx == nil || stream == nil || connection == nil {
		return status.Error(codes.FailedPrecondition, "Actor ingress is unavailable")
	}
	closeWriter, ok := connection.(actorIngressCloseWriter)
	if !ok {
		return status.Error(codes.FailedPrecondition, "Actor ingress transport does not support half-close")
	}

	pumpCtx, stopPumps := context.WithCancel(ctx)
	defer stopPumps()
	closeOnCancellationDone := make(chan struct{})
	defer close(closeOnCancellationDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.Close()
		case <-closeOnCancellationDone:
		}
	}()
	received := make(chan actorIngressReceiveResult, 1)
	read := make(chan actorIngressReadResult, 1)
	go receiveActorIngressFrames(pumpCtx, stream, received)
	go readActorIngressBytes(pumpCtx, connection, read)

	clientHalfClosed := false
	serverHalfClosed := false
	for {
		select {
		case <-ctx.Done():
			return actorIngressContextStatus(ctx)
		case result := <-received:
			if result.err != nil {
				if errors.Is(result.err, io.EOF) && clientHalfClosed {
					received = nil
					if serverHalfClosed {
						return nil
					}
					continue
				}
				_ = sendActorIngressReset(stream)
				return actorIngressReceiveStatus(ctx, result.err, false)
			}
			if err := applyActorIngressClientFrame(result.frame, connection, closeWriter, &clientHalfClosed); err != nil {
				_ = sendActorIngressReset(stream)
				return err
			}
			if result.frame.GetReset_() != nil || clientHalfClosed && serverHalfClosed {
				return nil
			}
		case result := <-read:
			switch {
			case result.err == nil:
				if len(result.data) == 0 || len(result.data) > maxActorIngressDataBytes {
					_ = sendActorIngressReset(stream)
					return status.Error(codes.Unavailable, "Actor ingress transport failed")
				}
				if err := stream.Send(&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_Data{Data: slices.Clone(result.data)}}); err != nil {
					return actorIngressSendStatus(ctx)
				}
			case errors.Is(result.err, io.EOF):
				if serverHalfClosed {
					_ = sendActorIngressReset(stream)
					return status.Error(codes.Unavailable, "Actor ingress transport failed")
				}
				serverHalfClosed = true
				read = nil
				if err := stream.Send(&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_HalfClose{HalfClose: &ateapipb.ActorIngressHalfClose{}}}); err != nil {
					return actorIngressSendStatus(ctx)
				}
				if clientHalfClosed {
					return nil
				}
			default:
				if err := sendActorIngressReset(stream); err != nil {
					return actorIngressSendStatus(ctx)
				}
				return nil
			}
		}
	}
}

func receiveActorIngressFrames(ctx context.Context, stream actorIngressStream, out chan<- actorIngressReceiveResult) {
	for {
		frame, err := stream.Recv()
		select {
		case out <- actorIngressReceiveResult{frame: frame, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func readActorIngressBytes(ctx context.Context, connection net.Conn, out chan<- actorIngressReadResult) {
	buffer := make([]byte, maxActorIngressDataBytes)
	for {
		count, err := connection.Read(buffer)
		if count > 0 {
			select {
			case out <- actorIngressReadResult{data: slices.Clone(buffer[:count])}:
			case <-ctx.Done():
				return
			}
		}
		if err != nil {
			select {
			case out <- actorIngressReadResult{err: err}:
			case <-ctx.Done():
			}
			return
		}
		if count == 0 {
			select {
			case out <- actorIngressReadResult{}:
			case <-ctx.Done():
			}
			return
		}
	}
}

func applyActorIngressClientFrame(frame *ateapipb.ActorIngressFrame, connection net.Conn, closeWriter actorIngressCloseWriter, halfClosed *bool) error {
	if frame == nil || len(validateNoUnknownFields(frame, field.NewPath("frame"))) != 0 {
		return status.Error(codes.InvalidArgument, "Invalid Actor ingress frame")
	}
	switch member := frame.GetFrame().(type) {
	case *ateapipb.ActorIngressFrame_Data:
		if *halfClosed || len(member.Data) == 0 || len(member.Data) > maxActorIngressDataBytes {
			return status.Error(codes.InvalidArgument, "Invalid Actor ingress data frame")
		}
		if err := writeAllActorIngress(connection, member.Data); err != nil {
			return status.Error(codes.Unavailable, "Actor ingress transport failed")
		}
		return nil
	case *ateapipb.ActorIngressFrame_HalfClose:
		if member.HalfClose == nil || *halfClosed {
			return status.Error(codes.InvalidArgument, "Invalid Actor ingress half-close")
		}
		if err := closeWriter.CloseWrite(); err != nil {
			return status.Error(codes.Unavailable, "Actor ingress transport failed")
		}
		*halfClosed = true
		return nil
	case *ateapipb.ActorIngressFrame_Reset_:
		if member.Reset_ == nil {
			return status.Error(codes.InvalidArgument, "Invalid Actor ingress reset")
		}
		return nil
	default:
		return status.Error(codes.InvalidArgument, "Actor ingress frame is out of order")
	}
}

func writeAllActorIngress(connection net.Conn, data []byte) error {
	for len(data) > 0 {
		count, err := connection.Write(data)
		if err != nil {
			return err
		}
		if count <= 0 || count > len(data) {
			return io.ErrUnexpectedEOF
		}
		data = data[count:]
	}
	return nil
}

func sendActorIngressReset(stream actorIngressStream) error {
	if stream == nil {
		return ErrActorIngressUnavailable
	}
	return stream.Send(&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_Reset_{Reset_: &ateapipb.ActorIngressReset{}}})
}

func actorIngressReceiveStatus(ctx context.Context, err error, first bool) error {
	if ctx != nil && ctx.Err() != nil {
		return actorIngressContextStatus(ctx)
	}
	if errors.Is(err, io.EOF) {
		if first {
			return status.Error(codes.InvalidArgument, "First Actor ingress frame must contain open")
		}
		return status.Error(codes.InvalidArgument, "Actor ingress stream ended before half-close")
	}
	return status.Error(codes.Unavailable, "Actor ingress stream failed")
}

func actorIngressSendStatus(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return actorIngressContextStatus(ctx)
	}
	return status.Error(codes.Unavailable, "Actor ingress stream failed")
}

func actorIngressContextStatus(ctx context.Context) error {
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "Actor ingress deadline exceeded")
	}
	return status.Error(codes.Canceled, "Actor ingress was canceled")
}
