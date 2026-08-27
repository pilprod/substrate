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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	testActorIngressUID  = "00000000-0000-4000-8000-000000000101"
	testWorkerIngressUID = "00000000-0000-4000-8000-000000000202"
)

type actorIngressTestStore struct {
	actor  *ateapipb.Actor
	worker *ateapipb.Worker

	actorErr  error
	workerErr error
	leaseErr  error
	leased    atomic.Bool
	released  atomic.Bool
	acquires  atomic.Int32
}

func (s *actorIngressTestStore) GetActor(context.Context, resources.ActorRef) (*ateapipb.Actor, error) {
	if s.actorErr != nil || s.actor == nil {
		return nil, s.actorErr
	}
	return proto.Clone(s.actor).(*ateapipb.Actor), nil
}

func (s *actorIngressTestStore) GetWorker(context.Context, string) (*ateapipb.Worker, error) {
	if s.workerErr != nil || s.worker == nil {
		return nil, s.workerErr
	}
	return proto.Clone(s.worker).(*ateapipb.Worker), nil
}

func (s *actorIngressTestStore) AcquireLease(ctx context.Context, _ string) (*store.Lease, error) {
	s.acquires.Add(1)
	if s.leaseErr != nil {
		return nil, s.leaseErr
	}
	if !s.leased.CompareAndSwap(false, true) {
		return nil, store.ErrLeaseConflict
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	return store.NewLease(leaseCtx, func() {
		s.released.Store(true)
		s.leased.Store(false)
		cancel()
	}), nil
}

type actorIngressTestDialer struct {
	connection net.Conn
	err        error

	mu         sync.Mutex
	assignment *ateapipb.WorkerAssignment
}

type actorIngressBlockingDialer struct {
	connections  []net.Conn
	firstEntered chan struct{}
	releaseFirst chan struct{}
	calls        atomic.Int32
}

func (d *actorIngressBlockingDialer) DialContext(_ context.Context, _ *ateapipb.WorkerAssignment) (net.Conn, error) {
	call := int(d.calls.Add(1)) - 1
	if call >= len(d.connections) {
		return nil, errors.New("unexpected extra dial")
	}
	if call == 0 {
		close(d.firstEntered)
		<-d.releaseFirst
	}
	return d.connections[call], nil
}

func (d *actorIngressTestDialer) DialContext(_ context.Context, assignment *ateapipb.WorkerAssignment) (net.Conn, error) {
	d.mu.Lock()
	d.assignment = proto.Clone(assignment).(*ateapipb.WorkerAssignment)
	d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
	return d.connection, nil
}

type actorIngressTestStream struct {
	ctx  context.Context
	recv chan *ateapipb.ActorIngressFrame

	mu   sync.Mutex
	sent []*ateapipb.ActorIngressFrame
}

func newActorIngressTestStream(frames ...*ateapipb.ActorIngressFrame) *actorIngressTestStream {
	recv := make(chan *ateapipb.ActorIngressFrame, len(frames))
	for _, frame := range frames {
		recv <- proto.Clone(frame).(*ateapipb.ActorIngressFrame)
	}
	close(recv)
	return &actorIngressTestStream{ctx: context.Background(), recv: recv}
}

func (s *actorIngressTestStream) Context() context.Context { return s.ctx }

func (s *actorIngressTestStream) Recv() (*ateapipb.ActorIngressFrame, error) {
	frame, ok := <-s.recv
	if !ok {
		return nil, io.EOF
	}
	return proto.Clone(frame).(*ateapipb.ActorIngressFrame), nil
}

func (s *actorIngressTestStream) Send(frame *ateapipb.ActorIngressFrame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, proto.Clone(frame).(*ateapipb.ActorIngressFrame))
	return nil
}

func (s *actorIngressTestStream) sentFrames() []*ateapipb.ActorIngressFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*ateapipb.ActorIngressFrame, 0, len(s.sent))
	for _, frame := range s.sent {
		result = append(result, proto.Clone(frame).(*ateapipb.ActorIngressFrame))
	}
	return result
}

type actorIngressTestRead struct {
	data []byte
	err  error
}

type actorIngressTestConn struct {
	reads  chan actorIngressTestRead
	writes chan []byte
	closed chan struct{}

	closeOnce      sync.Once
	closeWriteOnce sync.Once
	closeWrites    atomic.Int32
}

func newActorIngressTestConn(reads ...actorIngressTestRead) *actorIngressTestConn {
	readQueue := make(chan actorIngressTestRead, len(reads))
	for _, read := range reads {
		readQueue <- actorIngressTestRead{data: slices.Clone(read.data), err: read.err}
	}
	return &actorIngressTestConn{
		reads:  readQueue,
		writes: make(chan []byte, 16),
		closed: make(chan struct{}),
	}
}

func (c *actorIngressTestConn) Read(buffer []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	case result := <-c.reads:
		return copy(buffer, result.data), result.err
	}
}

func (c *actorIngressTestConn) Write(data []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	case c.writes <- slices.Clone(data):
		return len(data), nil
	}
}

func (c *actorIngressTestConn) CloseWrite() error {
	c.closeWriteOnce.Do(func() { c.closeWrites.Add(1) })
	return nil
}

func (c *actorIngressTestConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (*actorIngressTestConn) LocalAddr() net.Addr              { return actorIngressTestAddr("local") }
func (*actorIngressTestConn) RemoteAddr() net.Addr             { return actorIngressTestAddr("remote") }
func (*actorIngressTestConn) SetDeadline(time.Time) error      { return nil }
func (*actorIngressTestConn) SetReadDeadline(time.Time) error  { return nil }
func (*actorIngressTestConn) SetWriteDeadline(time.Time) error { return nil }

type actorIngressTestAddr string

func (actorIngressTestAddr) Network() string  { return "test" }
func (a actorIngressTestAddr) String() string { return string(a) }

func actorIngressTestResources() (*ateapipb.Actor, *ateapipb.Worker) {
	external := &ateapipb.ExternalSlotIdentity{
		ExecutionIdentity: "registration.slot.execution",
		LocalityIdentity:  "registration.locality",
		OwnerAtespace:     "team-a",
	}
	assignment := &ateapipb.WorkerAssignment{
		Worker:            &ateapipb.ObjectRef{Name: "external-worker"},
		WorkerNamespace:   "ate-workers",
		WorkerPool:        "external-pool",
		Provider:          ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
		ExternalSlot:      proto.Clone(external).(*ateapipb.ExternalSlotIdentity),
		WorkerResourceUid: testWorkerIngressUID,
	}
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "agent-one", Uid: testActorIngressUID, Version: 7},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: assignment,
		},
	}
	worker := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "external-worker", Uid: testWorkerIngressUID, Version: 9},
		WorkerNamespace: "ate-workers",
		WorkerPool:      "external-pool",
		Provider:        ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
		ExternalSlot:    external,
		Status: &ateapipb.WorkerStatus{
			State: ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			Assignment: &ateapipb.ActorAssignment{
				Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "agent-one"},
				ActorUid: testActorIngressUID,
			},
		},
	}
	return actor, worker
}

func actorIngressOpenFrame() *ateapipb.ActorIngressFrame {
	return &ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_Open{Open: &ateapipb.ActorIngressOpen{
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "agent-one"},
		ActorUid: testActorIngressUID,
	}}}
}

func TestValidateActorIngressOpen(t *testing.T) {
	unknown := actorIngressOpenFrame()
	unknown.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	tests := []struct {
		name  string
		frame *ateapipb.ActorIngressFrame
		code  codes.Code
	}{
		{name: "valid", frame: actorIngressOpenFrame(), code: codes.OK},
		{name: "nil", code: codes.InvalidArgument},
		{name: "data first", frame: &ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_Data{Data: []byte("x")}}, code: codes.InvalidArgument},
		{name: "missing open", frame: &ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_Open{}}, code: codes.InvalidArgument},
		{name: "invalid ref", frame: func() *ateapipb.ActorIngressFrame {
			frame := actorIngressOpenFrame()
			frame.GetOpen().Actor.Name = "Agent/One"
			return frame
		}(), code: codes.InvalidArgument},
		{name: "noncanonical uid", frame: func() *ateapipb.ActorIngressFrame {
			frame := actorIngressOpenFrame()
			frame.GetOpen().ActorUid = "ABCDEFAB-0000-4000-8000-000000000101"
			return frame
		}(), code: codes.InvalidArgument},
		{name: "unknown field", frame: unknown, code: codes.InvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			open, err := validateActorIngressOpen(test.frame)
			if got := status.Code(err); got != test.code {
				t.Fatalf("validateActorIngressOpen() code = %v, want %v (err %v)", got, test.code, err)
			}
			if test.code == codes.OK && open == nil {
				t.Fatal("validateActorIngressOpen() returned nil open")
			}
		})
	}
}

func TestResolveActorIngressAssignmentFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ateapipb.Actor, *ateapipb.Worker)
		uid    string
		code   codes.Code
	}{
		{name: "valid", code: codes.OK},
		{name: "actor uid changed", uid: "00000000-0000-4000-8000-000000000999", code: codes.FailedPrecondition},
		{name: "actor not running", mutate: func(actor *ateapipb.Actor, _ *ateapipb.Worker) {
			actor.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		}, code: codes.FailedPrecondition},
		{name: "Kubernetes assignment", mutate: func(actor *ateapipb.Actor, _ *ateapipb.Worker) {
			actor.Status.WorkerAssignment.Provider = ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD
		}, code: codes.FailedPrecondition},
		{name: "stale Worker uid", mutate: func(_ *ateapipb.Actor, worker *ateapipb.Worker) {
			worker.Metadata.Uid = "00000000-0000-4000-8000-000000000303"
		}, code: codes.FailedPrecondition},
		{name: "Worker offline", mutate: func(_ *ateapipb.Actor, worker *ateapipb.Worker) {
			worker.Status.State = ateapipb.WorkerState_WORKER_STATE_OFFLINE
		}, code: codes.FailedPrecondition},
		{name: "inverse assignment changed", mutate: func(_ *ateapipb.Actor, worker *ateapipb.Worker) {
			worker.Status.Assignment.ActorUid = "00000000-0000-4000-8000-000000000999"
		}, code: codes.FailedPrecondition},
		{name: "route identity changed", mutate: func(_ *ateapipb.Actor, worker *ateapipb.Worker) {
			worker.ExternalSlot.ExecutionIdentity = "different.execution"
		}, code: codes.FailedPrecondition},
		{name: "owner atespace changed", mutate: func(actor *ateapipb.Actor, worker *ateapipb.Worker) {
			actor.Status.WorkerAssignment.ExternalSlot.OwnerAtespace = "team-b"
			worker.ExternalSlot.OwnerAtespace = "team-b"
		}, code: codes.FailedPrecondition},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actor, worker := actorIngressTestResources()
			if test.mutate != nil {
				test.mutate(actor, worker)
			}
			uid := test.uid
			if uid == "" {
				uid = testActorIngressUID
			}
			service := &RPCService{actorIngressStore: &actorIngressTestStore{actor: actor, worker: worker}}
			assignment, err := service.resolveActorIngressAssignment(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "agent-one"}, uid)
			if got := status.Code(err); got != test.code {
				t.Fatalf("resolveActorIngressAssignment() code = %v, want %v (err %v)", got, test.code, err)
			}
			if test.code == codes.OK && !proto.Equal(assignment, actor.GetStatus().GetWorkerAssignment()) {
				t.Fatalf("assignment = %v, want %v", assignment, actor.GetStatus().GetWorkerAssignment())
			}
		})
	}
}

func TestServeActorIngressBridgesOrderedBytesAndHalfCloses(t *testing.T) {
	actor, worker := actorIngressTestResources()
	st := &actorIngressTestStore{actor: actor, worker: worker}
	connection := newActorIngressTestConn(
		actorIngressTestRead{data: []byte("provider-reply")},
		actorIngressTestRead{err: io.EOF},
	)
	dialer := &actorIngressTestDialer{connection: connection}
	service := &RPCService{actorIngressStore: st}
	if err := service.BindActorIngress(dialer); err != nil {
		t.Fatalf("BindActorIngress() error = %v", err)
	}
	stream := newActorIngressTestStream(
		actorIngressOpenFrame(),
		&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_Data{Data: []byte("client-request")}},
		&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_HalfClose{HalfClose: &ateapipb.ActorIngressHalfClose{}}},
	)
	if err := service.serveActorIngress(stream); err != nil {
		t.Fatalf("serveActorIngress() error = %v", err)
	}

	select {
	case written := <-connection.writes:
		if string(written) != "client-request" {
			t.Fatalf("provider write = %q, want client-request", written)
		}
	default:
		t.Fatal("client data was not written to the provider connection")
	}
	if connection.closeWrites.Load() != 1 {
		t.Fatalf("CloseWrite calls = %d, want 1", connection.closeWrites.Load())
	}
	frames := stream.sentFrames()
	if len(frames) != 3 || frames[0].GetOpened() == nil || string(frames[1].GetData()) != "provider-reply" || frames[2].GetHalfClose() == nil {
		t.Fatalf("server frames = %v, want Opened/Data/HalfClose", frames)
	}
	if !st.released.Load() {
		t.Fatal("Actor lifecycle lease was not released")
	}
	dialer.mu.Lock()
	gotAssignment := proto.Clone(dialer.assignment).(*ateapipb.WorkerAssignment)
	dialer.mu.Unlock()
	if !proto.Equal(gotAssignment, actor.GetStatus().GetWorkerAssignment()) {
		t.Fatalf("dial assignment = %v, want exact Actor assignment", gotAssignment)
	}
}

func TestServeActorIngressRejectsDataAfterHalfClose(t *testing.T) {
	actor, worker := actorIngressTestResources()
	connection := newActorIngressTestConn()
	service := &RPCService{actorIngressStore: &actorIngressTestStore{actor: actor, worker: worker}}
	if err := service.BindActorIngress(&actorIngressTestDialer{connection: connection}); err != nil {
		t.Fatalf("BindActorIngress() error = %v", err)
	}
	stream := newActorIngressTestStream(
		actorIngressOpenFrame(),
		&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_HalfClose{HalfClose: &ateapipb.ActorIngressHalfClose{}}},
		&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_Data{Data: []byte("forbidden")}},
	)
	err := service.serveActorIngress(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("serveActorIngress() code = %v, want InvalidArgument (err %v)", status.Code(err), err)
	}
	for _, frame := range stream.sentFrames() {
		if strings.Contains(frame.String(), "forbidden") {
			t.Fatal("rejected data leaked into a server frame")
		}
	}
}

func TestServeActorIngressDoesNotLeakDialerErrors(t *testing.T) {
	actor, worker := actorIngressTestResources()
	service := &RPCService{actorIngressStore: &actorIngressTestStore{actor: actor, worker: worker}}
	if err := service.BindActorIngress(&actorIngressTestDialer{err: errors.New("secret-provider-detail")}); err != nil {
		t.Fatalf("BindActorIngress() error = %v", err)
	}
	err := service.serveActorIngress(newActorIngressTestStream(actorIngressOpenFrame()))
	if status.Code(err) != codes.Unavailable || strings.Contains(err.Error(), "secret-provider-detail") {
		t.Fatalf("serveActorIngress() error = %v, want sanitized Unavailable", err)
	}
}

func TestServeActorIngressConcurrentStreamsWaitOnlyForOpenFence(t *testing.T) {
	actor, worker := actorIngressTestResources()
	st := &actorIngressTestStore{actor: actor, worker: worker}
	dialer := &actorIngressBlockingDialer{
		connections:  []net.Conn{newActorIngressTestConn(), newActorIngressTestConn()},
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	service := &RPCService{actorIngressStore: st}
	if err := service.BindActorIngress(dialer); err != nil {
		t.Fatalf("BindActorIngress() error = %v", err)
	}

	serve := func(result chan<- error) {
		result <- service.serveActorIngress(newActorIngressTestStream(
			actorIngressOpenFrame(),
			&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_Reset_{Reset_: &ateapipb.ActorIngressReset{}}},
		))
	}
	results := make(chan error, 2)
	go serve(results)
	select {
	case <-dialer.firstEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first ingress did not reach the provider Open fence")
	}
	go serve(results)

	deadline := time.Now().Add(5 * time.Second)
	for st.acquires.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if st.acquires.Load() < 2 {
		t.Fatal("second ingress did not contend on the short actor Open fence")
	}
	close(dialer.releaseFirst)
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("concurrent serveActorIngress() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Actor ingress did not complete")
		}
	}
	if dialer.calls.Load() != 2 {
		t.Fatalf("provider dial calls = %d, want 2 independent streams", dialer.calls.Load())
	}
}

func TestBridgeActorIngressProviderErrorSendsSanitizedReset(t *testing.T) {
	connection := newActorIngressTestConn(actorIngressTestRead{err: errors.New("secret-peer-error")})
	stream := newActorIngressTestStream(
		&ateapipb.ActorIngressFrame{Frame: &ateapipb.ActorIngressFrame_HalfClose{HalfClose: &ateapipb.ActorIngressHalfClose{}}},
	)
	if err := bridgeActorIngress(context.Background(), stream, connection); err != nil {
		t.Fatalf("bridgeActorIngress() error = %v", err)
	}
	frames := stream.sentFrames()
	if len(frames) != 1 || frames[0].GetReset_() == nil || strings.Contains(frames[0].String(), "secret-peer-error") {
		t.Fatalf("provider error frames = %v, want one sanitized Reset", frames)
	}
}

func TestBindActorIngressIsSingleUse(t *testing.T) {
	service := &RPCService{}
	if err := service.BindActorIngress(nil); !errors.Is(err, ErrActorIngressUnavailable) {
		t.Fatalf("BindActorIngress(nil) error = %v", err)
	}
	first := &actorIngressTestDialer{}
	if err := service.BindActorIngress(first); err != nil {
		t.Fatalf("first BindActorIngress() error = %v", err)
	}
	if err := service.BindActorIngress(&actorIngressTestDialer{}); !errors.Is(err, ErrActorIngressAlreadyBound) {
		t.Fatalf("second BindActorIngress() error = %v", err)
	}
}
