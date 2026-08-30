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
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/grpc/codes"
)

type actorEgressGatewayFunc func(context.Context, SessionWorkerBinding, uint64) (net.Conn, error)

func (f actorEgressGatewayFunc) OpenActorEgress(ctx context.Context, binding SessionWorkerBinding, generation uint64, csr []byte) (net.Conn, *externalproviderpb.ActorEgressOpenAck, error) {
	if len(csr) == 0 {
		return nil, nil, errors.New("missing test CSR")
	}
	connection, err := f(ctx, binding, generation)
	if err != nil || connection == nil {
		return connection, nil, err
	}
	return connection, &externalproviderpb.ActorEgressOpenAck{
		CertificateChainDer:   [][]byte{[]byte("test-leaf")},
		GatewayServerName:     "egress.test",
		GatewayTrustBundlePem: []byte("test-trust"),
	}, nil
}

func TestActorEgressClientOpenUsesServerDerivedRoute(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	serverGateway, gatewayPeer := actorEgressTCPPair(t)
	t.Cleanup(func() { _ = gatewayPeer.Close() })
	var calls atomic.Int32
	fixture.forwarder.actorEgress = actorEgressGatewayFunc(func(_ context.Context, binding SessionWorkerBinding, generation uint64) (net.Conn, error) {
		calls.Add(1)
		if binding.SlotID() != "slot-a" || binding.WorkerName() != fixture.assignment.GetWorker().GetName() ||
			binding.WorkerUID() != fixture.assignment.GetWorkerResourceUid() || generation != 1 {
			t.Fatalf("server-derived egress binding = (%+v, %d)", binding, generation)
		}
		return serverGateway, nil
	})

	if err := fixture.session.applyClientFrame(clientOpenFrame(1, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a")); err != nil {
		t.Fatalf("apply ACTOR_EGRESS Open: %v", err)
	}
	ack := nextExecutionServerFrame(t, fixture.sender)
	if ack.GetOpenAck().GetChannelId() != 1 || !ack.GetOpenAck().GetAccepted() || ack.GetOpenAck().GetErrorMessage() != "" {
		t.Fatalf("OpenAck = %v, want accepted channel 1", ack)
	}
	if calls.Load() != 1 {
		t.Fatalf("gateway calls = %d, want 1", calls.Load())
	}

	if err := fixture.session.applyClientFrame(clientDataFrame(1, 1, []byte("from-actor"))); err != nil {
		t.Fatalf("apply Actor data: %v", err)
	}
	read := make(chan string, 1)
	go func() {
		buffer := make([]byte, 32)
		count, err := gatewayPeer.Read(buffer)
		if err != nil {
			read <- "error: " + err.Error()
			return
		}
		read <- string(buffer[:count])
	}()
	select {
	case got := <-read:
		if got != "from-actor" {
			t.Fatalf("gateway read = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Actor bytes did not reach the fixed gateway stream")
	}

	if _, err := gatewayPeer.Write([]byte("from-gateway")); err != nil {
		t.Fatalf("gateway write: %v", err)
	}
	data := nextExecutionServerFrame(t, fixture.sender)
	if data.GetData().GetChannelId() != 1 || string(data.GetData().GetData()) != "from-gateway" {
		t.Fatalf("server data = %v", data)
	}

	if err := fixture.session.applyClientFrame(clientHalfCloseFrame(1, 1)); err != nil {
		t.Fatalf("apply Actor half-close: %v", err)
	}
	if err := gatewayPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set gateway read deadline: %v", err)
	}
	buffer := make([]byte, 1)
	if count, err := gatewayPeer.Read(buffer); count != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("gateway read after Actor half-close = (%d, %v), want EOF", count, err)
	}
	if err := gatewayPeer.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear gateway read deadline: %v", err)
	}
	if _, err := gatewayPeer.Write([]byte("after-half-close")); err != nil {
		t.Fatalf("gateway response after Actor half-close: %v", err)
	}
	afterHalfClose := nextExecutionServerFrame(t, fixture.sender)
	if string(afterHalfClose.GetData().GetData()) != "after-half-close" {
		t.Fatalf("response after Actor half-close = %v", afterHalfClose)
	}
	if err := gatewayPeer.CloseWrite(); err != nil {
		t.Fatalf("gateway CloseWrite: %v", err)
	}
	serverHalfClose := nextExecutionServerFrame(t, fixture.sender)
	if serverHalfClose.GetHalfClose().GetChannelId() != 1 {
		t.Fatalf("server half-close = %v", serverHalfClose)
	}
}

func actorEgressTCPPair(t *testing.T) (net.Conn, *net.TCPConn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan *net.TCPConn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		tcp, ok := connection.(*net.TCPConn)
		if !ok {
			_ = connection.Close()
			acceptErr <- errors.New("accepted connection is not TCP")
			return
		}
		accepted <- tcp
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case server := <-accepted:
		return client, server
	case err := <-acceptErr:
		_ = client.Close()
		t.Fatalf("accept: %v", err)
	case <-time.After(time.Second):
		_ = client.Close()
		t.Fatal("timed out accepting TCP pair")
	}
	return nil, nil
}

func TestActorEgressRejectsUnprovenOpenWithoutDroppingSession(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	var calls atomic.Int32
	fixture.forwarder.actorEgress = actorEgressGatewayFunc(func(context.Context, SessionWorkerBinding, uint64) (net.Conn, error) {
		calls.Add(1)
		return nil, ErrExternalActorEgressUnavailable
	})
	if err := fixture.session.applyClientFrame(clientOpenFrame(1, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a")); err != nil {
		t.Fatalf("apply rejected ACTOR_EGRESS Open: %v", err)
	}
	ack := nextExecutionServerFrame(t, fixture.sender)
	if ack.GetOpenAck().GetChannelId() != 1 || ack.GetOpenAck().GetAccepted() || ack.GetOpenAck().GetErrorMessage() == "" {
		t.Fatalf("OpenAck = %v, want bounded rejection", ack)
	}
	if fixture.session.done() == nil || fixture.session.ctx.Err() != nil || calls.Load() != 1 {
		t.Fatalf("rejected open fenced live session: sessionErr=%v calls=%d", fixture.session.ctx.Err(), calls.Load())
	}
}

func TestActorEgressRejectsWrongSlotAndStaleGenerationBeforeGateway(t *testing.T) {
	tests := []struct {
		name  string
		frame *externalproviderpb.ClientFrame
	}{
		{name: "wrong slot", frame: clientOpenFrame(1, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-other")},
		{name: "stale generation", frame: clientOpenFrame(2, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutionForwarderFixture(t, nil)
			var calls atomic.Int32
			fixture.forwarder.actorEgress = actorEgressGatewayFunc(func(context.Context, SessionWorkerBinding, uint64) (net.Conn, error) {
				calls.Add(1)
				return nil, nil
			})
			if err := fixture.session.applyClientFrame(test.frame); !errors.Is(err, ErrChannelProtocolViolation) {
				t.Fatalf("applyClientFrame() error = %v, want protocol rejection", err)
			}
			if calls.Load() != 0 {
				t.Fatalf("gateway calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestActorEgressCancellationClosesGateway(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	gateway := newBlockingActorEgressConn()
	fixture.forwarder.actorEgress = actorEgressGatewayFunc(func(context.Context, SessionWorkerBinding, uint64) (net.Conn, error) {
		return gateway, nil
	})
	if err := fixture.session.applyClientFrame(clientOpenFrame(1, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a")); err != nil {
		t.Fatalf("apply ACTOR_EGRESS Open: %v", err)
	}
	_ = nextExecutionServerFrame(t, fixture.sender)
	fixture.cancel()
	select {
	case <-gateway.closed:
	case <-time.After(time.Second):
		t.Fatal("session cancellation did not close Actor egress gateway")
	}
}

func TestActorEgressReceiveBackpressureResetsOnlyChannel(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, func(limits *ExecutionForwardingLimits) {
		limits.ReceiveQueueDepth = 1
	})
	gateway := newBlockingActorEgressConn()
	fixture.forwarder.actorEgress = actorEgressGatewayFunc(func(context.Context, SessionWorkerBinding, uint64) (net.Conn, error) {
		return gateway, nil
	})
	if err := fixture.session.applyClientFrame(clientOpenFrame(1, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a")); err != nil {
		t.Fatalf("apply ACTOR_EGRESS Open: %v", err)
	}
	_ = nextExecutionServerFrame(t, fixture.sender)
	if err := fixture.session.applyClientFrame(clientDataFrame(1, 1, []byte("first"))); err != nil {
		t.Fatalf("apply first data: %v", err)
	}
	select {
	case <-gateway.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("gateway write did not block")
	}
	if err := fixture.session.applyClientFrame(clientDataFrame(1, 1, []byte("second"))); err != nil {
		t.Fatalf("apply second data: %v", err)
	}
	if err := fixture.session.applyClientFrame(clientDataFrame(1, 1, []byte("overflow"))); err != nil {
		t.Fatalf("apply overflow data: %v", err)
	}
	reset := nextExecutionServerFrame(t, fixture.sender)
	if reset.GetReset_().GetChannelId() != 1 || reset.GetReset_().GetGrpcCode() != uint32(codes.ResourceExhausted) {
		t.Fatalf("reset = %v, want ResourceExhausted channel 1", reset)
	}
	if fixture.session.ctx.Err() != nil {
		t.Fatalf("channel backpressure fenced session: %v", fixture.session.ctx.Err())
	}
}

func TestActorEgressCrossedClientTerminalDoesNotFenceSession(t *testing.T) {
	fixture := newExecutionForwarderFixture(t, nil)
	gateway := newBlockingActorEgressConn()
	fixture.forwarder.actorEgress = actorEgressGatewayFunc(func(context.Context, SessionWorkerBinding, uint64) (net.Conn, error) {
		return gateway, nil
	})
	if err := fixture.session.applyClientFrame(clientOpenFrame(1, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a")); err != nil {
		t.Fatalf("apply ACTOR_EGRESS Open: %v", err)
	}
	_ = nextExecutionServerFrame(t, fixture.sender)
	connection := fixture.session.forwarding.connection(1)
	if connection == nil {
		t.Fatal("accepted Actor egress connection is missing")
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("server close Actor egress connection: %v", err)
	}
	reset := nextExecutionServerFrame(t, fixture.sender)
	if reset.GetReset_().GetChannelId() != 1 {
		t.Fatalf("server reset = %v, want channel 1", reset)
	}
	if err := fixture.session.applyClientFrame(clientDataFrame(1, 1, []byte("crossed client data"))); err != nil {
		t.Fatalf("apply crossed client Data: %v", err)
	}
	if err := fixture.session.applyClientFrame(clientResetFrame(1, 1, uint32(codes.Canceled), "crossed client reset")); err != nil {
		t.Fatalf("apply crossed client Reset: %v", err)
	}
	if err := fixture.session.applyClientFrame(clientHalfCloseFrame(1, 1)); err != nil {
		t.Fatalf("apply crossed client HalfClose: %v", err)
	}
	if fixture.session.ctx.Err() != nil {
		t.Fatalf("crossed channel terminal fenced session: %v", fixture.session.ctx.Err())
	}
}

type blockingActorEgressConn struct {
	closed       chan struct{}
	writeStarted chan struct{}
	closeOnce    sync.Once
	writeOnce    sync.Once
}

func newBlockingActorEgressConn() *blockingActorEgressConn {
	return &blockingActorEgressConn{closed: make(chan struct{}), writeStarted: make(chan struct{})}
}

func (c *blockingActorEgressConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}

func (c *blockingActorEgressConn) Write([]byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writeStarted) })
	<-c.closed
	return 0, net.ErrClosed
}

func (c *blockingActorEgressConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (*blockingActorEgressConn) LocalAddr() net.Addr              { return executionAddr("gateway-local") }
func (*blockingActorEgressConn) RemoteAddr() net.Addr             { return executionAddr("gateway-remote") }
func (*blockingActorEgressConn) SetDeadline(time.Time) error      { return nil }
func (*blockingActorEgressConn) SetReadDeadline(time.Time) error  { return nil }
func (*blockingActorEgressConn) SetWriteDeadline(time.Time) error { return nil }
