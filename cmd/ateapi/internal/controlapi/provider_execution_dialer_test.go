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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/ateletpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestProviderExecutionDialerPreservesKubernetesPath(t *testing.T) {
	kubernetesConnection, err := grpc.NewClient("passthrough:///kubernetes-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer kubernetesConnection.Close()
	kubernetes := &recordingProviderKubernetesDialer{workerConnection: kubernetesConnection, snapshotConnection: kubernetesConnection}
	external := &servingExternalExecutionDialer{}
	t.Cleanup(external.Close)
	dialer, err := NewProviderExecutionDialer(kubernetes)
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()
	if err := dialer.BindExternal(external); err != nil {
		t.Fatal(err)
	}

	legacy := &ateapipb.WorkerAssignment{WorkerNamespace: "workers", WorkerPod: "worker-1"}
	got, err := dialer.DialForWorker(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if got != kubernetesConnection || kubernetes.workerAssignment != legacy {
		t.Fatalf("Kubernetes worker route was not preserved")
	}
	local := &ateapipb.LocalSnapshotInfo{NodeVmsWithLocalSnapshots: []string{"node-1"}}
	got, err = dialer.DialForLocalSnapshot(local)
	if err != nil {
		t.Fatal(err)
	}
	if got != kubernetesConnection || kubernetes.localSnapshot != local {
		t.Fatalf("local snapshot route was not preserved")
	}
	if external.Count() != 0 {
		t.Fatalf("external dial count = %d, want 0", external.Count())
	}
}

func TestProviderExecutionDialerReusesOneExternalTransportPerWorkerIncarnation(t *testing.T) {
	kubernetesConnection, err := grpc.NewClient("passthrough:///kubernetes-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer kubernetesConnection.Close()
	external := &servingExternalExecutionDialer{}
	t.Cleanup(external.Close)
	dialer, err := NewProviderExecutionDialer(
		&recordingProviderKubernetesDialer{workerConnection: kubernetesConnection, snapshotConnection: kubernetesConnection},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()
	if err := dialer.BindExternal(external); err != nil {
		t.Fatal(err)
	}

	assignment := externalExecutionAssignment()
	first, err := dialer.DialForWorker(assignment)
	if err != nil {
		t.Fatal(err)
	}
	second, err := dialer.DialForWorker(proto.Clone(assignment).(*ateapipb.WorkerAssignment))
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("same Worker incarnation did not reuse its gRPC transport")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := ateletpb.NewAteomHerderClient(first)
	if _, err := client.Run(ctx, &ateletpb.RunRequest{}); err != nil {
		t.Fatalf("first external Run: %v", err)
	}
	if _, err := client.Run(ctx, &ateletpb.RunRequest{}); err != nil {
		t.Fatalf("second external Run: %v", err)
	}
	if external.Count() != 1 {
		t.Fatalf("external dial count = %d, want 1", external.Count())
	}
	if got := external.LastAssignment(); !proto.Equal(got, assignment) || got == assignment {
		t.Fatalf("external assignment was not passed as an independent exact snapshot")
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := dialer.DialForWorker(assignment)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("closed external transport remained cached")
	}
	if _, err := ateletpb.NewAteomHerderClient(third).Run(ctx, &ateletpb.RunRequest{}); err != nil {
		t.Fatalf("replacement external Run: %v", err)
	}
	if external.Count() != 2 {
		t.Fatalf("external dial count = %d, want 2", external.Count())
	}
}

func TestOneShotExternalTransportNeverReconnects(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	dials := 0
	oneShot := &oneShotExternalTransport{dial: func(context.Context) (net.Conn, error) {
		dials++
		return client, nil
	}}
	got, err := oneShot.DialContext(context.Background(), "ignored")
	if err != nil {
		t.Fatal(err)
	}
	if got != client {
		t.Fatal("unexpected first transport")
	}
	if _, err := oneShot.DialContext(context.Background(), "ignored"); !errors.Is(err, ErrExternalExecutionTransportConsumed) {
		t.Fatalf("second dial error = %v, want consumed", err)
	}
	if dials != 1 {
		t.Fatalf("dial calls = %d, want 1", dials)
	}
}

func TestProviderExecutionDialerDoesNotReconnectAcrossTransportLoss(t *testing.T) {
	kubernetesConnection, err := grpc.NewClient("passthrough:///kubernetes-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer kubernetesConnection.Close()
	external := &servingExternalExecutionDialer{}
	t.Cleanup(external.Close)
	dialer, err := NewProviderExecutionDialer(
		&recordingProviderKubernetesDialer{workerConnection: kubernetesConnection, snapshotConnection: kubernetesConnection},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()
	if err := dialer.BindExternal(external); err != nil {
		t.Fatal(err)
	}

	assignment := externalExecutionAssignment()
	oldConnection, err := dialer.DialForWorker(assignment)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := ateletpb.NewAteomHerderClient(oldConnection).Run(ctx, &ateletpb.RunRequest{}); err != nil {
		t.Fatalf("initial external Run: %v", err)
	}
	if external.Count() != 1 {
		t.Fatalf("initial dial count = %d, want 1", external.Count())
	}
	external.StopLatest()

	failureCtx, failureCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer failureCancel()
	if _, err := ateletpb.NewAteomHerderClient(oldConnection).Run(failureCtx, &ateletpb.RunRequest{}); err == nil {
		t.Fatal("old gRPC connection survived external transport loss")
	}
	if external.Count() != 1 {
		t.Fatalf("old ClientConn redialed the provider %d times, want 1", external.Count())
	}
	waitForExternalCacheRemoval(t, dialer, assignment)

	newConnection, err := dialer.DialForWorker(assignment)
	if err != nil {
		t.Fatal(err)
	}
	if newConnection == oldConnection {
		t.Fatal("fresh DialForWorker reused a generation-fenced ClientConn")
	}
	if _, err := ateletpb.NewAteomHerderClient(newConnection).Run(ctx, &ateletpb.RunRequest{}); err != nil {
		t.Fatalf("replacement external Run: %v", err)
	}
	if external.Count() != 2 {
		t.Fatalf("replacement dial count = %d, want 2", external.Count())
	}
}

func TestProviderExecutionDialerValidatesExactAssignmentBeforeCacheLookup(t *testing.T) {
	kubernetesConnection, err := grpc.NewClient("passthrough:///kubernetes-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer kubernetesConnection.Close()
	external := &servingExternalExecutionDialer{}
	t.Cleanup(external.Close)
	dialer, err := NewProviderExecutionDialer(
		&recordingProviderKubernetesDialer{workerConnection: kubernetesConnection, snapshotConnection: kubernetesConnection},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()
	if err := dialer.BindExternal(external); err != nil {
		t.Fatal(err)
	}
	valid := externalExecutionAssignment()
	if _, err := dialer.DialForWorker(valid); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*ateapipb.WorkerAssignment)
	}{
		{name: "namespaced Worker", mutate: func(value *ateapipb.WorkerAssignment) { value.Worker.Atespace = "team-a" }},
		{name: "noncanonical Worker UID", mutate: func(value *ateapipb.WorkerAssignment) {
			value.WorkerResourceUid = "00000000-0000-4000-8000-00000000000A"
		}},
		{name: "invalid Worker identity", mutate: func(value *ateapipb.WorkerAssignment) { value.Worker.Name = "worker/one" }},
		{name: "invalid execution identity", mutate: func(value *ateapipb.WorkerAssignment) { value.ExternalSlot.ExecutionIdentity = "registration one" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			assignment := proto.Clone(valid).(*ateapipb.WorkerAssignment)
			test.mutate(assignment)
			if _, err := dialer.DialForWorker(assignment); !errors.Is(err, ErrExternalExecutionDialerUnavailable) {
				t.Fatalf("DialForWorker error = %v", err)
			}
		})
	}
}

func TestProviderExecutionDialerBoundsCachedExternalConnections(t *testing.T) {
	kubernetesConnection, err := grpc.NewClient("passthrough:///kubernetes-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer kubernetesConnection.Close()
	external := &servingExternalExecutionDialer{}
	t.Cleanup(external.Close)
	dialer, err := NewProviderExecutionDialer(
		&recordingProviderKubernetesDialer{workerConnection: kubernetesConnection, snapshotConnection: kubernetesConnection},
		ProviderExecutionDialerLimits{MaxExternalConnections: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer dialer.Close()
	if err := dialer.BindExternal(external); err != nil {
		t.Fatal(err)
	}
	first, err := dialer.DialForWorker(externalExecutionAssignment())
	if err != nil {
		t.Fatal(err)
	}
	secondAssignment := externalExecutionAssignment()
	secondAssignment.Worker.Name = "external-worker-2"
	secondAssignment.WorkerResourceUid = "00000000-0000-4000-8000-000000000002"
	secondAssignment.ExternalSlot.ExecutionIdentity = "registration-1.slot-2"
	if _, err := dialer.DialForWorker(secondAssignment); !errors.Is(err, ErrExternalExecutionDialerCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := dialer.DialForWorker(secondAssignment); err != nil {
		t.Fatalf("closed cache entry was not reclaimable: %v", err)
	}
}

func TestProviderExecutionDialerFailsClosed(t *testing.T) {
	if _, err := NewProviderExecutionDialer(nil); !errors.Is(err, ErrExternalExecutionDialerUnavailable) {
		t.Fatalf("constructor error = %v", err)
	}
	kubernetesConnection, err := grpc.NewClient("passthrough:///kubernetes-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer kubernetesConnection.Close()
	dialer, err := NewProviderExecutionDialer(
		&recordingProviderKubernetesDialer{workerConnection: kubernetesConnection, snapshotConnection: kubernetesConnection},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dialer.DialForWorker(externalExecutionAssignment()); !errors.Is(err, ErrExternalExecutionDialerUnavailable) {
		t.Fatalf("unbound external dialer error = %v", err)
	}
	external := &servingExternalExecutionDialer{}
	t.Cleanup(external.Close)
	if err := dialer.BindExternal(external); err != nil {
		t.Fatal(err)
	}
	if err := dialer.BindExternal(external); !errors.Is(err, ErrExternalExecutionDialerUnavailable) {
		t.Fatalf("duplicate bind error = %v", err)
	}
	if _, err := dialer.DialForWorker(&ateapipb.WorkerAssignment{Provider: ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT}); !errors.Is(err, ErrExternalExecutionDialerUnavailable) {
		t.Fatalf("incomplete assignment error = %v", err)
	}
	if err := dialer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := dialer.DialForWorker(externalExecutionAssignment()); !errors.Is(err, ErrExternalExecutionDialerUnavailable) {
		t.Fatalf("closed dialer error = %v", err)
	}
}

type recordingProviderKubernetesDialer struct {
	workerConnection   *grpc.ClientConn
	snapshotConnection *grpc.ClientConn
	workerAssignment   *ateapipb.WorkerAssignment
	localSnapshot      *ateapipb.LocalSnapshotInfo
}

func (d *recordingProviderKubernetesDialer) DialForWorker(assignment *ateapipb.WorkerAssignment) (*grpc.ClientConn, error) {
	d.workerAssignment = assignment
	return d.workerConnection, nil
}

func (d *recordingProviderKubernetesDialer) DialForLocalSnapshot(local *ateapipb.LocalSnapshotInfo) (*grpc.ClientConn, error) {
	d.localSnapshot = local
	return d.snapshotConnection, nil
}

type servingExternalExecutionDialer struct {
	mu         sync.Mutex
	count      int
	assignment *ateapipb.WorkerAssignment
	endpoints  []*servingExternalEndpoint
}

type servingExternalEndpoint struct {
	server   *grpc.Server
	listener *singleConnectionListener
}

func (d *servingExternalExecutionDialer) DialContext(ctx context.Context, assignment *ateapipb.WorkerAssignment) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, server := net.Pipe()
	grpcServer := grpc.NewServer()
	ateletpb.RegisterAteomHerderServer(grpcServer, testExternalAteomHerder{})
	listener := newSingleConnectionListener(server)
	d.mu.Lock()
	d.count++
	d.assignment = proto.Clone(assignment).(*ateapipb.WorkerAssignment)
	d.endpoints = append(d.endpoints, &servingExternalEndpoint{server: grpcServer, listener: listener})
	d.mu.Unlock()
	go grpcServer.Serve(listener)
	return client, nil
}

func (d *servingExternalExecutionDialer) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

func (d *servingExternalExecutionDialer) LastAssignment() *ateapipb.WorkerAssignment {
	d.mu.Lock()
	defer d.mu.Unlock()
	return proto.Clone(d.assignment).(*ateapipb.WorkerAssignment)
}

func (d *servingExternalExecutionDialer) StopLatest() {
	d.mu.Lock()
	if len(d.endpoints) == 0 {
		d.mu.Unlock()
		return
	}
	endpoint := d.endpoints[len(d.endpoints)-1]
	d.mu.Unlock()
	endpoint.server.Stop()
	_ = endpoint.listener.Close()
}

func (d *servingExternalExecutionDialer) Close() {
	d.mu.Lock()
	endpoints := append([]*servingExternalEndpoint(nil), d.endpoints...)
	d.endpoints = nil
	d.mu.Unlock()
	for _, endpoint := range endpoints {
		endpoint.server.Stop()
		_ = endpoint.listener.Close()
	}
}

type testExternalAteomHerder struct {
	ateletpb.UnimplementedAteomHerderServer
}

func (testExternalAteomHerder) Run(context.Context, *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	return &ateletpb.RunResponse{}, nil
}

type singleConnectionListener struct {
	connection net.Conn
	accepted   chan struct{}
	closed     chan struct{}
	acceptOnce sync.Once
	closeOnce  sync.Once
}

func newSingleConnectionListener(connection net.Conn) *singleConnectionListener {
	return &singleConnectionListener{connection: connection, accepted: make(chan struct{}), closed: make(chan struct{})}
}

func (l *singleConnectionListener) Accept() (net.Conn, error) {
	accepted := false
	l.acceptOnce.Do(func() {
		accepted = true
		close(l.accepted)
	})
	if accepted {
		return l.connection, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *singleConnectionListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closed)
		_ = l.connection.Close()
	})
	return nil
}

func (l *singleConnectionListener) Addr() net.Addr { return testExecutionAddr("external-test") }

type testExecutionAddr string

func (testExecutionAddr) Network() string  { return "external-test" }
func (a testExecutionAddr) String() string { return string(a) }

func externalExecutionAssignment() *ateapipb.WorkerAssignment {
	return &ateapipb.WorkerAssignment{
		Worker:            &ateapipb.ObjectRef{Name: "external-worker-1"},
		Provider:          ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
		WorkerResourceUid: "00000000-0000-4000-8000-000000000001",
		ExternalSlot: &ateapipb.ExternalSlotIdentity{
			ExecutionIdentity: "registration-1.slot-1",
			LocalityIdentity:  "registration-1",
			OwnerAtespace:     "team-a",
		},
	}
}

func waitForExternalCacheRemoval(t *testing.T, dialer *ProviderExecutionDialer, assignment *ateapipb.WorkerAssignment) {
	t.Helper()
	key, err := externalKeyForAssignment(assignment)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		dialer.mu.Lock()
		_, found := dialer.externalConns[key]
		dialer.mu.Unlock()
		if !found {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("external transport was not removed after connection loss")
}
