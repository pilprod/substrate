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
	"fmt"
	"net"
	"sync"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxExternalExecutionConnections uint32 = 65536
	maximumExternalExecutionConnections    uint32 = 1 << 20
)

var (
	// ErrExternalExecutionDialerUnavailable reports that the provider-aware
	// dialer cannot create any new ExternalSlot transports.
	ErrExternalExecutionDialerUnavailable = errors.New("external execution dialer is unavailable")

	// ErrExternalExecutionTransportConsumed prevents a grpc.ClientConn from
	// reconnecting through a newer external-provider session generation.
	ErrExternalExecutionTransportConsumed = errors.New("external execution transport has already been consumed")

	// ErrExternalExecutionDialerCapacity reports that the explicit process-wide
	// external gRPC connection bound has been reached.
	ErrExternalExecutionDialerCapacity = errors.New("external execution dialer capacity is exhausted")
)

// externalExecutionByteDialer opens one authenticated, generation-fenced byte
// stream for an exact ExternalSlot Worker assignment. The external-provider
// package implements this boundary; controlapi owns the gRPC client transport.
type externalExecutionByteDialer interface {
	DialContext(context.Context, *ateapipb.WorkerAssignment) (net.Conn, error)
}

// ProviderExecutionDialer preserves the existing Kubernetes atelet path and
// adds a separately fenced ExternalSlot path. One gRPC transport is shared by
// concurrent workflows for one immutable Worker incarnation, keeping logical
// EXECUTION_GRPC channel use bounded by the number of live external Workers.
type ProviderExecutionDialer struct {
	kubernetes workerExecutionDialer
	external   externalExecutionByteDialer

	mu            sync.Mutex
	closed        bool
	externalBound bool
	maxExternal   uint32
	externalConns map[externalExecutionKey]*externalExecutionConnection
}

type externalExecutionConnection struct {
	client *grpc.ClientConn
}

type externalExecutionKey struct {
	workerName        string
	workerUID         string
	executionIdentity string
}

var _ workerExecutionDialer = (*ProviderExecutionDialer)(nil)

// ProviderExecutionDialerLimits bounds the number of cached ExternalSlot gRPC
// transports. The cache key is an immutable Worker resource incarnation.
type ProviderExecutionDialerLimits struct {
	MaxExternalConnections uint32
}

// DefaultProviderExecutionDialerLimits supports 256 full 256-slot provider
// registrations while retaining an explicit process-wide memory bound.
func DefaultProviderExecutionDialerLimits() ProviderExecutionDialerLimits {
	return ProviderExecutionDialerLimits{MaxExternalConnections: defaultMaxExternalExecutionConnections}
}

// NewProviderExecutionDialer constructs a provider-aware execution dialer with
// the existing Kubernetes authority. ExternalSlot execution remains unavailable
// until BindExternal succeeds. This two-phase boundary breaks the startup cycle:
// SessionAuthority needs the control service as its Worker reconciler, while the
// control service must already hold this stable dialer instance.
func NewProviderExecutionDialer(kubernetes workerExecutionDialer, configured ...ProviderExecutionDialerLimits) (*ProviderExecutionDialer, error) {
	if kubernetes == nil {
		return nil, fmt.Errorf("%w: Kubernetes dialer is required", ErrExternalExecutionDialerUnavailable)
	}
	if len(configured) > 1 {
		return nil, fmt.Errorf("%w: connection limits may be supplied at most once", ErrExternalExecutionDialerUnavailable)
	}
	limits := DefaultProviderExecutionDialerLimits()
	if len(configured) == 1 {
		limits = configured[0]
	}
	if limits.MaxExternalConnections == 0 || limits.MaxExternalConnections > maximumExternalExecutionConnections {
		return nil, fmt.Errorf("%w: external connection limit must be between 1 and %d", ErrExternalExecutionDialerUnavailable, maximumExternalExecutionConnections)
	}
	return &ProviderExecutionDialer{
		kubernetes:    kubernetes,
		maxExternal:   limits.MaxExternalConnections,
		externalConns: make(map[externalExecutionKey]*externalExecutionConnection),
	}, nil
}

// BindExternal installs the sole ExternalSlot byte-stream authority. It may be
// called exactly once and must complete before the Broker listener starts.
func (d *ProviderExecutionDialer) BindExternal(external externalExecutionByteDialer) error {
	if d == nil || external == nil {
		return fmt.Errorf("%w: ExternalSlot dialer is required", ErrExternalExecutionDialerUnavailable)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return fmt.Errorf("%w: provider execution dialer is closed", ErrExternalExecutionDialerUnavailable)
	}
	if d.externalBound {
		return fmt.Errorf("%w: ExternalSlot dialer is already bound", ErrExternalExecutionDialerUnavailable)
	}
	d.external = external
	d.externalBound = true
	return nil
}

// DialForWorker routes only by the persisted provider discriminator. Legacy
// UNSPECIFIED remains KubernetesPod through effectiveWorkerProvider; no other
// provider is guessed from identity fields.
func (d *ProviderExecutionDialer) DialForWorker(assignment *ateapipb.WorkerAssignment) (*grpc.ClientConn, error) {
	if d == nil {
		return nil, ErrExternalExecutionDialerUnavailable
	}
	switch effectiveWorkerProvider(assignment.GetProvider()) {
	case ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD:
		return d.kubernetes.DialForWorker(assignment)
	case ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT:
		return d.dialExternal(assignment)
	default:
		return nil, ErrExternalExecutionDialerUnavailable
	}
}

// DialForLocalSnapshot is intentionally Kubernetes-only. ExternalSlot
// ActorTemplates must not produce node-local Substrate snapshots.
func (d *ProviderExecutionDialer) DialForLocalSnapshot(local *ateapipb.LocalSnapshotInfo) (*grpc.ClientConn, error) {
	if d == nil || d.kubernetes == nil {
		return nil, ErrExternalExecutionDialerUnavailable
	}
	return d.kubernetes.DialForLocalSnapshot(local)
}

func (d *ProviderExecutionDialer) dialExternal(assignment *ateapipb.WorkerAssignment) (*grpc.ClientConn, error) {
	key, err := externalKeyForAssignment(assignment)
	if err != nil {
		return nil, err
	}
	assignment = proto.Clone(assignment).(*ateapipb.WorkerAssignment)

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || !d.externalBound || d.external == nil {
		return nil, ErrExternalExecutionDialerUnavailable
	}
	if existing := d.externalConns[key]; existing != nil {
		if existing.client.GetState() != connectivity.Shutdown {
			return existing.client, nil
		}
		delete(d.externalConns, key)
	}
	if uint32(len(d.externalConns)) >= d.maxExternal {
		d.discardShutdownExternalLocked()
		if uint32(len(d.externalConns)) >= d.maxExternal {
			return nil, ErrExternalExecutionDialerCapacity
		}
	}

	oneShot := &oneShotExternalTransport{}
	var client *grpc.ClientConn
	var cached *externalExecutionConnection
	external := d.external
	oneShot.dial = func(ctx context.Context) (net.Conn, error) {
		connection, dialErr := external.DialContext(ctx, assignment)
		if dialErr != nil {
			d.forgetExternal(key, cached)
			return nil, dialErr
		}
		return &observedExternalConn{Conn: connection, closed: func() {
			d.forgetExternal(key, cached)
		}}, nil
	}
	client, err = grpc.NewClient(
		"passthrough:///external-execution",
		grpc.WithContextDialer(oneShot.DialContext),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("create external execution gRPC transport: %w", err)
	}
	cached = &externalExecutionConnection{client: client}
	d.externalConns[key] = cached
	return client, nil
}

func (d *ProviderExecutionDialer) discardShutdownExternalLocked() {
	for key, connection := range d.externalConns {
		if connection.client.GetState() == connectivity.Shutdown {
			delete(d.externalConns, key)
		}
	}
}

func externalKeyForAssignment(assignment *ateapipb.WorkerAssignment) (externalExecutionKey, error) {
	if assignment == nil || assignment.GetProvider() != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT ||
		assignment.GetWorker() == nil || assignment.GetWorker().GetAtespace() != "" ||
		!externalprovider.IsValidIdentity(assignment.GetWorker().GetName()) || assignment.GetExternalSlot() == nil ||
		!externalprovider.IsValidIdentity(assignment.GetExternalSlot().GetExecutionIdentity()) {
		return externalExecutionKey{}, ErrExternalExecutionDialerUnavailable
	}
	parsedUID, err := uuid.Parse(assignment.GetWorkerResourceUid())
	if err != nil || parsedUID.String() != assignment.GetWorkerResourceUid() {
		return externalExecutionKey{}, ErrExternalExecutionDialerUnavailable
	}
	return externalExecutionKey{
		workerName:        assignment.GetWorker().GetName(),
		workerUID:         assignment.GetWorkerResourceUid(),
		executionIdentity: assignment.GetExternalSlot().GetExecutionIdentity(),
	}, nil
}

func (d *ProviderExecutionDialer) forgetExternal(key externalExecutionKey, expected *externalExecutionConnection) {
	if d == nil || expected == nil {
		return
	}
	d.mu.Lock()
	if d.externalConns[key] == expected {
		delete(d.externalConns, key)
	}
	d.mu.Unlock()
	// grpc may invoke net.Conn.Close from one of ClientConn's own transport
	// goroutines. Close asynchronously so that callback cannot deadlock waiting
	// for the goroutine which is currently unwinding.
	go expected.client.Close()
}

// Close prevents new external transports and releases every cached
// grpc.ClientConn. The Kubernetes atelet dialer retains its process-lifetime
// cache and is drained by the existing server shutdown path.
func (d *ProviderExecutionDialer) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	connections := make([]*grpc.ClientConn, 0, len(d.externalConns))
	for _, connection := range d.externalConns {
		connections = append(connections, connection.client)
	}
	clear(d.externalConns)
	d.mu.Unlock()
	var closeErr error
	for _, connection := range connections {
		closeErr = errors.Join(closeErr, connection.Close())
	}
	return closeErr
}

type oneShotExternalTransport struct {
	mu   sync.Mutex
	used bool
	dial func(context.Context) (net.Conn, error)
}

func (d *oneShotExternalTransport) DialContext(ctx context.Context, _ string) (net.Conn, error) {
	if d == nil || ctx == nil || d.dial == nil {
		return nil, ErrExternalExecutionDialerUnavailable
	}
	d.mu.Lock()
	if d.used {
		d.mu.Unlock()
		return nil, ErrExternalExecutionTransportConsumed
	}
	d.used = true
	d.mu.Unlock()
	return d.dial(ctx)
}

type observedExternalConn struct {
	net.Conn
	once   sync.Once
	closed func()
}

func (c *observedExternalConn) Close() error {
	if c == nil || c.Conn == nil {
		return nil
	}
	err := c.Conn.Close()
	c.once.Do(func() {
		if c.closed != nil {
			c.closed()
		}
	})
	return err
}
