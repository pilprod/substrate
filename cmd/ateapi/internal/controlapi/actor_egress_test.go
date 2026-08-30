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
	"sync/atomic"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actorEgressTestCA struct{}

func (actorEgressTestCA) IssueExternalActorCertificate(context.Context, []byte, *substratex509.ActorIdentity, *substratex509.ExternalRouteBinding) ([][]byte, error) {
	return [][]byte{[]byte("test-leaf")}, nil
}

func (actorEgressTestCA) VerifyExternalActorCertificate(context.Context, [][]byte) (*substratex509.ActorIdentity, *substratex509.ExternalRouteBinding, error) {
	return nil, nil, errors.New("not used")
}

func testActorEgressBinding() actorEgressBindingSnapshot {
	return actorEgressBindingSnapshot{
		workerName:        "external-worker",
		workerUID:         testWorkerIngressUID,
		workerNamespace:   "ate-workers",
		workerPool:        "external-pool",
		executionIdentity: "registration.slot.execution",
		localityIdentity:  "registration.locality",
		ownerAtespace:     "team-a",
	}
}

func TestDialActorEgressDerivesAssignmentAndUsesConfiguredGateway(t *testing.T) {
	actor, worker := actorIngressTestResources()
	st := &actorIngressTestStore{actor: actor, worker: worker}
	serverConn, gatewayPeer := net.Pipe()
	defer gatewayPeer.Close()
	var calls atomic.Int32
	service := &RPCService{
		actorIngressStore:     st,
		actorEgressGateway:    "egress-gateway.ate-system.svc:8443",
		actorEgressCA:         actorEgressTestCA{},
		actorEgressServerName: "egress-gateway.ate-system.svc",
		actorEgressTrustPEM:   []byte("test-trust"),
		actorEgressDial: func(_ context.Context, network, address string) (net.Conn, error) {
			calls.Add(1)
			if network != "tcp" || address != "egress-gateway.ate-system.svc:8443" {
				t.Fatalf("gateway dial = %s %s", network, address)
			}
			return serverConn, nil
		},
	}
	connection, credential, err := service.openActorEgress(context.Background(), testActorEgressBinding(), 9, []byte("test-csr"))
	if err != nil || connection != serverConn {
		t.Fatalf("openActorEgress() = (%v, %v, %v)", connection, credential, err)
	}
	if credential == nil || credential.GetGatewayServerName() != "egress-gateway.ate-system.svc" {
		t.Fatalf("credential = %v", credential)
	}
	defer connection.Close()
	if calls.Load() != 1 || st.acquires.Load() != 1 || !st.released.Load() {
		t.Fatalf("calls/acquires/released = %d/%d/%v", calls.Load(), st.acquires.Load(), st.released.Load())
	}
}

func TestDialActorEgressRejectsUnprovenAssignmentBeforeGateway(t *testing.T) {
	tests := []struct {
		name       string
		generation uint64
		mutate     func(*ateapipb.Actor, *ateapipb.Worker, *actorEgressBindingSnapshot)
	}{
		{name: "stale zero generation", generation: 0},
		{name: "no Worker assignment", generation: 1, mutate: func(_ *ateapipb.Actor, worker *ateapipb.Worker, _ *actorEgressBindingSnapshot) {
			worker.Status.Assignment = nil
		}},
		{name: "wrong Worker incarnation", generation: 1, mutate: func(_ *ateapipb.Actor, _ *ateapipb.Worker, binding *actorEgressBindingSnapshot) {
			binding.workerUID = "00000000-0000-4000-8000-000000000999"
		}},
		{name: "Actor incarnation changed", generation: 1, mutate: func(actor *ateapipb.Actor, _ *ateapipb.Worker, _ *actorEgressBindingSnapshot) {
			actor.Metadata.Uid = "00000000-0000-4000-8000-000000000999"
		}},
		{name: "Actor not running", generation: 1, mutate: func(actor *ateapipb.Actor, _ *ateapipb.Worker, _ *actorEgressBindingSnapshot) {
			actor.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actor, worker := actorIngressTestResources()
			binding := testActorEgressBinding()
			if test.mutate != nil {
				test.mutate(actor, worker, &binding)
			}
			var calls atomic.Int32
			service := &RPCService{
				actorIngressStore:     &actorIngressTestStore{actor: actor, worker: worker},
				actorEgressGateway:    "egress-gateway.ate-system.svc:8443",
				actorEgressCA:         actorEgressTestCA{},
				actorEgressServerName: "egress-gateway.ate-system.svc",
				actorEgressTrustPEM:   []byte("test-trust"),
				actorEgressDial: func(context.Context, string, string) (net.Conn, error) {
					calls.Add(1)
					return nil, errors.New("must not dial")
				},
			}
			connection, _, err := service.openActorEgress(context.Background(), binding, test.generation, []byte("test-csr"))
			if connection != nil || !errors.Is(err, externalprovider.ErrExternalActorEgressUnavailable) {
				t.Fatalf("dialActorEgress() = (%v, %v), want unavailable", connection, err)
			}
			if calls.Load() != 0 {
				t.Fatalf("gateway calls = %d, want 0", calls.Load())
			}
		})
	}
}

func TestDialActorEgressRejectsMissingOrInvalidConfiguredGateway(t *testing.T) {
	actor, worker := actorIngressTestResources()
	for _, gateway := range []string{"", "not-a-host-port"} {
		service := &RPCService{
			actorIngressStore:  &actorIngressTestStore{actor: actor, worker: worker},
			actorEgressGateway: gateway,
			actorEgressDial: func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("dial called for invalid server gateway")
				return nil, nil
			},
		}
		if connection, _, err := service.openActorEgress(context.Background(), testActorEgressBinding(), 1, []byte("test-csr")); connection != nil ||
			!errors.Is(err, externalprovider.ErrExternalActorEgressUnavailable) {
			t.Fatalf("gateway %q result = (%v, %v)", gateway, connection, err)
		}
	}
}

type actorEgressAuthorizationTestCA struct{ verifyCalls atomic.Int32 }

func (*actorEgressAuthorizationTestCA) IssueExternalActorCertificate(context.Context, []byte, *substratex509.ActorIdentity, *substratex509.ExternalRouteBinding) ([][]byte, error) {
	return nil, errors.New("not used")
}

func (a *actorEgressAuthorizationTestCA) VerifyExternalActorCertificate(context.Context, [][]byte) (*substratex509.ActorIdentity, *substratex509.ExternalRouteBinding, error) {
	a.verifyCalls.Add(1)
	return nil, nil, errors.New("not used")
}

type actorEgressAuthorizationTestRoutes struct{}

func (actorEgressAuthorizationTestRoutes) ResolveExternalActorEgress(*substratex509.ExternalRouteBinding) (externalprovider.SessionWorkerBinding, bool) {
	return externalprovider.SessionWorkerBinding{}, false
}

func (actorEgressAuthorizationTestRoutes) GuardExternalActorEgress(context.Context, *substratex509.ExternalRouteBinding, func(externalprovider.SessionWorkerBinding) error) error {
	return externalprovider.ErrExternalActorEgressRouteUnauthorized
}

func TestAuthorizeExternalActorEgressRejectsWrongPrincipalBeforeCertificateParsing(t *testing.T) {
	certificateAuthority := &actorEgressAuthorizationTestCA{}
	service := &RPCService{
		actorEgressCA:        certificateAuthority,
		actorEgressRoutes:    actorEgressAuthorizationTestRoutes{},
		actorEgressPrincipal: "spiffe://cluster.local/ns/ate-system/sa/atenet-egress",
	}
	request := &ateapipb.AuthorizeExternalActorEgressRequest{ActorCertificateChainDer: [][]byte{[]byte("untrusted")}}
	contexts := []context.Context{
		context.Background(),
		principal.InjectContext(context.Background(), principal.PrincipalInfo{Kind: principal.KindJWT, ID: service.actorEgressPrincipal}),
		principal.InjectContext(context.Background(), principal.PrincipalInfo{Kind: principal.KindMTLS, ID: "spiffe://cluster.local/ns/other/sa/other"}),
	}
	for index, ctx := range contexts {
		if response, err := service.AuthorizeExternalActorEgress(ctx, request); response != nil || status.Code(err) != codes.PermissionDenied {
			t.Fatalf("case %d AuthorizeExternalActorEgress() = (%v, %v), want nil/PermissionDenied", index, response, err)
		}
	}
	if calls := certificateAuthority.verifyCalls.Load(); calls != 0 {
		t.Fatalf("certificate verification calls = %d, want 0", calls)
	}
}
