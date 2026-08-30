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
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"net/url"
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/externalprovider"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

const maxActorEgressTrustBundleBytes = 256 << 10

type externalActorCertificateAuthority interface {
	IssueExternalActorCertificate(context.Context, []byte, *substratex509.ActorIdentity, *substratex509.ExternalRouteBinding) ([][]byte, error)
	VerifyExternalActorCertificate(context.Context, [][]byte) (*substratex509.ActorIdentity, *substratex509.ExternalRouteBinding, error)
}

// ExternalActorEgressConfiguration contains only server-owned transport and
// trust authority. Provider frames cannot override any field.
type ExternalActorEgressConfiguration struct {
	CertificateAuthority     externalActorCertificateAuthority
	RouteAuthorizer          externalprovider.ExternalActorEgressRouteAuthorizer
	GatewayServerName        string
	GatewayTrustBundlePEM    []byte
	ExpectedGatewayPrincipal string
}

// ConfigureExternalActorEgress installs the immutable protocol-v3 credential
// authority before either ateapi listener starts.
func (s *RPCService) ConfigureExternalActorEgress(config ExternalActorEgressConfiguration) error {
	if s == nil || config.CertificateAuthority == nil || config.RouteAuthorizer == nil ||
		s.actorEgressGateway == "" || config.GatewayServerName == "" || config.ExpectedGatewayPrincipal == "" {
		return fmt.Errorf("external Actor egress configuration is incomplete")
	}
	if _, _, err := net.SplitHostPort(s.actorEgressGateway); err != nil {
		return fmt.Errorf("external Actor egress gateway address is invalid: %w", err)
	}
	if problems := k8svalidation.IsDNS1123Subdomain(config.GatewayServerName); len(problems) != 0 {
		return fmt.Errorf("external Actor egress gateway server name is invalid")
	}
	if err := validateActorEgressTrustBundle(config.GatewayTrustBundlePEM); err != nil {
		return err
	}
	identity, err := url.Parse(config.ExpectedGatewayPrincipal)
	if err != nil || identity.Scheme != "spiffe" || identity.Host == "" || identity.Path == "" ||
		identity.User != nil || identity.RawQuery != "" || identity.Fragment != "" || identity.String() != config.ExpectedGatewayPrincipal {
		return fmt.Errorf("external Actor egress gateway principal is invalid")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actorEgressCA != nil || s.actorEgressRoutes != nil {
		return fmt.Errorf("external Actor egress is already configured")
	}
	s.actorEgressCA = config.CertificateAuthority
	s.actorEgressRoutes = config.RouteAuthorizer
	s.actorEgressServerName = config.GatewayServerName
	s.actorEgressTrustPEM = slices.Clone(config.GatewayTrustBundlePEM)
	s.actorEgressPrincipal = config.ExpectedGatewayPrincipal
	return nil
}

func validateActorEgressTrustBundle(bundle []byte) error {
	if len(bundle) == 0 || len(bundle) > maxActorEgressTrustBundleBytes {
		return fmt.Errorf("external Actor egress gateway trust bundle length is invalid")
	}
	rest := bytes.TrimSpace(bundle)
	certificates := 0
	for len(rest) != 0 {
		block, remaining := pem.Decode(rest)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return fmt.Errorf("external Actor egress gateway trust bundle is not certificate-only PEM")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA {
			return fmt.Errorf("external Actor egress gateway trust bundle contains an invalid CA")
		}
		certificates++
		rest = bytes.TrimSpace(remaining)
	}
	if certificates == 0 {
		return fmt.Errorf("external Actor egress gateway trust bundle contains no certificates")
	}
	return nil
}

// OpenActorEgress resolves a protocol-v3 client-opened slot back to the exact
// current Worker and Actor under the Actor lifecycle lease, issues a
// generation-bound certificate from the CSR public key, and dials only the
// cluster gateway configured on ateapi.
func (s *RPCService) OpenActorEgress(
	ctx context.Context,
	binding externalprovider.SessionWorkerBinding,
	generation uint64,
	csr []byte,
) (net.Conn, *externalproviderpb.ActorEgressOpenAck, error) {
	return s.openActorEgress(ctx, actorEgressBindingSnapshot{
		registrationUID:   binding.RegistrationUID(),
		slotID:            binding.SlotID(),
		workerName:        binding.WorkerName(),
		workerUID:         binding.WorkerUID(),
		workerNamespace:   binding.WorkerNamespace(),
		workerPool:        binding.WorkerPool(),
		executionIdentity: binding.ExecutionIdentity(),
		localityIdentity:  binding.LocalityIdentity(),
		ownerAtespace:     binding.OwnerAtespace(),
	}, generation, csr)
}

type actorEgressBindingSnapshot struct {
	registrationUID   string
	slotID            string
	workerName        string
	workerUID         string
	workerNamespace   string
	workerPool        string
	executionIdentity string
	localityIdentity  string
	ownerAtespace     string
}

func (s *RPCService) openActorEgress(ctx context.Context, binding actorEgressBindingSnapshot, generation uint64, csr []byte) (net.Conn, *externalproviderpb.ActorEgressOpenAck, error) {
	if s == nil || ctx == nil || generation == 0 || len(csr) == 0 || s.actorIngressStore == nil || s.actorEgressDial == nil || s.actorEgressGateway == "" {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}
	if _, _, err := net.SplitHostPort(s.actorEgressGateway); err != nil {
		return nil, nil, fmt.Errorf("%w: invalid configured gateway", externalprovider.ErrExternalActorEgressUnavailable)
	}
	s.mu.RLock()
	certificateAuthority := s.actorEgressCA
	gatewayServerName := s.actorEgressServerName
	gatewayTrustPEM := slices.Clone(s.actorEgressTrustPEM)
	s.mu.RUnlock()
	if certificateAuthority == nil || gatewayServerName == "" || len(gatewayTrustPEM) == 0 {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}

	// This first lookup derives the Actor reference from the exact durable
	// Worker name. It grants no authority: all state is fetched and checked
	// again after taking the Actor lifecycle lease.
	worker, err := s.actorIngressStore.GetWorker(ctx, binding.workerName)
	if err != nil || worker == nil || !actorEgressWorkerMatchesBinding(worker, binding) {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}
	assigned := worker.GetStatus().GetAssignment()
	actorRef := resources.ActorRefFromObjectRef(assigned.GetActor())
	expectedActorUID := assigned.GetActorUid()
	actorUID, actorUIDErr := uuid.Parse(expectedActorUID)
	if !resources.IsValidResourceName(actorRef.Atespace) || !resources.IsValidResourceName(actorRef.Name) ||
		actorUIDErr != nil || actorUID.String() != expectedActorUID {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}

	lease, err := s.acquireActorIngressLease(ctx, actorRef)
	if err != nil {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}
	defer lease.Close()

	worker, err = s.actorIngressStore.GetWorker(lease.Context(), binding.workerName)
	if err != nil || worker == nil || !actorEgressWorkerMatchesBinding(worker, binding) {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}
	assigned = worker.GetStatus().GetAssignment()
	if resources.ActorRefFromObjectRef(assigned.GetActor()) != actorRef || assigned.GetActorUid() != expectedActorUID {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}
	actor, err := s.actorIngressStore.GetActor(lease.Context(), actorRef)
	if err != nil || actor == nil || actor.GetMetadata().GetUid() != assigned.GetActorUid() ||
		actor.GetMetadata().GetAtespace() != actorRef.Atespace || actor.GetMetadata().GetName() != actorRef.Name ||
		actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}
	assignment := actor.GetStatus().GetWorkerAssignment()
	if !isExternalActorIngressAssignment(assignment) || !workerMatchesActorIngress(actor, worker, assignment) ||
		!actorEgressAssignmentMatchesBinding(assignment, binding) {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}
	certificateChain, err := certificateAuthority.IssueExternalActorCertificate(
		lease.Context(),
		csr,
		&substratex509.ActorIdentity{
			Atespace:  actorRef.Atespace,
			ActorName: actorRef.Name,
			ActorUid:  expectedActorUID,
			Purpose:   substratex509.ActorIdentityPurposeAtunnel,
		},
		&substratex509.ExternalRouteBinding{
			Version:           substratex509.ExternalRouteBindingVersion,
			RegistrationUID:   binding.registrationUID,
			SlotID:            binding.slotID,
			WorkerUID:         binding.workerUID,
			SessionGeneration: generation,
			ExecutionIdentity: binding.executionIdentity,
		},
	)
	if err != nil || len(certificateChain) == 0 {
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}

	connection, err := s.actorEgressDial(lease.Context(), "tcp", s.actorEgressGateway)
	if err != nil || connection == nil {
		if connection != nil {
			_ = connection.Close()
		}
		return nil, nil, externalprovider.ErrExternalActorEgressUnavailable
	}
	return connection, &externalproviderpb.ActorEgressOpenAck{
		CertificateChainDer:   cloneByteSlices(certificateChain),
		GatewayServerName:     gatewayServerName,
		GatewayTrustBundlePem: gatewayTrustPEM,
	}, nil
}

func cloneByteSlices(values [][]byte) [][]byte {
	cloned := make([][]byte, len(values))
	for index := range values {
		cloned[index] = slices.Clone(values[index])
	}
	return cloned
}

// AuthorizeExternalActorEgress is the online generation fence used by atenet
// for every CONNECT presenting a signed ExternalRouteBinding. It accepts no
// caller-populated route or identity fields.
func (s *RPCService) AuthorizeExternalActorEgress(ctx context.Context, request *ateapipb.AuthorizeExternalActorEgressRequest) (*ateapipb.AuthorizeExternalActorEgressResponse, error) {
	if s == nil || ctx == nil || request == nil {
		return nil, status.Error(codes.PermissionDenied, "external Actor egress is not authorized")
	}
	s.mu.RLock()
	certificateAuthority := s.actorEgressCA
	routes := s.actorEgressRoutes
	expectedPrincipal := s.actorEgressPrincipal
	s.mu.RUnlock()
	if certificateAuthority == nil || routes == nil || expectedPrincipal == "" {
		return nil, status.Error(codes.Unavailable, "external Actor egress authorization is unavailable")
	}
	caller, ok := principal.FromContext(ctx)
	if !ok || caller.Kind != principal.KindMTLS || caller.ID != expectedPrincipal {
		return nil, status.Error(codes.PermissionDenied, "external Actor egress is not authorized")
	}
	identity, routeClaim, err := certificateAuthority.VerifyExternalActorCertificate(ctx, request.GetActorCertificateChainDer())
	if err != nil || identity == nil || routeClaim == nil ||
		!resources.IsValidResourceName(identity.Atespace) || !resources.IsValidResourceName(identity.ActorName) {
		return nil, status.Error(codes.PermissionDenied, "external Actor egress is not authorized")
	}
	routeBinding, ok := routes.ResolveExternalActorEgress(routeClaim)
	if !ok {
		return nil, status.Error(codes.PermissionDenied, "external Actor egress is not authorized")
	}
	binding := actorEgressBindingSnapshotFrom(routeBinding)
	worker, err := s.actorIngressStore.GetWorker(ctx, binding.workerName)
	if err != nil || worker == nil || !actorEgressWorkerMatchesBinding(worker, binding) {
		return nil, status.Error(codes.PermissionDenied, "external Actor egress is not authorized")
	}
	assigned := worker.GetStatus().GetAssignment()
	actorRef := resources.ActorRefFromObjectRef(assigned.GetActor())
	if actorRef.Atespace != identity.Atespace || actorRef.Name != identity.ActorName || assigned.GetActorUid() != identity.ActorUid {
		return nil, status.Error(codes.PermissionDenied, "external Actor egress is not authorized")
	}
	lease, err := s.acquireActorIngressLease(ctx, actorRef)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "external Actor egress is not authorized")
	}
	defer lease.Close()
	err = routes.GuardExternalActorEgress(lease.Context(), routeClaim, func(current externalprovider.SessionWorkerBinding) error {
		if actorEgressBindingSnapshotFrom(current) != binding {
			return externalprovider.ErrExternalActorEgressRouteUnauthorized
		}
		return s.validateExternalActorEgressAssignment(lease.Context(), binding, actorRef, identity.ActorUid)
	})
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "external Actor egress is not authorized")
	}
	return &ateapipb.AuthorizeExternalActorEgressResponse{}, nil
}

func actorEgressBindingSnapshotFrom(binding externalprovider.SessionWorkerBinding) actorEgressBindingSnapshot {
	return actorEgressBindingSnapshot{
		registrationUID:   binding.RegistrationUID(),
		slotID:            binding.SlotID(),
		workerName:        binding.WorkerName(),
		workerUID:         binding.WorkerUID(),
		workerNamespace:   binding.WorkerNamespace(),
		workerPool:        binding.WorkerPool(),
		executionIdentity: binding.ExecutionIdentity(),
		localityIdentity:  binding.LocalityIdentity(),
		ownerAtespace:     binding.OwnerAtespace(),
	}
}

func (s *RPCService) validateExternalActorEgressAssignment(
	ctx context.Context,
	binding actorEgressBindingSnapshot,
	actorRef resources.ActorRef,
	actorUID string,
) error {
	worker, err := s.actorIngressStore.GetWorker(ctx, binding.workerName)
	if err != nil || worker == nil || !actorEgressWorkerMatchesBinding(worker, binding) {
		return externalprovider.ErrExternalActorEgressRouteUnauthorized
	}
	assigned := worker.GetStatus().GetAssignment()
	if resources.ActorRefFromObjectRef(assigned.GetActor()) != actorRef || assigned.GetActorUid() != actorUID {
		return externalprovider.ErrExternalActorEgressRouteUnauthorized
	}
	actor, err := s.actorIngressStore.GetActor(ctx, actorRef)
	if err != nil || actor == nil || actor.GetMetadata().GetUid() != actorUID ||
		actor.GetMetadata().GetAtespace() != actorRef.Atespace || actor.GetMetadata().GetName() != actorRef.Name ||
		actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return externalprovider.ErrExternalActorEgressRouteUnauthorized
	}
	assignment := actor.GetStatus().GetWorkerAssignment()
	if !isExternalActorIngressAssignment(assignment) || !workerMatchesActorIngress(actor, worker, assignment) ||
		!actorEgressAssignmentMatchesBinding(assignment, binding) {
		return externalprovider.ErrExternalActorEgressRouteUnauthorized
	}
	return nil
}

func actorEgressWorkerMatchesBinding(worker *ateapipb.Worker, binding actorEgressBindingSnapshot) bool {
	if worker == nil || worker.GetProvider() != ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT ||
		worker.GetMetadata().GetAtespace() != "" || worker.GetMetadata().GetName() != binding.workerName ||
		worker.GetMetadata().GetUid() != binding.workerUID || worker.GetWorkerNamespace() != binding.workerNamespace ||
		worker.GetWorkerPool() != binding.workerPool || worker.GetWorkerPod() != "" || worker.GetWorkerPodUid() != "" ||
		worker.GetNodeName() != "" || worker.GetIp() != "" || worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		return false
	}
	externalSlot := worker.GetExternalSlot()
	return externalSlot != nil && externalSlot.GetExecutionIdentity() == binding.executionIdentity &&
		externalSlot.GetLocalityIdentity() == binding.localityIdentity &&
		externalSlot.GetOwnerAtespace() == binding.ownerAtespace
}

func actorEgressAssignmentMatchesBinding(assignment *ateapipb.WorkerAssignment, binding actorEgressBindingSnapshot) bool {
	return assignment != nil && assignment.GetWorker() != nil && assignment.GetWorker().GetAtespace() == "" &&
		assignment.GetWorker().GetName() == binding.workerName && assignment.GetWorkerResourceUid() == binding.workerUID &&
		assignment.GetWorkerNamespace() == binding.workerNamespace && assignment.GetWorkerPool() == binding.workerPool &&
		assignment.GetProvider() == ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT &&
		proto.Equal(assignment.GetExternalSlot(), &ateapipb.ExternalSlotIdentity{
			ExecutionIdentity: binding.executionIdentity,
			LocalityIdentity:  binding.localityIdentity,
			OwnerAtespace:     binding.ownerAtespace,
		})
}
