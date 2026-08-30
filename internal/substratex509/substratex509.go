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

// Package substratex509 contains routines for creating and parsing x509
// certificates that embed Substrate-specific X.509 extensions communicating
// the identity of a given workload. It is modeled on the upstream Kubernetes
// component-helpers/kubernetesx509 package, but encodes extension values as
// JSON instead of ASN.1.
package substratex509

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

var (
	// GoogleSubstratePEN is the ASN.1 Private Enterprise Number arc used to
	// name X.509 extensions that communicate Substrate-specific concepts.
	GoogleSubstratePEN = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 11129, 2, 12}

	// oidPodIdentity identifies the Kubernetes PodIdentity X.509 extension specifically in substrate.
	oidPodIdentity = makeSubstrateOID(1)
	// oidActorIdentity identifies the Substrate ActorIdentity X.509 extension specifically in substrate.
	oidActorIdentity = makeSubstrateOID(2)
	// oidExternalRouteBinding binds an actor certificate to one exact live
	// external-provider route generation. It is separate from ActorIdentity so
	// existing in-cluster actor certificates retain their current semantics.
	oidExternalRouteBinding = makeSubstrateOID(3)
)

func makeSubstrateOID(subIDs ...int) asn1.ObjectIdentifier {
	base := asn1.ObjectIdentifier{}
	base = append(base, GoogleSubstratePEN...)
	base = append(base, subIDs...)
	return base
}

// PodIdentity is the Kubernetes Pod Identity of a pod, as embedded in the
// oidPodIdentity extension of its certificate.
type PodIdentity struct {
	Namespace          string
	ServiceAccountName string
	ServiceAccountUID  string
	PodName            string
	PodUID             string
	NodeName           string
	NodeUID            string
}

func AddPodIdentityToCertificate(pod *PodIdentity, template *x509.Certificate) error {
	if err := validatePodIdentity(pod); err != nil {
		return fmt.Errorf("while validating PodIdentity input: %w", err)
	}
	podIdentityBytes, err := json.Marshal(pod)
	if err != nil {
		return fmt.Errorf("while json-marshaling PodIdentity extension: %w", err)
	}

	template.ExtraExtensions = append(template.ExtraExtensions, pkix.Extension{
		Id:    oidPodIdentity,
		Value: podIdentityBytes,
	})

	return nil
}

func PodIdentityFromCertificate(cert *x509.Certificate) (*PodIdentity, error) {
	podIdentityCount := 0

	var podIdentityValue []byte
	for _, ext := range cert.Extensions {
		if ext.Id.Equal(oidPodIdentity) {
			podIdentityCount++
			podIdentityValue = ext.Value
		}
	}

	if podIdentityCount == 0 {
		return nil, nil
	}
	if podIdentityCount > 1 {
		return nil, fmt.Errorf("certificate contains multiple PodIdentity extensions")
	}

	pod := &PodIdentity{}
	if err := json.Unmarshal(podIdentityValue, pod); err != nil {
		return nil, fmt.Errorf("while json-unmarshaling PodIdentity extension: %w", err)
	}

	if err := validatePodIdentity(pod); err != nil {
		return nil, fmt.Errorf("while validating PodIdentity extension: %w", err)
	}

	return pod, nil
}

func validatePodIdentity(pod *PodIdentity) error {
	var empty []string
	if pod.Namespace == "" {
		empty = append(empty, "Namespace")
	}
	if pod.ServiceAccountName == "" {
		empty = append(empty, "ServiceAccountName")
	}
	if pod.ServiceAccountUID == "" {
		empty = append(empty, "ServiceAccountUID")
	}
	if pod.PodName == "" {
		empty = append(empty, "PodName")
	}
	if pod.PodUID == "" {
		empty = append(empty, "PodUID")
	}
	if pod.NodeName == "" {
		empty = append(empty, "NodeName")
	}
	if pod.NodeUID == "" {
		empty = append(empty, "NodeUID")
	}
	if len(empty) > 0 {
		return fmt.Errorf("empty fields: %s", strings.Join(empty, ", "))
	}
	return nil
}

// ActorIdentity is the Substrate Actor Identity of an Actor, as embedded in the
// oidActorIdentity extension of its certificate.
type ActorIdentityPurpose string

const ActorIdentityPurposeAtunnel ActorIdentityPurpose = "atunnel"

type ActorIdentity struct {
	Atespace  string
	ActorName string
	ActorUid  string
	Purpose   ActorIdentityPurpose
}

func AddActorIdentityToCertificate(actor *ActorIdentity, template *x509.Certificate) error {
	if err := validateActorIdentity(actor); err != nil {
		return fmt.Errorf("while validating ActorIdentity input: %w", err)
	}
	actorIdentityBytes, err := json.Marshal(actor)
	if err != nil {
		return fmt.Errorf("while json-marshaling ActorIdentity extension: %w", err)
	}

	template.ExtraExtensions = append(template.ExtraExtensions, pkix.Extension{
		Id:    oidActorIdentity,
		Value: actorIdentityBytes,
	})

	return nil
}

func ActorIdentityFromCertificate(cert *x509.Certificate) (*ActorIdentity, error) {
	actorIdentityCount := 0

	var actorIdentityValue []byte
	for _, ext := range cert.Extensions {
		if ext.Id.Equal(oidActorIdentity) {
			actorIdentityCount++
			actorIdentityValue = ext.Value
		}
	}

	if actorIdentityCount == 0 {
		return nil, nil
	}
	if actorIdentityCount > 1 {
		return nil, fmt.Errorf("certificate contains multiple ActorIdentity extensions")
	}

	actor := &ActorIdentity{}
	if err := json.Unmarshal(actorIdentityValue, actor); err != nil {
		return nil, fmt.Errorf("while json-unmarshaling ActorIdentity extension: %w", err)
	}

	if err := validateActorIdentity(actor); err != nil {
		return nil, fmt.Errorf("while validating ActorIdentity extension: %w", err)
	}

	return actor, nil
}

func validateActorIdentity(actor *ActorIdentity) error {
	var empty []string
	if actor.Atespace == "" {
		empty = append(empty, "Atespace")
	}
	if actor.ActorName == "" {
		empty = append(empty, "ActorName")
	}
	if actor.ActorUid == "" {
		empty = append(empty, "ActorUid")
	}
	if actor.Purpose == "" {
		empty = append(empty, "Purpose")
	}
	if len(empty) > 0 {
		return fmt.Errorf("empty fields: %s", strings.Join(empty, ", "))
	}
	if actor.Purpose != ActorIdentityPurposeAtunnel {
		return fmt.Errorf("unsupported Purpose %q", actor.Purpose)
	}
	return nil
}

// ExternalRouteBindingVersion is the only signed route-binding encoding
// currently understood by ateapi and atenet.
const ExternalRouteBindingVersion uint32 = 1

// ExternalRouteBinding is the server-derived ExternalSlot route authority
// embedded in a short-lived actor certificate. None of these fields may come
// from the CSR or provider client.
type ExternalRouteBinding struct {
	Version           uint32
	RegistrationUID   string
	SlotID            string
	WorkerUID         string
	SessionGeneration uint64
	ExecutionIdentity string
}

// AddExternalRouteBindingToCertificate appends one validated, signed route
// binding extension to template.
func AddExternalRouteBindingToCertificate(binding *ExternalRouteBinding, template *x509.Certificate) error {
	if template == nil {
		return fmt.Errorf("certificate template is required")
	}
	if err := validateExternalRouteBinding(binding); err != nil {
		return fmt.Errorf("while validating ExternalRouteBinding input: %w", err)
	}
	wire, err := json.Marshal(binding)
	if err != nil {
		return fmt.Errorf("while json-marshaling ExternalRouteBinding extension: %w", err)
	}
	template.ExtraExtensions = append(template.ExtraExtensions, pkix.Extension{
		Id:    oidExternalRouteBinding,
		Value: wire,
	})
	return nil
}

// ExternalRouteBindingFromCertificate returns the single validated route
// binding, nil when the extension is absent, and an error for duplicates or a
// malformed value.
func ExternalRouteBindingFromCertificate(cert *x509.Certificate) (*ExternalRouteBinding, error) {
	if cert == nil {
		return nil, fmt.Errorf("certificate is required")
	}
	count := 0
	var value []byte
	for _, ext := range cert.Extensions {
		if ext.Id.Equal(oidExternalRouteBinding) {
			count++
			value = ext.Value
		}
	}
	if count == 0 {
		return nil, nil
	}
	if count > 1 {
		return nil, fmt.Errorf("certificate contains multiple ExternalRouteBinding extensions")
	}
	binding := &ExternalRouteBinding{}
	if err := json.Unmarshal(value, binding); err != nil {
		return nil, fmt.Errorf("while json-unmarshaling ExternalRouteBinding extension: %w", err)
	}
	if err := validateExternalRouteBinding(binding); err != nil {
		return nil, fmt.Errorf("while validating ExternalRouteBinding extension: %w", err)
	}
	return binding, nil
}

func validateExternalRouteBinding(binding *ExternalRouteBinding) error {
	if binding == nil {
		return fmt.Errorf("binding is required")
	}
	if binding.Version != ExternalRouteBindingVersion {
		return fmt.Errorf("unsupported Version %d", binding.Version)
	}
	if !validExternalRouteIdentity(binding.RegistrationUID) {
		return fmt.Errorf("invalid RegistrationUID")
	}
	if !validExternalRouteIdentity(binding.SlotID) {
		return fmt.Errorf("invalid SlotID")
	}
	workerUID, err := uuid.Parse(binding.WorkerUID)
	if err != nil || workerUID.String() != binding.WorkerUID {
		return fmt.Errorf("invalid WorkerUID")
	}
	if binding.SessionGeneration == 0 {
		return fmt.Errorf("SessionGeneration must be nonzero")
	}
	if !validExternalRouteIdentity(binding.ExecutionIdentity) {
		return fmt.Errorf("invalid ExecutionIdentity")
	}
	return nil
}

func validExternalRouteIdentity(value string) bool {
	if len(value) == 0 || len(value) > 253 || !asciiAlphaNumeric(value[0]) || !asciiAlphaNumeric(value[len(value)-1]) {
		return false
	}
	for index := 1; index+1 < len(value); index++ {
		char := value[index]
		if !asciiAlphaNumeric(char) && char != '.' && char != '_' && char != '~' && char != '-' {
			return false
		}
	}
	return true
}

func asciiAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}
