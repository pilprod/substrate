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
	"errors"
	"fmt"
	"slices"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/validate/content"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	connectProtocolVersion = 2
	maxClientFrameBytes    = 1 << 20
	maxSlotLabels          = 64
	maxSandboxClassBytes   = 253
)

var (
	// ErrInvalidConnectAdmission identifies a first-frame or authenticated
	// claim which cannot be admitted. It carries no transport status.
	ErrInvalidConnectAdmission = errors.New("invalid external provider Connect admission")
)

// ConnectAdmission is an immutable, non-secret snapshot of one validated
// Connect hello and its authenticated authority. Its accessors return copies.
type ConnectAdmission struct {
	registration Registration
	generation   uint64
	slots        []AdmittedSlot
}

// prevalidatedConnectHello is the immutable, non-secret result of every
// first-frame check which does not require consuming a session credential.
// Broker.Connect can construct this before the atomic database claim, so a
// malformed hello never burns a one-time session token.
type prevalidatedConnectHello struct {
	registrationUID string
	policyDigest    string
	slots           []prevalidatedSlot
}

type prevalidatedSlot struct {
	slotID    string
	profileID string
}

// Registration returns the authenticated registration and immutable scope.
func (a *ConnectAdmission) Registration() Registration {
	return a.registration
}

// Generation returns the nonzero fencing generation assigned by PostgreSQL.
func (a *ConnectAdmission) Generation() uint64 {
	return a.generation
}

// Slots returns independent copies in ascending SlotID order.
func (a *ConnectAdmission) Slots() []AdmittedSlot {
	slots := make([]AdmittedSlot, len(a.slots))
	for index := range a.slots {
		slots[index] = a.slots[index].clone()
	}
	return slots
}

type admittedLabel struct {
	key   string
	value string
}

// AdmittedSlot is immutable provider-neutral scheduling data for one slot.
// Map and protobuf accessors allocate fresh values on every call.
type AdmittedSlot struct {
	slotID       string
	profileID    string
	sandboxClass string
	labels       []admittedLabel
	cpuMilli     int64
	memoryBytes  int64
}

// SlotID returns the registration-scoped stable slot identity.
func (s AdmittedSlot) SlotID() string {
	return s.slotID
}

// ProfileID returns the server-issued profile selected for this slot.
func (s AdmittedSlot) ProfileID() string {
	return s.profileID
}

// SandboxClass returns the opaque scheduling class.
func (s AdmittedSlot) SandboxClass() string {
	return s.sandboxClass
}

// Labels returns a fresh Kubernetes label map.
func (s AdmittedSlot) Labels() map[string]string {
	labels := make(map[string]string, len(s.labels))
	for _, label := range s.labels {
		labels[label.key] = label.value
	}
	return labels
}

// Capacity returns a fresh copy of the exact server-issued positive capacity.
func (s AdmittedSlot) Capacity() *ateapipb.WorkerCapacity {
	return &ateapipb.WorkerCapacity{
		CpuMilli:    s.cpuMilli,
		MemoryBytes: s.memoryBytes,
	}
}

func (s AdmittedSlot) clone() AdmittedSlot {
	clone := s
	clone.labels = slices.Clone(s.labels)
	return clone
}

// ValidateConnectAdmission validates an authenticated claim and the first
// client frame without claiming credentials or performing transport, storage,
// or Worker operations. Broker.Connect should call prevalidateConnectHello
// before claiming the one-time session credential, then call
// validatePrevalidatedConnectAdmission with the resulting value.
func ValidateConnectAdmission(claim SessionClaim, frame *externalproviderpb.ClientFrame) (*ConnectAdmission, error) {
	hello, err := prevalidateConnectHello(frame)
	if err != nil {
		return nil, err
	}
	return validatePrevalidatedConnectAdmission(claim, hello)
}

func prevalidateConnectHello(frame *externalproviderpb.ClientFrame) (*prevalidatedConnectHello, error) {
	if frame == nil {
		return nil, invalidConnectAdmission("frame", "is required")
	}
	if proto.Size(frame) > maxClientFrameBytes {
		return nil, invalidConnectAdmission("frame", "exceeds the 1 MiB serialized limit")
	}
	if frame.GetSessionGeneration() != 0 {
		return nil, invalidConnectAdmission("frame.session_generation", "must be zero in the first frame")
	}
	helloFrame, ok := frame.GetFrame().(*externalproviderpb.ClientFrame_Hello)
	if !ok || helloFrame.Hello == nil {
		return nil, invalidConnectAdmission("frame", "must contain only a nonnil hello")
	}
	hello := helloFrame.Hello
	if hello.GetProtocolVersion() != connectProtocolVersion {
		return nil, invalidConnectAdmission("frame.hello.protocol_version", "must equal 2")
	}
	if !IsValidIdentity(hello.GetRegistrationUid()) {
		return nil, invalidConnectAdmission("frame.hello.registration_uid", "has invalid identity syntax")
	}
	if !isCapabilityPolicyDigest(hello.GetSlotPolicyDigest()) {
		return nil, invalidConnectAdmission("frame.hello.slot_policy_digest", "must be a lowercase SHA-256 digest")
	}

	slots := hello.GetSlots()
	if len(slots) == 0 {
		return nil, invalidConnectAdmission("frame.hello.slots", "must contain at least one slot")
	}
	if len(slots) > maxSlots {
		return nil, invalidConnectAdmission("frame.hello.slots", "exceeds the protocol slot limit")
	}

	normalized := make([]prevalidatedSlot, len(slots))
	for index, slot := range slots {
		path := fmt.Sprintf("frame.hello.slots[%d]", index)
		if slot == nil {
			return nil, invalidConnectAdmission(path, "is required")
		}
		slotID := slot.GetSlotId()
		if !IsValidIdentity(slotID) {
			return nil, invalidConnectAdmission(path+".slot_id", "has invalid identity syntax")
		}
		if index > 0 {
			previous := slots[index-1].GetSlotId()
			switch {
			case slotID == previous:
				return nil, invalidConnectAdmission(path+".slot_id", "duplicates the previous slot_id")
			case slotID < previous:
				return nil, invalidConnectAdmission(path+".slot_id", "is not in ascending order")
			}
		}

		if slot.GetSandboxClass() != "" {
			return nil, invalidConnectAdmission(path+".sandbox_class", "is server-owned and must be empty")
		}
		if len(slot.GetLabels()) != 0 {
			return nil, invalidConnectAdmission(path+".labels", "are server-owned and must be empty")
		}
		profileID := slot.GetProfileId()
		if !IsValidIdentity(profileID) {
			return nil, invalidConnectAdmission(path+".profile_id", "has invalid identity syntax")
		}
		if slot.GetCapacity() != nil {
			return nil, invalidConnectAdmission(path+".capacity", "is server-owned and must be absent")
		}
		normalized[index] = prevalidatedSlot{
			slotID:    slotID,
			profileID: profileID,
		}
	}

	return &prevalidatedConnectHello{
		registrationUID: hello.GetRegistrationUid(),
		policyDigest:    hello.GetSlotPolicyDigest(),
		slots:           normalized,
	}, nil
}

func validatePrevalidatedConnectAdmission(claim SessionClaim, hello *prevalidatedConnectHello) (*ConnectAdmission, error) {
	if err := validateAdmissionClaim(claim); err != nil {
		return nil, err
	}
	if hello == nil || !IsValidIdentity(hello.registrationUID) || len(hello.slots) == 0 || len(hello.slots) > maxSlots {
		return nil, invalidConnectAdmission("prevalidated_hello", "is invalid")
	}
	if hello.registrationUID != claim.Registration.UID {
		return nil, invalidConnectAdmission("frame.hello.registration_uid", "does not match the authenticated registration")
	}
	policy := claim.Registration.Scope.SlotPolicy
	if hello.policyDigest != policy.DigestHex() {
		return nil, invalidConnectAdmission("frame.hello.slot_policy_digest", "does not match the authenticated registration")
	}
	slotLimit := claim.Registration.Scope.MaxSlots
	if uint64(len(hello.slots)) > uint64(slotLimit) {
		return nil, invalidConnectAdmission("frame.hello.slots", "exceeds the authenticated slot limit")
	}

	profileCounts := make(map[string]uint32)
	admittedSlots := make([]AdmittedSlot, len(hello.slots))
	for index, slot := range hello.slots {
		profile, found := policy.profile(slot.profileID)
		if !found {
			return nil, invalidConnectAdmission(fmt.Sprintf("frame.hello.slots[%d].profile_id", index), "is not granted by the authenticated registration")
		}
		profileCounts[profile.ProfileID]++
		if profileCounts[profile.ProfileID] > profile.MaxSlots {
			return nil, invalidConnectAdmission(fmt.Sprintf("frame.hello.slots[%d].profile_id", index), "exceeds the profile slot limit")
		}
		labels, err := normalizeAdmissionLabels(fmt.Sprintf("claim.registration.scope.slot_policy.profiles[%s].labels", profile.ProfileID), profile.Labels)
		if err != nil {
			return nil, err
		}
		admittedSlots[index] = AdmittedSlot{
			slotID:       slot.slotID,
			profileID:    profile.ProfileID,
			sandboxClass: profile.SandboxClass,
			labels:       labels,
			cpuMilli:     profile.CPUMilli,
			memoryBytes:  profile.MemoryBytes,
		}
	}
	return &ConnectAdmission{
		registration: claim.Registration,
		generation:   claim.Generation,
		slots:        admittedSlots,
	}, nil
}

func validateAdmissionClaim(claim SessionClaim) error {
	if claim.Generation == 0 {
		return invalidConnectAdmission("claim.generation", "must be nonzero")
	}
	registration := claim.Registration
	if !IsValidIdentity(registration.UID) {
		return invalidConnectAdmission("claim.registration.uid", "has invalid identity syntax")
	}
	if !IsValidIdentity(registration.EnrollmentUID) {
		return invalidConnectAdmission("claim.registration.enrollment_uid", "has invalid identity syntax")
	}
	if !resources.IsValidResourceName(registration.Scope.OwnerAtespace) {
		return invalidConnectAdmission("claim.registration.scope.owner_atespace", "is invalid")
	}
	if len(content.IsDNS1123Label(registration.Scope.WorkerNamespace)) != 0 {
		return invalidConnectAdmission("claim.registration.scope.worker_namespace", "is not a DNS-1123 label")
	}
	if len(content.IsDNS1123Subdomain(registration.Scope.WorkerPool)) != 0 {
		return invalidConnectAdmission("claim.registration.scope.worker_pool", "is not a DNS-1123 subdomain")
	}
	if registration.Scope.MaxSlots == 0 || registration.Scope.MaxSlots > uint32(maxSlots) {
		return invalidConnectAdmission("claim.registration.scope.max_slots", "must be between 1 and 256")
	}
	if err := registration.Scope.SlotPolicy.Validate(); err != nil {
		return invalidConnectAdmission("claim.registration.scope.slot_policy", "is invalid")
	}
	var grantedSlots uint64
	for _, profile := range registration.Scope.SlotPolicy.Profiles() {
		grantedSlots += uint64(profile.MaxSlots)
	}
	if grantedSlots < uint64(registration.Scope.MaxSlots) {
		return invalidConnectAdmission("claim.registration.scope.slot_policy", "grants fewer slots than max_slots")
	}
	return nil
}

func normalizeAdmissionLabels(path string, labels map[string]string) ([]admittedLabel, error) {
	if len(labels) > maxSlotLabels {
		return nil, invalidConnectAdmission(path, "exceeds 64 entries")
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	normalized := make([]admittedLabel, 0, len(keys))
	for _, key := range keys {
		if len(k8svalidation.IsQualifiedName(key)) != 0 {
			return nil, invalidConnectAdmission(path, "contains an invalid Kubernetes label key")
		}
		value := labels[key]
		if len(k8svalidation.IsValidLabelValue(value)) != 0 {
			return nil, invalidConnectAdmission(path, "contains an invalid Kubernetes label value")
		}
		normalized = append(normalized, admittedLabel{key: key, value: value})
	}
	return normalized, nil
}

func invalidConnectAdmission(path, reason string) error {
	return fmt.Errorf("%w: %s %s", ErrInvalidConnectAdmission, path, reason)
}
