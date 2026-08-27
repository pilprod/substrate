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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
)

const (
	// SlotCapabilityPolicyVersion is the only canonical policy version this
	// binary understands. Unknown versions fail closed.
	SlotCapabilityPolicyVersion = 1
	maxSlotProfiles             = 64
	maxCanonicalPolicyBytes     = 2 << 20
	capabilityPolicyDigestBytes = sha256.Size
	capabilityPolicyDigestHex   = sha256.Size * 2
	capabilityPolicyDomain      = "agent-substrate/external-provider/slot-capability-policy/v1\x00"
)

// ErrInvalidSlotCapabilityPolicy reports malformed or noncanonical authority.
var ErrInvalidSlotCapabilityPolicy = errors.New("invalid external provider slot capability policy")

// SlotProfile is operator-issued authority for one kind of external slot.
// Launcher and model selection stay outside this provider-neutral contract.
type SlotProfile struct {
	ProfileID    string
	SandboxClass string
	Labels       map[string]string
	MaxSlots     uint32
	CPUMilli     int64
	MemoryBytes  int64
}

// SlotCapabilityPolicy is an immutable, comparable value suitable for Scope.
// The private canonical representation prevents map/slice aliasing after an
// enrollment has been issued.
type SlotCapabilityPolicy struct {
	canonical string
	digest    [capabilityPolicyDigestBytes]byte
}

type canonicalSlotCapabilityPolicy struct {
	Version  uint32                 `json:"version"`
	Profiles []canonicalSlotProfile `json:"profiles"`
}

type canonicalSlotProfile struct {
	ProfileID    string           `json:"profileId"`
	SandboxClass string           `json:"sandboxClass"`
	Labels       []canonicalLabel `json:"labels"`
	MaxSlots     uint32           `json:"maxSlots"`
	CPUMilli     int64            `json:"cpuMilli"`
	MemoryBytes  int64            `json:"memoryBytes"`
}

type canonicalLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NewSlotCapabilityPolicy validates and canonicalizes a server-owned profile
// grant. Input order and Go map iteration do not affect its digest.
func NewSlotCapabilityPolicy(version uint32, profiles []SlotProfile) (SlotCapabilityPolicy, error) {
	if version != SlotCapabilityPolicyVersion {
		return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy("version", "must equal 1")
	}
	if len(profiles) == 0 || len(profiles) > maxSlotProfiles {
		return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy("profiles", "must contain between 1 and 64 entries")
	}

	canonicalProfiles := make([]canonicalSlotProfile, len(profiles))
	for index, profile := range profiles {
		path := fmt.Sprintf("profiles[%d]", index)
		if !IsValidIdentity(profile.ProfileID) {
			return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy(path+".profile_id", "has invalid identity syntax")
		}
		if !utf8.ValidString(profile.SandboxClass) || profile.SandboxClass == "" || len(profile.SandboxClass) > maxSandboxClassBytes {
			return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy(path+".sandbox_class", "must contain 1..253 valid UTF-8 bytes")
		}
		switch strings.ToLower(profile.SandboxClass) {
		case "native", "docker":
			return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy(path+".sandbox_class", "must describe isolation, not a launcher")
		}
		if profile.MaxSlots == 0 || profile.MaxSlots > maxSlots {
			return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy(path+".max_slots", "must be between 1 and 256")
		}
		if profile.CPUMilli <= 0 {
			return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy(path+".cpu_milli", "must be positive")
		}
		if profile.MemoryBytes <= 0 {
			return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy(path+".memory_bytes", "must be positive")
		}

		labels, err := normalizeAdmissionLabels(path+".labels", profile.Labels)
		if err != nil {
			return SlotCapabilityPolicy{}, fmt.Errorf("%w: %v", ErrInvalidSlotCapabilityPolicy, err)
		}
		canonicalLabels := make([]canonicalLabel, len(labels))
		for labelIndex, label := range labels {
			canonicalLabels[labelIndex] = canonicalLabel{Key: label.key, Value: label.value}
		}
		canonicalProfiles[index] = canonicalSlotProfile{
			ProfileID:    profile.ProfileID,
			SandboxClass: profile.SandboxClass,
			Labels:       canonicalLabels,
			MaxSlots:     profile.MaxSlots,
			CPUMilli:     profile.CPUMilli,
			MemoryBytes:  profile.MemoryBytes,
		}
	}
	slices.SortFunc(canonicalProfiles, func(left, right canonicalSlotProfile) int {
		return strings.Compare(left.ProfileID, right.ProfileID)
	})
	for index := 1; index < len(canonicalProfiles); index++ {
		if canonicalProfiles[index-1].ProfileID == canonicalProfiles[index].ProfileID {
			return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy("profiles", "contains a duplicate profile_id")
		}
	}

	canonical, err := json.Marshal(canonicalSlotCapabilityPolicy{
		Version:  version,
		Profiles: canonicalProfiles,
	})
	if err != nil {
		return SlotCapabilityPolicy{}, fmt.Errorf("%w: canonical encoding: %v", ErrInvalidSlotCapabilityPolicy, err)
	}
	if len(canonical) > maxCanonicalPolicyBytes {
		return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy("canonical", "exceeds 2 MiB")
	}
	digest := digestCapabilityPolicy(canonical)
	return SlotCapabilityPolicy{canonical: string(canonical), digest: digest}, nil
}

// RestoreSlotCapabilityPolicy verifies the exact canonical bytes and digest
// loaded from persistence. Semantically equivalent but noncanonical encodings
// are rejected so every replica computes one stable registration authority.
func RestoreSlotCapabilityPolicy(canonical, digest []byte) (SlotCapabilityPolicy, error) {
	if len(canonical) == 0 || len(canonical) > maxCanonicalPolicyBytes || len(digest) != capabilityPolicyDigestBytes {
		return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy("persistence", "canonical policy and SHA-256 digest are required")
	}
	var wire canonicalSlotCapabilityPolicy
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy("persistence", "canonical policy is malformed")
	}
	profiles := make([]SlotProfile, len(wire.Profiles))
	for index, profile := range wire.Profiles {
		labels := make(map[string]string, len(profile.Labels))
		for _, label := range profile.Labels {
			if _, duplicate := labels[label.Key]; duplicate {
				return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy("persistence", "canonical policy contains a duplicate label")
			}
			labels[label.Key] = label.Value
		}
		profiles[index] = SlotProfile{
			ProfileID:    profile.ProfileID,
			SandboxClass: profile.SandboxClass,
			Labels:       labels,
			MaxSlots:     profile.MaxSlots,
			CPUMilli:     profile.CPUMilli,
			MemoryBytes:  profile.MemoryBytes,
		}
	}
	rebuilt, err := NewSlotCapabilityPolicy(wire.Version, profiles)
	if err != nil {
		return SlotCapabilityPolicy{}, err
	}
	if !bytes.Equal(canonical, rebuilt.CanonicalBytes()) || !bytes.Equal(digest, rebuilt.digest[:]) {
		return SlotCapabilityPolicy{}, invalidSlotCapabilityPolicy("persistence", "canonical policy or digest does not match")
	}
	return rebuilt, nil
}

// Validate checks that this immutable value has a canonical representation
// and matching digest.
func (p SlotCapabilityPolicy) Validate() error {
	_, err := RestoreSlotCapabilityPolicy([]byte(p.canonical), p.digest[:])
	return err
}

// CanonicalBytes returns a caller-owned copy for persistence.
func (p SlotCapabilityPolicy) CanonicalBytes() []byte {
	return []byte(p.canonical)
}

// DigestBytes returns a caller-owned copy for persistence.
func (p SlotCapabilityPolicy) DigestBytes() []byte {
	return append([]byte(nil), p.digest[:]...)
}

// DigestHex returns the lowercase public digest used by ConnectHello.
func (p SlotCapabilityPolicy) DigestHex() string {
	return hex.EncodeToString(p.digest[:])
}

// String renders only the public digest, not the potentially large profile
// document.
func (p SlotCapabilityPolicy) String() string {
	return fmt.Sprintf("SlotCapabilityPolicy{Digest:%q}", p.DigestHex())
}

// GoString renders only the public digest.
func (p SlotCapabilityPolicy) GoString() string { return p.String() }

// Profiles returns independent copies sorted by profile ID.
func (p SlotCapabilityPolicy) Profiles() []SlotProfile {
	var wire canonicalSlotCapabilityPolicy
	if json.Unmarshal([]byte(p.canonical), &wire) != nil {
		return nil
	}
	profiles := make([]SlotProfile, len(wire.Profiles))
	for index, profile := range wire.Profiles {
		labels := make(map[string]string, len(profile.Labels))
		for _, label := range profile.Labels {
			labels[label.Key] = label.Value
		}
		profiles[index] = SlotProfile{
			ProfileID:    profile.ProfileID,
			SandboxClass: profile.SandboxClass,
			Labels:       labels,
			MaxSlots:     profile.MaxSlots,
			CPUMilli:     profile.CPUMilli,
			MemoryBytes:  profile.MemoryBytes,
		}
	}
	return profiles
}

func (p SlotCapabilityPolicy) profile(profileID string) (SlotProfile, bool) {
	profiles := p.Profiles()
	index, found := slices.BinarySearchFunc(profiles, profileID, func(profile SlotProfile, candidate string) int {
		return strings.Compare(profile.ProfileID, candidate)
	})
	if !found {
		return SlotProfile{}, false
	}
	profile := profiles[index]
	profile.Labels = maps.Clone(profile.Labels)
	return profile, true
}

// Proto returns a caller-owned public representation suitable for Enroll and
// MintSessionToken responses.
func (p SlotCapabilityPolicy) Proto() *externalproviderpb.SlotCapabilityPolicy {
	profiles := p.Profiles()
	if len(profiles) == 0 {
		return nil
	}
	result := &externalproviderpb.SlotCapabilityPolicy{
		Version:  SlotCapabilityPolicyVersion,
		Digest:   p.DigestHex(),
		Profiles: make([]*externalproviderpb.SlotProfile, len(profiles)),
	}
	for index, profile := range profiles {
		result.Profiles[index] = &externalproviderpb.SlotProfile{
			ProfileId:    profile.ProfileID,
			SandboxClass: profile.SandboxClass,
			Labels:       maps.Clone(profile.Labels),
			MaxSlots:     profile.MaxSlots,
			Capacity: &ateapipb.WorkerCapacity{
				CpuMilli:    profile.CPUMilli,
				MemoryBytes: profile.MemoryBytes,
			},
		}
	}
	return result
}

func digestCapabilityPolicy(canonical []byte) [capabilityPolicyDigestBytes]byte {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(capabilityPolicyDomain))
	_, _ = hasher.Write(canonical)
	var digest [capabilityPolicyDigestBytes]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

func isCapabilityPolicyDigest(value string) bool {
	if len(value) != capabilityPolicyDigestHex {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}

func invalidSlotCapabilityPolicy(path, reason string) error {
	return fmt.Errorf("%w: %s %s", ErrInvalidSlotCapabilityPolicy, path, reason)
}
