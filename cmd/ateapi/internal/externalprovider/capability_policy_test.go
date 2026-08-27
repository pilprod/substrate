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
	"maps"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func testSlotProfiles() []SlotProfile {
	return []SlotProfile{
		{
			ProfileID:    "large",
			SandboxClass: "microvm",
			Labels:       map[string]string{"security.example/isolation": "strong", "region": "south"},
			MaxSlots:     2,
			CPUMilli:     8_000,
			MemoryBytes:  32 << 30,
		},
		{
			ProfileID:    "standard",
			SandboxClass: "gvisor",
			Labels:       map[string]string{"region": "south", "runtime.example/family": "coding"},
			MaxSlots:     8,
			CPUMilli:     4_000,
			MemoryBytes:  16 << 30,
		},
	}
}

func TestSlotCapabilityPolicyCanonicalizationAndDigestAreStable(t *testing.T) {
	profiles := testSlotProfiles()
	first, err := NewSlotCapabilityPolicy(SlotCapabilityPolicyVersion, profiles)
	if err != nil {
		t.Fatal(err)
	}
	profiles[0], profiles[1] = profiles[1], profiles[0]
	profiles[0].Labels = map[string]string{"runtime.example/family": "coding", "region": "south"}
	profiles[1].Labels = map[string]string{"region": "south", "security.example/isolation": "strong"}
	second, err := NewSlotCapabilityPolicy(SlotCapabilityPolicyVersion, profiles)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("equivalent policies differ:\n%s\n%s", first.CanonicalBytes(), second.CanonicalBytes())
	}
	if got, want := first.DigestHex(), "ff18c3f6ec07ab5d2f2861a788cb556a7ace75840feafcde72bbf3d63e10cde1"; got != want {
		t.Fatalf("slot policy digest = %q, want %q", got, want)
	}
	if !strings.HasPrefix(string(first.CanonicalBytes()), `{"version":1,"profiles":[{"profileId":"large"`) {
		t.Fatalf("canonical policy order = %s", first.CanonicalBytes())
	}
}

func TestSlotCapabilityPolicyRestorationRejectsTamperingAndNoncanonicalData(t *testing.T) {
	policy, err := NewSlotCapabilityPolicy(SlotCapabilityPolicyVersion, testSlotProfiles())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := RestoreSlotCapabilityPolicy(policy.CanonicalBytes(), policy.DigestBytes())
	if err != nil {
		t.Fatalf("RestoreSlotCapabilityPolicy() error = %v", err)
	}
	if restored != policy {
		t.Fatal("restored policy differs")
	}

	tests := []struct {
		name      string
		canonical []byte
		digest    []byte
	}{
		{name: "missing policy"},
		{name: "digest mismatch", canonical: policy.CanonicalBytes(), digest: make([]byte, 32)},
		{name: "trailing whitespace", canonical: append(policy.CanonicalBytes(), '\n'), digest: policy.DigestBytes()},
		{name: "unknown field", canonical: []byte(`{"version":1,"profiles":[],"unknown":true}`), digest: policy.DigestBytes()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := RestoreSlotCapabilityPolicy(test.canonical, test.digest); !errors.Is(err, ErrInvalidSlotCapabilityPolicy) {
				t.Fatalf("RestoreSlotCapabilityPolicy() error = %v", err)
			}
		})
	}
}

func TestSlotCapabilityPolicyRejectsInvalidOrCollidingAuthority(t *testing.T) {
	valid := testSlotProfiles()[0]
	tests := []struct {
		name     string
		version  uint32
		profiles []SlotProfile
	}{
		{name: "unknown version", version: 2, profiles: []SlotProfile{valid}},
		{name: "empty profiles", version: 1},
		{name: "duplicate profile", version: 1, profiles: []SlotProfile{valid, valid}},
		{name: "invalid profile ID", version: 1, profiles: []SlotProfile{{ProfileID: "/bad", SandboxClass: "gvisor", MaxSlots: 1, CPUMilli: 1, MemoryBytes: 1}}},
		{name: "launcher native as sandbox", version: 1, profiles: []SlotProfile{{ProfileID: "p", SandboxClass: "native", MaxSlots: 1, CPUMilli: 1, MemoryBytes: 1}}},
		{name: "launcher docker as sandbox", version: 1, profiles: []SlotProfile{{ProfileID: "p", SandboxClass: "docker", MaxSlots: 1, CPUMilli: 1, MemoryBytes: 1}}},
		{name: "invalid label", version: 1, profiles: []SlotProfile{{ProfileID: "p", SandboxClass: "gvisor", Labels: map[string]string{"bad key": "value"}, MaxSlots: 1, CPUMilli: 1, MemoryBytes: 1}}},
		{name: "zero profile slots", version: 1, profiles: []SlotProfile{{ProfileID: "p", SandboxClass: "gvisor", CPUMilli: 1, MemoryBytes: 1}}},
		{name: "excess profile slots", version: 1, profiles: []SlotProfile{{ProfileID: "p", SandboxClass: "gvisor", MaxSlots: maxSlots + 1, CPUMilli: 1, MemoryBytes: 1}}},
		{name: "zero CPU ceiling", version: 1, profiles: []SlotProfile{{ProfileID: "p", SandboxClass: "gvisor", MaxSlots: 1, MemoryBytes: 1}}},
		{name: "zero memory ceiling", version: 1, profiles: []SlotProfile{{ProfileID: "p", SandboxClass: "gvisor", MaxSlots: 1, CPUMilli: 1}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewSlotCapabilityPolicy(test.version, test.profiles); !errors.Is(err, ErrInvalidSlotCapabilityPolicy) {
				t.Fatalf("NewSlotCapabilityPolicy() error = %v", err)
			}
		})
	}

	tooMany := make([]SlotProfile, maxSlotProfiles+1)
	for index := range tooMany {
		tooMany[index] = SlotProfile{ProfileID: fmt.Sprintf("p-%03d", index), SandboxClass: "gvisor", MaxSlots: 1, CPUMilli: 1, MemoryBytes: 1}
	}
	if _, err := NewSlotCapabilityPolicy(1, tooMany); !errors.Is(err, ErrInvalidSlotCapabilityPolicy) {
		t.Fatalf("too many profiles error = %v", err)
	}
}

func TestSlotCapabilityPolicyDoesNotAliasInputsOrAccessors(t *testing.T) {
	profiles := testSlotProfiles()
	policy, err := NewSlotCapabilityPolicy(SlotCapabilityPolicyVersion, profiles)
	if err != nil {
		t.Fatal(err)
	}
	profiles[0].SandboxClass = "mutated"
	profiles[0].Labels["region"] = "mutated"

	first := policy.Profiles()
	first[0].SandboxClass = "mutated"
	first[0].Labels["region"] = "mutated"
	second := policy.Profiles()
	if second[0].SandboxClass != "microvm" || second[0].Labels["region"] != "south" {
		t.Fatalf("policy aliased mutable data: %+v", second[0])
	}

	pb := policy.Proto()
	pb.Profiles[0].Labels["region"] = "mutated"
	pb.Profiles[0].Capacity.CpuMilli = 1
	secondPB := policy.Proto()
	if !maps.Equal(secondPB.Profiles[0].Labels, map[string]string{"region": "south", "security.example/isolation": "strong"}) ||
		secondPB.Profiles[0].Capacity.CpuMilli != 8_000 {
		t.Fatalf("Proto() aliased policy: %v", secondPB)
	}
	if !proto.Equal(policy.Proto(), secondPB) {
		t.Fatal("Proto() is not deterministic")
	}
}

func TestScopeRejectsPolicyWhoseProfilesGrantTooFewSlots(t *testing.T) {
	policy, err := NewSlotCapabilityPolicy(SlotCapabilityPolicyVersion, []SlotProfile{{
		ProfileID: "standard", SandboxClass: "gvisor", MaxSlots: 1, CPUMilli: 1_000, MemoryBytes: 1 << 30,
	}})
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{
		OwnerAtespace: "tenant-a", WorkerNamespace: "workers", WorkerPool: "pool-a",
		MaxSlots: 2, SlotPolicy: policy,
	}
	if err := scope.Validate(); err == nil || !strings.Contains(err.Error(), "fewer than max slots") {
		t.Fatalf("Scope.Validate() error = %v, want insufficient policy grant", err)
	}
}
