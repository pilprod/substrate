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
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func validSessionClaim(scopeMaxSlots uint32) SessionClaim {
	policy, err := NewSlotCapabilityPolicy(SlotCapabilityPolicyVersion, []SlotProfile{{
		ProfileID:    "standard",
		SandboxClass: "gvisor",
		Labels:       map[string]string{"region": "south", "topology.example/zone": "zone-a"},
		MaxSlots:     maxSlots,
		CPUMilli:     2_000,
		MemoryBytes:  4 << 30,
	}})
	if err != nil {
		panic(err)
	}
	return SessionClaim{
		Registration: Registration{
			UID:           "registration-a",
			EnrollmentUID: "enrollment-a",
			Scope: Scope{
				OwnerAtespace:   "tenant-a",
				WorkerNamespace: "workers",
				WorkerPool:      "pool-a",
				MaxSlots:        scopeMaxSlots,
				SlotPolicy:      policy,
			},
		},
		Generation: 7,
	}
}

func validExternalSlot(id string) *externalproviderpb.ExternalSlot {
	return &externalproviderpb.ExternalSlot{
		SlotId:    id,
		ProfileId: "standard",
	}
}

func validClientFrame() *externalproviderpb.ClientFrame {
	claim := validSessionClaim(1)
	return &externalproviderpb.ClientFrame{
		Frame: &externalproviderpb.ClientFrame_Hello{Hello: &externalproviderpb.ConnectHello{
			RegistrationUid:  "registration-a",
			ProtocolVersion:  connectProtocolVersion,
			SlotPolicyDigest: claim.Registration.Scope.SlotPolicy.DigestHex(),
			Slots:            []*externalproviderpb.ExternalSlot{validExternalSlot("slot-a")},
		}},
	}
}

func cloneClientFrame(frame *externalproviderpb.ClientFrame) *externalproviderpb.ClientFrame {
	return proto.Clone(frame).(*externalproviderpb.ClientFrame)
}

func requireInvalidAdmission(t *testing.T, claim SessionClaim, frame *externalproviderpb.ClientFrame, path string) {
	t.Helper()
	_, err := ValidateConnectAdmission(claim, frame)
	if !errors.Is(err, ErrInvalidConnectAdmission) {
		t.Fatalf("ValidateConnectAdmission() error = %v, want ErrInvalidConnectAdmission", err)
	}
	if path != "" && !strings.Contains(err.Error(), path) {
		t.Fatalf("ValidateConnectAdmission() error = %q, want path %q", err, path)
	}
}

func TestValidateConnectAdmissionNormalizesHello(t *testing.T) {
	claim := validSessionClaim(4)
	frame := validClientFrame()
	admission, err := ValidateConnectAdmission(claim, frame)
	if err != nil {
		t.Fatalf("ValidateConnectAdmission() error = %v", err)
	}
	if got := admission.Registration(); got != claim.Registration {
		t.Errorf("Registration() = %+v, want %+v", got, claim.Registration)
	}
	if got := admission.Generation(); got != claim.Generation {
		t.Errorf("Generation() = %d, want %d", got, claim.Generation)
	}
	slots := admission.Slots()
	if len(slots) != 1 {
		t.Fatalf("Slots() length = %d, want 1", len(slots))
	}
	if got := slots[0].SlotID(); got != "slot-a" {
		t.Errorf("SlotID() = %q, want slot-a", got)
	}
	if got := slots[0].ProfileID(); got != "standard" {
		t.Errorf("ProfileID() = %q, want standard", got)
	}
	if got := slots[0].SandboxClass(); got != "gvisor" {
		t.Errorf("SandboxClass() = %q, want gvisor", got)
	}
	wantLabels := map[string]string{"region": "south", "topology.example/zone": "zone-a"}
	if got := slots[0].Labels(); !maps.Equal(got, wantLabels) {
		t.Errorf("Labels() = %v, want %v", got, wantLabels)
	}
	if got, want := slots[0].Capacity(), (&ateapipb.WorkerCapacity{CpuMilli: 2_000, MemoryBytes: 4 << 30}); !proto.Equal(got, want) {
		t.Errorf("Capacity() = %v, want %v", got, want)
	}
	if got := []string{slots[0].labels[0].key, slots[0].labels[1].key}; !slices.Equal(got, []string{"region", "topology.example/zone"}) {
		t.Errorf("normalized label order = %v, want [region topology.example/zone]", got)
	}
}

func TestPrevalidateConnectHelloSeparatesCredentialFreeChecks(t *testing.T) {
	frame := validClientFrame()
	frame.GetHello().Slots = []*externalproviderpb.ExternalSlot{validExternalSlot("slot-a"), validExternalSlot("slot-b")}
	prevalidated, err := prevalidateConnectHello(frame)
	if err != nil {
		t.Fatalf("prevalidateConnectHello() error = %v", err)
	}

	// The prevalidated value owns its normalized input and can safely survive
	// while Connect performs the one-time credential claim.
	frame.GetHello().RegistrationUid = "registration-mutated"
	frame.GetHello().Slots[0].SlotId = "slot-mutated"
	frame.GetHello().Slots[0].ProfileId = "mutated"
	claim := validSessionClaim(2)
	admission, err := validatePrevalidatedConnectAdmission(claim, prevalidated)
	if err != nil {
		t.Fatalf("validatePrevalidatedConnectAdmission() error = %v", err)
	}
	if got := admission.Slots()[0].SlotID(); got != "slot-a" {
		t.Fatalf("prevalidated slot ID = %q, want slot-a", got)
	}

	claim.Registration.Scope.MaxSlots = 1
	if _, err := validatePrevalidatedConnectAdmission(claim, prevalidated); !errors.Is(err, ErrInvalidConnectAdmission) || !strings.Contains(err.Error(), "authenticated slot limit") {
		t.Fatalf("slot authority error = %v, want authenticated slot limit", err)
	}
	if _, err := validatePrevalidatedConnectAdmission(validSessionClaim(2), nil); !errors.Is(err, ErrInvalidConnectAdmission) {
		t.Fatalf("nil prevalidated hello error = %v, want ErrInvalidConnectAdmission", err)
	}
}

func TestValidateConnectAdmissionRejectsInvalidClaimAndFirstFrame(t *testing.T) {
	baseClaim := validSessionClaim(2)
	baseFrame := validClientFrame()

	t.Run("nil frame", func(t *testing.T) {
		requireInvalidAdmission(t, baseClaim, nil, "frame")
	})
	t.Run("exact serialized limit", func(t *testing.T) {
		frame := cloneClientFrame(baseFrame)
		padFrameToSerializedSize(t, frame, maxClientFrameBytes)
		if _, err := ValidateConnectAdmission(baseClaim, frame); err != nil {
			t.Fatalf("ValidateConnectAdmission(exact 1 MiB frame) error = %v", err)
		}
	})
	t.Run("oversized serialized frame", func(t *testing.T) {
		frame := cloneClientFrame(baseFrame)
		// Field 15, varint zero, is valid unknown protobuf wire data. Repeating it
		// makes the serialized message exceed the frame limit without relying on
		// another admission rule.
		frame.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x78, 0x00}, maxClientFrameBytes/2+1))
		if size := proto.Size(frame); size <= maxClientFrameBytes {
			t.Fatalf("oversized test frame size = %d", size)
		}
		requireInvalidAdmission(t, baseClaim, frame, "frame")
	})

	claimCases := []struct {
		name string
		path string
		edit func(*SessionClaim)
	}{
		{name: "zero claim generation", path: "claim.generation", edit: func(claim *SessionClaim) { claim.Generation = 0 }},
		{name: "invalid registration UID", path: "claim.registration.uid", edit: func(claim *SessionClaim) { claim.Registration.UID = "/invalid" }},
		{name: "invalid enrollment UID", path: "claim.registration.enrollment_uid", edit: func(claim *SessionClaim) { claim.Registration.EnrollmentUID = "/invalid" }},
		{name: "invalid owner atespace", path: "claim.registration.scope.owner_atespace", edit: func(claim *SessionClaim) { claim.Registration.Scope.OwnerAtespace = "Bad_Name" }},
		{name: "invalid worker namespace", path: "claim.registration.scope.worker_namespace", edit: func(claim *SessionClaim) { claim.Registration.Scope.WorkerNamespace = "Bad_Name" }},
		{name: "invalid worker pool", path: "claim.registration.scope.worker_pool", edit: func(claim *SessionClaim) { claim.Registration.Scope.WorkerPool = "Bad_Name" }},
		{name: "zero slot authority", path: "claim.registration.scope.max_slots", edit: func(claim *SessionClaim) { claim.Registration.Scope.MaxSlots = 0 }},
		{name: "excessive slot authority", path: "claim.registration.scope.max_slots", edit: func(claim *SessionClaim) { claim.Registration.Scope.MaxSlots = maxSlots + 1 }},
	}
	for _, test := range claimCases {
		t.Run(test.name, func(t *testing.T) {
			claim := baseClaim
			test.edit(&claim)
			requireInvalidAdmission(t, claim, cloneClientFrame(baseFrame), test.path)
		})
	}

	frameCases := []struct {
		name string
		path string
		edit func(*externalproviderpb.ClientFrame)
	}{
		{name: "nonzero first-frame generation", path: "frame.session_generation", edit: func(frame *externalproviderpb.ClientFrame) { frame.SessionGeneration = 1 }},
		{name: "missing frame member", path: "frame", edit: func(frame *externalproviderpb.ClientFrame) { frame.Frame = nil }},
		{name: "nil hello", path: "frame", edit: func(frame *externalproviderpb.ClientFrame) { frame.Frame = &externalproviderpb.ClientFrame_Hello{} }},
		{name: "protocol zero", path: "protocol_version", edit: func(frame *externalproviderpb.ClientFrame) { frame.GetHello().ProtocolVersion = 0 }},
		{name: "unsupported protocol", path: "protocol_version", edit: func(frame *externalproviderpb.ClientFrame) { frame.GetHello().ProtocolVersion = 3 }},
		{name: "registration mismatch", path: "registration_uid", edit: func(frame *externalproviderpb.ClientFrame) { frame.GetHello().RegistrationUid = "registration-b" }},
		{name: "no slots", path: "frame.hello.slots", edit: func(frame *externalproviderpb.ClientFrame) { frame.GetHello().Slots = nil }},
		{name: "over authenticated slot limit", path: "frame.hello.slots", edit: func(frame *externalproviderpb.ClientFrame) {
			frame.GetHello().Slots = []*externalproviderpb.ExternalSlot{validExternalSlot("slot-a"), validExternalSlot("slot-b"), validExternalSlot("slot-c")}
		}},
	}
	for _, test := range frameCases {
		t.Run(test.name, func(t *testing.T) {
			frame := cloneClientFrame(baseFrame)
			test.edit(frame)
			requireInvalidAdmission(t, baseClaim, frame, test.path)
		})
	}
}

func padFrameToSerializedSize(t *testing.T, frame *externalproviderpb.ClientFrame, target int) {
	t.Helper()
	base := proto.Size(frame)
	for payloadBytes := target - base - 16; payloadBytes <= target-base; payloadBytes++ {
		if payloadBytes < 0 {
			continue
		}
		unknown := protowire.AppendTag(nil, 15, protowire.BytesType)
		unknown = protowire.AppendBytes(unknown, make([]byte, payloadBytes))
		frame.ProtoReflect().SetUnknown(unknown)
		if proto.Size(frame) == target {
			return
		}
	}
	t.Fatalf("could not pad frame from %d to %d serialized bytes", base, target)
}

func TestValidateConnectAdmissionRequiresHelloOnly(t *testing.T) {
	claim := validSessionClaim(1)
	tests := []struct {
		name  string
		frame func() *externalproviderpb.ClientFrame
	}{
		{name: "open", frame: func() *externalproviderpb.ClientFrame {
			return &externalproviderpb.ClientFrame{Frame: &externalproviderpb.ClientFrame_Open{Open: &externalproviderpb.OpenChannel{}}}
		}},
		{name: "open ack", frame: func() *externalproviderpb.ClientFrame {
			return &externalproviderpb.ClientFrame{Frame: &externalproviderpb.ClientFrame_OpenAck{OpenAck: &externalproviderpb.OpenChannelAck{}}}
		}},
		{name: "data", frame: func() *externalproviderpb.ClientFrame {
			return &externalproviderpb.ClientFrame{Frame: &externalproviderpb.ClientFrame_Data{Data: &externalproviderpb.ChannelData{}}}
		}},
		{name: "half close", frame: func() *externalproviderpb.ClientFrame {
			return &externalproviderpb.ClientFrame{Frame: &externalproviderpb.ClientFrame_HalfClose{HalfClose: &externalproviderpb.HalfCloseChannel{}}}
		}},
		{name: "reset", frame: func() *externalproviderpb.ClientFrame {
			return &externalproviderpb.ClientFrame{Frame: &externalproviderpb.ClientFrame_Reset_{Reset_: &externalproviderpb.ResetChannel{}}}
		}},
		{name: "heartbeat", frame: func() *externalproviderpb.ClientFrame {
			return &externalproviderpb.ClientFrame{Frame: &externalproviderpb.ClientFrame_Heartbeat{Heartbeat: &externalproviderpb.Heartbeat{}}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireInvalidAdmission(t, claim, test.frame(), "frame")
		})
	}
}

func TestValidateConnectAdmissionSlotCountBoundaries(t *testing.T) {
	makeSlots := func(count int) []*externalproviderpb.ExternalSlot {
		slots := make([]*externalproviderpb.ExternalSlot, count)
		for index := range slots {
			slots[index] = validExternalSlot(fmt.Sprintf("slot-%03d", index))
		}
		return slots
	}

	frame := validClientFrame()
	frame.GetHello().Slots = makeSlots(maxSlots)
	admission, err := ValidateConnectAdmission(validSessionClaim(maxSlots), frame)
	if err != nil {
		t.Fatalf("ValidateConnectAdmission(256 slots) error = %v", err)
	}
	if got := len(admission.Slots()); got != maxSlots {
		t.Errorf("admitted slots = %d, want %d", got, maxSlots)
	}

	frame = validClientFrame()
	frame.GetHello().Slots = makeSlots(maxSlots + 1)
	requireInvalidAdmission(t, validSessionClaim(maxSlots), frame, "frame.hello.slots")
}

func TestValidateConnectAdmissionSlotRules(t *testing.T) {
	claim := validSessionClaim(2)
	tests := []struct {
		name string
		path string
		edit func(*externalproviderpb.ConnectHello)
	}{
		{name: "nil slot", path: "slots[0]", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0] = nil }},
		{name: "empty slot ID", path: "slot_id", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].SlotId = "" }},
		{name: "leading punctuation", path: "slot_id", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].SlotId = "-slot" }},
		{name: "trailing punctuation", path: "slot_id", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].SlotId = "slot-" }},
		{name: "invalid slot character", path: "slot_id", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].SlotId = "slot/a" }},
		{name: "non-ASCII slot ID", path: "slot_id", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].SlotId = "slót" }},
		{name: "long slot ID", path: "slot_id", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].SlotId = strings.Repeat("a", 254) }},
		{name: "duplicate slot", path: "slot_id", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots = []*externalproviderpb.ExternalSlot{validExternalSlot("slot-a"), validExternalSlot("slot-a")}
		}},
		{name: "unsorted slots", path: "slot_id", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots = []*externalproviderpb.ExternalSlot{validExternalSlot("slot-b"), validExternalSlot("slot-a")}
		}},
		{name: "invalid UTF-8 sandbox", path: "sandbox_class", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].SandboxClass = string([]byte{0xff}) }},
		{name: "long sandbox", path: "sandbox_class", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].SandboxClass = strings.Repeat("é", 127) }},
		{name: "too many labels", path: "labels", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots[0].Labels = make(map[string]string, maxSlotLabels+1)
			for index := range maxSlotLabels + 1 {
				hello.Slots[0].Labels[fmt.Sprintf("label-%02d", index)] = "value"
			}
		}},
		{name: "invalid label key", path: "labels", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots[0].Labels = map[string]string{"bad key": "value"}
		}},
		{name: "invalid label value", path: "labels", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots[0].Labels = map[string]string{"key": "bad value"}
		}},
		{name: "empty profile", path: "profile_id", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].ProfileId = "" }},
		{name: "uppercase policy digest", path: "slot_policy_digest", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.SlotPolicyDigest = strings.ToUpper(hello.SlotPolicyDigest)
		}},
		{name: "capacity spoof", path: "capacity", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots[0].Capacity = &ateapipb.WorkerCapacity{CpuMilli: 1, MemoryBytes: 1}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := validClientFrame()
			test.edit(frame.GetHello())
			requireInvalidAdmission(t, claim, frame, test.path)
		})
	}
}

func TestValidateConnectAdmissionEnforcesAuthenticatedProfile(t *testing.T) {
	claim := validSessionClaim(2)
	tests := []struct {
		name string
		path string
		edit func(*externalproviderpb.ConnectHello)
	}{
		{name: "stale policy digest", path: "slot_policy_digest", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.SlotPolicyDigest = strings.Repeat("0", capabilityPolicyDigestHex)
		}},
		{name: "unknown profile", path: "profile_id", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots[0].ProfileId = "admin"
		}},
		{name: "sandbox spoof", path: "sandbox_class", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots[0].SandboxClass = "privileged"
		}},
		{name: "label spoof", path: "labels", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots[0].Labels = map[string]string{"security.example/tier": "trusted"}
		}},
		{name: "capacity spoof", path: "capacity", edit: func(hello *externalproviderpb.ConnectHello) {
			hello.Slots[0].Capacity = &ateapipb.WorkerCapacity{CpuMilli: 8_001, MemoryBytes: 16<<30 + 1}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := validClientFrame()
			test.edit(frame.GetHello())
			requireInvalidAdmission(t, claim, frame, test.path)
		})
	}

	limitedPolicy, err := NewSlotCapabilityPolicy(SlotCapabilityPolicyVersion, []SlotProfile{
		{ProfileID: "standard", SandboxClass: "gvisor", MaxSlots: 1, CPUMilli: 8_000, MemoryBytes: 16 << 30},
		{ProfileID: "unused", SandboxClass: "gvisor", MaxSlots: 1, CPUMilli: 8_000, MemoryBytes: 16 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim.Registration.Scope.SlotPolicy = limitedPolicy
	frame := validClientFrame()
	frame.GetHello().SlotPolicyDigest = limitedPolicy.DigestHex()
	frame.GetHello().Slots = []*externalproviderpb.ExternalSlot{validExternalSlot("slot-a"), validExternalSlot("slot-b")}
	requireInvalidAdmission(t, claim, frame, "profile_id")
}

func TestConnectAdmissionDoesNotAliasInputOrAccessors(t *testing.T) {
	claim := validSessionClaim(1)
	frame := validClientFrame()
	admission, err := ValidateConnectAdmission(claim, frame)
	if err != nil {
		t.Fatalf("ValidateConnectAdmission() error = %v", err)
	}

	claim.Registration.UID = "registration-mutated"
	claim.Registration.Scope.WorkerPool = "pool-mutated"
	sourceSlot := frame.GetHello().Slots[0]
	frame.GetHello().RegistrationUid = "registration-mutated"
	sourceSlot.SlotId = "slot-mutated"
	sourceSlot.ProfileId = "profile-mutated"
	sourceSlot.Labels = map[string]string{"region": "mutated"}
	sourceSlot.Capacity = &ateapipb.WorkerCapacity{CpuMilli: 1, MemoryBytes: 2}

	first := admission.Slots()
	first[0].slotID = "returned-slot-mutated"
	first[0].sandboxClass = "returned-sandbox-mutated"
	first[0].labels[0].value = "returned-label-mutated"
	first[0].cpuMilli = 3
	labels := first[0].Labels()
	labels["region"] = "accessor-mutated"
	capacity := first[0].Capacity()
	capacity.CpuMilli = 4

	if got := admission.Registration().UID; got != "registration-a" {
		t.Errorf("Registration().UID = %q after input mutation, want registration-a", got)
	}
	second := admission.Slots()
	if second[0].SlotID() != "slot-a" || second[0].SandboxClass() != "gvisor" {
		t.Errorf("slot scalar values alias caller data: %+v", second[0])
	}
	if got := second[0].Labels()["region"]; got != "south" {
		t.Errorf("Labels()[region] = %q after mutation, want south", got)
	}
	if got := second[0].Capacity(); got.GetCpuMilli() != 2_000 || got.GetMemoryBytes() != 4<<30 {
		t.Errorf("Capacity() = %v after mutation, want original", got)
	}
}

func TestConnectAdmissionAccessorsAreRaceSafe(t *testing.T) {
	admission, err := ValidateConnectAdmission(validSessionClaim(1), validClientFrame())
	if err != nil {
		t.Fatalf("ValidateConnectAdmission() error = %v", err)
	}

	var wait sync.WaitGroup
	for worker := range 32 {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for range 100 {
				if admission.Generation() != 7 || admission.Registration().UID != "registration-a" {
					t.Errorf("immutable admission authority changed")
					return
				}
				slot := admission.Slots()[0]
				labels := slot.Labels()
				labels["region"] = fmt.Sprintf("worker-%d", worker)
				capacity := slot.Capacity()
				capacity.CpuMilli = int64(worker)
			}
		}(worker)
	}
	wait.Wait()
}
