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
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func validSessionClaim(maxSlots uint32) SessionClaim {
	return SessionClaim{
		Registration: Registration{
			UID:           "registration-a",
			EnrollmentUID: "enrollment-a",
			Scope: Scope{
				OwnerAtespace:   "tenant-a",
				WorkerNamespace: "workers",
				WorkerPool:      "pool-a",
				MaxSlots:        maxSlots,
			},
		},
		Generation: 7,
	}
}

func validExternalSlot(id string) *externalproviderpb.ExternalSlot {
	return &externalproviderpb.ExternalSlot{
		SlotId:       id,
		SandboxClass: "gvisor",
		Labels: map[string]string{
			"region":                "south",
			"topology.example/zone": "zone-a",
		},
		Capacity: &ateapipb.WorkerCapacity{CpuMilli: 2_000, MemoryBytes: 4 << 30},
	}
}

func validClientFrame() *externalproviderpb.ClientFrame {
	return &externalproviderpb.ClientFrame{
		Frame: &externalproviderpb.ClientFrame_Hello{Hello: &externalproviderpb.ConnectHello{
			RegistrationUid: "registration-a",
			ProtocolVersion: connectProtocolVersion,
			Slots:           []*externalproviderpb.ExternalSlot{validExternalSlot("slot-a")},
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
	frame.GetHello().Slots[0].Labels = map[string]string{
		"z.example/key": "last",
		"a":             "",
	}

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
	if got := slots[0].SandboxClass(); got != "gvisor" {
		t.Errorf("SandboxClass() = %q, want gvisor", got)
	}
	wantLabels := map[string]string{"a": "", "z.example/key": "last"}
	if got := slots[0].Labels(); !maps.Equal(got, wantLabels) {
		t.Errorf("Labels() = %v, want %v", got, wantLabels)
	}
	if got, want := slots[0].Capacity(), (&ateapipb.WorkerCapacity{CpuMilli: 2_000, MemoryBytes: 4 << 30}); !proto.Equal(got, want) {
		t.Errorf("Capacity() = %v, want %v", got, want)
	}
	if got := []string{slots[0].labels[0].key, slots[0].labels[1].key}; !slices.Equal(got, []string{"a", "z.example/key"}) {
		t.Errorf("normalized label order = %v, want [a z.example/key]", got)
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
		{name: "unsupported protocol", path: "protocol_version", edit: func(frame *externalproviderpb.ClientFrame) { frame.GetHello().ProtocolVersion = 2 }},
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
	admission, err := ValidateConnectAdmission(validSessionClaim(maxSlots+44), frame)
	if err != nil {
		t.Fatalf("ValidateConnectAdmission(256 slots) error = %v", err)
	}
	if got := len(admission.Slots()); got != maxSlots {
		t.Errorf("admitted slots = %d, want %d", got, maxSlots)
	}

	frame = validClientFrame()
	frame.GetHello().Slots = makeSlots(maxSlots + 1)
	requireInvalidAdmission(t, validSessionClaim(maxSlots+44), frame, "frame.hello.slots")
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
		{name: "negative CPU", path: "cpu_milli", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].Capacity.CpuMilli = -1 }},
		{name: "negative memory", path: "memory_bytes", edit: func(hello *externalproviderpb.ConnectHello) { hello.Slots[0].Capacity.MemoryBytes = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := validClientFrame()
			test.edit(frame.GetHello())
			requireInvalidAdmission(t, claim, frame, test.path)
		})
	}
}

func TestValidateConnectAdmissionPublishedPermissiveBoundaries(t *testing.T) {
	frame := validClientFrame()
	slot := frame.GetHello().Slots[0]
	slot.SlotId = strings.Repeat("a", 253)
	if !IsValidIdentity(slot.SlotId) {
		t.Fatalf("test slot ID is invalid: %q", slot.SlotId)
	}
	// Empty is not forbidden by the published opaque sandbox_class contract or
	// by ateapi.Worker semantics.
	slot.SandboxClass = ""
	slot.Labels = map[string]string{"example.com/key": "", "key": strings.Repeat("a", 63)}
	slot.Capacity = &ateapipb.WorkerCapacity{CpuMilli: math.MaxInt64, MemoryBytes: math.MaxInt64}
	if _, err := ValidateConnectAdmission(validSessionClaim(1), frame); err != nil {
		t.Fatalf("ValidateConnectAdmission(permissive boundaries) error = %v", err)
	}

	frame = validClientFrame()
	frame.GetHello().Slots[0].SandboxClass = strings.Repeat("é", 126) + "a"
	frame.GetHello().Slots[0].Labels = make(map[string]string, maxSlotLabels)
	for index := range maxSlotLabels {
		frame.GetHello().Slots[0].Labels[fmt.Sprintf("label-%02d", index)] = "value"
	}
	frame.GetHello().Slots[0].Capacity = nil
	admission, err := ValidateConnectAdmission(validSessionClaim(1), frame)
	if err != nil {
		t.Fatalf("ValidateConnectAdmission(max labels/UTF-8 sandbox/nil capacity) error = %v", err)
	}
	if got := admission.Slots()[0].Capacity(); got.GetCpuMilli() != 0 || got.GetMemoryBytes() != 0 {
		t.Errorf("nil capacity normalized to %v, want zero capacity", got)
	}
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
	sourceSlot.SandboxClass = "sandbox-mutated"
	sourceSlot.Labels["region"] = "mutated"
	sourceSlot.Labels["new"] = "value"
	sourceSlot.Capacity.CpuMilli = 1
	sourceSlot.Capacity.MemoryBytes = 2

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
