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
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/proto"
)

func newChannelTestAdmission(t *testing.T) *ConnectAdmission {
	t.Helper()
	frame := validClientFrame()
	frame.GetHello().Slots = []*externalproviderpb.ExternalSlot{
		validExternalSlot("slot-a"),
		validExternalSlot("slot-b"),
	}
	admission, err := ValidateConnectAdmission(validSessionClaim(2), frame)
	if err != nil {
		t.Fatalf("ValidateConnectAdmission() error = %v", err)
	}
	return admission
}

func newChannelTestState(t *testing.T, edit func(*ChannelSessionLimits)) *ChannelSessionState {
	t.Helper()
	limits := ChannelSessionLimits{
		MaxOpenChannels:        4,
		MaxDataBytes:           16,
		RememberedChannelLimit: 8,
		PendingHeartbeatLimit:  2,
	}
	if edit != nil {
		edit(&limits)
	}
	state, err := NewChannelSessionState(newChannelTestAdmission(t), limits)
	if err != nil {
		t.Fatalf("NewChannelSessionState() error = %v", err)
	}
	return state
}

func clientOpenFrame(generation, channelID uint64, kind externalproviderpb.ChannelKind, slotID string) *externalproviderpb.ClientFrame {
	return &externalproviderpb.ClientFrame{
		SessionGeneration: generation,
		Frame: &externalproviderpb.ClientFrame_Open{Open: &externalproviderpb.OpenChannel{
			ChannelId: channelID,
			Kind:      kind,
			SlotId:    slotID,
		}},
	}
}

func clientAckFrame(generation, channelID uint64, accepted bool, message string) *externalproviderpb.ClientFrame {
	return &externalproviderpb.ClientFrame{
		SessionGeneration: generation,
		Frame: &externalproviderpb.ClientFrame_OpenAck{OpenAck: &externalproviderpb.OpenChannelAck{
			ChannelId:    channelID,
			Accepted:     accepted,
			ErrorMessage: message,
		}},
	}
}

func clientDataFrame(generation, channelID uint64, data []byte) *externalproviderpb.ClientFrame {
	return &externalproviderpb.ClientFrame{
		SessionGeneration: generation,
		Frame: &externalproviderpb.ClientFrame_Data{Data: &externalproviderpb.ChannelData{
			ChannelId: channelID,
			Data:      data,
		}},
	}
}

func clientHalfCloseFrame(generation, channelID uint64) *externalproviderpb.ClientFrame {
	return &externalproviderpb.ClientFrame{
		SessionGeneration: generation,
		Frame:             &externalproviderpb.ClientFrame_HalfClose{HalfClose: &externalproviderpb.HalfCloseChannel{ChannelId: channelID}},
	}
}

func clientResetFrame(generation, channelID uint64, grpcCode uint32, reason string) *externalproviderpb.ClientFrame {
	return &externalproviderpb.ClientFrame{
		SessionGeneration: generation,
		Frame: &externalproviderpb.ClientFrame_Reset_{Reset_: &externalproviderpb.ResetChannel{
			ChannelId: channelID,
			GrpcCode:  grpcCode,
			Reason:    reason,
		}},
	}
}

func clientHeartbeatFrame(generation, nonce uint64, acknowledgement bool) *externalproviderpb.ClientFrame {
	return &externalproviderpb.ClientFrame{
		SessionGeneration: generation,
		Frame: &externalproviderpb.ClientFrame_Heartbeat{Heartbeat: &externalproviderpb.Heartbeat{
			Nonce:           nonce,
			Acknowledgement: acknowledgement,
		}},
	}
}

func requireProtocolError(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrChannelProtocolViolation) {
		t.Fatalf("error = %v, want ErrChannelProtocolViolation", err)
	}
}

func requireOperationError(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrInvalidChannelOperation) {
		t.Fatalf("error = %v, want ErrInvalidChannelOperation", err)
	}
}

func acceptServerChannel(t *testing.T, state *ChannelSessionState, channelID uint64) {
	t.Helper()
	if _, err := state.OpenServerChannel(channelID, externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, "slot-a"); err != nil {
		t.Fatalf("OpenServerChannel() error = %v", err)
	}
	effect, err := state.ApplyClientFrame(clientAckFrame(state.Generation(), channelID, true, ""))
	if err != nil {
		t.Fatalf("ApplyClientFrame(ack) error = %v", err)
	}
	ack, ok := effect.(*ServerOpenAckEffect)
	if !ok || !ack.Accepted() || ack.ChannelID() != channelID {
		t.Fatalf("ack effect = %#v, want accepted channel %d", effect, channelID)
	}
}

func acceptClientChannel(t *testing.T, state *ChannelSessionState, channelID uint64) {
	t.Helper()
	if _, err := state.ApplyClientFrame(clientOpenFrame(state.Generation(), channelID, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-b")); err != nil {
		t.Fatalf("ApplyClientFrame(open) error = %v", err)
	}
	if _, err := state.AcknowledgeClientOpen(channelID, true, ""); err != nil {
		t.Fatalf("AcknowledgeClientOpen() error = %v", err)
	}
}

func TestNewChannelSessionStatePinsAdmissionAndBounds(t *testing.T) {
	admission := newChannelTestAdmission(t)
	state, err := NewChannelSessionState(admission, ChannelSessionLimits{MaxOpenChannels: 2, MaxDataBytes: 64})
	if err != nil {
		t.Fatalf("NewChannelSessionState() error = %v", err)
	}
	if got, want := state.Generation(), uint64(7); got != want {
		t.Errorf("Generation() = %d, want %d", got, want)
	}
	if got, want := state.SlotIDs(), []string{"slot-a", "slot-b"}; !slices.Equal(got, want) {
		t.Errorf("SlotIDs() = %v, want %v", got, want)
	}
	first := state.SlotIDs()
	first[0] = "mutated"
	if got := state.SlotIDs()[0]; got != "slot-a" {
		t.Errorf("SlotIDs() after caller mutation = %q, want slot-a", got)
	}
	limits := state.Limits()
	if limits.RememberedChannelLimit != defaultRememberedChannelLimit || limits.PendingHeartbeatLimit != defaultPendingHeartbeatLimit {
		t.Errorf("normalized limits = %+v", limits)
	}

	// The state owns a snapshot, not admission's backing storage.
	admission.slots[0].slotID = "mutated"
	if got := state.SlotIDs()[0]; got != "slot-a" {
		t.Errorf("SlotIDs() after admission mutation = %q, want slot-a", got)
	}
}

func TestNewChannelSessionStateRejectsInvalidLimits(t *testing.T) {
	admission := newChannelTestAdmission(t)
	tests := []struct {
		name   string
		limits ChannelSessionLimits
	}{
		{name: "zero open limit", limits: ChannelSessionLimits{MaxDataBytes: 1}},
		{name: "large open limit", limits: ChannelSessionLimits{MaxOpenChannels: maxReadyOpenChannels + 1, MaxDataBytes: 1}},
		{name: "zero data limit", limits: ChannelSessionLimits{MaxOpenChannels: 1}},
		{name: "large data limit", limits: ChannelSessionLimits{MaxOpenChannels: 1, MaxDataBytes: maxReadyDataBytes + 1}},
		{name: "remembered below open", limits: ChannelSessionLimits{MaxOpenChannels: 2, MaxDataBytes: 1, RememberedChannelLimit: 1}},
		{name: "remembered too large", limits: ChannelSessionLimits{MaxOpenChannels: 1, MaxDataBytes: 1, RememberedChannelLimit: maximumRememberedChannelLimit + 1}},
		{name: "heartbeat too large", limits: ChannelSessionLimits{MaxOpenChannels: 1, MaxDataBytes: 1, PendingHeartbeatLimit: maximumPendingHeartbeatLimit + 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewChannelSessionState(admission, test.limits)
			if !errors.Is(err, ErrInvalidChannelStateConfig) {
				t.Fatalf("NewChannelSessionState() error = %v, want ErrInvalidChannelStateConfig", err)
			}
		})
	}
	if _, err := NewChannelSessionState(nil, ChannelSessionLimits{MaxOpenChannels: 1, MaxDataBytes: 1}); !errors.Is(err, ErrInvalidChannelStateConfig) {
		t.Fatalf("NewChannelSessionState(nil) error = %v", err)
	}
}

func TestServerOpenValidationAndFrame(t *testing.T) {
	tests := []struct {
		name      string
		channelID uint64
		kind      externalproviderpb.ChannelKind
		slotID    string
		valid     bool
	}{
		{name: "execution", channelID: 2, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, slotID: "slot-a", valid: true},
		{name: "actor ingress", channelID: 4, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_INGRESS, slotID: "slot-b", valid: true},
		{name: "zero ID", channelID: 0, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, slotID: "slot-a"},
		{name: "odd ID", channelID: 3, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, slotID: "slot-a"},
		{name: "unspecified kind", channelID: 2, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_UNSPECIFIED, slotID: "slot-a"},
		{name: "client kind", channelID: 2, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, slotID: "slot-a"},
		{name: "unknown slot", channelID: 2, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, slotID: "slot-c"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := newChannelTestState(t, nil)
			effect, err := state.OpenServerChannel(test.channelID, test.kind, test.slotID)
			if !test.valid {
				requireOperationError(t, err)
				if got := state.Stats(); got != (ChannelSessionStats{}) {
					t.Fatalf("Stats() after rejected open = %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("OpenServerChannel() error = %v", err)
			}
			frame := effect.Frame()
			if frame.GetSessionGeneration() != state.Generation() || frame.GetOpen().GetChannelId() != test.channelID || frame.GetOpen().GetKind() != test.kind || frame.GetOpen().GetSlotId() != test.slotID {
				t.Errorf("server open frame = %v", frame)
			}
			if got := state.Stats(); got.RememberedChannels != 1 || got.NonterminalChannels != 1 {
				t.Errorf("Stats() = %+v", got)
			}
		})
	}
}

func TestClientOpenValidationAndNoReuse(t *testing.T) {
	state := newChannelTestState(t, func(limits *ChannelSessionLimits) {
		limits.MaxOpenChannels = 1
		limits.RememberedChannelLimit = 2
	})
	generation := state.Generation()

	invalid := []struct {
		name      string
		channelID uint64
		kind      externalproviderpb.ChannelKind
		slot      string
	}{
		{name: "zero ID", channelID: 0, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, slot: "slot-a"},
		{name: "even ID", channelID: 2, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, slot: "slot-a"},
		{name: "server execution kind", channelID: 1, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, slot: "slot-a"},
		{name: "server ingress kind", channelID: 1, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_INGRESS, slot: "slot-a"},
		{name: "unknown slot", channelID: 1, kind: externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, slot: "slot-c"},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, err := state.ApplyClientFrame(clientOpenFrame(generation, test.channelID, test.kind, test.slot))
			requireProtocolError(t, err)
		})
	}

	effect, err := state.ApplyClientFrame(clientOpenFrame(generation, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a"))
	if err != nil {
		t.Fatalf("ApplyClientFrame(valid open) error = %v", err)
	}
	opened, ok := effect.(*ClientOpenEffect)
	if !ok || opened.ChannelID() != 1 || opened.Kind() != externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS || opened.SlotID() != "slot-a" {
		t.Fatalf("open effect = %#v", effect)
	}
	_, err = state.ApplyClientFrame(clientOpenFrame(generation, 3, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a"))
	requireProtocolError(t, err)

	if _, err := state.AcknowledgeClientOpen(1, false, "route unavailable"); err != nil {
		t.Fatalf("AcknowledgeClientOpen(reject) error = %v", err)
	}
	_, err = state.ApplyClientFrame(clientOpenFrame(generation, 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a"))
	requireProtocolError(t, err)

	if _, err := state.ApplyClientFrame(clientOpenFrame(generation, 3, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a")); err != nil {
		t.Fatalf("ApplyClientFrame(second lifetime open) error = %v", err)
	}
	if _, err := state.AcknowledgeClientOpen(3, false, "done"); err != nil {
		t.Fatalf("AcknowledgeClientOpen(second reject) error = %v", err)
	}
	_, err = state.OpenServerChannel(2, externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, "slot-a")
	requireOperationError(t, err)
}

func TestClientFrameEnvelopeRules(t *testing.T) {
	state := newChannelTestState(t, nil)
	generation := state.Generation()
	frames := []struct {
		name  string
		frame *externalproviderpb.ClientFrame
	}{
		{name: "nil", frame: nil},
		{name: "zero generation", frame: clientHeartbeatFrame(0, 1, false)},
		{name: "stale generation", frame: clientHeartbeatFrame(generation-1, 1, false)},
		{name: "newer generation", frame: clientHeartbeatFrame(generation+1, 1, false)},
		{name: "no member", frame: &externalproviderpb.ClientFrame{SessionGeneration: generation}},
		{name: "hello", frame: &externalproviderpb.ClientFrame{SessionGeneration: generation, Frame: &externalproviderpb.ClientFrame_Hello{Hello: &externalproviderpb.ConnectHello{}}}},
		{name: "nil open", frame: &externalproviderpb.ClientFrame{SessionGeneration: generation, Frame: &externalproviderpb.ClientFrame_Open{}}},
		{name: "nil ack", frame: &externalproviderpb.ClientFrame{SessionGeneration: generation, Frame: &externalproviderpb.ClientFrame_OpenAck{}}},
		{name: "nil data", frame: &externalproviderpb.ClientFrame{SessionGeneration: generation, Frame: &externalproviderpb.ClientFrame_Data{}}},
		{name: "nil half close", frame: &externalproviderpb.ClientFrame{SessionGeneration: generation, Frame: &externalproviderpb.ClientFrame_HalfClose{}}},
		{name: "nil reset", frame: &externalproviderpb.ClientFrame{SessionGeneration: generation, Frame: &externalproviderpb.ClientFrame_Reset_{}}},
		{name: "nil heartbeat", frame: &externalproviderpb.ClientFrame{SessionGeneration: generation, Frame: &externalproviderpb.ClientFrame_Heartbeat{}}},
	}
	for _, test := range frames {
		t.Run(test.name, func(t *testing.T) {
			_, err := state.ApplyClientFrame(test.frame)
			requireProtocolError(t, err)
		})
	}

	exact := clientHeartbeatFrame(generation, 1, false)
	padFrameToSerializedSize(t, exact, maxClientFrameBytes)
	if _, err := state.ApplyClientFrame(exact); err != nil {
		t.Fatalf("ApplyClientFrame(exact limit) error = %v", err)
	}
	oversized := clientHeartbeatFrame(generation, 2, false)
	oversized.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x78, 0x00}, maxClientFrameBytes/2+1))
	if proto.Size(oversized) <= maxClientFrameBytes {
		t.Fatalf("oversized frame size = %d", proto.Size(oversized))
	}
	if _, err := state.ApplyClientFrame(oversized); err == nil {
		t.Fatal("ApplyClientFrame(oversized) succeeded")
	} else {
		requireProtocolError(t, err)
	}
}

func TestOpenAckPairingAndExactlyOnce(t *testing.T) {
	t.Run("client acknowledges server open", func(t *testing.T) {
		state := newChannelTestState(t, nil)
		if _, err := state.OpenServerChannel(2, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_INGRESS, "slot-a"); err != nil {
			t.Fatalf("OpenServerChannel() error = %v", err)
		}
		for _, test := range []struct {
			name     string
			accepted bool
			message  string
		}{
			{name: "accepted with message", accepted: true, message: "unexpected"},
			{name: "rejected without message", accepted: false, message: ""},
			{name: "invalid UTF-8", accepted: false, message: string([]byte{0xff})},
			{name: "too long", accepted: false, message: strings.Repeat("x", maxChannelTextBytes+1)},
		} {
			t.Run(test.name, func(t *testing.T) {
				_, err := state.ApplyClientFrame(clientAckFrame(state.Generation(), 2, test.accepted, test.message))
				requireProtocolError(t, err)
			})
		}
		effect, err := state.ApplyClientFrame(clientAckFrame(state.Generation(), 2, false, strings.Repeat("x", maxChannelTextBytes)))
		if err != nil {
			t.Fatalf("ApplyClientFrame(valid reject) error = %v", err)
		}
		ack := effect.(*ServerOpenAckEffect)
		if ack.Accepted() || len(ack.ErrorMessage()) != maxChannelTextBytes {
			t.Errorf("ack effect = %#v", ack)
		}
		_, err = state.ApplyClientFrame(clientAckFrame(state.Generation(), 2, false, "again"))
		requireProtocolError(t, err)
	})

	t.Run("server acknowledges client open", func(t *testing.T) {
		state := newChannelTestState(t, nil)
		if _, err := state.ApplyClientFrame(clientOpenFrame(state.Generation(), 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-a")); err != nil {
			t.Fatalf("ApplyClientFrame(open) error = %v", err)
		}
		for _, test := range []struct {
			name     string
			accepted bool
			message  string
		}{
			{name: "accepted with message", accepted: true, message: "unexpected"},
			{name: "rejected without message", accepted: false},
			{name: "invalid UTF-8", accepted: false, message: string([]byte{0xff})},
			{name: "too long", accepted: false, message: strings.Repeat("x", maxChannelTextBytes+1)},
		} {
			t.Run(test.name, func(t *testing.T) {
				_, err := state.AcknowledgeClientOpen(1, test.accepted, test.message)
				requireOperationError(t, err)
			})
		}
		effect, err := state.AcknowledgeClientOpen(1, true, "")
		if err != nil {
			t.Fatalf("AcknowledgeClientOpen(valid accept) error = %v", err)
		}
		if ack := effect.Frame().GetOpenAck(); ack.GetChannelId() != 1 || !ack.GetAccepted() || ack.GetErrorMessage() != "" {
			t.Errorf("server ack frame = %v", ack)
		}
		_, err = state.AcknowledgeClientOpen(1, true, "")
		requireOperationError(t, err)
		_, err = state.ApplyClientFrame(clientAckFrame(state.Generation(), 1, true, ""))
		requireProtocolError(t, err)
	})
}

func TestDataRequiresAcceptedChannelAndCopiesBytes(t *testing.T) {
	state := newChannelTestState(t, nil)
	if _, err := state.OpenServerChannel(2, externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, "slot-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := state.SendServerData(2, []byte("pending")); err == nil {
		t.Fatal("SendServerData(pending) succeeded")
	} else {
		requireOperationError(t, err)
	}
	if _, err := state.ApplyClientFrame(clientDataFrame(state.Generation(), 2, []byte("pending"))); err == nil {
		t.Fatal("ApplyClientFrame(data pending) succeeded")
	} else {
		requireProtocolError(t, err)
	}
	if _, err := state.ApplyClientFrame(clientAckFrame(state.Generation(), 2, true, "")); err != nil {
		t.Fatal(err)
	}

	for _, data := range [][]byte{nil, {}, bytes.Repeat([]byte{'x'}, 17)} {
		if _, err := state.SendServerData(2, data); err == nil {
			t.Fatalf("SendServerData(%d bytes) succeeded", len(data))
		} else {
			requireOperationError(t, err)
		}
		if _, err := state.ApplyClientFrame(clientDataFrame(state.Generation(), 2, data)); err == nil {
			t.Fatalf("ApplyClientFrame(%d bytes) succeeded", len(data))
		} else {
			requireProtocolError(t, err)
		}
	}

	serverPayload := []byte("server")
	serverEffect, err := state.SendServerData(2, serverPayload)
	if err != nil {
		t.Fatalf("SendServerData() error = %v", err)
	}
	serverPayload[0] = 'X'
	firstFrame := serverEffect.Frame()
	firstFrame.GetData().Data[0] = 'Y'
	if got := string(serverEffect.Frame().GetData().GetData()); got != "server" {
		t.Errorf("server effect data = %q, want server", got)
	}

	clientPayload := []byte("client")
	clientFrame := clientDataFrame(state.Generation(), 2, clientPayload)
	clientEffectValue, err := state.ApplyClientFrame(clientFrame)
	if err != nil {
		t.Fatalf("ApplyClientFrame(data) error = %v", err)
	}
	clientEffect := clientEffectValue.(*ClientDataEffect)
	clientPayload[0] = 'X'
	clientFrame.GetData().Data[1] = 'Y'
	firstData := clientEffect.Data()
	firstData[2] = 'Z'
	if got := string(clientEffect.Data()); got != "client" {
		t.Errorf("client effect data = %q, want client", got)
	}
}

func TestHalfCloseStateTransitions(t *testing.T) {
	tests := []struct {
		name        string
		serverFirst bool
	}{
		{name: "server then client", serverFirst: true},
		{name: "client then server"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := newChannelTestState(t, nil)
			acceptServerChannel(t, state, 2)
			if test.serverFirst {
				if _, err := state.HalfCloseServerChannel(2); err != nil {
					t.Fatal(err)
				}
				if _, err := state.SendServerData(2, []byte("later")); err == nil {
					t.Fatal("server data after half-close succeeded")
				}
				if _, err := state.ApplyClientFrame(clientDataFrame(state.Generation(), 2, []byte("peer still open"))); err != nil {
					t.Fatalf("client data after server half-close error = %v", err)
				}
				if _, err := state.ApplyClientFrame(clientHalfCloseFrame(state.Generation(), 2)); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := state.ApplyClientFrame(clientHalfCloseFrame(state.Generation(), 2)); err != nil {
					t.Fatal(err)
				}
				if _, err := state.ApplyClientFrame(clientDataFrame(state.Generation(), 2, []byte("later"))); err == nil {
					t.Fatal("client data after half-close succeeded")
				}
				if _, err := state.SendServerData(2, []byte("still open")); err != nil {
					t.Fatalf("server data after client half-close error = %v", err)
				}
				if _, err := state.HalfCloseServerChannel(2); err != nil {
					t.Fatal(err)
				}
			}
			if got := state.Stats().NonterminalChannels; got != 0 {
				t.Errorf("NonterminalChannels = %d, want 0", got)
			}
			if _, err := state.SendServerData(2, []byte("terminal")); err == nil {
				t.Fatal("server data after terminal close succeeded")
			}
			if _, err := state.ApplyClientFrame(clientHalfCloseFrame(state.Generation(), 2)); err == nil {
				t.Fatal("client frame after terminal close succeeded")
			}
		})
	}

	t.Run("duplicate each side", func(t *testing.T) {
		state := newChannelTestState(t, nil)
		acceptServerChannel(t, state, 2)
		if _, err := state.HalfCloseServerChannel(2); err != nil {
			t.Fatal(err)
		}
		if _, err := state.HalfCloseServerChannel(2); err == nil {
			t.Fatal("duplicate server half-close succeeded")
		}
		if _, err := state.ApplyClientFrame(clientHalfCloseFrame(state.Generation(), 2)); err != nil {
			t.Fatal(err)
		}
		if _, err := state.ApplyClientFrame(clientHalfCloseFrame(state.Generation(), 2)); err == nil {
			t.Fatal("duplicate client half-close succeeded")
		}
	})
}

func TestResetValidationTerminalizesAcceptedChannels(t *testing.T) {
	for _, test := range []struct {
		name   string
		code   uint32
		reason string
		valid  bool
	}{
		{name: "minimum code empty reason", code: 1, valid: true},
		{name: "maximum code and reason", code: 16, reason: strings.Repeat("x", maxChannelTextBytes), valid: true},
		{name: "OK forbidden", code: 0},
		{name: "code too large", code: 17},
		{name: "invalid UTF-8", code: 1, reason: string([]byte{0xff})},
		{name: "reason too long", code: 1, reason: strings.Repeat("x", maxChannelTextBytes+1)},
	} {
		t.Run("server "+test.name, func(t *testing.T) {
			state := newChannelTestState(t, nil)
			acceptClientChannel(t, state, 1)
			effect, err := state.ResetServerChannel(1, test.code, test.reason)
			if !test.valid {
				requireOperationError(t, err)
				if state.Stats().NonterminalChannels != 1 {
					t.Fatal("invalid reset changed channel state")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if reset := effect.Frame().GetReset_(); reset.GetGrpcCode() != test.code || reset.GetReason() != test.reason {
				t.Errorf("reset frame = %v", reset)
			}
			if state.Stats().NonterminalChannels != 0 {
				t.Fatal("valid reset did not terminalize channel")
			}
			if _, err := state.ResetServerChannel(1, 1, "again"); err == nil {
				t.Fatal("second reset succeeded")
			}
		})

		t.Run("client "+test.name, func(t *testing.T) {
			state := newChannelTestState(t, nil)
			acceptServerChannel(t, state, 2)
			effect, err := state.ApplyClientFrame(clientResetFrame(state.Generation(), 2, test.code, test.reason))
			if !test.valid {
				requireProtocolError(t, err)
				if state.Stats().NonterminalChannels != 1 {
					t.Fatal("invalid reset changed channel state")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			reset := effect.(*ClientResetEffect)
			if reset.GRPCCode() != test.code || reset.Reason() != test.reason {
				t.Errorf("reset effect = %#v", reset)
			}
			if _, err := state.ApplyClientFrame(clientDataFrame(state.Generation(), 2, []byte("later"))); err == nil {
				t.Fatal("frame after reset succeeded")
			}
		})
	}

	t.Run("pending channel cannot reset", func(t *testing.T) {
		state := newChannelTestState(t, nil)
		if _, err := state.OpenServerChannel(2, externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, "slot-a"); err != nil {
			t.Fatal(err)
		}
		if _, err := state.ResetServerChannel(2, 1, "pending"); err == nil {
			t.Fatal("server reset before ack succeeded")
		}
		if _, err := state.ApplyClientFrame(clientResetFrame(state.Generation(), 2, 1, "pending")); err == nil {
			t.Fatal("client reset before ack succeeded")
		}
	})
}

func TestHeartbeatNonceAndAcknowledgementRules(t *testing.T) {
	state := newChannelTestState(t, nil)
	if _, err := state.SendServerHeartbeat(0); err == nil {
		t.Fatal("SendServerHeartbeat(0) succeeded")
	}
	first, err := state.SendServerHeartbeat(10)
	if err != nil {
		t.Fatal(err)
	}
	if heartbeat := first.Frame().GetHeartbeat(); heartbeat.GetNonce() != 10 || heartbeat.GetAcknowledgement() {
		t.Errorf("server heartbeat = %v", heartbeat)
	}
	if _, err := state.SendServerHeartbeat(10); err == nil {
		t.Fatal("duplicate pending nonce succeeded")
	}
	if _, err := state.SendServerHeartbeat(11); err != nil {
		t.Fatal(err)
	}
	if _, err := state.SendServerHeartbeat(12); err == nil {
		t.Fatal("pending heartbeat limit was not enforced")
	}

	if _, err := state.ApplyClientFrame(clientHeartbeatFrame(state.Generation(), 0, false)); err == nil {
		t.Fatal("zero client nonce succeeded")
	}
	if _, err := state.ApplyClientFrame(clientHeartbeatFrame(state.Generation(), 12, true)); err == nil {
		t.Fatal("ack of unknown nonce succeeded")
	}
	ackEffect, err := state.ApplyClientFrame(clientHeartbeatFrame(state.Generation(), 10, true))
	if err != nil {
		t.Fatal(err)
	}
	if ack := ackEffect.(*ServerHeartbeatAckEffect); ack.Nonce() != 10 {
		t.Errorf("heartbeat ack nonce = %d", ack.Nonce())
	}
	if _, err := state.ApplyClientFrame(clientHeartbeatFrame(state.Generation(), 10, true)); err == nil {
		t.Fatal("duplicate ack succeeded")
	}
	if _, err := state.SendServerHeartbeat(10); err != nil {
		t.Fatalf("nonce reuse after ack error = %v", err)
	}

	probeEffect, err := state.ApplyClientFrame(clientHeartbeatFrame(state.Generation(), 99, false))
	if err != nil {
		t.Fatal(err)
	}
	reply := probeEffect.(*SendServerFrameEffect).Frame().GetHeartbeat()
	if reply.GetNonce() != 99 || !reply.GetAcknowledgement() {
		t.Errorf("client probe reply = %v", reply)
	}
	// An acknowledgement is consumed, never acknowledged with another frame.
	if _, ok := ackEffect.(*SendServerFrameEffect); ok {
		t.Fatal("server probe acknowledgement produced a wire reply")
	}
}

func TestChannelSessionStateConcurrentUse(t *testing.T) {
	const channels = 64
	state := newChannelTestState(t, func(limits *ChannelSessionLimits) {
		limits.MaxOpenChannels = channels
		limits.RememberedChannelLimit = channels
		limits.PendingHeartbeatLimit = channels
	})
	var wg sync.WaitGroup
	errorsSeen := make(chan error, channels*2)
	sequences := make(chan uint64, channels*2)
	for index := range channels {
		channelID := uint64(index*2 + 2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			effect, err := state.OpenServerChannel(channelID, externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, "slot-a")
			if err != nil {
				errorsSeen <- fmt.Errorf("open %d: %w", channelID, err)
				return
			}
			sequences <- effect.Sequence()
		}()
	}
	wg.Wait()
	for index := range channels {
		channelID := uint64(index*2 + 2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			effect, err := state.ApplyClientFrame(clientAckFrame(state.Generation(), channelID, true, ""))
			if err != nil {
				errorsSeen <- fmt.Errorf("ack %d: %w", channelID, err)
				return
			}
			sequences <- effect.Sequence()
		}()
	}
	wg.Wait()
	close(errorsSeen)
	close(sequences)
	for err := range errorsSeen {
		t.Error(err)
	}
	if got := state.Stats(); got.RememberedChannels != channels || got.NonterminalChannels != channels {
		t.Errorf("Stats() = %+v", got)
	}
	gotSequences := make([]uint64, 0, channels*2)
	for sequence := range sequences {
		gotSequences = append(gotSequences, sequence)
	}
	slices.Sort(gotSequences)
	for index, sequence := range gotSequences {
		if want := uint64(index + 1); sequence != want {
			t.Fatalf("effect sequences = %v, want contiguous sequence at %d", gotSequences, want)
		}
	}
}

func TestStateTransitionsRejectWrongChannelOwnership(t *testing.T) {
	state := newChannelTestState(t, nil)
	if _, err := state.OpenServerChannel(2, externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC, "slot-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyClientFrame(clientOpenFrame(state.Generation(), 1, externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS, "slot-b")); err != nil {
		t.Fatal(err)
	}

	clientFrames := []struct {
		name  string
		frame *externalproviderpb.ClientFrame
	}{
		{name: "ack own open", frame: clientAckFrame(state.Generation(), 1, true, "")},
		{name: "data server pending", frame: clientDataFrame(state.Generation(), 2, []byte("x"))},
		{name: "data client pending", frame: clientDataFrame(state.Generation(), 1, []byte("x"))},
		{name: "half-close server pending", frame: clientHalfCloseFrame(state.Generation(), 2)},
		{name: "reset client pending", frame: clientResetFrame(state.Generation(), 1, 1, "pending")},
		{name: "unknown channel", frame: clientDataFrame(state.Generation(), 99, []byte("x"))},
	}
	for _, test := range clientFrames {
		t.Run(test.name, func(t *testing.T) {
			_, err := state.ApplyClientFrame(test.frame)
			requireProtocolError(t, err)
		})
	}
	if _, err := state.AcknowledgeClientOpen(2, true, ""); err == nil {
		t.Fatal("server acked its own open")
	}
	if _, err := state.SendServerData(1, []byte("pending")); err == nil {
		t.Fatal("server data before client-open ack succeeded")
	}
}
