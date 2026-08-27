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
	"math"
	"slices"
	"sync"
	"unicode/utf8"

	"github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"
	"google.golang.org/protobuf/proto"
)

const (
	maxServerFrameBytes                  = 1 << 20
	maxChannelTextBytes                  = 1024
	maxReadyOpenChannels                 = 65535
	maxReadyDataBytes                    = 65536
	defaultRememberedChannelLimit uint32 = 65535
	maximumRememberedChannelLimit uint32 = 1 << 20
	defaultPendingHeartbeatLimit  uint32 = 64
	maximumPendingHeartbeatLimit  uint32 = 65535
)

var (
	// ErrInvalidChannelStateConfig identifies an unusable post-Ready state
	// configuration. It is never caused by an untrusted stream frame.
	ErrInvalidChannelStateConfig = errors.New("invalid external provider channel state configuration")

	// ErrInvalidChannelOperation identifies a locally requested server action
	// which is inconsistent with the current channel state.
	ErrInvalidChannelOperation = errors.New("invalid external provider channel operation")

	// ErrChannelProtocolViolation identifies an invalid post-Ready client
	// frame. Error strings contain field names and fixed reasons, never payload
	// bytes or peer-provided error text.
	ErrChannelProtocolViolation = errors.New("external provider channel protocol violation")
)

// ChannelSessionLimits bounds all live and retained state for one generation.
// MaxOpenChannels and MaxDataBytes are the values already sent in
// ConnectReady. RememberedChannelLimit bounds the IDs retained to enforce the
// no-reuse rule; once exhausted, the session must be replaced with a newer
// generation. PendingHeartbeatLimit bounds server probes awaiting an ack. Zero
// values for the latter two select conservative defaults.
type ChannelSessionLimits struct {
	MaxOpenChannels        uint32
	MaxDataBytes           uint32
	RememberedChannelLimit uint32
	PendingHeartbeatLimit  uint32
}

// ChannelSessionState is a transport-independent post-ConnectReady state
// machine. All methods are safe for concurrent use. It performs no network,
// persistence, Worker, or routing operation; callers execute the returned
// effects in stream order.
type ChannelSessionState struct {
	mu sync.Mutex

	generation uint64
	slotIDs    []string
	slots      map[string]struct{}
	limits     ChannelSessionLimits

	channels          map[uint64]*channelState
	nonterminal       uint32
	pendingHeartbeats map[uint64]struct{}
	effectSequence    uint64
}

type channelOrigin uint8

const (
	channelOriginServer channelOrigin = iota + 1
	channelOriginClient
)

type channelPhase uint8

const (
	channelAwaitingClientAck channelPhase = iota + 1
	channelAwaitingServerAck
	channelAccepted
	channelTerminal
)

type channelState struct {
	origin           channelOrigin
	phase            channelPhase
	kind             externalproviderpb.ChannelKind
	slotID           string
	serverHalfClosed bool
	clientHalfClosed bool
}

// ChannelSessionStats is a non-secret point-in-time snapshot.
type ChannelSessionStats struct {
	RememberedChannels  uint32
	NonterminalChannels uint32
	PendingHeartbeats   uint32
}

// SessionEffect is a closed set of immutable effects returned by the state
// machine. Concrete effect accessors return copies of byte slices and protobuf
// messages.
type SessionEffect interface {
	// Sequence orders effects by the state transition that produced them. It
	// lets a transport owner serialize effects returned to concurrent callers.
	Sequence() uint64
	isSessionEffect()
}

type effectBase struct{ sequence uint64 }

func (e effectBase) Sequence() uint64          { return e.sequence }
func (e *effectBase) setSequence(value uint64) { e.sequence = value }

// SendServerFrameEffect asks the transport owner to send one validated frame.
type SendServerFrameEffect struct {
	effectBase
	frame *externalproviderpb.ServerFrame
}

func (*SendServerFrameEffect) isSessionEffect() {}

// Frame returns an independent copy safe for caller mutation.
func (e *SendServerFrameEffect) Frame() *externalproviderpb.ServerFrame {
	if e == nil || e.frame == nil {
		return nil
	}
	return proto.Clone(e.frame).(*externalproviderpb.ServerFrame)
}

// ClientOpenEffect asks the session owner to bind or reject a client-opened
// ACTOR_EGRESS channel. AcknowledgeClientOpen completes that decision.
type ClientOpenEffect struct {
	effectBase
	channelID uint64
	kind      externalproviderpb.ChannelKind
	slotID    string
}

func (*ClientOpenEffect) isSessionEffect() {}

func (e *ClientOpenEffect) ChannelID() uint64                    { return e.channelID }
func (e *ClientOpenEffect) Kind() externalproviderpb.ChannelKind { return e.kind }
func (e *ClientOpenEffect) SlotID() string                       { return e.slotID }

// ServerOpenAckEffect reports the client's exactly-once decision for a
// server-opened channel. ErrorMessage is bounded, valid UTF-8, untrusted text.
type ServerOpenAckEffect struct {
	effectBase
	channelID    uint64
	accepted     bool
	errorMessage string
}

func (*ServerOpenAckEffect) isSessionEffect() {}

func (e *ServerOpenAckEffect) ChannelID() uint64    { return e.channelID }
func (e *ServerOpenAckEffect) Accepted() bool       { return e.accepted }
func (e *ServerOpenAckEffect) ErrorMessage() string { return e.errorMessage }

// ClientDataEffect delivers copied bytes received on an accepted channel.
type ClientDataEffect struct {
	effectBase
	channelID uint64
	data      []byte
}

func (*ClientDataEffect) isSessionEffect() {}

func (e *ClientDataEffect) ChannelID() uint64 { return e.channelID }
func (e *ClientDataEffect) Data() []byte      { return slices.Clone(e.data) }

// ClientHalfCloseEffect reports that the client has ended its data direction.
type ClientHalfCloseEffect struct {
	effectBase
	channelID uint64
}

func (*ClientHalfCloseEffect) isSessionEffect()    {}
func (e *ClientHalfCloseEffect) ChannelID() uint64 { return e.channelID }

// ClientResetEffect reports immediate peer termination of an accepted channel.
// Reason is bounded, valid UTF-8, untrusted text.
type ClientResetEffect struct {
	effectBase
	channelID uint64
	grpcCode  uint32
	reason    string
}

func (*ClientResetEffect) isSessionEffect() {}

func (e *ClientResetEffect) ChannelID() uint64 { return e.channelID }
func (e *ClientResetEffect) GRPCCode() uint32  { return e.grpcCode }
func (e *ClientResetEffect) Reason() string    { return e.reason }

// ServerHeartbeatAckEffect reports completion of one outstanding server probe.
type ServerHeartbeatAckEffect struct {
	effectBase
	nonce uint64
}

func (*ServerHeartbeatAckEffect) isSessionEffect() {}
func (e *ServerHeartbeatAckEffect) Nonce() uint64  { return e.nonce }

// NewChannelSessionState snapshots the generation and admitted slots. It may
// be constructed immediately before ConnectReady, but callers must not apply
// post-Ready frames until that boundary succeeds.
func NewChannelSessionState(admission *ConnectAdmission, limits ChannelSessionLimits) (*ChannelSessionState, error) {
	if admission == nil || admission.Generation() == 0 {
		return nil, invalidChannelConfig("admission", "must pin a nonzero generation")
	}
	limits, err := normalizeChannelSessionLimits(limits)
	if err != nil {
		return nil, err
	}

	slots := admission.Slots()
	if len(slots) == 0 || len(slots) > maxSlots {
		return nil, invalidChannelConfig("admission.slots", "must contain 1..256 admitted slots")
	}
	slotIDs := make([]string, len(slots))
	slotSet := make(map[string]struct{}, len(slots))
	for index, slot := range slots {
		slotID := slot.SlotID()
		if !IsValidIdentity(slotID) {
			return nil, invalidChannelConfig("admission.slots", "contains an invalid slot identity")
		}
		if _, exists := slotSet[slotID]; exists {
			return nil, invalidChannelConfig("admission.slots", "contains a duplicate slot identity")
		}
		slotIDs[index] = slotID
		slotSet[slotID] = struct{}{}
	}
	slices.Sort(slotIDs)

	return &ChannelSessionState{
		generation:        admission.Generation(),
		slotIDs:           slotIDs,
		slots:             slotSet,
		limits:            limits,
		channels:          make(map[uint64]*channelState),
		pendingHeartbeats: make(map[uint64]struct{}),
	}, nil
}

func normalizeChannelSessionLimits(limits ChannelSessionLimits) (ChannelSessionLimits, error) {
	if limits.MaxOpenChannels == 0 || limits.MaxOpenChannels > maxReadyOpenChannels {
		return ChannelSessionLimits{}, invalidChannelConfig("max_open_channels", "must be in 1..65535")
	}
	if limits.MaxDataBytes == 0 || limits.MaxDataBytes > maxReadyDataBytes {
		return ChannelSessionLimits{}, invalidChannelConfig("max_data_bytes", "must be in 1..65536")
	}
	if limits.RememberedChannelLimit == 0 {
		limits.RememberedChannelLimit = defaultRememberedChannelLimit
	}
	if limits.RememberedChannelLimit < limits.MaxOpenChannels || limits.RememberedChannelLimit > maximumRememberedChannelLimit {
		return ChannelSessionLimits{}, invalidChannelConfig("remembered_channel_limit", "must be at least max_open_channels and at most 1048576")
	}
	if limits.PendingHeartbeatLimit == 0 {
		limits.PendingHeartbeatLimit = defaultPendingHeartbeatLimit
	}
	if limits.PendingHeartbeatLimit > maximumPendingHeartbeatLimit {
		return ChannelSessionLimits{}, invalidChannelConfig("pending_heartbeat_limit", "must be at most 65535")
	}
	return limits, nil
}

// Generation returns the pinned fencing generation.
func (s *ChannelSessionState) Generation() uint64 {
	if s == nil {
		return 0
	}
	return s.generation
}

// SlotIDs returns a sorted independent copy of admitted slot identities.
func (s *ChannelSessionState) SlotIDs() []string {
	if s == nil {
		return nil
	}
	return slices.Clone(s.slotIDs)
}

// Limits returns the normalized immutable limits.
func (s *ChannelSessionState) Limits() ChannelSessionLimits {
	if s == nil {
		return ChannelSessionLimits{}
	}
	return s.limits
}

// Stats returns a concurrency-safe state snapshot.
func (s *ChannelSessionState) Stats() ChannelSessionStats {
	if s == nil {
		return ChannelSessionStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return ChannelSessionStats{
		RememberedChannels:  uint32(len(s.channels)),
		NonterminalChannels: s.nonterminal,
		PendingHeartbeats:   uint32(len(s.pendingHeartbeats)),
	}
}

// OpenServerChannel begins an even-ID EXECUTION_GRPC or ACTOR_INGRESS channel.
func (s *ChannelSessionState) OpenServerChannel(channelID uint64, kind externalproviderpb.ChannelKind, slotID string) (*SendServerFrameEffect, error) {
	if s == nil {
		return nil, invalidChannelOperation("state", "is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateNewChannelLocked(channelID, channelOriginServer, kind, slotID); err != nil {
		return nil, err
	}
	frame, err := s.serverFrame(&externalproviderpb.ServerFrame{Frame: &externalproviderpb.ServerFrame_Open{Open: &externalproviderpb.OpenChannel{
		ChannelId: channelID,
		Kind:      kind,
		SlotId:    slotID,
	}}})
	if err != nil {
		return nil, err
	}
	effect := &SendServerFrameEffect{frame: frame}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	s.channels[channelID] = &channelState{origin: channelOriginServer, phase: channelAwaitingClientAck, kind: kind, slotID: slotID}
	s.nonterminal++
	return effect, nil
}

// AcknowledgeClientOpen answers one client-opened channel exactly once.
func (s *ChannelSessionState) AcknowledgeClientOpen(channelID uint64, accepted bool, errorMessage string) (*SendServerFrameEffect, error) {
	if s == nil {
		return nil, invalidChannelOperation("state", "is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.localChannelLocked(channelID, channelOriginClient, channelAwaitingServerAck)
	if err != nil {
		return nil, err
	}
	if err := validateAckPairing(accepted, errorMessage); err != nil {
		return nil, invalidChannelOperation("open_ack", err.Error())
	}
	frame, err := s.serverFrame(&externalproviderpb.ServerFrame{Frame: &externalproviderpb.ServerFrame_OpenAck{OpenAck: &externalproviderpb.OpenChannelAck{
		ChannelId:    channelID,
		Accepted:     accepted,
		ErrorMessage: errorMessage,
	}}})
	if err != nil {
		return nil, err
	}
	effect := &SendServerFrameEffect{frame: frame}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	if accepted {
		channel.phase = channelAccepted
	} else {
		s.terminalizeLocked(channel)
	}
	return effect, nil
}

// SendServerData emits copied, nonempty data on an accepted channel.
func (s *ChannelSessionState) SendServerData(channelID uint64, data []byte) (*SendServerFrameEffect, error) {
	if s == nil {
		return nil, invalidChannelOperation("state", "is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.localAcceptedChannelLocked(channelID)
	if err != nil {
		return nil, err
	}
	if channel.serverHalfClosed {
		return nil, invalidChannelOperation("data", "cannot follow the server half-close")
	}
	if len(data) == 0 || uint64(len(data)) > uint64(s.limits.MaxDataBytes) {
		return nil, invalidChannelOperation("data", "length must be in 1..max_data_bytes")
	}
	frame, err := s.serverFrame(&externalproviderpb.ServerFrame{Frame: &externalproviderpb.ServerFrame_Data{Data: &externalproviderpb.ChannelData{
		ChannelId: channelID,
		Data:      slices.Clone(data),
	}}})
	if err != nil {
		return nil, err
	}
	effect := &SendServerFrameEffect{frame: frame}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	return effect, nil
}

// HalfCloseServerChannel ends the server data direction at most once.
func (s *ChannelSessionState) HalfCloseServerChannel(channelID uint64) (*SendServerFrameEffect, error) {
	if s == nil {
		return nil, invalidChannelOperation("state", "is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.localAcceptedChannelLocked(channelID)
	if err != nil {
		return nil, err
	}
	if channel.serverHalfClosed {
		return nil, invalidChannelOperation("half_close", "server direction is already closed")
	}
	frame, err := s.serverFrame(&externalproviderpb.ServerFrame{Frame: &externalproviderpb.ServerFrame_HalfClose{HalfClose: &externalproviderpb.HalfCloseChannel{ChannelId: channelID}}})
	if err != nil {
		return nil, err
	}
	effect := &SendServerFrameEffect{frame: frame}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	channel.serverHalfClosed = true
	if channel.clientHalfClosed {
		s.terminalizeLocked(channel)
	}
	return effect, nil
}

// ResetServerChannel terminates an accepted channel in both directions.
func (s *ChannelSessionState) ResetServerChannel(channelID uint64, grpcCode uint32, reason string) (*SendServerFrameEffect, error) {
	if s == nil {
		return nil, invalidChannelOperation("state", "is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	channel, err := s.localAcceptedChannelLocked(channelID)
	if err != nil {
		return nil, err
	}
	if err := validateReset(grpcCode, reason); err != nil {
		return nil, invalidChannelOperation("reset", err.Error())
	}
	frame, err := s.serverFrame(&externalproviderpb.ServerFrame{Frame: &externalproviderpb.ServerFrame_Reset_{Reset_: &externalproviderpb.ResetChannel{
		ChannelId: channelID,
		GrpcCode:  grpcCode,
		Reason:    reason,
	}}})
	if err != nil {
		return nil, err
	}
	effect := &SendServerFrameEffect{frame: frame}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	s.terminalizeLocked(channel)
	return effect, nil
}

// SendServerHeartbeat starts one bounded probe. A nonce may not be pending
// twice; it may be reused only after its acknowledgement was consumed.
func (s *ChannelSessionState) SendServerHeartbeat(nonce uint64) (*SendServerFrameEffect, error) {
	if s == nil {
		return nil, invalidChannelOperation("state", "is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if nonce == 0 {
		return nil, invalidChannelOperation("heartbeat.nonce", "must be nonzero")
	}
	if _, pending := s.pendingHeartbeats[nonce]; pending {
		return nil, invalidChannelOperation("heartbeat.nonce", "is already pending")
	}
	if uint32(len(s.pendingHeartbeats)) >= s.limits.PendingHeartbeatLimit {
		return nil, invalidChannelOperation("heartbeat", "pending heartbeat limit is exhausted")
	}
	frame, err := s.serverFrame(&externalproviderpb.ServerFrame{Frame: &externalproviderpb.ServerFrame_Heartbeat{Heartbeat: &externalproviderpb.Heartbeat{Nonce: nonce}}})
	if err != nil {
		return nil, err
	}
	effect := &SendServerFrameEffect{frame: frame}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	s.pendingHeartbeats[nonce] = struct{}{}
	return effect, nil
}

// ApplyClientFrame validates and atomically applies one post-Ready client
// frame. Every success returns exactly one immutable effect.
func (s *ChannelSessionState) ApplyClientFrame(frame *externalproviderpb.ClientFrame) (SessionEffect, error) {
	if s == nil {
		return nil, protocolViolation("state", "is required")
	}
	if frame == nil {
		return nil, protocolViolation("frame", "is required")
	}
	if proto.Size(frame) > maxClientFrameBytes {
		return nil, protocolViolation("frame", "exceeds the 1 MiB serialized limit")
	}
	if frame.GetSessionGeneration() != s.generation {
		return nil, protocolViolation("frame.session_generation", "does not match this session")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	switch member := frame.GetFrame().(type) {
	case *externalproviderpb.ClientFrame_Open:
		if member == nil || member.Open == nil {
			return nil, protocolViolation("frame.open", "is required")
		}
		return s.applyClientOpenLocked(member.Open)
	case *externalproviderpb.ClientFrame_OpenAck:
		if member == nil || member.OpenAck == nil {
			return nil, protocolViolation("frame.open_ack", "is required")
		}
		return s.applyClientAckLocked(member.OpenAck)
	case *externalproviderpb.ClientFrame_Data:
		if member == nil || member.Data == nil {
			return nil, protocolViolation("frame.data", "is required")
		}
		return s.applyClientDataLocked(member.Data)
	case *externalproviderpb.ClientFrame_HalfClose:
		if member == nil || member.HalfClose == nil {
			return nil, protocolViolation("frame.half_close", "is required")
		}
		return s.applyClientHalfCloseLocked(member.HalfClose)
	case *externalproviderpb.ClientFrame_Reset_:
		if member == nil || member.Reset_ == nil {
			return nil, protocolViolation("frame.reset", "is required")
		}
		return s.applyClientResetLocked(member.Reset_)
	case *externalproviderpb.ClientFrame_Heartbeat:
		if member == nil || member.Heartbeat == nil {
			return nil, protocolViolation("frame.heartbeat", "is required")
		}
		return s.applyClientHeartbeatLocked(member.Heartbeat)
	case *externalproviderpb.ClientFrame_Hello:
		return nil, protocolViolation("frame.hello", "is forbidden after ConnectReady")
	default:
		return nil, protocolViolation("frame", "must contain exactly one non-Hello member")
	}
}

func (s *ChannelSessionState) applyClientOpenLocked(open *externalproviderpb.OpenChannel) (SessionEffect, error) {
	if err := s.validateNewChannelLocked(open.GetChannelId(), channelOriginClient, open.GetKind(), open.GetSlotId()); err != nil {
		return nil, asProtocolViolation(err)
	}
	effect := &ClientOpenEffect{channelID: open.GetChannelId(), kind: open.GetKind(), slotID: open.GetSlotId()}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	s.channels[open.GetChannelId()] = &channelState{
		origin: channelOriginClient,
		phase:  channelAwaitingServerAck,
		kind:   open.GetKind(),
		slotID: open.GetSlotId(),
	}
	s.nonterminal++
	return effect, nil
}

func (s *ChannelSessionState) applyClientAckLocked(ack *externalproviderpb.OpenChannelAck) (SessionEffect, error) {
	channel, err := s.peerChannelLocked(ack.GetChannelId(), channelOriginServer, channelAwaitingClientAck)
	if err != nil {
		return nil, err
	}
	if err := validateAckPairing(ack.GetAccepted(), ack.GetErrorMessage()); err != nil {
		return nil, protocolViolation("frame.open_ack", err.Error())
	}
	effect := &ServerOpenAckEffect{channelID: ack.GetChannelId(), accepted: ack.GetAccepted(), errorMessage: ack.GetErrorMessage()}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	if ack.GetAccepted() {
		channel.phase = channelAccepted
	} else {
		s.terminalizeLocked(channel)
	}
	return effect, nil
}

func (s *ChannelSessionState) applyClientDataLocked(data *externalproviderpb.ChannelData) (SessionEffect, error) {
	channel, err := s.peerAcceptedChannelLocked(data.GetChannelId())
	if err != nil {
		return nil, err
	}
	if channel.clientHalfClosed {
		return nil, protocolViolation("frame.data", "cannot follow the client half-close")
	}
	if len(data.GetData()) == 0 || uint64(len(data.GetData())) > uint64(s.limits.MaxDataBytes) {
		return nil, protocolViolation("frame.data.data", "length must be in 1..max_data_bytes")
	}
	effect := &ClientDataEffect{channelID: data.GetChannelId(), data: slices.Clone(data.GetData())}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	return effect, nil
}

func (s *ChannelSessionState) applyClientHalfCloseLocked(halfClose *externalproviderpb.HalfCloseChannel) (SessionEffect, error) {
	channel, err := s.peerAcceptedChannelLocked(halfClose.GetChannelId())
	if err != nil {
		return nil, err
	}
	if channel.clientHalfClosed {
		return nil, protocolViolation("frame.half_close", "client direction is already closed")
	}
	effect := &ClientHalfCloseEffect{channelID: halfClose.GetChannelId()}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	channel.clientHalfClosed = true
	if channel.serverHalfClosed {
		s.terminalizeLocked(channel)
	}
	return effect, nil
}

func (s *ChannelSessionState) applyClientResetLocked(reset *externalproviderpb.ResetChannel) (SessionEffect, error) {
	channel, err := s.peerAcceptedChannelLocked(reset.GetChannelId())
	if err != nil {
		return nil, err
	}
	if err := validateReset(reset.GetGrpcCode(), reset.GetReason()); err != nil {
		return nil, protocolViolation("frame.reset", err.Error())
	}
	effect := &ClientResetEffect{channelID: reset.GetChannelId(), grpcCode: reset.GetGrpcCode(), reason: reset.GetReason()}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	s.terminalizeLocked(channel)
	return effect, nil
}

func (s *ChannelSessionState) applyClientHeartbeatLocked(heartbeat *externalproviderpb.Heartbeat) (SessionEffect, error) {
	if heartbeat.GetNonce() == 0 {
		return nil, protocolViolation("frame.heartbeat.nonce", "must be nonzero")
	}
	if heartbeat.GetAcknowledgement() {
		if _, pending := s.pendingHeartbeats[heartbeat.GetNonce()]; !pending {
			return nil, protocolViolation("frame.heartbeat", "acknowledges no pending server probe")
		}
		effect := &ServerHeartbeatAckEffect{nonce: heartbeat.GetNonce()}
		if err := s.stampEffectLocked(effect); err != nil {
			return nil, err
		}
		delete(s.pendingHeartbeats, heartbeat.GetNonce())
		return effect, nil
	}
	frame, err := s.serverFrame(&externalproviderpb.ServerFrame{Frame: &externalproviderpb.ServerFrame_Heartbeat{Heartbeat: &externalproviderpb.Heartbeat{
		Nonce:           heartbeat.GetNonce(),
		Acknowledgement: true,
	}}})
	if err != nil {
		return nil, err
	}
	effect := &SendServerFrameEffect{frame: frame}
	if err := s.stampEffectLocked(effect); err != nil {
		return nil, err
	}
	return effect, nil
}

func (s *ChannelSessionState) validateNewChannelLocked(channelID uint64, origin channelOrigin, kind externalproviderpb.ChannelKind, slotID string) error {
	if channelID == 0 {
		return invalidChannelOperation("open.channel_id", "must be nonzero")
	}
	if origin == channelOriginServer {
		if channelID%2 != 0 {
			return invalidChannelOperation("open.channel_id", "server-opened IDs must be even")
		}
		if kind != externalproviderpb.ChannelKind_CHANNEL_KIND_EXECUTION_GRPC && kind != externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_INGRESS {
			return invalidChannelOperation("open.kind", "server may open only EXECUTION_GRPC or ACTOR_INGRESS")
		}
	} else {
		if channelID%2 == 0 {
			return invalidChannelOperation("open.channel_id", "client-opened IDs must be odd")
		}
		if kind != externalproviderpb.ChannelKind_CHANNEL_KIND_ACTOR_EGRESS {
			return invalidChannelOperation("open.kind", "client may open only ACTOR_EGRESS")
		}
	}
	if _, admitted := s.slots[slotID]; !admitted {
		return invalidChannelOperation("open.slot_id", "is not admitted for this generation")
	}
	if _, used := s.channels[channelID]; used {
		return invalidChannelOperation("open.channel_id", "cannot be reused in this generation")
	}
	if s.nonterminal >= s.limits.MaxOpenChannels {
		return invalidChannelOperation("open", "max_open_channels is exhausted")
	}
	if uint32(len(s.channels)) >= s.limits.RememberedChannelLimit {
		return invalidChannelOperation("open", "remembered channel ID limit is exhausted")
	}
	return nil
}

func (s *ChannelSessionState) localChannelLocked(channelID uint64, origin channelOrigin, phase channelPhase) (*channelState, error) {
	channel := s.channels[channelID]
	if channel == nil || channel.origin != origin || channel.phase != phase {
		return nil, invalidChannelOperation("channel_id", "does not identify the required pending channel")
	}
	return channel, nil
}

func (s *ChannelSessionState) peerChannelLocked(channelID uint64, origin channelOrigin, phase channelPhase) (*channelState, error) {
	channel := s.channels[channelID]
	if channel == nil || channel.origin != origin || channel.phase != phase {
		return nil, protocolViolation("frame.channel_id", "does not identify the required pending channel")
	}
	return channel, nil
}

func (s *ChannelSessionState) localAcceptedChannelLocked(channelID uint64) (*channelState, error) {
	channel := s.channels[channelID]
	if channel == nil || channel.phase != channelAccepted {
		return nil, invalidChannelOperation("channel_id", "does not identify an accepted nonterminal channel")
	}
	return channel, nil
}

func (s *ChannelSessionState) peerAcceptedChannelLocked(channelID uint64) (*channelState, error) {
	channel := s.channels[channelID]
	if channel == nil || channel.phase != channelAccepted {
		return nil, protocolViolation("frame.channel_id", "does not identify an accepted nonterminal channel")
	}
	return channel, nil
}

func (s *ChannelSessionState) terminalizeLocked(channel *channelState) {
	if channel.phase != channelTerminal {
		channel.phase = channelTerminal
		s.nonterminal--
	}
}

type effectSequenceSetter interface {
	setSequence(uint64)
}

func (s *ChannelSessionState) stampEffectLocked(effect effectSequenceSetter) error {
	if s.effectSequence == math.MaxUint64 {
		return invalidChannelOperation("effect_sequence", "is exhausted; replace the session generation")
	}
	s.effectSequence++
	effect.setSequence(s.effectSequence)
	return nil
}

func (s *ChannelSessionState) serverFrame(frame *externalproviderpb.ServerFrame) (*externalproviderpb.ServerFrame, error) {
	frame.SessionGeneration = s.generation
	if proto.Size(frame) > maxServerFrameBytes {
		return nil, invalidChannelOperation("server_frame", "exceeds the 1 MiB serialized limit")
	}
	return frame, nil
}

func validateAckPairing(accepted bool, errorMessage string) error {
	if !utf8.ValidString(errorMessage) || len(errorMessage) > maxChannelTextBytes {
		return errors.New("error_message must be valid UTF-8 and at most 1024 bytes")
	}
	if accepted && errorMessage != "" {
		return errors.New("accepted ack requires an empty error_message")
	}
	if !accepted && errorMessage == "" {
		return errors.New("rejected ack requires a nonempty error_message")
	}
	return nil
}

func validateReset(grpcCode uint32, reason string) error {
	if grpcCode == 0 || grpcCode > 16 {
		return errors.New("grpc_code must be in 1..16")
	}
	if !utf8.ValidString(reason) || len(reason) > maxChannelTextBytes {
		return errors.New("reason must be valid UTF-8 and at most 1024 bytes")
	}
	return nil
}

func invalidChannelConfig(path, reason string) error {
	return fmt.Errorf("%w: %s %s", ErrInvalidChannelStateConfig, path, reason)
}

func invalidChannelOperation(path, reason string) error {
	return fmt.Errorf("%w: %s %s", ErrInvalidChannelOperation, path, reason)
}

func protocolViolation(path, reason string) error {
	return fmt.Errorf("%w: %s %s", ErrChannelProtocolViolation, path, reason)
}

func asProtocolViolation(err error) error {
	if err == nil {
		return nil
	}
	return protocolViolation("frame.open", err.Error())
}
