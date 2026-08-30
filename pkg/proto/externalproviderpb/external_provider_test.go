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

package externalproviderpb

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestExternalProviderBrokerDescriptor(t *testing.T) {
	if got, want := string(File_external_provider_proto.Package()), "externalprovider"; got != want {
		t.Fatalf("protobuf package = %q, want %q", got, want)
	}

	services := File_external_provider_proto.Services()
	if got, want := services.Len(), 2; got != want {
		t.Fatalf("service count = %d, want %d", got, want)
	}
	service := services.ByName("ExternalProviderBroker")
	if service == nil {
		t.Fatal("ExternalProviderBroker service is missing")
	}
	if got, want := service.FullName(), protoreflect.FullName("externalprovider.ExternalProviderBroker"); got != want {
		t.Fatalf("service full name = %q, want %q", got, want)
	}
	fileDescriptor := protodesc.ToFileDescriptorProto(File_external_provider_proto)
	if got, want := fileDescriptor.GetOptions().GetGoPackage(), "github.com/agent-substrate/substrate/pkg/proto/externalproviderpb"; got != want {
		t.Fatalf("go_package = %q, want %q", got, want)
	}

	tests := []struct {
		name            protoreflect.Name
		clientStreaming bool
		serverStreaming bool
	}{
		{name: "Enroll"},
		{name: "MintSessionToken"},
		{name: "Connect", clientStreaming: true, serverStreaming: true},
	}
	if got, want := service.Methods().Len(), len(tests); got != want {
		t.Fatalf("method count = %d, want %d", got, want)
	}
	for _, test := range tests {
		method := service.Methods().ByName(test.name)
		if method == nil {
			t.Errorf("method %q is missing", test.name)
			continue
		}
		if got := method.IsStreamingClient(); got != test.clientStreaming {
			t.Errorf("%s client streaming = %t, want %t", test.name, got, test.clientStreaming)
		}
		if got := method.IsStreamingServer(); got != test.serverStreaming {
			t.Errorf("%s server streaming = %t, want %t", test.name, got, test.serverStreaming)
		}
	}
	admin := services.ByName("ExternalProviderAdmin")
	if admin == nil || admin.FullName() != "externalprovider.ExternalProviderAdmin" {
		t.Fatalf("ExternalProviderAdmin service = %v", admin)
	}
	if got, want := admin.Methods().Len(), 1; got != want || admin.Methods().ByName("CreateExternalProviderEnrollment") == nil {
		t.Fatalf("ExternalProviderAdmin methods = %v, want CreateExternalProviderEnrollment", admin.Methods())
	}

	capacity := (&ExternalSlot{}).ProtoReflect().Descriptor().Fields().ByName("capacity").Message()
	wantCapacity := (&ateapipb.WorkerCapacity{}).ProtoReflect().Descriptor()
	if capacity != wantCapacity {
		t.Fatalf("ExternalSlot.capacity descriptor = %v, want public ateapipb.WorkerCapacity descriptor %v", capacity.FullName(), wantCapacity.FullName())
	}
	foundAteapiImport := false
	imports := File_external_provider_proto.Imports()
	for i := 0; i < imports.Len(); i++ {
		imported := imports.Get(i).FileDescriptor
		if imported.Path() != "ateapi.proto" {
			continue
		}
		foundAteapiImport = true
		if imported.IsPlaceholder() {
			t.Error("ateapi.proto import resolved to a placeholder descriptor")
		}
	}
	if !foundAteapiImport {
		t.Error("resolved ateapi.proto import is missing")
	}
}

func TestSensitiveFieldsAreDebugRedacted(t *testing.T) {
	want := map[protoreflect.FullName]protoreflect.Kind{
		"externalprovider.CreateExternalProviderEnrollmentResponse.enrollment_credential": protoreflect.BytesKind,
		"externalprovider.EnrollResponse.refresh_credential":                              protoreflect.BytesKind,
		"externalprovider.MintSessionTokenResponse.session_token":                         protoreflect.BytesKind,
		"externalprovider.ChannelData.data":                                               protoreflect.BytesKind,
		"externalprovider.OpenChannelAck.error_message":                                   protoreflect.StringKind,
		"externalprovider.ActorEgressOpen.certificate_signing_request_der":                protoreflect.BytesKind,
		"externalprovider.ActorEgressOpenAck.certificate_chain_der":                       protoreflect.BytesKind,
		"externalprovider.ActorEgressOpenAck.gateway_trust_bundle_pem":                    protoreflect.BytesKind,
		"externalprovider.ResetChannel.reason":                                            protoreflect.StringKind,
	}
	found := make(map[protoreflect.FullName]bool, len(want))

	visitMessages(File_external_provider_proto.Messages(), func(message protoreflect.MessageDescriptor) {
		fields := message.Fields()
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			wantKind, isSensitive := want[field.FullName()]
			if !isSensitive {
				if field.Kind() != protoreflect.BytesKind {
					continue
				}
				t.Errorf("unexpected bytes field %s; credential and data bytes require an explicit redaction decision", field.FullName())
				continue
			}
			if field.Kind() != wantKind {
				t.Errorf("field %s kind = %v, want %v", field.FullName(), field.Kind(), wantKind)
			}
			options, ok := field.Options().(*descriptorpb.FieldOptions)
			if !ok || !options.GetDebugRedact() {
				t.Errorf("field %s does not set debug_redact", field.FullName())
			}
			found[field.FullName()] = true
		}
	})

	for field := range want {
		if !found[field] {
			t.Errorf("sensitive field %s is missing", field)
		}
	}
}

func TestRequestMessagesCarryNoCredentialFields(t *testing.T) {
	hello := (&ConnectHello{}).ProtoReflect().Descriptor()
	fields := hello.Fields()
	if got, want := fields.Len(), 4; got != want {
		t.Fatalf("ConnectHello field count = %d, want %d", got, want)
	}
	if fields.ByName("registration_uid") == nil || fields.ByName("slots") == nil || fields.ByName("protocol_version") == nil || fields.ByName("slot_policy_digest") == nil {
		t.Fatalf("ConnectHello fields = %v, want registration_uid, slots, protocol_version, and slot_policy_digest", fieldNames(fields))
	}

	for _, message := range []protoreflect.MessageDescriptor{
		(&CreateExternalProviderEnrollmentRequest{}).ProtoReflect().Descriptor(),
		(&EnrollRequest{}).ProtoReflect().Descriptor(),
		(&MintSessionTokenRequest{}).ProtoReflect().Descriptor(),
		(&ClientFrame{}).ProtoReflect().Descriptor(),
		hello,
		(&ExternalSlot{}).ProtoReflect().Descriptor(),
	} {
		messageFields := message.Fields()
		for i := 0; i < messageFields.Len(); i++ {
			name := strings.ToLower(string(messageFields.Get(i).Name()))
			for _, forbidden := range []string{
				"authorization",
				"credential",
				"token",
			} {
				if strings.Contains(name, forbidden) {
					t.Errorf("%s exposes forbidden field %q", message.FullName(), name)
				}
			}
		}
	}
}

func TestWireExposesNoProviderRoute(t *testing.T) {
	visitMessages(File_external_provider_proto.Messages(), func(message protoreflect.MessageDescriptor) {
		fields := message.Fields()
		for i := 0; i < fields.Len(); i++ {
			name := strings.ToLower(string(fields.Get(i).Name()))
			for _, forbidden := range []string{
				"endpoint",
				"execution_identity",
				"locality_identity",
				"route",
			} {
				if strings.Contains(name, forbidden) {
					t.Errorf("%s exposes forbidden route field %q", message.FullName(), name)
				}
			}
		}
	})
}

func TestProviderNeutralChannelKinds(t *testing.T) {
	enum := ChannelKind(0).Descriptor()
	want := []protoreflect.Name{
		"CHANNEL_KIND_UNSPECIFIED",
		"CHANNEL_KIND_EXECUTION_GRPC",
		"CHANNEL_KIND_ACTOR_INGRESS",
		"CHANNEL_KIND_ACTOR_EGRESS",
	}
	if got := enum.Values().Len(); got != len(want) {
		t.Fatalf("ChannelKind value count = %d, want %d", got, len(want))
	}
	for i, name := range want {
		if got := enum.Values().Get(i).Name(); got != name {
			t.Errorf("ChannelKind value %d = %q, want %q", i, got, name)
		}
	}

	descriptor := protodesc.ToFileDescriptorProto(File_external_provider_proto)
	descriptor.SourceCodeInfo = nil
	text := strings.ToLower(descriptor.String())
	for _, forbidden := range []string{"claude", "codex", "docker", "native"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("provider wire descriptor contains product/runtime term %q", forbidden)
		}
	}
}

func TestPublishedSchemaBaseline(t *testing.T) {
	descriptor := protodesc.ToFileDescriptorProto(File_external_provider_proto)
	descriptor.SourceCodeInfo = nil
	// This is an intentional exact baseline of the published protobuf schema.
	// Go package identity is asserted separately above, so the hash covers the
	// protobuf package, services, messages, options, and wire encoding.
	if descriptor.Options != nil {
		descriptor.Options.GoPackage = nil
	}
	wireDescriptor, err := proto.MarshalOptions{Deterministic: true}.Marshal(descriptor)
	if err != nil {
		t.Fatalf("marshal protobuf descriptor: %v", err)
	}
	sum := sha256.Sum256(wireDescriptor)
	got := hex.EncodeToString(sum[:])
	const want = "c8284eceb6636e10e7d4d4bbf4fb408c6da26bec17184ee5019806b12510ae85"
	if got != want {
		t.Fatalf("wire descriptor SHA-256 = %q, want %q", got, want)
	}
}

func visitMessages(messages protoreflect.MessageDescriptors, visit func(protoreflect.MessageDescriptor)) {
	for i := 0; i < messages.Len(); i++ {
		message := messages.Get(i)
		visit(message)
		visitMessages(message.Messages(), visit)
	}
}

func fieldNames(fields protoreflect.FieldDescriptors) []protoreflect.Name {
	names := make([]protoreflect.Name, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		names = append(names, fields.Get(i).Name())
	}
	return names
}
