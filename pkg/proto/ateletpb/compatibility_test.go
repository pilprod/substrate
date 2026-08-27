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

package ateletpb

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
)

const publicGoPackage = "github.com/agent-substrate/substrate/pkg/proto/ateletpb"

func TestPublicContractIdentity(t *testing.T) {
	if got, want := string(File_atelet_proto.Package()), "atelet"; got != want {
		t.Fatalf("protobuf package = %q, want %q", got, want)
	}
	descriptor := protodesc.ToFileDescriptorProto(File_atelet_proto)
	if got := descriptor.GetOptions().GetGoPackage(); got != publicGoPackage {
		t.Fatalf("Go package = %q, want %q", got, publicGoPackage)
	}
	if got, want := CredentialBroker_ServiceDesc.ServiceName, "atelet.CredentialBroker"; got != want {
		t.Fatalf("CredentialBroker service name = %q, want %q", got, want)
	}
	if got, want := AteomHerder_ServiceDesc.ServiceName, "atelet.AteomHerder"; got != want {
		t.Fatalf("AteomHerder service name = %q, want %q", got, want)
	}
}

func TestPublishedSchemaBaseline(t *testing.T) {
	descriptor := protodesc.ToFileDescriptorProto(File_atelet_proto)
	// This is intentionally an exact published-schema freeze, not a general
	// protobuf compatibility checker. Even an additive or reordered declaration
	// must receive explicit public-API review before this baseline is updated.
	// GoPackage and source locations are excluded because they do not change the
	// protobuf contract and are asserted separately above where relevant.
	if descriptor.Options != nil {
		descriptor.Options.GoPackage = nil
	}
	descriptor.SourceCodeInfo = nil

	wireDescriptor, err := proto.MarshalOptions{Deterministic: true}.Marshal(descriptor)
	if err != nil {
		t.Fatalf("marshal protobuf descriptor: %v", err)
	}
	sum := sha256.Sum256(wireDescriptor)
	got := hex.EncodeToString(sum[:])
	const want = "f5b83385317f1965cf3b9d91a5e5ffe40c0f4852f601e34190f3c0040a2b9f46"
	if got != want {
		t.Fatalf("published schema SHA-256 = %q, want %q", got, want)
	}
}
