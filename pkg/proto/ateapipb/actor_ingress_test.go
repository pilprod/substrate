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

package ateapipb

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestActorIngressDescriptor(t *testing.T) {
	control := File_ateapi_proto.Services().ByName("Control")
	if control == nil {
		t.Fatal("Control service is missing")
	}
	method := control.Methods().ByName("OpenActorIngress")
	if method == nil {
		t.Fatal("Control.OpenActorIngress is missing")
	}
	if !method.IsStreamingClient() || !method.IsStreamingServer() {
		t.Fatalf("OpenActorIngress streaming = client:%t server:%t, want bidirectional", method.IsStreamingClient(), method.IsStreamingServer())
	}
	wantFrame := (&ActorIngressFrame{}).ProtoReflect().Descriptor()
	if method.Input() != wantFrame || method.Output() != wantFrame {
		t.Fatalf("OpenActorIngress input/output = %v/%v, want %v", method.Input().FullName(), method.Output().FullName(), wantFrame.FullName())
	}

	fields := wantFrame.Fields()
	want := map[protoreflect.Name]protoreflect.Kind{
		"open":       protoreflect.MessageKind,
		"opened":     protoreflect.MessageKind,
		"data":       protoreflect.BytesKind,
		"half_close": protoreflect.MessageKind,
		"reset":      protoreflect.MessageKind,
	}
	if fields.Len() != len(want) {
		t.Fatalf("ActorIngressFrame field count = %d, want %d", fields.Len(), len(want))
	}
	for name, kind := range want {
		field := fields.ByName(name)
		if field == nil || field.Kind() != kind || field.ContainingOneof() == nil {
			t.Errorf("ActorIngressFrame.%s = %v, want %v oneof member", name, field, kind)
		}
	}
	dataOptions, ok := fields.ByName("data").Options().(*descriptorpb.FieldOptions)
	if !ok || !dataOptions.GetDebugRedact() {
		t.Error("ActorIngressFrame.data does not set debug_redact")
	}
	open := (&ActorIngressOpen{}).ProtoReflect().Descriptor()
	if open.Fields().Len() != 2 || open.Fields().ByName("actor") == nil || open.Fields().ByName("actor_uid") == nil {
		t.Fatalf("ActorIngressOpen fields = %v, want actor and actor_uid", open.Fields())
	}
}
