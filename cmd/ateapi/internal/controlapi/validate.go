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

package controlapi

import (
	"context"
	"reflect"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/reflect/protoreflect"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func toGRPCStatusError(errs field.ErrorList) error {
	return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
}

// validateNoUnknownFields reports an error for every unknown field in m, at
// every level of nesting.
//
// A client newer than this binary can set fields this binary has no descriptor
// for. protobuf keeps those bytes on the parsed message, so an Update — which
// replaces the whole object — would persist them. The server cannot validate
// such a field.
func validateNoUnknownFields(m proto.Message, fldPath *field.Path) field.ErrorList {
	r := m.ProtoReflect()
	if !r.IsValid() {
		return nil
	}

	var errs field.ErrorList
	if err := protorange.Range(r, func(p protopath.Values) error {
		msg, ok := p.Index(-1).Value.Interface().(protoreflect.Message)
		if !ok || len(msg.GetUnknown()) == 0 {
			return nil
		}
		// Report the path of the unknwown field
		errs = append(errs, field.Invalid(toFieldPath(p.Path, fldPath), field.OmitValueType{}, ""))
		return nil
	}); err != nil {
		errs = append(errs, field.InternalError(fldPath, err))
	}
	return errs
}

// toFieldPath renders a protopath as a field.Path rooted at root.
func toFieldPath(p protopath.Path, root *field.Path) *field.Path {
	out := root
	for _, step := range p {
		switch step.Kind() {
		case protopath.FieldAccessStep:
			out = out.Child(step.FieldDescriptor().TextName())
		case protopath.ListIndexStep:
			out = out.Index(step.ListIndex())
		case protopath.MapIndexStep:
			out = out.Key(step.MapIndex().String())
		}
	}
	return out
}

func toGRPCInternalError(errs field.ErrorList) error {
	return status.Error(codes.Internal, errs.ToAggregate().Error())
}

// scrubResourceMetadataForCreate removes fields that should not be set by the
// user when creating a resource.
func scrubResourceMetadataForCreate(in *ateapipb.ResourceMetadata) {
	if in == nil {
		return // validation will flag it
	}
	in.Uid = ""         // will be set later
	in.Version = 0      // will be set later
	in.CreateTime = nil // will be set later
	in.UpdateTime = nil // will be set later
}

// scrubResourceMetadataForUpdate removes fields that should not be set by the
// user when updating a resource.
func scrubResourceMetadataForUpdate(in *ateapipb.ResourceMetadata) {
	if in == nil {
		return // validation will flag it
	}
	// in.Uid and in.Version are preconditions, so we don't scrub them.
	in.CreateTime = nil // will be set later
	in.UpdateTime = nil // will be set later
}

// ateDeepEqual compares two values of any type, using proto.Equal if both are
// proto messages, and reflect.DeepEqual otherwise.  This is called by
// declarative validation's generated code.
func ateDeepEqual[T any](a, b T) bool {
	asProto := func(x any) proto.Message {
		pm, ok := x.(proto.Message)
		if !ok {
			return nil
		}
		return pm
	}

	if pa, pb := asProto(a), asProto(b); pa != nil && pb != nil {
		return proto.Equal(pa, pb)
	}
	return reflect.DeepEqual(a, b)
}

// This exists only because nested subfield tags are not supported yet.
func ValidateCustom_UpdateActorRequest_Actor(ctx context.Context, op operation.Operation, fldPath *field.Path, actor, _ *ateapipb.Actor) field.ErrorList {
	if actor == nil || actor.Metadata == nil {
		return nil // handled by DV
	}

	// Updates are validated in 2 steps: first the update request and then the
	// resource itself. DV for the request doesn't descend into the resource
	// metadata.  Once DV supports nested subfield tags, this can be changed to
	// something like:
	//   +k8s:subfield(metadata)=+k8s:subfield(atespace)=+k8s:required
	errs := Validate_ResourceMetadata(ctx, op, fldPath.Child("metadata"), actor.Metadata, nil)
	errs = append(errs, validate.RequiredValue(ctx, op, fldPath.Child("metadata", "atespace"), &actor.Metadata.Atespace, nil)...)
	return errs
}

// This is needed because DV doesn't have a standard format for IP addresses yet.
func ValidateCustom_WorkerAssignment_WorkerPodIp(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if value == nil || *value == "" {
		return nil
	}
	return validation.IsValidIP(fldPath, *value)
}

// ValidateCustom_WorkerAssignment applies the provider-specific identity
// contract that cannot be expressed as independent field validations.
func ValidateCustom_WorkerAssignment(_ context.Context, _ operation.Operation, fldPath *field.Path, assignment, _ *ateapipb.WorkerAssignment) field.ErrorList {
	if assignment == nil {
		return nil
	}
	var errs field.ErrorList
	switch effectiveWorkerProvider(assignment.GetProvider()) {
	case ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD:
		if assignment.GetExternalSlot() != nil {
			errs = append(errs, field.Forbidden(fldPath.Child("external_slot"), "must be empty for a KubernetesPod worker"))
		}
		for _, f := range []struct {
			name  string
			value string
		}{
			{name: "worker_pod", value: assignment.GetWorkerPod()},
			{name: "worker_pod_uid", value: assignment.GetWorkerPodUid()},
			{name: "worker_pod_ip", value: assignment.GetWorkerPodIp()},
		} {
			if f.value == "" {
				errs = append(errs, field.Required(fldPath.Child(f.name), ""))
			}
		}
	case ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT:
		if assignment.GetWorkerResourceUid() == "" {
			errs = append(errs, field.Required(fldPath.Child("worker_resource_uid"), "required for an ExternalSlot assignment"))
		}
		if assignment.GetExternalSlot() == nil {
			errs = append(errs, field.Required(fldPath.Child("external_slot"), ""))
		}
		for _, f := range []struct {
			name  string
			value string
		}{
			{name: "worker_pod", value: assignment.GetWorkerPod()},
			{name: "worker_pod_uid", value: assignment.GetWorkerPodUid()},
			{name: "worker_pod_ip", value: assignment.GetWorkerPodIp()},
		} {
			if f.value != "" {
				errs = append(errs, field.Forbidden(fldPath.Child(f.name), "must be empty for an ExternalSlot worker"))
			}
		}
	default:
		errs = append(errs, field.NotSupported(fldPath.Child("provider"), assignment.GetProvider().String(), []string{
			ateapipb.WorkerProvider_WORKER_PROVIDER_UNSPECIFIED.String(),
			ateapipb.WorkerProvider_WORKER_PROVIDER_KUBERNETES_POD.String(),
			ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT.String(),
		}))
	}
	return errs
}

func ValidateCustom_ExternalSlotIdentity_ExecutionIdentity(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if value == nil {
		return nil
	}
	return validateExternalSlotIdentityCharacters(*value, fldPath)
}

func ValidateCustom_ExternalSlotIdentity_LocalityIdentity(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	if value == nil {
		return nil
	}
	return validateExternalSlotIdentityCharacters(*value, fldPath)
}
