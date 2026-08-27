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
	"net"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/ateletpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fixedExecutionDialer struct {
	connection *grpc.ClientConn
}

func (d *fixedExecutionDialer) DialForWorker(*ateapipb.WorkerAssignment) (*grpc.ClientConn, error) {
	return d.connection, nil
}

func (d *fixedExecutionDialer) DialForLocalSnapshot(*ateapipb.LocalSnapshotInfo) (*grpc.ClientConn, error) {
	return d.connection, nil
}

type recordingAteomHerder struct {
	ateletpb.UnimplementedAteomHerderServer
	runCalls     int
	restoreCalls int
	runRequest   *ateletpb.RunRequest
}

func (s *recordingAteomHerder) Run(_ context.Context, request *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	s.runCalls++
	s.runRequest = request
	return &ateletpb.RunResponse{}, nil
}

func (s *recordingAteomHerder) Restore(context.Context, *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	s.restoreCalls++
	return &ateletpb.RestoreResponse{}, nil
}

func TestExternalSlotSnapshotLifecycleRejectedBeforeMutation(t *testing.T) {
	operations := map[string]func(context.Context, *ActorWorkflow, resources.ActorRef) error{
		"pause": func(ctx context.Context, workflow *ActorWorkflow, actor resources.ActorRef) error {
			_, err := workflow.PauseActor(ctx, actor)
			return err
		},
		"suspend": func(ctx context.Context, workflow *ActorWorkflow, actor resources.ActorRef) error {
			_, err := workflow.SuspendActor(ctx, actor)
			return err
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "external-actor"}
			workflow := newTestActorWorkflowForProvider(t, persistence, "agents", "external", atev1alpha1.WorkerProviderExternalSlot)
			seedWorkflowActor(t, ctx, persistence, actorRef, "agents", "external", ateapipb.ActorState_ACTOR_STATE_RUNNING)

			err := operation(ctx, workflow, actorRef)
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
			}
			stored, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
				t.Fatalf("stored state = %v, want RUNNING", got)
			}
			if stored.GetStatus().GetInProgressSnapshotName() != "" || stored.GetStatus().GetInProgressLocalSnapshotName() != "" {
				t.Fatalf("snapshot transition was persisted: %+v", stored.GetStatus())
			}
		})
	}
}

func TestExternalSlotCreateFromSnapshotRejectedBeforeLookup(t *testing.T) {
	template := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "agents", Name: "external"},
		Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
	}
	workflow := newTestActorWorkflowForTemplate(t, nil, template)
	service := &ServiceImpl{actorTemplateLister: workflow.actorTemplateLister}
	_, err := service.CreateActor(context.Background(), &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "clone"},
		ActorTemplateNamespace: template.Namespace,
		ActorTemplateName:      template.Name,
		SourceSnapshotTag:      &ateapipb.ObjectRef{Atespace: "team-a", Name: "snapshot-tag"},
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
	}
}

func TestExternalSlotSnapshotResumeRejectedBeforeActorMutation(t *testing.T) {
	tests := []struct {
		name           string
		mutateTemplate func(*atev1alpha1.ActorTemplate)
		mutateActor    func(*ateapipb.Actor)
	}{
		{
			name: "latest durable snapshot",
			mutateActor: func(actor *ateapipb.Actor) {
				actor.Status.LatestSnapshot = &ateapipb.ObjectRef{Atespace: "team-a", Name: "snapshot-1"}
			},
		},
		{
			name: "local snapshot",
			mutateActor: func(actor *ateapipb.Actor) {
				actor.Status.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{SnapshotName: "local-1"}
			},
		},
		{
			name: "resolved source snapshot",
			mutateActor: func(actor *ateapipb.Actor) {
				actor.SourceSnapshotTag = &ateapipb.ObjectRef{Atespace: "team-a", Name: "source-tag"}
				actor.Status.SourceSnapshot = &ateapipb.ActorSourceSnapshotStatus{
					Snapshot: &ateapipb.ObjectRef{Atespace: "team-a", Name: "snapshot-1"},
				}
			},
		},
		{
			name: "inherited golden snapshot",
			mutateTemplate: func(template *atev1alpha1.ActorTemplate) {
				template.Status.GoldenSnapshot = "golden-1"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			template := &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Namespace: "agents", Name: "external"},
				Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
			}
			if test.mutateTemplate != nil {
				test.mutateTemplate(template)
			}
			workflow := newTestActorWorkflowForTemplate(t, persistence, template)
			actorRef := resources.ActorRef{Atespace: "team-a", Name: "external-actor"}
			var actorOptions []func(*ateapipb.Actor)
			if test.mutateActor != nil {
				actorOptions = append(actorOptions, test.mutateActor)
			}
			seedWorkflowActor(t, ctx, persistence, actorRef, template.Namespace, template.Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, actorOptions...)

			_, _, err := workflow.ResumeActor(ctx, actorRef, false)
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
			}
			stored, err := persistence.GetActor(ctx, actorRef)
			if err != nil {
				t.Fatalf("GetActor: %v", err)
			}
			if stored.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || stored.GetStatus().GetWorkerAssignment() != nil {
				t.Fatalf("snapshot resume mutated actor: %+v", stored.GetStatus())
			}
		})
	}
}

func TestExternalSlotColdResumeSourceRemainsAllowed(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	template := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "agents", Name: "external"},
		Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
	}
	workflow := newTestActorWorkflowForTemplate(t, persistence, template)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "external-actor"}
	seedWorkflowActor(t, ctx, persistence, actorRef, template.Namespace, template.Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	actor, gotTemplate, source, err := workflow.loadActorForResume(ctx, actorRef, false)
	if err != nil {
		t.Fatalf("loadActorForResume cold start: %v", err)
	}
	if actor == nil || gotTemplate == nil || !source.SnapshotURI.IsZero() || !source.GoldenSnapshotURI.IsZero() {
		t.Fatalf("cold resume source = actor:%v template:%v source:%+v", actor, gotTemplate, source)
	}
}

func TestExternalSlotColdBootIgnoresInheritedGoldenSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	template := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "agents", Name: "external"},
		Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
		Status:     atev1alpha1.ActorTemplateStatus{GoldenSnapshot: "legacy-golden"},
	}
	workflow := newTestActorWorkflowForTemplate(t, persistence, template)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "external-actor"}
	seedWorkflowActor(t, ctx, persistence, actorRef, template.Namespace, template.Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	actor, gotTemplate, source, err := workflow.loadActorForResume(ctx, actorRef, true)
	if err != nil {
		t.Fatalf("loadActorForResume cold boot: %v", err)
	}
	if actor == nil || gotTemplate == nil || !source.SnapshotURI.IsZero() || !source.GoldenSnapshotURI.IsZero() {
		t.Fatalf("cold boot source = actor:%v template:%v source:%+v", actor, gotTemplate, source)
	}
}

func TestExternalSlotColdBootCallsRunWithoutKubernetesSandboxAssets(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	template := &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{Namespace: "agents", Name: "external"},
		Spec: atev1alpha1.ActorTemplateSpec{
			WorkerProvider: atev1alpha1.WorkerProviderExternalSlot,
			Containers:     []atev1alpha1.Container{{Name: "runtime", Image: "runtime@example.invalid"}},
		},
		Status: atev1alpha1.ActorTemplateStatus{GoldenSnapshot: "legacy-golden"},
	}
	workflow := newTestActorWorkflowForTemplate(t, persistence, template)
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "external-actor"}
	seedWorkflowActor(t, ctx, persistence, actorRef, template.Namespace, template.Name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, func(actor *ateapipb.Actor) {
		actor.Status.WorkerAssignment = &ateapipb.WorkerAssignment{
			Worker:            &ateapipb.ObjectRef{Name: "external-worker"},
			WorkerResourceUid: "11111111-1111-4111-8111-111111111111",
			WorkerNamespace:   "workers",
			WorkerPool:        "external-pool",
			Provider:          ateapipb.WorkerProvider_WORKER_PROVIDER_EXTERNAL_SLOT,
			ExternalSlot:      &ateapipb.ExternalSlotIdentity{ExecutionIdentity: "external-execution"},
		}
	})

	actor, gotTemplate, source, err := workflow.loadActorForResume(ctx, actorRef, true)
	if err != nil {
		t.Fatalf("loadActorForResume cold boot: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for in-memory AteomHerder: %v", err)
	}
	server := grpc.NewServer()
	recorder := &recordingAteomHerder{}
	ateletpb.RegisterAteomHerderServer(server, recorder)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-serveDone
	})
	connection, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create in-memory AteomHerder client: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	workflow.dialer = &fixedExecutionDialer{connection: connection}

	telemetry, err := workflow.ensureAteletRestored(ctx, actorRef, actor, gotTemplate, source)
	if err != nil {
		t.Fatalf("ensureAteletRestored cold boot: %v", err)
	}
	if recorder.runCalls != 1 || recorder.restoreCalls != 0 {
		t.Fatalf("AteomHerder calls: Run=%d Restore=%d, want Run=1 Restore=0", recorder.runCalls, recorder.restoreCalls)
	}
	if recorder.runRequest == nil || recorder.runRequest.GetSandboxAssets() != nil {
		t.Fatalf("ExternalSlot Run sandbox assets = %+v, want nil", recorder.runRequest.GetSandboxAssets())
	}
	if telemetry.SnapshotKind != "boot" {
		t.Fatalf("snapshot telemetry kind = %q, want boot", telemetry.SnapshotKind)
	}
}
