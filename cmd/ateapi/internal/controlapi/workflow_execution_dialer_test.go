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
	"errors"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

func TestRPCServiceSharesUnboundProviderDialerWithWorkflowAndPreservesKubernetes(t *testing.T) {
	kubernetes := &recordingExecutionDialer{}
	provider, err := NewProviderExecutionDialer(kubernetes)
	if err != nil {
		t.Fatalf("NewProviderExecutionDialer() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	service := NewRPCService(
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		provider,
		nil,
		"",
		time.Second,
		nil,
	)
	if service.dialer != provider || service.actorWorkflow.dialer != provider {
		t.Fatal("RPC service and workflow do not share the provider execution dialer")
	}
	assignment := &ateapipb.WorkerAssignment{
		WorkerNamespace: "workers-a",
		WorkerPod:       "worker-a",
	}
	if connection, err := service.dialer.DialForWorker(assignment); err != nil || connection != nil {
		t.Fatalf("unbound provider Kubernetes DialForWorker() = (%v, %v), want nil/nil from Kubernetes test dialer", connection, err)
	}
	if kubernetes.workerAssignment != assignment {
		t.Fatal("unbound provider dialer did not preserve the Kubernetes execution path")
	}
}

type recordingExecutionDialer struct {
	workerAssignment *ateapipb.WorkerAssignment
	workerErr        error
	localSnapshot    *ateapipb.LocalSnapshotInfo
	localErr         error
}

func (d *recordingExecutionDialer) DialForWorker(assignment *ateapipb.WorkerAssignment) (*grpc.ClientConn, error) {
	d.workerAssignment = assignment
	return nil, d.workerErr
}

func (d *recordingExecutionDialer) DialForLocalSnapshot(local *ateapipb.LocalSnapshotInfo) (*grpc.ClientConn, error) {
	d.localSnapshot = local
	return nil, d.localErr
}

func TestActorWorkflowUsesExecutionDialerForNodeLocalState(t *testing.T) {
	wantErr := errors.New("node execution endpoint unavailable")
	dialer := &recordingExecutionDialer{localErr: wantErr}
	workflow := &ActorWorkflow{dialer: dialer}
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-a"}
	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name, Uid: "actor-uid"},
		ActorTemplateNamespace: "team-a",
		ActorTemplateName:      "template-a",
		Status: &ateapipb.ActorStatus{
			InProgressSnapshotName: "snapshot-a",
			LocalSnapshotInfo: &ateapipb.LocalSnapshotInfo{
				SnapshotName:              "local-snapshot-a",
				NodeVmsWithLocalSnapshots: []string{"node-a"},
			},
		},
	}

	_, err := workflow.ensurePausedSnapshotUploaded(context.Background(), actorRef, actor, &atev1alpha1.ActorTemplate{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ensurePausedSnapshotUploaded() error = %v, want %v", err, wantErr)
	}
	if dialer.localSnapshot != actor.GetStatus().GetLocalSnapshotInfo() {
		t.Fatalf("DialForLocalSnapshot() target = %v, want workflow local snapshot %v", dialer.localSnapshot, actor.GetStatus().GetLocalSnapshotInfo())
	}
	if got := dialer.localSnapshot.GetNodeVmsWithLocalSnapshots(); len(got) != 1 || got[0] != "node-a" {
		t.Fatalf("DialForLocalSnapshot() nodes = %v, want [node-a]", got)
	}
}

func TestActorWorkflowUsesExecutionDialerForAssignedWorker(t *testing.T) {
	wantErr := errors.New("execution endpoint unavailable")
	dialer := &recordingExecutionDialer{workerErr: wantErr}
	workflow := &ActorWorkflow{dialer: dialer}
	actorRef := resources.ActorRef{Atespace: "team-a", Name: "actor-a"}
	actor := &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: actorRef.Atespace, Name: actorRef.Name, Uid: "actor-uid"},
		ActorTemplateNamespace: "team-a",
		ActorTemplateName:      "template-a",
		Status: &ateapipb.ActorStatus{
			InProgressLocalSnapshotName: "snapshot-a",
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: "worker-ref-a"},
				WorkerNamespace: "workers-a",
				WorkerPod:       "worker-a",
				WorkerPodUid:    "worker-uid",
			},
		},
	}

	_, err := workflow.ensureAteletPaused(context.Background(), actorRef, actor, &atev1alpha1.ActorTemplate{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ensureAteletPaused() error = %v, want %v", err, wantErr)
	}
	if dialer.workerAssignment != actor.GetStatus().GetWorkerAssignment() {
		t.Fatalf("DialForWorker() target = %v, want workflow assignment %v", dialer.workerAssignment, actor.GetStatus().GetWorkerAssignment())
	}
	if got := dialer.workerAssignment.GetWorker().GetName(); got != "worker-ref-a" {
		t.Fatalf("DialForWorker() worker ref = %q, want worker-ref-a", got)
	}
	if gotNamespace, gotPod := dialer.workerAssignment.GetWorkerNamespace(), dialer.workerAssignment.GetWorkerPod(); gotNamespace != "workers-a" || gotPod != "worker-a" {
		t.Fatalf("DialForWorker() Kubernetes target = %q/%q, want workers-a/worker-a", gotNamespace, gotPod)
	}
}
