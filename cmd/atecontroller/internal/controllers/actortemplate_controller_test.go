// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func TestGoldenSnapshotWarmupFor(t *testing.T) {
	probe := &atev1alpha1.ContainerReadyz{
		HTTPGet: &atev1alpha1.HTTPGetAction{Port: 80},
	}

	tests := []struct {
		name       string
		containers []atev1alpha1.Container
		wantZero   bool
	}{
		{
			name:       "no containers keeps default warmup",
			containers: nil,
			wantZero:   false,
		},
		{
			name: "all containers have readyz skips warmup",
			containers: []atev1alpha1.Container{
				{Name: "a", Readyz: probe},
				{Name: "b", Readyz: probe},
			},
			wantZero: true,
		},
		{
			name: "single container with readyz skips warmup",
			containers: []atev1alpha1.Container{
				{Name: "a", Readyz: probe},
			},
			wantZero: true,
		},
		{
			name: "mixed containers keep warmup",
			containers: []atev1alpha1.Container{
				{Name: "a", Readyz: probe},
				{Name: "b"},
			},
			wantZero: false,
		},
		{
			name: "no readyz anywhere keeps warmup",
			containers: []atev1alpha1.Container{
				{Name: "a"},
				{Name: "b"},
			},
			wantZero: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at := &atev1alpha1.ActorTemplate{
				Spec: atev1alpha1.ActorTemplateSpec{Containers: tt.containers},
			}
			got := goldenSnapshotWarmupFor(at)
			if tt.wantZero && got != 0 {
				t.Errorf("goldenSnapshotWarmupFor = %v, want 0", got)
			}
			if !tt.wantZero && got != goldenSnapshotWarmup {
				t.Errorf("goldenSnapshotWarmupFor = %v, want %v", got, goldenSnapshotWarmup)
			}
		})
	}
}

type mockControlClient struct {
	ateapipb.ControlClient
	createAtespaceFn func(ctx context.Context, req *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error)
	createActorFn    func(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
	deleteActorFn    func(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
}

func (m *mockControlClient) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error) {
	if m.createAtespaceFn != nil {
		return m.createAtespaceFn(ctx, req, opts...)
	}
	return &ateapipb.Atespace{}, nil
}

func (m *mockControlClient) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	if m.createActorFn != nil {
		return m.createActorFn(ctx, req, opts...)
	}
	return &ateapipb.Actor{}, nil
}

func (m *mockControlClient) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	if m.deleteActorFn != nil {
		return m.deleteActorFn(ctx, req, opts...)
	}
	return &ateapipb.Actor{}, nil
}

func TestActorTemplateReconciler_Reconcile_PhaseInitial(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := atev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add scheme: %v", err)
	}

	const templateUID = "test-uid-12345"
	const expectedActorName = templateUID

	t.Run("external-only mode ignores Kubernetes-backed templates", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "kubernetes-template", Namespace: "default", UID: types.UID(templateUID)},
			Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderKubernetesPod},
		}
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()
		fakeAteClient := &mockControlClient{
			createAtespaceFn: func(context.Context, *ateapipb.CreateAtespaceRequest, ...grpc.CallOption) (*ateapipb.Atespace, error) {
				t.Fatal("external-only controller attempted a Kubernetes-backed golden lifecycle")
				return nil, nil
			},
		}
		reconciler := &ActorTemplateReconciler{
			Client:       fakeK8sClient,
			Scheme:       scheme,
			AteClient:    fakeAteClient,
			ExternalOnly: true,
		}
		key := types.NamespacedName{Name: template.Name, Namespace: template.Namespace}
		result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
		if err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
		if !result.IsZero() {
			t.Fatalf("Reconcile() result = %v, want zero", result)
		}
		reconciled := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(context.Background(), key, reconciled); err != nil {
			t.Fatalf("get ignored ActorTemplate: %v", err)
		}
		if reconciled.Status.Phase != atev1alpha1.PhaseInitial || len(reconciled.Status.Conditions) != 0 {
			t.Fatalf("ignored ActorTemplate status changed: %+v", reconciled.Status)
		}
	})

	t.Run("external slot becomes ready without golden lifecycle RPCs", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "external-template",
				Namespace: "default",
				UID:       types.UID(templateUID),
			},
			Spec: atev1alpha1.ActorTemplateSpec{
				WorkerProvider: atev1alpha1.WorkerProviderExternalSlot,
			},
		}
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()
		fakeAteClient := &mockControlClient{
			createAtespaceFn: func(context.Context, *ateapipb.CreateAtespaceRequest, ...grpc.CallOption) (*ateapipb.Atespace, error) {
				t.Fatal("external ActorTemplate attempted to create the golden atespace")
				return nil, nil
			},
			createActorFn: func(context.Context, *ateapipb.CreateActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
				t.Fatal("external ActorTemplate attempted to create a golden actor")
				return nil, nil
			},
		}
		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: fakeAteClient, ExternalOnly: true}
		ctx := context.Background()
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: template.Name, Namespace: template.Namespace}}

		for attempt := 0; attempt < 2; attempt++ {
			result, err := reconciler.Reconcile(ctx, req)
			if err != nil {
				t.Fatalf("Reconcile attempt %d returned error: %v", attempt+1, err)
			}
			if !result.IsZero() {
				t.Fatalf("Reconcile attempt %d returned requeue result: %v", attempt+1, result)
			}
		}

		reconciled := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, req.NamespacedName, reconciled); err != nil {
			t.Fatalf("get reconciled external ActorTemplate: %v", err)
		}
		if reconciled.Status.Phase != atev1alpha1.PhaseReady {
			t.Fatalf("status.Phase = %q, want %q", reconciled.Status.Phase, atev1alpha1.PhaseReady)
		}
		if reconciled.Status.GoldenActorID != "" || reconciled.Status.GoldenSnapshot != "" || !reconciled.Status.TakeGoldenSnapshotAt.IsZero() {
			t.Fatalf("external ActorTemplate retained golden lifecycle state: %+v", reconciled.Status)
		}
		condition := meta.FindStatusCondition(reconciled.Status.Conditions, "Ready")
		if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != ExternalSlotReadyReason {
			t.Fatalf("Ready condition = %+v, want True/%s", condition, ExternalSlotReadyReason)
		}
	})

	t.Run("external slot retires an inherited golden actor", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "external-stale", Namespace: "default", UID: types.UID(templateUID)},
			Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase:         atev1alpha1.PhaseResumeGoldenActor,
				GoldenActorID: expectedActorName,
			},
		}
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()
		var deleted *ateapipb.DeleteActorRequest
		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: &mockControlClient{
			deleteActorFn: func(_ context.Context, request *ateapipb.DeleteActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
				deleted = request
				return &ateapipb.Actor{}, nil
			},
		}}
		ctx := context.Background()
		key := types.NamespacedName{Name: template.Name, Namespace: template.Namespace}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		if deleted == nil || !deleted.GetAnyState() || deleted.GetActor().GetAtespace() != "ate-golden" || deleted.GetActor().GetName() != expectedActorName {
			t.Fatalf("DeleteActor request = %+v", deleted)
		}
		reconciled := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, key, reconciled); err != nil {
			t.Fatalf("get reconciled external ActorTemplate: %v", err)
		}
		if reconciled.Status.Phase != atev1alpha1.PhaseReady || reconciled.Status.GoldenActorID != "" || reconciled.Status.GoldenSnapshot != "" || !reconciled.Status.TakeGoldenSnapshotAt.IsZero() {
			t.Fatalf("external ActorTemplate status after migration = %+v", reconciled.Status)
		}
	})

	t.Run("external slot retires an inherited ready golden snapshot", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "external-ready-stale", Namespace: "default", UID: types.UID(templateUID)},
			Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase:          atev1alpha1.PhaseReady,
				GoldenActorID:  expectedActorName,
				GoldenSnapshot: "golden-snapshot-1",
			},
		}
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()
		deleteCalls := 0
		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: &mockControlClient{
			deleteActorFn: func(_ context.Context, request *ateapipb.DeleteActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
				deleteCalls++
				if request.GetActor().GetName() != expectedActorName || !request.GetAnyState() {
					t.Fatalf("DeleteActor request = %+v", request)
				}
				return &ateapipb.Actor{}, nil
			},
		}}
		ctx := context.Background()
		key := types.NamespacedName{Name: template.Name, Namespace: template.Namespace}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		if deleteCalls != 1 {
			t.Fatalf("DeleteActor calls = %d, want 1", deleteCalls)
		}
		reconciled := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, key, reconciled); err != nil {
			t.Fatalf("get reconciled external ActorTemplate: %v", err)
		}
		if reconciled.Status.Phase != atev1alpha1.PhaseReady || reconciled.Status.GoldenActorID != "" || reconciled.Status.GoldenSnapshot != "" || !reconciled.Status.TakeGoldenSnapshotAt.IsZero() {
			t.Fatalf("external ActorTemplate status after ready migration = %+v", reconciled.Status)
		}
	})

	t.Run("external slot clears inherited golden status without an actor identity", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "external-status-only", Namespace: "default", UID: types.UID(templateUID)},
			Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase:                atev1alpha1.PhaseReady,
				GoldenSnapshot:       "orphaned-status-snapshot",
				TakeGoldenSnapshotAt: metav1.NewTime(time.Now()),
			},
		}
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()
		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: &mockControlClient{
			deleteActorFn: func(context.Context, *ateapipb.DeleteActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
				t.Fatal("status-only migration attempted to delete an actor without an exact identity")
				return nil, nil
			},
		}}
		ctx := context.Background()
		key := types.NamespacedName{Name: template.Name, Namespace: template.Namespace}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		reconciled := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, key, reconciled); err != nil {
			t.Fatalf("get reconciled external ActorTemplate: %v", err)
		}
		if reconciled.Status.Phase != atev1alpha1.PhaseReady || reconciled.Status.GoldenActorID != "" || reconciled.Status.GoldenSnapshot != "" || !reconciled.Status.TakeGoldenSnapshotAt.IsZero() {
			t.Fatalf("external ActorTemplate retained stale status: %+v", reconciled.Status)
		}
	})

	for _, test := range []struct {
		name   string
		phase  atev1alpha1.PhaseType
		mutate func(*atev1alpha1.ActorTemplateStatus)
	}{
		{name: "initial snapshot only", phase: atev1alpha1.PhaseInitial, mutate: func(status *atev1alpha1.ActorTemplateStatus) { status.GoldenSnapshot = "legacy-golden" }},
		{name: "initial timestamp only", phase: atev1alpha1.PhaseInitial, mutate: func(status *atev1alpha1.ActorTemplateStatus) {
			status.TakeGoldenSnapshotAt = metav1.NewTime(time.Unix(10, 0))
		}},
		{name: "ready snapshot only", phase: atev1alpha1.PhaseReady, mutate: func(status *atev1alpha1.ActorTemplateStatus) { status.GoldenSnapshot = "legacy-golden" }},
		{name: "ready timestamp only", phase: atev1alpha1.PhaseReady, mutate: func(status *atev1alpha1.ActorTemplateStatus) {
			status.TakeGoldenSnapshotAt = metav1.NewTime(time.Unix(10, 0))
		}},
	} {
		t.Run("external slot clears "+test.name+" without an actor identity", func(t *testing.T) {
			statusValue := atev1alpha1.ActorTemplateStatus{Phase: test.phase}
			test.mutate(&statusValue)
			template := &atev1alpha1.ActorTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "external-field-only", Namespace: "default", UID: types.UID(templateUID)},
				Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
				Status:     statusValue,
			}
			fakeK8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
				WithObjects(template).
				Build()
			reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: &mockControlClient{
				deleteActorFn: func(context.Context, *ateapipb.DeleteActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
					t.Fatal("field-only migration attempted to delete an actor without an exact identity")
					return nil, nil
				},
			}}
			ctx := context.Background()
			key := types.NamespacedName{Name: template.Name, Namespace: template.Namespace}
			if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatalf("Reconcile returned error: %v", err)
			}
			reconciled := &atev1alpha1.ActorTemplate{}
			if err := fakeK8sClient.Get(ctx, key, reconciled); err != nil {
				t.Fatalf("get reconciled external ActorTemplate: %v", err)
			}
			if reconciled.Status.Phase != atev1alpha1.PhaseReady || reconciled.Status.GoldenActorID != "" || reconciled.Status.GoldenSnapshot != "" || !reconciled.Status.TakeGoldenSnapshotAt.IsZero() {
				t.Fatalf("external ActorTemplate retained field-only stale status: %+v", reconciled.Status)
			}
		})
	}

	t.Run("external slot refuses to delete a mismatched golden actor identity", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "external-foreign-golden", Namespace: "default", UID: types.UID(templateUID)},
			Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase:         atev1alpha1.PhaseReady,
				GoldenActorID: "another-template-uid",
				Conditions: []metav1.Condition{{
					Type: "Ready", Status: metav1.ConditionTrue, Reason: ExternalSlotReadyReason, Message: externalSlotReadyMessage,
				}},
			},
		}
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()
		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: &mockControlClient{
			deleteActorFn: func(context.Context, *ateapipb.DeleteActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
				t.Fatal("mismatched status attempted to delete another golden actor")
				return nil, nil
			},
		}}
		ctx := context.Background()
		key := types.NamespacedName{Name: template.Name, Namespace: template.Namespace}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err == nil {
			t.Fatal("Reconcile accepted a mismatched golden actor identity")
		}
		reconciled := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, key, reconciled); err != nil {
			t.Fatalf("get reconciled external ActorTemplate: %v", err)
		}
		if reconciled.Status.GoldenActorID != "another-template-uid" || reconciled.Status.Phase != atev1alpha1.PhaseReady {
			t.Fatalf("mismatched cleanup changed status: %+v", reconciled.Status)
		}
		condition := meta.FindStatusCondition(reconciled.Status.Conditions, "Ready")
		if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != externalSlotCleanupReason {
			t.Fatalf("mismatched cleanup Ready condition = %+v, want False/%s", condition, externalSlotCleanupReason)
		}
	})

	t.Run("external slot does not become ready when golden cleanup fails", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "external-cleanup-fails", Namespace: "default", UID: types.UID(templateUID)},
			Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase:          atev1alpha1.PhaseReady,
				GoldenActorID:  expectedActorName,
				GoldenSnapshot: "legacy-golden",
				Conditions: []metav1.Condition{{
					Type: "Ready", Status: metav1.ConditionTrue, Reason: ExternalSlotReadyReason, Message: externalSlotReadyMessage,
				}},
			},
		}
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()
		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: &mockControlClient{
			deleteActorFn: func(context.Context, *ateapipb.DeleteActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
				return nil, status.Error(codes.Unavailable, "cleanup unavailable")
			},
		}}
		ctx := context.Background()
		key := types.NamespacedName{Name: template.Name, Namespace: template.Namespace}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err == nil {
			t.Fatal("Reconcile succeeded while golden cleanup was unavailable")
		}
		reconciled := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, key, reconciled); err != nil {
			t.Fatalf("get reconciled external ActorTemplate: %v", err)
		}
		if reconciled.Status.Phase != atev1alpha1.PhaseReady || reconciled.Status.GoldenActorID != expectedActorName || reconciled.Status.GoldenSnapshot != "legacy-golden" {
			t.Fatalf("failed cleanup changed status: %+v", reconciled.Status)
		}
		condition := meta.FindStatusCondition(reconciled.Status.Conditions, "Ready")
		if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != externalSlotCleanupReason {
			t.Fatalf("failed cleanup Ready condition = %+v, want False/%s", condition, externalSlotCleanupReason)
		}
	})

	t.Run("external ready phase repairs its Ready condition", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "external-ready", Namespace: "default", UID: types.UID(templateUID)},
			Spec:       atev1alpha1.ActorTemplateSpec{WorkerProvider: atev1alpha1.WorkerProviderExternalSlot},
			Status:     atev1alpha1.ActorTemplateStatus{Phase: atev1alpha1.PhaseReady},
		}
		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()
		reconciler := &ActorTemplateReconciler{Client: fakeK8sClient, Scheme: scheme, AteClient: &mockControlClient{}}
		ctx := context.Background()
		key := types.NamespacedName{Name: template.Name, Namespace: template.Namespace}
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		reconciled := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, key, reconciled); err != nil {
			t.Fatalf("get reconciled external ActorTemplate: %v", err)
		}
		condition := meta.FindStatusCondition(reconciled.Status.Conditions, "Ready")
		if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != ExternalSlotReadyReason || condition.Message != externalSlotReadyMessage {
			t.Fatalf("Ready condition = %+v", condition)
		}
	})

	t.Run("creates golden actor using template UID", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-template",
				Namespace: "default",
				UID:       types.UID(templateUID),
			},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase: atev1alpha1.PhaseInitial,
			},
		}

		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()

		var createdActorName string
		fakeAteClient := &mockControlClient{
			createActorFn: func(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
				createdActorName = req.GetActor().GetMetadata().GetName()
				return &ateapipb.Actor{}, nil
			},
		}

		reconciler := &ActorTemplateReconciler{
			Client:    fakeK8sClient,
			Scheme:    scheme,
			AteClient: fakeAteClient,
		}

		ctx := context.Background()
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-template", Namespace: "default"}}
		res, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("Reconcile returned error: %v", err)
		}
		if !res.IsZero() {
			t.Errorf("unexpected requeue result: %v", res)
		}

		if createdActorName != expectedActorName {
			t.Errorf("created actor name = %q, want %q", createdActorName, expectedActorName)
		}

		reconciledTemplate := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, req.NamespacedName, reconciledTemplate); err != nil {
			t.Fatalf("failed to get reconciled ActorTemplate: %v", err)
		}

		if reconciledTemplate.Status.GoldenActorID != expectedActorName {
			t.Errorf("status.GoldenActorID = %q, want %q", reconciledTemplate.Status.GoldenActorID, expectedActorName)
		}
		if reconciledTemplate.Status.Phase != atev1alpha1.PhaseResumeGoldenActor {
			t.Errorf("status.Phase = %q, want %q", reconciledTemplate.Status.Phase, atev1alpha1.PhaseResumeGoldenActor)
		}
	})

	t.Run("handles AlreadyExists error when golden actor was created on prior attempt", func(t *testing.T) {
		template := &atev1alpha1.ActorTemplate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-template-retry",
				Namespace: "default",
				UID:       types.UID(templateUID),
			},
			Status: atev1alpha1.ActorTemplateStatus{
				Phase: atev1alpha1.PhaseInitial,
			},
		}

		fakeK8sClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&atev1alpha1.ActorTemplate{}).
			WithObjects(template).
			Build()

		fakeAteClient := &mockControlClient{
			createActorFn: func(ctx context.Context, req *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
				return nil, status.Error(codes.AlreadyExists, "actor already exists in ateapi")
			},
		}

		reconciler := &ActorTemplateReconciler{
			Client:    fakeK8sClient,
			Scheme:    scheme,
			AteClient: fakeAteClient,
		}

		ctx := context.Background()
		req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "my-template-retry", Namespace: "default"}}
		_, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			t.Fatalf("Reconcile returned error on AlreadyExists retry: %v", err)
		}

		reconciledTemplate := &atev1alpha1.ActorTemplate{}
		if err := fakeK8sClient.Get(ctx, req.NamespacedName, reconciledTemplate); err != nil {
			t.Fatalf("failed to get reconciled ActorTemplate: %v", err)
		}

		if reconciledTemplate.Status.GoldenActorID != expectedActorName {
			t.Errorf("status.GoldenActorID = %q, want %q", reconciledTemplate.Status.GoldenActorID, expectedActorName)
		}
		if reconciledTemplate.Status.Phase != atev1alpha1.PhaseResumeGoldenActor {
			t.Errorf("status.Phase = %q, want %q", reconciledTemplate.Status.Phase, atev1alpha1.PhaseResumeGoldenActor)
		}
	})
}
