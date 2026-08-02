/*
Copyright 2025 Priyo Lahiri.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
)

// ---------------------------------------------------------------------------
// Programmable fake Aura API.
//
// Every method delegates to an optional func hook the test sets; unset hooks
// return sensible zero-value defaults. Call flags / captured arguments let a
// test assert which side effects fired (create/pause/delete/restore) and with
// what payload.
// ---------------------------------------------------------------------------

type fakeAuraAPI struct {
	// Hooks. When nil, the method returns a benign default.
	listInstancesFn   func(ctx context.Context, tenantID string) ([]aura.InstanceSummary, error)
	getInstanceFn     func(ctx context.Context, id string) (*aura.Instance, error)
	createInstanceFn  func(ctx context.Context, req aura.CreateInstanceRequest) (*aura.CreateInstanceResponse, error)
	patchInstanceFn   func(ctx context.Context, id string, req aura.PatchInstanceRequest) (*aura.Instance, error)
	getTenantFn       func(ctx context.Context, id string) (*aura.Tenant, error)
	createSnapshotFn  func(ctx context.Context, instanceID string) (*aura.Snapshot, error)
	getSnapshotFn     func(ctx context.Context, instanceID, snapshotID string) (*aura.Snapshot, error)
	restoreSnapshotFn func(ctx context.Context, instanceID, snapshotID string) error

	// Call flags / captured args.
	listCalled     bool
	createCalled   bool
	patchCalled    bool
	pauseCalled    bool
	resumeCalled   bool
	upgradeCalled  bool
	deleteCalled   bool
	restoreCalled  bool
	lastPatch      aura.PatchInstanceRequest
	lastCreateReq  aura.CreateInstanceRequest
	getInstanceIDs []string
}

func (f *fakeAuraAPI) ListInstances(ctx context.Context, tenantID string) ([]aura.InstanceSummary, error) {
	f.listCalled = true
	if f.listInstancesFn != nil {
		return f.listInstancesFn(ctx, tenantID)
	}
	return nil, nil
}

func (f *fakeAuraAPI) GetInstance(ctx context.Context, id string) (*aura.Instance, error) {
	f.getInstanceIDs = append(f.getInstanceIDs, id)
	if f.getInstanceFn != nil {
		return f.getInstanceFn(ctx, id)
	}
	return &aura.Instance{ID: id, Status: aura.InstanceStatusRunning}, nil
}

func (f *fakeAuraAPI) CreateInstance(ctx context.Context, req aura.CreateInstanceRequest) (*aura.CreateInstanceResponse, error) {
	f.createCalled = true
	f.lastCreateReq = req
	if f.createInstanceFn != nil {
		return f.createInstanceFn(ctx, req)
	}
	return &aura.CreateInstanceResponse{ID: "created-id"}, nil
}

func (f *fakeAuraAPI) PatchInstance(ctx context.Context, id string, req aura.PatchInstanceRequest) (*aura.Instance, error) {
	f.patchCalled = true
	f.lastPatch = req
	if f.patchInstanceFn != nil {
		return f.patchInstanceFn(ctx, id, req)
	}
	return &aura.Instance{ID: id, Status: aura.InstanceStatusUpdating}, nil
}

func (f *fakeAuraAPI) PauseInstance(ctx context.Context, id string) error {
	f.pauseCalled = true
	return nil
}

func (f *fakeAuraAPI) ResumeInstance(ctx context.Context, id string) error {
	f.resumeCalled = true
	return nil
}

func (f *fakeAuraAPI) UpgradeInstance(ctx context.Context, id string) error {
	f.upgradeCalled = true
	return nil
}

func (f *fakeAuraAPI) DeleteInstance(ctx context.Context, id string) error {
	f.deleteCalled = true
	return nil
}

func (f *fakeAuraAPI) GetTenant(ctx context.Context, id string) (*aura.Tenant, error) {
	if f.getTenantFn != nil {
		return f.getTenantFn(ctx, id)
	}
	return &aura.Tenant{ID: id}, nil
}

func (f *fakeAuraAPI) CreateSnapshot(ctx context.Context, instanceID string) (*aura.Snapshot, error) {
	if f.createSnapshotFn != nil {
		return f.createSnapshotFn(ctx, instanceID)
	}
	return &aura.Snapshot{InstanceID: instanceID, SnapshotID: "snap-1", Status: aura.SnapshotStatusPending}, nil
}

func (f *fakeAuraAPI) GetSnapshot(ctx context.Context, instanceID, snapshotID string) (*aura.Snapshot, error) {
	if f.getSnapshotFn != nil {
		return f.getSnapshotFn(ctx, instanceID, snapshotID)
	}
	return &aura.Snapshot{InstanceID: instanceID, SnapshotID: snapshotID, Status: aura.SnapshotStatusInProgress}, nil
}

func (f *fakeAuraAPI) RestoreSnapshot(ctx context.Context, instanceID, snapshotID string) error {
	f.restoreCalled = true
	if f.restoreSnapshotFn != nil {
		return f.restoreSnapshotFn(ctx, instanceID, snapshotID)
	}
	return nil
}

// factoryFor returns a ClientFactory that always hands back the given fake,
// ignoring resolved credentials (the fake needs no real account).
func factoryFor(f *fakeAuraAPI) auraClientFactory {
	return func(auraCredentials) auraAPI { return f }
}

// ---------------------------------------------------------------------------
// Programmable fake Aura CMK API (separate interface from fakeAuraAPI).
// ---------------------------------------------------------------------------

type fakeCMKAPI struct {
	createFn func(ctx context.Context, req aura.CreateCMKRequest) (*aura.CustomerManagedKey, error)
	getFn    func(ctx context.Context, id string) (*aura.CustomerManagedKey, error)
	listFn   func(ctx context.Context, tenantID string) ([]aura.CustomerManagedKeySummary, error)
	deleteFn func(ctx context.Context, id string) error

	createCalled bool
	listCalled   bool
	deleteCalled bool
	lastCreate   aura.CreateCMKRequest
}

func (f *fakeCMKAPI) CreateCustomerManagedKey(ctx context.Context, req aura.CreateCMKRequest) (*aura.CustomerManagedKey, error) {
	f.createCalled = true
	f.lastCreate = req
	if f.createFn != nil {
		return f.createFn(ctx, req)
	}
	return &aura.CustomerManagedKey{ID: "cmk-created", Status: aura.CMKStatusPending}, nil
}

func (f *fakeCMKAPI) GetCustomerManagedKey(ctx context.Context, id string) (*aura.CustomerManagedKey, error) {
	if f.getFn != nil {
		return f.getFn(ctx, id)
	}
	return &aura.CustomerManagedKey{ID: id, Status: aura.CMKStatusReady}, nil
}

// Returns SUMMARIES, matching the real v1 list endpoint: id/name/tenant_id only.
// Do NOT widen this to the full CustomerManagedKey — the previous fake returned a
// fully-populated struct, which masked an adoption path that could never match.
func (f *fakeCMKAPI) ListCustomerManagedKeys(ctx context.Context, tenantID string) ([]aura.CustomerManagedKeySummary, error) {
	f.listCalled = true
	if f.listFn != nil {
		return f.listFn(ctx, tenantID)
	}
	return nil, nil
}

func (f *fakeCMKAPI) DeleteCustomerManagedKey(ctx context.Context, id string) error {
	f.deleteCalled = true
	if f.deleteFn != nil {
		return f.deleteFn(ctx, id)
	}
	return nil
}

func cmkFactoryFor(f *fakeCMKAPI) auraCMKClientFactory {
	return func(auraCredentials) auraCMKAPI { return f }
}

// ---------------------------------------------------------------------------
// Harness helpers.
// ---------------------------------------------------------------------------

const (
	testNS         = "default"
	testSecretName = "aura-api-creds"
)

// auraTestScheme registers the operator CRDs + core types on a fresh scheme.
func auraTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("adding client-go scheme: %v", err)
	}
	if err := neo4jv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding neo4j scheme: %v", err)
	}
	return scheme
}

// newAuraFakeClient builds a fake client with the status subresource enabled
// for every Aura Kind, seeded with the given objects plus a valid credentials
// Secret so resolveAuraCredentials always succeeds.
func newAuraFakeClient(t *testing.T, scheme *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	creds := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testSecretName, Namespace: testNS},
		Data: map[string][]byte{
			"clientId":     []byte("id"),
			"clientSecret": []byte("secret"),
		},
	}
	all := append([]client.Object{creds}, objs...)
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(
			&neo4jv1beta1.AuraInstance{},
			&neo4jv1beta1.AuraSnapshot{},
			&neo4jv1beta1.AuraRestore{},
			&neo4jv1beta1.AuraProviderConfig{},
			&neo4jv1beta1.AuraCustomerManagedKey{},
			&neo4jv1beta1.AuraIPFilter{},
			&neo4jv1beta1.AuraDatabase{},
			&neo4jv1beta1.AuraDatabaseBackup{},
			&neo4jv1beta1.AuraDatabaseRestore{},
			&neo4jv1beta1.AuraOrganizationMember{},
			&neo4jv1beta1.AuraProjectMember{},
			&neo4jv1beta1.AuraInvite{},
			// Needed by the Aura Fleet Manager provisioning tests, which write
			// status.auraFleetManagement on a cluster/standalone.
			&neo4jv1beta1.Neo4jEnterpriseCluster{},
			&neo4jv1beta1.Neo4jEnterpriseStandalone{},
		).
		WithObjects(all...).
		Build()
}

func credRef() *neo4jv1beta1.AuraCredentialsSecretRef {
	return &neo4jv1beta1.AuraCredentialsSecretRef{Name: testSecretName}
}

func reqFor(o client.Object) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(o)}
}

func conditionStatus(conds []metav1.Condition, condType string) metav1.ConditionStatus {
	c := meta.FindStatusCondition(conds, condType)
	if c == nil {
		return "<absent>"
	}
	return c.Status
}

// ---------------------------------------------------------------------------
// AuraProviderConfig.
// ---------------------------------------------------------------------------

func TestAuraProviderConfig_Ready(t *testing.T) {
	scheme := auraTestScheme(t)
	pc := &neo4jv1beta1.AuraProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "cfg", Namespace: testNS},
		Spec: neo4jv1beta1.AuraProviderConfigSpec{
			CredentialsSecretRef: *credRef(),
			DefaultProjectID:     "proj-1",
		},
	}
	cases := []struct {
		name       string
		listErr    error
		wantStatus metav1.ConditionStatus
	}{
		{"list succeeds", nil, metav1.ConditionTrue},
		{"list errors", errors.New("boom"), metav1.ConditionFalse},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAuraAPI{listInstancesFn: func(context.Context, string) ([]aura.InstanceSummary, error) {
				return nil, tc.listErr
			}}
			c := newAuraFakeClient(t, scheme, pc.DeepCopy())
			r := &AuraProviderConfigReconciler{
				Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
				ClientFactory: factoryFor(f),
			}
			if _, err := r.Reconcile(context.Background(), reqFor(pc)); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			got := &neo4jv1beta1.AuraProviderConfig{}
			if err := c.Get(context.Background(), reqFor(pc).NamespacedName, got); err != nil {
				t.Fatalf("Get: %v", err)
			}
			if s := conditionStatus(got.Status.Conditions, "Ready"); s != tc.wantStatus {
				t.Errorf("Ready = %q, want %q", s, tc.wantStatus)
			}
			if !f.listCalled {
				t.Error("expected ListInstances to be called to prove the credentials")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// AuraInstance — create / adopt / idempotent.
// ---------------------------------------------------------------------------

func newAuraInstance(name string) *neo4jv1beta1.AuraInstance {
	return &neo4jv1beta1.AuraInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: neo4jv1beta1.AuraInstanceSpec{
			CredentialsSecretRef: credRef(),
			ProjectID:            "proj-1",
			CloudProvider:        "gcp",
			Region:               "europe-west1",
			Type:                 "professional-db",
			Version:              "5",
			Memory:               "4GB",
		},
	}
}

func TestAuraInstance_Create(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := newAuraInstance("inst-create")
	f := &fakeAuraAPI{
		listInstancesFn: func(context.Context, string) ([]aura.InstanceSummary, error) { return nil, nil },
		getTenantFn: func(_ context.Context, id string) (*aura.Tenant, error) {
			return &aura.Tenant{ID: id}, nil // empty configs → oracle skipped
		},
		createInstanceFn: func(context.Context, aura.CreateInstanceRequest) (*aura.CreateInstanceResponse, error) {
			return &aura.CreateInstanceResponse{
				ID: "new-id", Username: "neo4j", Password: "pw", ConnectionURL: "neo4j+s://x",
			}, nil
		},
		getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
			// Match spec (name + memory) so reconcileDrift issues no PATCH and the
			// reconcile proceeds to syncStatus.
			return &aura.Instance{
				ID: id, Name: "inst-create", Status: aura.InstanceStatusRunning,
				Memory: "4GB", ConnectionURL: "neo4j+s://x",
			}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: factoryFor(f),
	}

	// First pass adds the finalizer; run twice so the create/observe path runs
	// against the finalized object.
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, reqFor(inst)); err != nil {
			t.Fatalf("Reconcile #%d: %v", i, err)
		}
	}

	if !f.createCalled {
		t.Fatal("expected CreateInstance to be called")
	}
	got := &neo4jv1beta1.AuraInstance{}
	if err := c.Get(ctx, reqFor(inst).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if id := got.Annotations[AuraExternalIDAnnotation]; id != "new-id" {
		t.Errorf("external-id annotation = %q, want new-id", id)
	}
	if s := conditionStatus(got.Status.Conditions, "Ready"); s != metav1.ConditionTrue {
		t.Errorf("Ready = %q, want True", s)
	}

	sec := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: "inst-create-conn", Namespace: testNS}, sec); err != nil {
		t.Fatalf("connection Secret: %v", err)
	}
	if pw := string(sec.Data["NEO4J_PASSWORD"]); pw != "pw" {
		t.Errorf("NEO4J_PASSWORD = %q, want pw", pw)
	}
}

func TestAuraInstance_Adopt(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := newAuraInstance("inst-adopt")
	f := &fakeAuraAPI{
		listInstancesFn: func(context.Context, string) ([]aura.InstanceSummary, error) {
			return []aura.InstanceSummary{{ID: "existing", Name: "inst-adopt"}}, nil
		},
		getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
			return &aura.Instance{ID: id, Status: aura.InstanceStatusRunning}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: factoryFor(f),
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, reqFor(inst)); err != nil {
			t.Fatalf("Reconcile #%d: %v", i, err)
		}
	}
	if f.createCalled {
		t.Error("CreateInstance must NOT be called when an instance with our name already exists")
	}
	got := &neo4jv1beta1.AuraInstance{}
	if err := c.Get(ctx, reqFor(inst).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if id := got.Annotations[AuraExternalIDAnnotation]; id != "existing" {
		t.Errorf("external-id annotation = %q, want existing", id)
	}
}

func TestAuraInstance_Idempotent(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := newAuraInstance("inst-idem")
	inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
	controllerutil.AddFinalizer(inst, AuraInstanceFinalizer)
	f := &fakeAuraAPI{
		getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
			return &aura.Instance{ID: id, Status: aura.InstanceStatusRunning}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: factoryFor(f),
	}
	if _, err := r.Reconcile(context.Background(), reqFor(inst)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if f.listCalled {
		t.Error("ListInstances must NOT be called when the external-id annotation is already set")
	}
	if f.createCalled {
		t.Error("CreateInstance must NOT be called when the external-id annotation is already set")
	}
	if len(f.getInstanceIDs) == 0 || f.getInstanceIDs[0] != "id1" {
		t.Errorf("expected GetInstance(id1); got %v", f.getInstanceIDs)
	}
}

// ---------------------------------------------------------------------------
// AuraInstance — pause / resume.
// ---------------------------------------------------------------------------

func TestAuraInstance_PauseResume(t *testing.T) {
	scheme := auraTestScheme(t)

	t.Run("pause", func(t *testing.T) {
		inst := newAuraInstance("inst-pause")
		inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
		controllerutil.AddFinalizer(inst, AuraInstanceFinalizer)
		inst.Spec.Paused = true
		// Match spec name/memory so drift doesn't short-circuit before pause/resume.
		f := &fakeAuraAPI{getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
			return &aura.Instance{ID: id, Name: "inst-pause", Memory: "4GB", Status: aura.InstanceStatusRunning}, nil
		}}
		c := newAuraFakeClient(t, scheme, inst)
		r := &AuraInstanceReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
			ClientFactory: factoryFor(f),
		}
		if _, err := r.Reconcile(context.Background(), reqFor(inst)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if !f.pauseCalled {
			t.Error("expected PauseInstance to be called")
		}
		if f.resumeCalled {
			t.Error("ResumeInstance must not be called on a pause")
		}
	})

	t.Run("resume", func(t *testing.T) {
		inst := newAuraInstance("inst-resume")
		inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
		controllerutil.AddFinalizer(inst, AuraInstanceFinalizer)
		inst.Spec.Paused = false
		f := &fakeAuraAPI{getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
			return &aura.Instance{ID: id, Status: aura.InstanceStatusPaused}, nil
		}}
		c := newAuraFakeClient(t, scheme, inst)
		r := &AuraInstanceReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
			ClientFactory: factoryFor(f),
		}
		if _, err := r.Reconcile(context.Background(), reqFor(inst)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if !f.resumeCalled {
			t.Error("expected ResumeInstance to be called")
		}
		if f.pauseCalled {
			t.Error("PauseInstance must not be called on a resume")
		}
	})
}

// ---------------------------------------------------------------------------
// AuraInstance — deletion (Orphan default vs Delete).
// ---------------------------------------------------------------------------

func TestAuraInstance_Deletion(t *testing.T) {
	scheme := auraTestScheme(t)

	run := func(t *testing.T, policy string) *fakeAuraAPI {
		t.Helper()
		inst := newAuraInstance("inst-del")
		inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
		inst.Spec.DeletionPolicy = policy
		// A finalizer is required so the fake client honours the delete request
		// by stamping a DeletionTimestamp rather than removing the object.
		controllerutil.AddFinalizer(inst, AuraInstanceFinalizer)
		f := &fakeAuraAPI{}
		c := newAuraFakeClient(t, scheme, inst)
		ctx := context.Background()
		if err := c.Delete(ctx, inst); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		r := &AuraInstanceReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
			ClientFactory: factoryFor(f),
		}
		if _, err := r.Reconcile(ctx, reqFor(inst)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		// Finalizer must be gone → object is now removable/removed.
		got := &neo4jv1beta1.AuraInstance{}
		err := c.Get(ctx, reqFor(inst).NamespacedName, got)
		if err == nil && controllerutil.ContainsFinalizer(got, AuraInstanceFinalizer) {
			t.Error("expected the finalizer to be removed after deletion handling")
		}
		return f
	}

	t.Run("orphan default", func(t *testing.T) {
		f := run(t, "Orphan")
		if f.deleteCalled {
			t.Error("DeleteInstance must NOT be called under deletionPolicy=Orphan")
		}
	})

	t.Run("delete", func(t *testing.T) {
		f := run(t, "Delete")
		if !f.deleteCalled {
			t.Error("expected DeleteInstance to be called under deletionPolicy=Delete")
		}
	})
}

func TestAuraInstance_Paused(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := newAuraInstance("inst-paused")
	inst.Annotations = map[string]string{AuraPausedAnnotation: "true"}
	f := &fakeAuraAPI{}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: factoryFor(f)}

	if _, err := r.Reconcile(context.Background(), reqFor(inst)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if f.listCalled || f.createCalled {
		t.Error("a paused instance must not touch the Aura API")
	}
	got := &neo4jv1beta1.AuraInstance{}
	if err := c.Get(context.Background(), reqFor(inst).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if s := conditionStatus(got.Status.Conditions, "Synced"); s != metav1.ConditionFalse {
		t.Errorf("Synced = %q, want False (Paused)", s)
	}
}

func TestAuraInstance_ObserveOnly_NoCreate(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := newAuraInstance("inst-observe")
	inst.Spec.ManagementPolicies = []string{"Observe"}
	f := &fakeAuraAPI{listInstancesFn: func(context.Context, string) ([]aura.InstanceSummary, error) { return nil, nil }}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: factoryFor(f)}

	ctx := context.Background()
	for i := 0; i < 2; i++ { // pass 1 adds the finalizer, pass 2 runs observe-only
		if _, err := r.Reconcile(ctx, reqFor(inst)); err != nil {
			t.Fatalf("Reconcile #%d: %v", i, err)
		}
	}
	if f.createCalled {
		t.Error("managementPolicies=[Observe] must not create an instance")
	}
	got := &neo4jv1beta1.AuraInstance{}
	if err := c.Get(ctx, reqFor(inst).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if s := conditionStatus(got.Status.Conditions, "Synced"); s != metav1.ConditionFalse {
		t.Errorf("Synced = %q, want False (AwaitingInstance)", s)
	}
}

func TestAuraInstance_ManagementPolicyBlocksDelete(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := newAuraInstance("inst-nodelete")
	inst.Annotations = map[string]string{AuraExternalIDAnnotation: "id1"}
	inst.Spec.DeletionPolicy = "Delete"
	inst.Spec.ManagementPolicies = []string{"Observe"} // Delete not permitted → orphan
	controllerutil.AddFinalizer(inst, AuraInstanceFinalizer)
	f := &fakeAuraAPI{}
	c := newAuraFakeClient(t, scheme, inst)
	ctx := context.Background()
	if err := c.Delete(ctx, inst); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	r := &AuraInstanceReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50), ClientFactory: factoryFor(f)}
	if _, err := r.Reconcile(ctx, reqFor(inst)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if f.deleteCalled {
		t.Error("DeleteInstance must NOT be called when managementPolicies omits Delete (should orphan)")
	}
}

// ---------------------------------------------------------------------------
// AuraSnapshot.
// ---------------------------------------------------------------------------

func TestAuraSnapshot_CreateThenComplete(t *testing.T) {
	scheme := auraTestScheme(t)
	// The snapshot resolves creds + external ID via a seeded AuraInstance.
	inst := newAuraInstance("inst-snap")
	inst.Annotations = map[string]string{AuraExternalIDAnnotation: "ext-1"}
	snap := &neo4jv1beta1.AuraSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap", Namespace: testNS},
		Spec:       neo4jv1beta1.AuraSnapshotSpec{InstanceRef: "inst-snap"},
	}
	f := &fakeAuraAPI{
		createSnapshotFn: func(_ context.Context, _ string) (*aura.Snapshot, error) {
			// Live shape (verified 2026-08-01): the 202 body is exactly
			// {"snapshot_id": …} — no status, no profile, no instance_id, no
			// exportable. The fixture used to hand back a Status the API never
			// sends, which hid that the controller was writing an empty phase.
			return &aura.Snapshot{SnapshotID: "snap-1"}, nil
		},
		getSnapshotFn: func(_ context.Context, instanceID, snapshotID string) (*aura.Snapshot, error) {
			return &aura.Snapshot{InstanceID: instanceID, SnapshotID: snapshotID, Status: aura.SnapshotStatusCompleted}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, inst, snap)
	r := &AuraSnapshotReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: factoryFor(f),
	}
	ctx := context.Background()

	// First pass: request the snapshot.
	if _, err := r.Reconcile(ctx, reqFor(snap)); err != nil {
		t.Fatalf("Reconcile #1: %v", err)
	}
	got := &neo4jv1beta1.AuraSnapshot{}
	if err := c.Get(ctx, reqFor(snap).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	// The create response carries no status, so the controller must supply an
	// honest initial phase rather than persisting "".
	if got.Status.Phase != aura.SnapshotStatusPending {
		t.Errorf("phase after create = %q, want %q — a create response with no status must not "+
			"leave the CR reporting an empty phase", got.Status.Phase, aura.SnapshotStatusPending)
	}
	if got.Status.SnapshotID != "snap-1" {
		t.Fatalf("status.snapshotId = %q, want snap-1", got.Status.SnapshotID)
	}

	// Second pass: poll to completion.
	if _, err := r.Reconcile(ctx, reqFor(snap)); err != nil {
		t.Fatalf("Reconcile #2: %v", err)
	}
	if err := c.Get(ctx, reqFor(snap).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.Phase != aura.SnapshotStatusCompleted {
		t.Errorf("status.phase = %q, want %q", got.Status.Phase, aura.SnapshotStatusCompleted)
	}
}

// ---------------------------------------------------------------------------
// AuraRestore.
// ---------------------------------------------------------------------------

func TestAuraRestore_IssueThenComplete(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := newAuraInstance("inst-restore")
	inst.Annotations = map[string]string{AuraExternalIDAnnotation: "ext-1"}
	restore := &neo4jv1beta1.AuraRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: testNS},
		Spec: neo4jv1beta1.AuraRestoreSpec{
			InstanceRef: "inst-restore",
			SnapshotID:  "snap-1",
		},
	}
	f := &fakeAuraAPI{
		getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
			return &aura.Instance{ID: id, Status: aura.InstanceStatusRunning}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, inst, restore)
	r := &AuraRestoreReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: factoryFor(f),
	}
	ctx := context.Background()

	// First pass: issue the restore, phase → Restoring.
	if _, err := r.Reconcile(ctx, reqFor(restore)); err != nil {
		t.Fatalf("Reconcile #1: %v", err)
	}
	if !f.restoreCalled {
		t.Fatal("expected RestoreSnapshot to be called")
	}
	got := &neo4jv1beta1.AuraRestore{}
	if err := c.Get(ctx, reqFor(restore).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.Phase != auraRestoreRestoring {
		t.Fatalf("status.phase = %q, want %q", got.Status.Phase, auraRestoreRestoring)
	}

	// Second pass: instance running → phase Completed.
	if _, err := r.Reconcile(ctx, reqFor(restore)); err != nil {
		t.Fatalf("Reconcile #2: %v", err)
	}
	if err := c.Get(ctx, reqFor(restore).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.Phase != auraRestoreCompleted {
		t.Errorf("status.phase = %q, want %q", got.Status.Phase, auraRestoreCompleted)
	}
}

// ---------------------------------------------------------------------------
// AuraInstance — tier upgrade (professional-db → business-critical).
// ---------------------------------------------------------------------------

func TestAuraInstance_Upgrade(t *testing.T) {
	scheme := auraTestScheme(t)
	// Spec asks for business-critical; the live instance is still professional-db.
	inst := newAuraInstance("inst-upgrade")
	inst.Spec.Type = "business-critical"
	inst.Annotations = map[string]string{AuraExternalIDAnnotation: "ext-up"}
	controllerutil.AddFinalizer(inst, AuraInstanceFinalizer)
	f := &fakeAuraAPI{
		getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
			return &aura.Instance{
				ID: id, Name: "inst-upgrade", Memory: "4GB",
				Type: "professional-db", Status: aura.InstanceStatusRunning,
			}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: factoryFor(f),
	}
	if _, err := r.Reconcile(context.Background(), reqFor(inst)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !f.upgradeCalled {
		t.Error("expected UpgradeInstance to be called for professional-db → business-critical")
	}
	// The tier change must take the upgrade path, not a PATCH resize.
	if f.patchCalled {
		t.Error("PatchInstance must NOT be called for a tier upgrade")
	}
}

func TestAuraInstance_NoUpgradeWhenTypeMatches(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := newAuraInstance("inst-noup")
	inst.Annotations = map[string]string{AuraExternalIDAnnotation: "ext-noup"}
	controllerutil.AddFinalizer(inst, AuraInstanceFinalizer)
	f := &fakeAuraAPI{
		getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
			return &aura.Instance{
				ID: id, Name: "inst-noup", Memory: "4GB",
				Type: "professional-db", Status: aura.InstanceStatusRunning,
			}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, inst)
	r := &AuraInstanceReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: factoryFor(f),
	}
	if _, err := r.Reconcile(context.Background(), reqFor(inst)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if f.upgradeCalled {
		t.Error("UpgradeInstance must NOT be called when the type already matches")
	}
}

// ---------------------------------------------------------------------------
// AuraCustomerManagedKey.
// ---------------------------------------------------------------------------

func newAuraCMK(name string) *neo4jv1beta1.AuraCustomerManagedKey {
	return &neo4jv1beta1.AuraCustomerManagedKey{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: neo4jv1beta1.AuraCustomerManagedKeySpec{
			CredentialsSecretRef: credRef(),
			ProjectID:            "proj-1",
			CloudProvider:        "gcp",
			Region:               "europe-west1",
			InstanceType:         "enterprise-db",
			KeyID:                "projects/p/locations/eu/keyRings/r/cryptoKeys/k",
		},
	}
}

func TestAuraCMK_CreateThenReady(t *testing.T) {
	scheme := auraTestScheme(t)
	cmk := newAuraCMK("cmk-create")
	f := &fakeCMKAPI{
		listFn: func(context.Context, string) ([]aura.CustomerManagedKeySummary, error) { return nil, nil },
		createFn: func(context.Context, aura.CreateCMKRequest) (*aura.CustomerManagedKey, error) {
			return &aura.CustomerManagedKey{ID: "cmk-new", Status: aura.CMKStatusPending}, nil
		},
		getFn: func(_ context.Context, id string) (*aura.CustomerManagedKey, error) {
			return &aura.CustomerManagedKey{ID: id, Status: aura.CMKStatusReady}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, cmk)
	r := &AuraCustomerManagedKeyReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: cmkFactoryFor(f),
	}
	ctx := context.Background()
	// Pass 1 adds the finalizer; run twice so create/observe runs finalized.
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, reqFor(cmk)); err != nil {
			t.Fatalf("Reconcile #%d: %v", i, err)
		}
	}
	if !f.createCalled {
		t.Fatal("expected CreateCustomerManagedKey to be called")
	}
	if f.lastCreate.KeyID != cmk.Spec.KeyID || f.lastCreate.InstanceType != "enterprise-db" {
		t.Errorf("create request = %+v, want the spec's keyId + enterprise-db", f.lastCreate)
	}
	got := &neo4jv1beta1.AuraCustomerManagedKey{}
	if err := c.Get(ctx, reqFor(cmk).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if id := got.Annotations[AuraExternalCMKAnnotation]; id != "cmk-new" {
		t.Errorf("external-cmk-id annotation = %q, want cmk-new", id)
	}
	if got.Status.CustomerManagedKeyID != "cmk-new" {
		t.Errorf("status.customerManagedKeyId = %q, want cmk-new", got.Status.CustomerManagedKeyID)
	}
	if s := conditionStatus(got.Status.Conditions, "Ready"); s != metav1.ConditionTrue {
		t.Errorf("Ready = %q, want True", s)
	}
}

// Adoption must survive a lost external-ID annotation. The list endpoint only
// gives id/name/tenant_id, so the controller narrows on NAME and then confirms
// against the per-key detail before adopting.
func TestAuraCMK_AdoptByName(t *testing.T) {
	scheme := auraTestScheme(t)
	cmk := newAuraCMK("cmk-adopt")
	f := &fakeCMKAPI{
		listFn: func(context.Context, string) ([]aura.CustomerManagedKeySummary, error) {
			// Exactly what v1 returns: no key_id/region/cloud_provider/instance_type.
			return []aura.CustomerManagedKeySummary{{
				ID: "existing-cmk", Name: "cmk-adopt", TenantID: "proj-1",
			}}, nil
		},
		getFn: func(_ context.Context, id string) (*aura.CustomerManagedKey, error) {
			return &aura.CustomerManagedKey{
				ID: id, Name: "cmk-adopt", KeyID: cmk.Spec.KeyID, Region: "europe-west1",
				CloudProvider: "gcp", InstanceType: "enterprise-db", Status: aura.CMKStatusReady,
			}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, cmk)
	r := &AuraCustomerManagedKeyReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: cmkFactoryFor(f),
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, reqFor(cmk)); err != nil {
			t.Fatalf("Reconcile #%d: %v", i, err)
		}
	}
	if f.createCalled {
		t.Error("CreateCustomerManagedKey must NOT be called when a matching key already exists")
	}
	got := &neo4jv1beta1.AuraCustomerManagedKey{}
	if err := c.Get(ctx, reqFor(cmk).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if id := got.Annotations[AuraExternalCMKAnnotation]; id != "existing-cmk" {
		t.Errorf("external-cmk-id annotation = %q, want existing-cmk", id)
	}
}

// A same-named key pointing at different material must NOT be adopted (that
// would bind the CR to the wrong key) and must NOT be duplicated by a create —
// the reconcile fails loudly instead.
func TestAuraCMK_SameNameDifferentKeyIsRefused(t *testing.T) {
	scheme := auraTestScheme(t)
	cmk := newAuraCMK("cmk-conflict")
	f := &fakeCMKAPI{
		listFn: func(context.Context, string) ([]aura.CustomerManagedKeySummary, error) {
			return []aura.CustomerManagedKeySummary{{
				ID: "other-cmk", Name: "cmk-conflict", TenantID: "proj-1",
			}}, nil
		},
		getFn: func(_ context.Context, id string) (*aura.CustomerManagedKey, error) {
			return &aura.CustomerManagedKey{
				ID: id, Name: "cmk-conflict",
				KeyID:         "projects/p/locations/eu/keyRings/r/cryptoKeys/SOMETHING-ELSE",
				Region:        "europe-west1",
				CloudProvider: "gcp", InstanceType: "enterprise-db", Status: aura.CMKStatusReady,
			}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, cmk)
	r := &AuraCustomerManagedKeyReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: cmkFactoryFor(f),
	}
	_, _ = r.Reconcile(context.Background(), reqFor(cmk))
	if f.createCalled {
		t.Error("must NOT create a duplicate key when the name is already taken by a different key")
	}
	got := &neo4jv1beta1.AuraCustomerManagedKey{}
	_ = c.Get(context.Background(), reqFor(cmk).NamespacedName, got)
	if id := got.Annotations[AuraExternalCMKAnnotation]; id != "" {
		t.Errorf("must NOT adopt a mismatched key, but annotation = %q", id)
	}
}

func TestAuraCMK_Deletion(t *testing.T) {
	scheme := auraTestScheme(t)

	t.Run("orphan default keeps the key", func(t *testing.T) {
		cmk := newAuraCMK("cmk-orphan")
		cmk.Annotations = map[string]string{AuraExternalCMKAnnotation: "ext-cmk"}
		controllerutil.AddFinalizer(cmk, AuraCMKFinalizer)
		now := metav1.Now()
		cmk.DeletionTimestamp = &now
		f := &fakeCMKAPI{}
		c := newAuraFakeClient(t, scheme, cmk)
		r := &AuraCustomerManagedKeyReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
			ClientFactory: cmkFactoryFor(f),
		}
		if _, err := r.Reconcile(context.Background(), reqFor(cmk)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if f.deleteCalled {
			t.Error("DeleteCustomerManagedKey must NOT be called under the Orphan policy")
		}
	})

	t.Run("delete policy deregisters the key", func(t *testing.T) {
		cmk := newAuraCMK("cmk-del")
		cmk.Spec.DeletionPolicy = "Delete"
		cmk.Annotations = map[string]string{AuraExternalCMKAnnotation: "ext-cmk"}
		controllerutil.AddFinalizer(cmk, AuraCMKFinalizer)
		now := metav1.Now()
		cmk.DeletionTimestamp = &now
		f := &fakeCMKAPI{}
		c := newAuraFakeClient(t, scheme, cmk)
		r := &AuraCustomerManagedKeyReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
			ClientFactory: cmkFactoryFor(f),
		}
		if _, err := r.Reconcile(context.Background(), reqFor(cmk)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if !f.deleteCalled {
			t.Error("expected DeleteCustomerManagedKey under the Delete policy")
		}
	})

	t.Run("active key blocks deletion", func(t *testing.T) {
		cmk := newAuraCMK("cmk-active")
		cmk.Spec.DeletionPolicy = "Delete"
		cmk.Annotations = map[string]string{AuraExternalCMKAnnotation: "ext-cmk"}
		controllerutil.AddFinalizer(cmk, AuraCMKFinalizer)
		now := metav1.Now()
		cmk.DeletionTimestamp = &now
		f := &fakeCMKAPI{
			deleteFn: func(context.Context, string) error {
				return &aura.APIError{StatusCode: 400, Reason: aura.ReasonEncryptionKeyActive, Message: "The encryption key is active"}
			},
		}
		c := newAuraFakeClient(t, scheme, cmk)
		r := &AuraCustomerManagedKeyReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
			ClientFactory: cmkFactoryFor(f),
		}
		res, err := r.Reconcile(context.Background(), reqFor(cmk))
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if res.RequeueAfter == 0 {
			t.Error("expected a requeue while the key is still in use")
		}
		// The finalizer must remain so we don't drop tracking of a still-registered key.
		got := &neo4jv1beta1.AuraCustomerManagedKey{}
		if err := c.Get(context.Background(), reqFor(cmk).NamespacedName, got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !controllerutil.ContainsFinalizer(got, AuraCMKFinalizer) {
			t.Error("finalizer must be retained while the key is active/in-use")
		}
	})
}

func TestAuraCMK_Paused(t *testing.T) {
	scheme := auraTestScheme(t)
	cmk := newAuraCMK("cmk-paused")
	cmk.Annotations = map[string]string{AuraPausedAnnotation: "true"}
	f := &fakeCMKAPI{}
	c := newAuraFakeClient(t, scheme, cmk)
	r := &AuraCustomerManagedKeyReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: cmkFactoryFor(f),
	}
	if _, err := r.Reconcile(context.Background(), reqFor(cmk)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if f.createCalled || f.listCalled {
		t.Error("a paused CMK must not touch the Aura API")
	}
}

func TestAuraCMK_ObserveOnly_NoCreate(t *testing.T) {
	scheme := auraTestScheme(t)
	cmk := newAuraCMK("cmk-observe")
	cmk.Spec.ManagementPolicies = []string{"Observe"}
	controllerutil.AddFinalizer(cmk, AuraCMKFinalizer)
	f := &fakeCMKAPI{
		listFn: func(context.Context, string) ([]aura.CustomerManagedKeySummary, error) { return nil, nil },
	}
	c := newAuraFakeClient(t, scheme, cmk)
	r := &AuraCustomerManagedKeyReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: cmkFactoryFor(f),
	}
	if _, err := r.Reconcile(context.Background(), reqFor(cmk)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if f.createCalled {
		t.Error("CreateCustomerManagedKey must NOT be called under an Observe-only policy")
	}
}

// ---------------------------------------------------------------------------
// Programmable fake Aura v2beta1 IP-filter API.
// ---------------------------------------------------------------------------

type fakeIPFilterAPI struct {
	createFn func(ctx context.Context, orgID string, req aura.CreateIPFilterRequest) (*aura.IPFilter, error)
	getFn    func(ctx context.Context, orgID, id string) (*aura.IPFilter, error)
	listFn   func(ctx context.Context, orgID string) ([]aura.IPFilter, error)
	updateFn func(ctx context.Context, orgID, id string, req aura.UpdateIPFilterRequest) (*aura.IPFilter, error)

	createCalled bool
	listCalled   bool
	updateCalled bool
	deleteCalled bool
	lastCreate   aura.CreateIPFilterRequest
	lastUpdate   aura.UpdateIPFilterRequest
}

func (f *fakeIPFilterAPI) CreateIPFilter(ctx context.Context, orgID string, req aura.CreateIPFilterRequest) (*aura.IPFilter, error) {
	f.createCalled = true
	f.lastCreate = req
	if f.createFn != nil {
		return f.createFn(ctx, orgID, req)
	}
	return &aura.IPFilter{ID: "ipf-created", AllowList: req.AllowList, FilteredEntities: req.Entities}, nil
}

func (f *fakeIPFilterAPI) GetIPFilter(ctx context.Context, orgID, id string) (*aura.IPFilter, error) {
	if f.getFn != nil {
		return f.getFn(ctx, orgID, id)
	}
	return &aura.IPFilter{ID: id}, nil
}

func (f *fakeIPFilterAPI) ListIPFilters(ctx context.Context, orgID string) ([]aura.IPFilter, error) {
	f.listCalled = true
	if f.listFn != nil {
		return f.listFn(ctx, orgID)
	}
	return nil, nil
}

func (f *fakeIPFilterAPI) UpdateIPFilter(ctx context.Context, orgID, id string, req aura.UpdateIPFilterRequest) (*aura.IPFilter, error) {
	f.updateCalled = true
	f.lastUpdate = req
	if f.updateFn != nil {
		return f.updateFn(ctx, orgID, id, req)
	}
	return &aura.IPFilter{ID: id}, nil
}

func (f *fakeIPFilterAPI) DeleteIPFilter(ctx context.Context, orgID, id string) error {
	f.deleteCalled = true
	return nil
}

func ipFilterFactoryFor(f *fakeIPFilterAPI) auraIPFilterClientFactory {
	return func(auraCredentials) auraIPFilterAPI { return f }
}

func newAuraIPFilter(name string) *neo4jv1beta1.AuraIPFilter {
	return &neo4jv1beta1.AuraIPFilter{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: neo4jv1beta1.AuraIPFilterSpec{
			CredentialsSecretRef: credRef(),
			OrganizationID:       "org-1",
			AllowList: []neo4jv1beta1.AuraIPFilterAllowEntry{
				{Address: "203.0.113.0", PrefixLen: 24},
			},
		},
	}
}

func TestAuraIPFilter_CreateThenReady(t *testing.T) {
	scheme := auraTestScheme(t)
	f := newAuraIPFilter("ipf-create")
	api := &fakeIPFilterAPI{
		listFn: func(context.Context, string) ([]aura.IPFilter, error) { return nil, nil },
		createFn: func(_ context.Context, _ string, req aura.CreateIPFilterRequest) (*aura.IPFilter, error) {
			return &aura.IPFilter{ID: "ipf-new", AllowList: req.AllowList, FilteredEntities: req.Entities}, nil
		},
		getFn: func(_ context.Context, _, id string) (*aura.IPFilter, error) {
			// Echo the spec name + allow list so reconcileDrift issues no update and
			// the reconcile proceeds to syncStatus.
			return &aura.IPFilter{
				ID:        id,
				Name:      "ipf-create",
				AllowList: []aura.IPFilterAllowEntry{{Address: "203.0.113.0", PrefixLen: 24}},
			}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, f)
	r := &AuraIPFilterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: ipFilterFactoryFor(api),
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, reqFor(f)); err != nil {
			t.Fatalf("Reconcile #%d: %v", i, err)
		}
	}
	if !api.createCalled {
		t.Fatal("expected CreateIPFilter to be called")
	}
	// Create body must carry allow_list (address/prefix_len), never a cidrs list.
	if len(api.lastCreate.AllowList) != 1 || api.lastCreate.AllowList[0].PrefixLen != 24 {
		t.Errorf("create allow list = %+v, want one /24 entry", api.lastCreate.AllowList)
	}
	got := &neo4jv1beta1.AuraIPFilter{}
	if err := c.Get(ctx, reqFor(f).NamespacedName, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if id := got.Annotations[AuraExternalIPFilterAnnotation]; id != "ipf-new" {
		t.Errorf("external-ipfilter-id annotation = %q, want ipf-new", id)
	}
	if s := conditionStatus(got.Status.Conditions, "Ready"); s != metav1.ConditionTrue {
		t.Errorf("Ready = %q, want True", s)
	}
}

func TestAuraIPFilter_DriftUpdatesAllowList(t *testing.T) {
	scheme := auraTestScheme(t)
	f := newAuraIPFilter("ipf-drift")
	f.Spec.AllowList = []neo4jv1beta1.AuraIPFilterAllowEntry{
		{Address: "203.0.113.0", PrefixLen: 24},
		{Address: "198.51.100.7", PrefixLen: 32},
	}
	f.Annotations = map[string]string{AuraExternalIPFilterAnnotation: "ipf-1"}
	controllerutil.AddFinalizer(f, AuraIPFilterFinalizer)
	api := &fakeIPFilterAPI{
		getFn: func(_ context.Context, _, id string) (*aura.IPFilter, error) {
			// Observed has only one allow entry → drift → Update expected.
			return &aura.IPFilter{
				ID:        id,
				Name:      "ipf-drift",
				AllowList: []aura.IPFilterAllowEntry{{Address: "203.0.113.0", PrefixLen: 24}},
			}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, f)
	r := &AuraIPFilterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: ipFilterFactoryFor(api),
	}
	if _, err := r.Reconcile(context.Background(), reqFor(f)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !api.updateCalled {
		t.Error("expected UpdateIPFilter when the observed allow list differs from spec")
	}
	if api.lastUpdate.AllowList == nil || len(*api.lastUpdate.AllowList) != 2 {
		t.Errorf("update allow list = %+v, want 2 entries", api.lastUpdate.AllowList)
	}
}

func TestAuraIPFilter_Deletion(t *testing.T) {
	scheme := auraTestScheme(t)

	t.Run("orphan default keeps the filter", func(t *testing.T) {
		f := newAuraIPFilter("ipf-orphan")
		f.Annotations = map[string]string{AuraExternalIPFilterAnnotation: "ipf-1"}
		controllerutil.AddFinalizer(f, AuraIPFilterFinalizer)
		now := metav1.Now()
		f.DeletionTimestamp = &now
		api := &fakeIPFilterAPI{}
		c := newAuraFakeClient(t, scheme, f)
		r := &AuraIPFilterReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
			ClientFactory: ipFilterFactoryFor(api),
		}
		if _, err := r.Reconcile(context.Background(), reqFor(f)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if api.deleteCalled {
			t.Error("DeleteIPFilter must NOT be called under the Orphan policy")
		}
	})

	t.Run("delete policy removes the filter", func(t *testing.T) {
		f := newAuraIPFilter("ipf-del")
		f.Spec.DeletionPolicy = "Delete"
		f.Annotations = map[string]string{AuraExternalIPFilterAnnotation: "ipf-1"}
		controllerutil.AddFinalizer(f, AuraIPFilterFinalizer)
		now := metav1.Now()
		f.DeletionTimestamp = &now
		api := &fakeIPFilterAPI{}
		c := newAuraFakeClient(t, scheme, f)
		r := &AuraIPFilterReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
			ClientFactory: ipFilterFactoryFor(api),
		}
		if _, err := r.Reconcile(context.Background(), reqFor(f)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if !api.deleteCalled {
			t.Error("expected DeleteIPFilter under the Delete policy")
		}
	})
}

func TestAuraIPFilter_Paused(t *testing.T) {
	scheme := auraTestScheme(t)
	f := newAuraIPFilter("ipf-paused")
	f.Annotations = map[string]string{AuraPausedAnnotation: "true"}
	api := &fakeIPFilterAPI{}
	c := newAuraFakeClient(t, scheme, f)
	r := &AuraIPFilterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(50),
		ClientFactory: ipFilterFactoryFor(api),
	}
	if _, err := r.Reconcile(context.Background(), reqFor(f)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if api.createCalled || api.listCalled {
		t.Error("a paused AuraIPFilter must not touch the Aura API")
	}
}
