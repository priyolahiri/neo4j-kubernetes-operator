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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/aura"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// These tests pin the WIRING of the no-effect warnings
// (docs/knowledge/operations.md, "A schema field that no code reads must warn
// when set"): the validators produce the text (internal/validation), and each
// reconciler must turn it into a Warning event with reason ValidationWarning
// without letting it block the reconcile. A warning that is computed and never
// emitted is invisible, which is the failure mode the rule exists to prevent.

// requireWarningEvent asserts that events holds a "Warning ValidationWarning"
// event for path, in the standard no-effect shape.
func requireWarningEvent(t *testing.T, events []string, path string) {
	t.Helper()
	want := "Warning " + EventReasonValidationWarning + " " + path + " is accepted but has no effect today: "
	for _, e := range events {
		if strings.HasPrefix(e, want) {
			return
		}
	}
	t.Fatalf("no %q event among %q", want, events)
}

// requireNoFailureEvent asserts nothing in events reports the spec as invalid.
func requireNoFailureEvent(t *testing.T, events []string) {
	t.Helper()
	for _, e := range events {
		assert.NotContains(t, e, EventReasonValidationFailed, "the warning must not make the spec invalid: %q", e)
	}
}

func noEffectTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	return scheme
}

func TestRecordValidationWarnings_KeepsEachReason(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	r := &Neo4jEnterpriseClusterReconciler{Recorder: rec}

	r.recordValidationWarnings(&neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "c"}},
		validation.ClusterValidationResult{
			Warnings:         []string{"topology looks odd"},
			NoEffectWarnings: []string{"spec.tls.certificateSecret is accepted but has no effect today: x"},
		})

	events := drainEvents(rec)
	require.Len(t, events, 2)
	assert.Equal(t, "Warning "+EventReasonTopologyWarning+" topology looks odd", events[0],
		"topology warnings keep their documented reason")
	requireWarningEvent(t, events, "spec.tls.certificateSecret")
}

func TestStandaloneReconcile_WarnsOnTLSCertificateSecret(t *testing.T) {
	scheme := noEffectTestScheme(t)
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
			TLS: &neo4jv1beta1.TLSSpec{
				Mode:              "disabled",
				CertificateSecret: "my-cert",
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(20)
	r := &Neo4jEnterpriseStandaloneReconciler{
		Client: c, Scheme: scheme, Recorder: rec, Validator: validation.NewStandaloneValidator(),
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(standalone)})
	require.NoError(t, err)

	events := drainEvents(rec)
	requireWarningEvent(t, events, "spec.tls.certificateSecret")
	// The reconcile carried on past validation (it stops later, at the missing
	// StorageClass) — it was warned about, not rejected.
	requireNoFailureEvent(t, events)
}

func TestBackupReconcile_WarnsOnNoEffectCloudIdentity(t *testing.T) {
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default", Finalizers: []string{BackupFinalizer}},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:  "not-created-yet",
			AllDatabases: true,
			Storage: neo4jv1beta1.StorageLocation{
				Type: "s3", Bucket: "b",
				Cloud: &neo4jv1beta1.CloudBlock{
					Provider: "aws",
					Identity: &neo4jv1beta1.CloudIdentity{Provider: "aws", ServiceAccount: "my-sa"},
				},
			},
		},
	}
	r := newBackupTestReconcilerWithStatus(t, backup)
	rec := record.NewFakeRecorder(20)
	r.Recorder = rec

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(backup)})
	require.NoError(t, err)

	requireWarningEvent(t, drainEvents(rec), "spec.storage.cloud.identity.serviceAccount")

	got := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(backup), got))
	assert.Equal(t, "Waiting", got.Status.Phase,
		"the backup is accepted and waits for its target; the warning must not make it Invalid")
}

func TestRestoreReconcile_WarnsOnNoEffectCloudIdentity(t *testing.T) {
	scheme := noEffectTestScheme(t)
	restore := &neo4jv1beta1.Neo4jRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "default", Finalizers: []string{RestoreFinalizer}},
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			InstanceRef: "not-created-yet",
			Database:    "neo4j",
			Source: neo4jv1beta1.RestoreSource{
				Type:       "storage",
				BackupPath: "nightly",
				Storage: &neo4jv1beta1.StorageLocation{
					Type: "gcs", Bucket: "b",
					Cloud: &neo4jv1beta1.CloudBlock{
						Provider: "gcp",
						Identity: &neo4jv1beta1.CloudIdentity{
							Provider:   "gcp",
							AutoCreate: &neo4jv1beta1.AutoCreateSpec{Enabled: false},
						},
					},
				},
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(restore).
		WithStatusSubresource(&neo4jv1beta1.Neo4jRestore{}).Build()
	rec := record.NewFakeRecorder(20)
	r := &Neo4jRestoreReconciler{Client: c, Scheme: scheme, Recorder: rec}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(restore)})
	require.NoError(t, err)

	requireWarningEvent(t, drainEvents(rec), "spec.source.storage.cloud.identity.autoCreate.enabled")

	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(restore), got))
	assert.Equal(t, StatusPending, got.Status.Phase,
		"the restore is accepted and waits for its target; the warning must not make it Failed")
}

func TestAuraInstanceReconcile_WarnsOnCustomConnectionSecretFormat(t *testing.T) {
	scheme := auraTestScheme(t)
	inst := newAuraInstance("inst-custom")
	inst.Spec.ConnectionSecretFormat = "custom"
	f := &fakeAuraAPI{
		listInstancesFn: func(context.Context, string) ([]aura.InstanceSummary, error) { return nil, nil },
		getTenantFn: func(_ context.Context, id string) (*aura.Tenant, error) {
			return &aura.Tenant{ID: id}, nil
		},
		createInstanceFn: func(context.Context, aura.CreateInstanceRequest) (*aura.CreateInstanceResponse, error) {
			return &aura.CreateInstanceResponse{
				ID: "new-id", Username: "neo4j", Password: "pw", ConnectionURL: "neo4j+s://x",
			}, nil
		},
		getInstanceFn: func(_ context.Context, id string) (*aura.Instance, error) {
			return &aura.Instance{
				ID: id, Name: "inst-custom", Status: aura.InstanceStatusRunning,
				Memory: "4GB", ConnectionURL: "neo4j+s://x",
			}, nil
		},
	}
	c := newAuraFakeClient(t, scheme, inst)
	rec := record.NewFakeRecorder(50)
	r := &AuraInstanceReconciler{Client: c, Scheme: scheme, Recorder: rec, ClientFactory: factoryFor(f)}

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		_, err := r.Reconcile(ctx, reqFor(inst))
		require.NoError(t, err)
	}

	events := drainEvents(rec)
	requireWarningEvent(t, events, "spec.connectionSecretFormat")

	// Still reconciled in full: the instance was created and the Secret written
	// with exactly the neo4j-driver keys the warning says "custom" produces.
	assert.True(t, f.createCalled, "the AuraInstance must still be created")
	sec := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "inst-custom-conn", Namespace: testNS}, sec))
	assert.Equal(t, "pw", string(sec.Data["NEO4J_PASSWORD"]))

	// A real format is not warned about.
	rec2 := record.NewFakeRecorder(50)
	inst2 := newAuraInstance("inst-driver")
	inst2.Spec.ConnectionSecretFormat = "neo4j-driver"
	c2 := newAuraFakeClient(t, scheme, inst2)
	r2 := &AuraInstanceReconciler{Client: c2, Scheme: scheme, Recorder: rec2, ClientFactory: factoryFor(f)}
	for i := 0; i < 2; i++ {
		_, err := r2.Reconcile(ctx, reqFor(inst2))
		require.NoError(t, err)
	}
	for _, e := range drainEvents(rec2) {
		assert.NotContains(t, e, "has no effect today", "neo4j-driver is a real format")
	}
}
