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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const irsaRole = "eks.amazonaws.com/role-arn"

func standaloneWithIdentity(arn string) *neo4jv1beta1.Neo4jEnterpriseStandalone {
	s := standaloneForSTS("2026.08.1-enterprise")
	s.UID = "sa-uid"
	s.Spec.PodServiceAccountAnnotations = map[string]string{irsaRole: arn}
	return s
}

func getSA(t *testing.T, r *Neo4jEnterpriseStandaloneReconciler) (*corev1.ServiceAccount, error) {
	t.Helper()
	sa := &corev1.ServiceAccount{}
	err := r.Get(context.Background(), types.NamespacedName{Name: "sa-neo4j", Namespace: "default"}, sa)
	return sa, err
}

// Without the field the pod template is what it always was, so a standalone
// that does not use it is not restarted by the upgrade that adds it.
func TestCreateStatefulSet_PodIdentityOnlyWhenSet(t *testing.T) {
	r := &Neo4jEnterpriseStandaloneReconciler{}
	plain := r.createStatefulSet(context.Background(), standaloneForSTS("2026.08.1-enterprise"))
	assert.Empty(t, plain.Spec.Template.Spec.ServiceAccountName)
	assert.NotContains(t, plain.Spec.Template.Annotations, standalonePodIdentityAnnotation)

	withID := r.createStatefulSet(context.Background(), standaloneWithIdentity("arn:aws:iam::1:role/a"))
	assert.Equal(t, "sa-neo4j", withID.Spec.Template.Spec.ServiceAccountName)
	hashA := withID.Spec.Template.Annotations[standalonePodIdentityAnnotation]
	require.NotEmpty(t, hashA)

	// A new role must reach a new pod: identity webhooks inject at admission.
	changed := r.createStatefulSet(context.Background(), standaloneWithIdentity("arn:aws:iam::1:role/b"))
	assert.NotEqual(t, hashA, changed.Spec.Template.Annotations[standalonePodIdentityAnnotation])
}

func TestStandaloneServiceAccount_Lifecycle(t *testing.T) {
	r, _ := standaloneCMTestReconciler(t)
	ctx := context.Background()
	s := standaloneWithIdentity("arn:aws:iam::1:role/a")

	// Created before the StatefulSet that references it, owned by the standalone.
	require.NoError(t, r.ensureStandaloneServiceAccount(ctx, s))
	sa, err := getSA(t, r)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::1:role/a", sa.Annotations[irsaRole])
	require.NotNil(t, metav1.GetControllerOf(sa))
	assert.Equal(t, s.UID, metav1.GetControllerOf(sa).UID)

	// Foreign annotations survive; a key dropped from the spec goes.
	sa.Annotations["other.example/keep"] = "yes"
	require.NoError(t, r.Update(ctx, sa))
	s.Spec.PodServiceAccountAnnotations = map[string]string{"iam.gke.io/gcp-service-account": "x@p.iam.gserviceaccount.com"}
	require.NoError(t, r.ensureStandaloneServiceAccount(ctx, s))
	sa, _ = getSA(t, r)
	assert.NotContains(t, sa.Annotations, irsaRole)
	assert.Equal(t, "yes", sa.Annotations["other.example/keep"])
	assert.Equal(t, "x@p.iam.gserviceaccount.com", sa.Annotations["iam.gke.io/gcp-service-account"])

	// Removed from the spec: kept while the pods still run under it…
	require.NoError(t, r.reconcileStatefulSet(ctx, s))
	s.Spec.PodServiceAccountAnnotations = nil
	stale := getSTS(t, r)
	stale.Spec.Template.Spec.ServiceAccountName = "sa-neo4j"
	require.NoError(t, r.Update(ctx, stale))
	require.NoError(t, r.cleanupStandaloneServiceAccount(ctx, s))
	_, err = getSA(t, r)
	require.NoError(t, err, "the pod still runs under it")

	// …and deleted once the StatefulSet no longer names it.
	require.NoError(t, r.reconcileStatefulSet(ctx, s))
	sts := getSTS(t, r)
	assert.Empty(t, sts.Spec.Template.Spec.ServiceAccountName)
	assert.NotContains(t, sts.Spec.Template.Annotations, standalonePodIdentityAnnotation)
	require.NoError(t, r.cleanupStandaloneServiceAccount(ctx, s))
	_, err = getSA(t, r)
	assert.True(t, apierrors.IsNotFound(err), "an identity-bearing ServiceAccount does not outlive its use")
}

// A ServiceAccount of that name the operator did not create is not deleted.
func TestCleanupStandaloneServiceAccount_LeavesAForeignOne(t *testing.T) {
	r, _ := standaloneCMTestReconciler(t)
	ctx := context.Background()
	require.NoError(t, r.Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "sa-neo4j", Namespace: "default"}}))
	require.NoError(t, r.cleanupStandaloneServiceAccount(ctx, standaloneForSTS("2026.08.1-enterprise")))
	_, err := getSA(t, r)
	assert.NoError(t, err)
}

// A cloud backup without a credentialsSecretRef authenticates by pod identity.
// A standalone with one restores it online; one without still takes the Job,
// whose ServiceAccount carries the identity.
func TestStandaloneOfflineReason_PodIdentity(t *testing.T) {
	restore := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "neo4j-1.backup", ArtifactType: "FULL"})
	restore.Status.ResolvedSource.Storage.Cloud.CredentialsSecretRef = ""

	why := standaloneOfflineReason(restore, standaloneForSTS("2026.08.1-enterprise"))
	assert.Contains(t, why, "spec.podServiceAccountAnnotations")
	assert.Equal(t, why, standaloneOfflineReason(restore, nil), "an unknown target has no identity")
	assert.Empty(t, standaloneOfflineReason(restore, standaloneWithIdentity("arn:aws:iam::1:role/a")))
}

// The identity is live spec; a restore keeps the path it started on.
func TestRestoreOnJobPath_PinnedOnceStarted(t *testing.T) {
	byIdentity := func() *neo4jv1beta1.Neo4jRestore {
		r := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "neo4j-1.backup", ArtifactType: "FULL"})
		r.Status.ResolvedSource.Storage.Cloud.CredentialsSecretRef = ""
		return r
	}
	plain := minimalStandaloneForRestore("sa", "ns")
	withID := minimalStandaloneForRestore("sa", "ns")
	withID.Spec.PodServiceAccountAnnotations = map[string]string{irsaRole: "arn"}

	assert.False(t, statusRestoreReconciler(t, withID).restoreOnJobPath(context.Background(), byIdentity(), false))
	assert.True(t, statusRestoreReconciler(t, plain).restoreOnJobPath(context.Background(), byIdentity(), false))

	issued := byIdentity()
	issued.Annotations = map[string]string{AnnotationCypherRestoreIssued: "2026-10-06T00:00:00Z"}
	assert.False(t, statusRestoreReconciler(t, plain).restoreOnJobPath(context.Background(), issued, false),
		"issued online, then the identity was removed: it stays online")

	looping := byIdentity()
	looping.Spec.AllDatabases = true
	looping.Status.DatabaseResults = []neo4jv1beta1.DatabaseRestoreResult{{Database: "db0"}}
	assert.False(t, statusRestoreReconciler(t, plain).restoreOnJobPath(context.Background(), looping, false),
		"an all-databases restore that has started its loop stays online")

	held := minimalStandaloneForRestore("sa", "ns")
	held.Spec.PodServiceAccountAnnotations = map[string]string{irsaRole: "arn"}
	held.Annotations = map[string]string{RestoreInProgressAnnotation: "r"}
	assert.True(t, statusRestoreReconciler(t, held).restoreOnJobPath(context.Background(), byIdentity(), false),
		"a Job restore holding the instance stays on the Job")
}

// Identity webhooks inject at pod admission: seeding before the pod was
// recreated under the ServiceAccount fails inside Neo4j with no retry.
func TestAwaitSeedIdentity(t *testing.T) {
	s := minimalStandaloneForRestore("sa", "ns")
	s.Spec.PodServiceAccountAnnotations = map[string]string{irsaRole: "arn"}
	cloud := neo4jv1beta1.StorageLocation{Type: "s3", Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws"}}
	one := int32(1)
	sts := func(saName string, hash string, updated int32) *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "ns", Generation: 2},
			Spec: appsv1.StatefulSetSpec{Replicas: &one, Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{standalonePodIdentityAnnotation: hash}},
				Spec:       corev1.PodSpec{ServiceAccountName: saName},
			}},
			Status: appsv1.StatefulSetStatus{ObservedGeneration: 2, UpdatedReplicas: updated, ReadyReplicas: 1},
		}
	}
	hash := podIdentityHash(s.Spec.PodServiceAccountAnnotations)
	cases := []struct {
		name    string
		target  restoreTarget
		storage neo4jv1beta1.StorageLocation
		sts     *appsv1.StatefulSet
		ready   bool
	}{
		{"pod not yet under the ServiceAccount", restoreTarget{standalone: s}, cloud, sts("", "", 1), false},
		{"template updated, pod not recreated yet", restoreTarget{standalone: s}, cloud, sts("sa-neo4j", hash, 0), false},
		{"template from an older identity", restoreTarget{standalone: s}, cloud, sts("sa-neo4j", "old", 1), false},
		{"rolled out", restoreTarget{standalone: s}, cloud, sts("sa-neo4j", hash, 1), true},
		{"credentials Secret: no identity needed", restoreTarget{standalone: s},
			neo4jv1beta1.StorageLocation{Type: "s3", Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws", CredentialsSecretRef: "c"}}, sts("", "", 1), true},
		{"PVC", restoreTarget{standalone: s}, neo4jv1beta1.StorageLocation{Type: "pvc"}, sts("", "", 1), true},
		{"cluster", restoreTarget{cluster: minimalClusterForRestore("c", "ns")}, cloud, sts("", "", 1), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := minimalRestore("r", "ns", "sa")
			r := statusRestoreReconciler(t, restore, tc.sts)
			_, ready := r.awaitSeedIdentity(context.Background(), restore, tc.target, tc.storage)
			assert.Equal(t, tc.ready, ready)
			if !ready {
				got := &neo4jv1beta1.Neo4jRestore{}
				require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
				assert.Equal(t, StatusPending, got.Status.Phase)
				assert.Contains(t, got.Status.Message, `ServiceAccount "sa-neo4j"`)
			}
		})
	}
}
