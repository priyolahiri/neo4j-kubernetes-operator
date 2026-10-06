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

// A standalone restores ONLINE (rule 108): dbms.recreateDatabase / CREATE
// DATABASE … seedURI against the running instance, where the neo4j-admin Job
// stopped it and took every database offline to restore one. Only a
// point-in-time restore still takes the Job.

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// statusRestoreReconciler is newRestoreTestReconciler with Neo4jRestore status
// writes enabled, for tests that read the phase and message back.
func statusRestoreReconciler(t *testing.T, objs ...runtime.Object) *Neo4jRestoreReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jRestore{}).Build()
	return &Neo4jRestoreReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(16)}
}

func TestRestoreRunsOffline(t *testing.T) {
	at := metav1.Now()
	cases := []struct {
		name      string
		source    neo4jv1beta1.RestoreSource
		cluster   bool
		wantJobOn bool
	}{
		{"standalone, backupRef", neo4jv1beta1.RestoreSource{Type: "backup", BackupRef: "b"}, false, false},
		{"standalone, storage", neo4jv1beta1.RestoreSource{Type: "storage"}, false, false},
		{"standalone, pitr", neo4jv1beta1.RestoreSource{Type: "pitr"}, false, true},
		{"standalone, backupRef + pointInTime", neo4jv1beta1.RestoreSource{Type: "backup", BackupRef: "b", PointInTime: &at}, false, true},
		{"cluster, backupRef", neo4jv1beta1.RestoreSource{Type: "backup", BackupRef: "b"}, true, false},
		{"cluster, pitr (refused by validation, never a Job)", neo4jv1beta1.RestoreSource{Type: "pitr"}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := &neo4jv1beta1.Neo4jRestore{Spec: neo4jv1beta1.Neo4jRestoreSpec{Source: tc.source}}
			assert.Equal(t, tc.wantJobOn, restoreRunsOffline(restore, tc.cluster))
		})
	}
}

// The online path was written for clusters; these are the places a standalone
// differs, and getting any of them wrong fails only on a standalone.
func TestRestoreTarget_StandaloneDiffersFromCluster(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	sa.Spec.Env = []corev1.EnvVar{{Name: "A", Value: "1"}}
	st := restoreTarget{standalone: sa}
	assert.True(t, st.isStandalone())
	assert.Equal(t, "standalone", st.kind())
	assert.Equal(t, "sa", st.statefulSetName(), "a standalone's StatefulSet is <name>, not <name>-server")
	assert.Equal(t, map[string]string{"app": "sa"}, st.podLabels(), "a standalone's pods carry app=<name>, not neo4j.com/cluster")
	assert.Equal(t, sa.Spec.Env, st.env())

	ct := restoreTarget{cluster: minimalClusterForRestore("c", "ns")}
	assert.Equal(t, "cluster", ct.kind())
	assert.Equal(t, "c-server", ct.statefulSetName())
	assert.Equal(t, map[string]string{"neo4j.com/cluster": "c"}, ct.podLabels())
}

func TestGetRestoreTarget(t *testing.T) {
	r := newRestoreTestReconciler(t, minimalStandaloneForRestore("sa", "ns"), minimalClusterForRestore("c", "ns"))
	got, err := r.getRestoreTarget(context.Background(), minimalRestore("r", "ns", "sa"))
	require.NoError(t, err)
	assert.True(t, got.isStandalone())
	got, err = r.getRestoreTarget(context.Background(), minimalRestore("r", "ns", "c"))
	require.NoError(t, err)
	assert.False(t, got.isStandalone())
	_, err = r.getRestoreTarget(context.Background(), minimalRestore("r", "ns", "missing"))
	assert.Error(t, err)
}

// Cloud seeds are fetched by the server, so its credentials and endpoint must
// land on the STANDALONE CR — the cluster code Got/Updated a
// Neo4jEnterpriseCluster, which does not exist for a standalone.
func TestProjectSeedConfig_OntoAStandalone(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	sa.Annotations = map[string]string{AutoInheritSeedCredsAnnotation: "true"}
	r := newRestoreTestReconciler(t, sa)
	cloud := &neo4jv1beta1.CloudBlock{Provider: "aws", CredentialsSecretRef: "minio-creds", EndpointURL: "http://minio:9000", ForcePathStyle: true}

	projected, missing, err := r.projectSeedConfig(context.Background(), restoreTarget{standalone: sa}, "minio-creds", cloud)
	require.NoError(t, err)
	assert.Empty(t, missing)
	assert.True(t, projected)

	got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sa", Namespace: "ns"}, got))
	assert.True(t, hasSecretEnvFrom(got.Spec.ExtraEnvFrom, "minio-creds"))
	assert.True(t, envHasSeedEndpoint(got.Spec.Env))
	assert.Equal(t, "seed-config", got.Annotations[AutoInheritedFromAnnotation])

	// Without the opt-in the standalone is not patched, and the message says so.
	plain := minimalStandaloneForRestore("plain", "ns")
	r = newRestoreTestReconciler(t, plain)
	_, missing, err = r.projectSeedConfig(context.Background(), restoreTarget{standalone: plain}, "minio-creds", cloud)
	assert.ErrorIs(t, err, errSeedConfigNotAutoInherited)
	assert.Len(t, missing, 2)
}

// The seed-proxy NetworkPolicy admitted only pods labelled neo4j.com/cluster —
// a standalone's pods carry app=<name>, so on an enforcing CNI the standalone
// could not reach its own seed. Kind does not enforce NetworkPolicy, so no
// live walk would have shown it.
func TestSeedProxyNetworkPolicy_AdmitsTheStandalonesPods(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := minimalRestore("r", "ns", "sa")
	r := newRestoreTestReconciler(t, sa, restore)
	require.NoError(t, ensurePVCSeedProxyNetworkPolicy(context.Background(), r.Client, r.Scheme, restore, restore.Name,
		restoreTarget{standalone: sa}.podLabels()))
	np := &networkingv1.NetworkPolicy{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "backup-seed-proxy-r", Namespace: "ns"}, np))
	assert.Equal(t, map[string]string{"app": "sa"}, np.Spec.Ingress[0].From[0].PodSelector.MatchLabels)
}

func TestSeedCredsRolledOut_ReadsTheStandalonesStatefulSet(t *testing.T) {
	one := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "ns", Generation: 2},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &one,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:    "neo4j",
				EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "creds"}}}},
			}}}},
		},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 2, UpdatedReplicas: 1, ReadyReplicas: 1},
	}
	r := newRestoreTestReconciler(t, sts)
	ok, err := r.seedCredsRolledOut(context.Background(), restoreTarget{standalone: minimalStandaloneForRestore("sa", "ns")}, "creds")
	require.NoError(t, err)
	assert.True(t, ok, "a standalone's StatefulSet is <name>; looking for <name>-server never found it")
}

// End to end through startRestore: a standalone backup restore no longer stops
// the instance or builds a Job — it takes the online path, which here stops at
// the seed-credentials check and names the standalone.
func TestStartRestore_StandaloneTakesTheOnlinePath(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := minimalRestore("r", "ns", "sa")
	restore.Spec.StopCluster = true
	restore.Spec.Source = neo4jv1beta1.RestoreSource{
		Type:       "storage",
		BackupPath: "nightly/neo4j-2026-10-05T08-43-31.backup",
		Storage: &neo4jv1beta1.StorageLocation{
			Type: "s3", Bucket: "neo4j-backups", Path: "prod",
			Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws", CredentialsSecretRef: "creds"},
		},
	}
	r := statusRestoreReconciler(t, sa, restore)

	_, _ = r.startRestore(context.Background(), restore, standaloneAsCluster(sa))

	jobs := &batchv1.JobList{}
	require.NoError(t, r.List(context.Background(), jobs))
	assert.Empty(t, jobs.Items, "an online restore creates no Job")

	gotSA := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sa", Namespace: "ns"}, gotSA))
	assert.Empty(t, gotSA.Annotations[RestoreInProgressAnnotation], "the standalone is not stopped, despite stopCluster: true")

	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
	assert.Equal(t, StatusFailed, got.Status.Phase)
	assert.True(t, strings.HasPrefix(got.Status.Message, `standalone "sa"'s server pods can't reach the seed source`), got.Status.Message)
}

// A point-in-time restore into a standalone still takes the Job: recreate has
// no restore-until option.
func TestStartRestore_StandalonePointInTimeStaysOnTheJob(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := minimalRestore("r", "ns", "sa")
	at := metav1.Now()
	restore.Spec.Source = neo4jv1beta1.RestoreSource{
		Type: "storage", BackupPath: "nightly", PointInTime: &at,
		Storage: &neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}},
	}
	restore.Spec.Options = &neo4jv1beta1.RestoreOptionsSpec{ReplaceExisting: true}
	r := statusRestoreReconciler(t, sa, restore)

	_, _ = r.startRestore(context.Background(), restore, standaloneAsCluster(sa))

	jobs := &batchv1.JobList{}
	require.NoError(t, r.List(context.Background(), jobs))
	require.Len(t, jobs.Items, 1, "a point-in-time restore into a standalone runs the neo4j-admin Job")
	assert.Contains(t, strings.Join(jobs.Items[0].Spec.Template.Spec.Containers[0].Args, " "), "--restore-until")
}

// The online path skipped restore hooks entirely — a standalone that used to
// run its post-restore hooks through the Job would silently lose them. The
// completion step now runs them; a hook Job that never finishes fails the
// restore instead of Completing it.
func TestCompleteOnlineRestore_RunsPostRestoreHooks(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := minimalRestore("r", "ns", "sa")
	restore.Spec.Options = &neo4jv1beta1.RestoreOptionsSpec{PostRestore: &neo4jv1beta1.RestoreHooks{
		Job: &neo4jv1beta1.RestoreHookJob{
			Template: neo4jv1beta1.JobTemplateSpec{Container: neo4jv1beta1.ContainerSpec{Image: "busybox", Command: []string{"true"}}},
			Timeout:  "1s",
		},
	}}
	r := statusRestoreReconciler(t, sa, restore)

	_, err := r.completeOnlineRestore(context.Background(), restore, standaloneAsCluster(sa), "test")
	require.Error(t, err)

	jobs := &batchv1.JobList{}
	require.NoError(t, r.List(context.Background(), jobs))
	assert.Len(t, jobs.Items, 1, "the post-restore hook Job was created")
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
	assert.Equal(t, StatusFailed, got.Status.Phase)
	assert.Contains(t, got.Status.Message, "Post-restore hooks failed")

	// No hooks: straight to Completed.
	plain := minimalRestore("p", "ns", "sa")
	r = statusRestoreReconciler(t, sa, plain)
	_, err = r.completeOnlineRestore(context.Background(), plain, standaloneAsCluster(sa), "test")
	require.NoError(t, err)
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "p", Namespace: "ns"}, got))
	assert.Equal(t, StatusCompleted, got.Status.Phase)
}

// The all-databases loop is re-entered on every requeue while it waits for
// credentials or the seed proxy; its pre-restore hooks must run once per
// attempt, and run again on a fresh attempt.
func TestRestorePreHooksRanMarker(t *testing.T) {
	restore := minimalRestore("r", "ns", "sa")
	r := statusRestoreReconciler(t, restore)
	require.NoError(t, r.markRestorePreHooksRan(context.Background(), restore))
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
	assert.Equal(t, "true", got.Annotations[AnnotationRestorePreHooksRan])

	require.NoError(t, r.clearCypherRestoreIssued(context.Background(), got))
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
	assert.Empty(t, got.Annotations[AnnotationRestorePreHooksRan], "a fresh attempt runs its hooks again")
}
