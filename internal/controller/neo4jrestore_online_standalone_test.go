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
	"fmt"
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

// pinnedBackupRestore is a `source.type: backup` restore whose source is
// already resolved, as ensureResolvedBackupSource leaves it.
func pinnedBackupRestore(storageType string, snap neo4jv1beta1.ResolvedRestoreSource) *neo4jv1beta1.Neo4jRestore {
	restore := minimalRestore("r", "ns", "sa")
	restore.Spec.Source = neo4jv1beta1.RestoreSource{Type: "backup", BackupRef: "b"}
	snap.BackupRef = "b"
	snap.Storage = &neo4jv1beta1.StorageLocation{Type: storageType}
	if storageType != "pvc" {
		snap.Storage.Cloud = &neo4jv1beta1.CloudBlock{Provider: "aws", CredentialsSecretRef: "creds"}
	}
	restore.Status.ResolvedSource = &snap
	return restore
}

func TestRestoreRunsOffline(t *testing.T) {
	at := metav1.Now()
	full := neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "neo4j-1.backup", ArtifactType: "FULL"}
	diff := neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "neo4j-2.backup", ArtifactType: "DIFF"}
	untyped := neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "neo4j-3.backup"}
	allDBs := func(types ...string) neo4jv1beta1.ResolvedRestoreSource {
		snap := neo4jv1beta1.ResolvedRestoreSource{DatabaseArtifacts: []neo4jv1beta1.DatabaseArtifact{
			{Database: "system", Filename: "system-1.backup"}, // never restored, never consulted
		}}
		for i, ty := range types {
			snap.DatabaseArtifacts = append(snap.DatabaseArtifacts,
				neo4jv1beta1.DatabaseArtifact{Database: fmt.Sprintf("db%d", i), Filename: fmt.Sprintf("db%d-1.backup", i), Type: ty})
		}
		return snap
	}

	cases := []struct {
		name    string
		restore func() *neo4jv1beta1.Neo4jRestore
		cluster bool
		offline bool
	}{
		{"PVC FULL", func() *neo4jv1beta1.Neo4jRestore { return pinnedBackupRestore("pvc", full) }, false, false},
		{"PVC DIFF: the seed proxy merges the chain", func() *neo4jv1beta1.Neo4jRestore { return pinnedBackupRestore("pvc", diff) }, false, false},
		{"PVC, type not recorded: merged too", func() *neo4jv1beta1.Neo4jRestore { return pinnedBackupRestore("pvc", untyped) }, false, false},
		{"cloud DIFF: the seed provider reads the chain", func() *neo4jv1beta1.Neo4jRestore { return pinnedBackupRestore("s3", diff) }, false, false},
		{"cloud, type not recorded", func() *neo4jv1beta1.Neo4jRestore { return pinnedBackupRestore("s3", untyped) }, false, false},
		{"cloud by pod identity: a standalone's pods cannot carry one", func() *neo4jv1beta1.Neo4jRestore {
			r := pinnedBackupRestore("s3", full)
			r.Status.ResolvedSource.Storage.Cloud.CredentialsSecretRef = ""
			return r
		}, false, true},
		{"cluster, cloud by pod identity (spec.podServiceAccountAnnotations)", func() *neo4jv1beta1.Neo4jRestore {
			r := pinnedBackupRestore("s3", full)
			r.Status.ResolvedSource.Storage.Cloud = nil
			return r
		}, true, false},
		{"cloud, no artifact recorded", func() *neo4jv1beta1.Neo4jRestore {
			return pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{})
		}, false, true},
		{"not resolved yet", func() *neo4jv1beta1.Neo4jRestore {
			r := pinnedBackupRestore("pvc", full)
			r.Status.ResolvedSource = nil
			return r
		}, false, true},
		{"source.type storage: the path may be a directory", func() *neo4jv1beta1.Neo4jRestore {
			r := minimalRestore("r", "ns", "sa")
			r.Spec.Source = neo4jv1beta1.RestoreSource{Type: "storage", BackupPath: "nightly/neo4j-1.backup"}
			return r
		}, false, true},
		{"pitr", func() *neo4jv1beta1.Neo4jRestore {
			r := minimalRestore("r", "ns", "sa")
			r.Spec.Source = neo4jv1beta1.RestoreSource{Type: "pitr"}
			return r
		}, false, true},
		{"backupRef + pointInTime", func() *neo4jv1beta1.Neo4jRestore {
			r := pinnedBackupRestore("s3", full)
			r.Spec.Source.PointInTime = &at
			return r
		}, false, true},
		{"all databases on a PVC, all FULL", func() *neo4jv1beta1.Neo4jRestore {
			r := pinnedBackupRestore("pvc", allDBs("FULL", "FULL"))
			r.Spec.AllDatabases = true
			return r
		}, false, false},
		{"all databases on a PVC, one DIFF", func() *neo4jv1beta1.Neo4jRestore {
			r := pinnedBackupRestore("pvc", allDBs("FULL", "DIFF"))
			r.Spec.AllDatabases = true
			return r
		}, false, false},
		{"one database of an all-databases backup, FULL", func() *neo4jv1beta1.Neo4jRestore {
			r := pinnedBackupRestore("pvc", allDBs("DIFF", "FULL"))
			r.Spec.Source.SourceDatabase = "db1"
			return r
		}, false, false},
		{"one database of an all-databases backup, DIFF", func() *neo4jv1beta1.Neo4jRestore {
			r := pinnedBackupRestore("pvc", allDBs("DIFF", "FULL"))
			r.Spec.Source.SourceDatabase = "db0"
			return r
		}, false, false},
		{"cluster: never the Job, whatever the source", func() *neo4jv1beta1.Neo4jRestore { return pinnedBackupRestore("pvc", diff) }, true, false},
		{"cluster pitr (refused by validation, never a Job)", func() *neo4jv1beta1.Neo4jRestore {
			r := minimalRestore("r", "ns", "c")
			r.Spec.Source = neo4jv1beta1.RestoreSource{Type: "pitr"}
			return r
		}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := tc.restore()
			assert.Equal(t, tc.offline, restoreRunsOffline(restore, tc.cluster, nil), standaloneOfflineReason(restore, nil))
		})
	}
}

// A Job restore an earlier operator started holds the standalone stopped;
// finishing it online would talk Bolt to an instance with no pods.
func TestRestoreOnJobPath_FinishesAJobRestoreItAlreadyHolds(t *testing.T) {
	restore := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "neo4j-1.backup"})
	require.False(t, restoreRunsOffline(restore, false, nil), "on its own this restore would run online")

	held := minimalStandaloneForRestore("sa", "ns")
	held.Annotations = map[string]string{RestoreInProgressAnnotation: "r"}
	assert.True(t, statusRestoreReconciler(t, held).restoreOnJobPath(context.Background(), restore, false))

	other := minimalStandaloneForRestore("sa", "ns")
	other.Annotations = map[string]string{RestoreInProgressAnnotation: "another-restore"}
	assert.False(t, statusRestoreReconciler(t, other).restoreOnJobPath(context.Background(), restore, false))
	assert.False(t, statusRestoreReconciler(t, held).restoreOnJobPath(context.Background(), restore, true), "a cluster never takes the Job")
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
	restore := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{
		BackupPath: "nightly", ArtifactFilename: "neo4j-2026-10-05T08-43-31.backup", ArtifactType: "DIFF",
	})
	restore.Spec.StopCluster = true
	restore.Status.ResolvedSource.Storage = &neo4jv1beta1.StorageLocation{
		Type: "s3", Bucket: "neo4j-backups", Path: "prod",
		Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws", CredentialsSecretRef: "creds"},
	}
	backup := &neo4jv1beta1.Neo4jBackup{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}}
	r := statusRestoreReconciler(t, sa, restore, backup)

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

// A scheduled PVC backup's latest artifact is usually a DIFF (backupType AUTO).
// Served over HTTP Neo4j refuses it, so the seed proxy merges its chain first:
// a standalone restores it online, with no Job.
func TestStartRestore_StandalonePVCDiffMergesOnline(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	restore := pinnedBackupRestore("pvc", neo4jv1beta1.ResolvedRestoreSource{
		BackupPath: "nightly/", ArtifactFilename: "neo4j-2026-10-06T07-40-04.backup", ArtifactType: "DIFF",
	})
	restore.Status.ResolvedSource.Storage.PVC = &neo4jv1beta1.PVCSpec{Name: "backups"}
	restore.Spec.Options = &neo4jv1beta1.RestoreOptionsSpec{ReplaceExisting: true}
	backup := &neo4jv1beta1.Neo4jBackup{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}}
	r := statusRestoreReconciler(t, sa, restore, backup)

	_, _ = r.startRestore(context.Background(), restore, standaloneAsCluster(sa))

	jobs := &batchv1.JobList{}
	require.NoError(t, r.List(context.Background(), jobs))
	assert.Empty(t, jobs.Items, "a PVC DIFF no longer needs the neo4j-admin Job")
	proxy := &appsv1.Deployment{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: pvcSeedProxyName("r"), Namespace: "ns"}, proxy))
	pod := proxy.Spec.Template.Spec
	require.Len(t, pod.InitContainers, 1, "the proxy merges the chain before serving it")
	assert.Equal(t, "neo4j:5.26-enterprise", pod.InitContainers[0].Image, "with the standalone's own Neo4j image")
	assert.Equal(t, []string{"nightly", "neo4j-2026-10-06T07-40-04.backup", "neo4j", "merge"}, pod.InitContainers[0].Command[4:])
}

// Without stopCluster the Job path refuses a running standalone; now that most
// standalone restores run online, the refusal has to say why this one doesn't.
func TestStartRestore_StandaloneOfflineRefusalSaysWhy(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sa-0", Namespace: "ns", Labels: map[string]string{"app": "sa"}}}
	restore := minimalRestore("r", "ns", "sa")
	restore.Spec.Source = neo4jv1beta1.RestoreSource{
		Type: "storage", BackupPath: "nightly/neo4j-2026-10-06T07-40-04.backup",
		Storage: &neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}},
	}
	restore.Spec.Options = &neo4jv1beta1.RestoreOptionsSpec{ReplaceExisting: true}
	r := statusRestoreReconciler(t, sa, pod, restore)

	_, _ = r.startRestore(context.Background(), restore, standaloneAsCluster(sa))

	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
	assert.Equal(t, StatusFailed, got.Status.Phase)
	assert.Contains(t, got.Status.Message, `it restores offline: source.type "storage" gives a path`)
	assert.Contains(t, got.Status.Message, "spec.stopCluster=true")
}

// A cluster restoring a PVC DIFF used to create the database and leave it
// offline ("not part of a valid backup chain"). The seed proxy now merges the
// chain first, with the cluster's Neo4j image.
func TestClusterRestore_MergesAPVCDiffBeforeSeeding(t *testing.T) {
	cluster := minimalClusterForRestore("c", "ns")
	restore := pinnedBackupRestore("pvc", neo4jv1beta1.ResolvedRestoreSource{
		BackupPath: "nightly/", ArtifactFilename: "neo4j-2026-10-06T07-40-04.backup", ArtifactType: "DIFF",
	})
	restore.Spec.InstanceRef = "c"
	restore.Status.ResolvedSource.Storage.PVC = &neo4jv1beta1.PVCSpec{Name: "backups"}
	r := statusRestoreReconciler(t, cluster, restore)

	_, err := r.startClusterCypherRestore(context.Background(), restore, cluster)
	require.NoError(t, err)

	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
	assert.Equal(t, StatusPending, got.Status.Phase, "waiting for the proxy to merge and come up")
	proxy := &appsv1.Deployment{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: pvcSeedProxyName("r"), Namespace: "ns"}, proxy))
	require.Len(t, proxy.Spec.Template.Spec.InitContainers, 1)
	assert.Contains(t, proxy.Spec.Template.Spec.InitContainers[0].Env, corev1.EnvVar{Name: "SEED_MERGE_COMMAND", Value: "neo4j-admin database aggregate-backup"})

	// A known full backup is served as it always was.
	full := pinnedBackupRestore("pvc", neo4jv1beta1.ResolvedRestoreSource{
		BackupPath: "nightly/", ArtifactFilename: "neo4j-2026-10-06T07-40-04.backup", ArtifactType: "FULL",
	})
	full.Name, full.Spec.InstanceRef = "f", "c"
	full.Status.ResolvedSource.Storage.PVC = &neo4jv1beta1.PVCSpec{Name: "backups"}
	r = statusRestoreReconciler(t, cluster, full)
	_, err = r.startClusterCypherRestore(context.Background(), full, cluster)
	require.NoError(t, err)
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: pvcSeedProxyName("f"), Namespace: "ns"}, proxy))
	assert.Empty(t, proxy.Spec.Template.Spec.InitContainers)
}

// A merge that failed does not heal by waiting: the restore fails at once with
// neo4j-admin's reason.
func TestClusterRestore_FailsAtOnceWhenTheMergeFails(t *testing.T) {
	cluster := minimalClusterForRestore("c", "ns")
	restore := pinnedBackupRestore("pvc", neo4jv1beta1.ResolvedRestoreSource{
		BackupPath: "nightly/", ArtifactFilename: "neo4j-2026-10-06T07-40-04.backup", ArtifactType: "DIFF",
	})
	restore.Spec.InstanceRef = "c"
	restore.Status.ResolvedSource.Storage.PVC = &neo4jv1beta1.PVCSpec{Name: "backups"}
	failed := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "proxy-0", Namespace: "ns", Labels: map[string]string{
			"app.kubernetes.io/name": "backup-seed-proxy", "app.kubernetes.io/instance": "r"}},
		Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
			Name: seedMergeContainerName,
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Message: "could not merge the backup chain ending at nightly/x: no space left"}},
		}}},
	}
	r := statusRestoreReconciler(t, cluster, restore, failed)

	_, _ = r.startClusterCypherRestore(context.Background(), restore, cluster) // creates the proxy
	_, err := r.startClusterCypherRestore(context.Background(), restore, cluster)
	require.Error(t, err)
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
	assert.Equal(t, StatusFailed, got.Status.Phase)
	assert.Contains(t, got.Status.Message, "could not merge the backup chain: could not merge the backup chain ending at nightly/x: no space left")
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

// An all-databases restore ends in its aggregate step, Completed or Failed;
// both carry status.completionTime, as the single-database paths do.
func TestFinishAllDatabasesRestore_StampsCompletionTime(t *testing.T) {
	sa := minimalStandaloneForRestore("sa", "ns")
	for _, tc := range []struct {
		name      string
		dbPhases  []string
		wantPhase string
	}{
		{"all restored", []string{StatusCompleted, StatusCompleted}, StatusCompleted},
		{"one failed", []string{StatusCompleted, StatusFailed}, StatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restore := minimalRestore("r", "ns", "sa")
			for i, p := range tc.dbPhases {
				restore.Status.DatabaseResults = append(restore.Status.DatabaseResults,
					neo4jv1beta1.DatabaseRestoreResult{Database: fmt.Sprintf("db%d", i), Phase: p})
			}
			r := statusRestoreReconciler(t, sa, restore)
			_, err := r.finishAllDatabasesRestore(context.Background(), restore, standaloneAsCluster(sa))
			require.NoError(t, err)
			got := &neo4jv1beta1.Neo4jRestore{}
			require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
			assert.Equal(t, tc.wantPhase, got.Status.Phase)
			assert.NotNil(t, got.Status.CompletionTime)
		})
	}
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
