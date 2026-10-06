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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// shardedSeedReconciler is a sharded-database reconciler on a fake client with
// status writes and a recorder.
func shardedSeedReconciler(t *testing.T, objs ...runtime.Object) *Neo4jShardedDatabaseReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jShardedDatabase{}).Build()
	return &Neo4jShardedDatabaseReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(16)}
}

func seedTestShardedDB() *neo4jv1beta1.Neo4jShardedDatabase {
	return &neo4jv1beta1.Neo4jShardedDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "sdb2", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jShardedDatabaseSpec{
			ClusterRef: "sc", Name: "sdata2", SeedBackupRef: "sbackup", SeedSourceDatabase: "sdata",
		},
	}
}

func seedTestCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := minimalClusterForRestore("sc", "default")
	c.Spec.Image.Tag = "2026.08.1-enterprise"
	return c
}

func seedTestPVCBackup(shards ...neo4jv1beta1.ShardArtifact) *neo4jv1beta1.Neo4jBackup {
	done := metav1.Now()
	return &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "sbackup", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{InstanceRef: "sc", ShardedDatabase: "sdb",
			Storage: neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "shard-backups"}}},
		Status: neo4jv1beta1.Neo4jBackupStatus{History: []neo4jv1beta1.BackupRun{{
			RunID: "r", Status: "Succeeded", BackupsPath: "sbackup", CompletionTime: &done, ShardArtifacts: shards,
		}}},
	}
}

// Each shard is a database of its own, and neo4j-admin logs its type under
// that name: a sharded run and an all-databases run's catalogued family both
// record it.
func TestRecordArtifactTypes_Shards(t *testing.T) {
	log := "Start differential backup of database 'sdata-g000'.\n" +
		"Start differential backup of database 'sdata-p000'.\nFalling back to full backup of database 'sdata-p000'.\n"
	run := neo4jv1beta1.BackupRun{
		ShardArtifacts: []neo4jv1beta1.ShardArtifact{{ShardName: "sdata-g000"}, {ShardName: "sdata-p000"}, {ShardName: "sdata-p001"}},
		ShardedFamilies: []neo4jv1beta1.ShardedFamilyArtifacts{{Family: "sdata",
			ShardArtifacts: []neo4jv1beta1.ShardArtifact{{ShardName: "sdata-g000"}, {ShardName: "sdata-p000"}}}},
	}
	recordArtifactTypes(&run, log, "")
	assert.Equal(t, []string{"DIFF", "FULL", ""}, []string{run.ShardArtifacts[0].Type, run.ShardArtifacts[1].Type, run.ShardArtifacts[2].Type})
	assert.Equal(t, "DIFF", run.ShardedFamilies[0].ShardArtifacts[0].Type)
	assert.Equal(t, "FULL", run.ShardedFamilies[0].ShardArtifacts[1].Type)

	assert.Equal(t, []string{"sdata-g000", "sdata-g000"}, runDifferentialShards(&run), "a sharded run's shards and every catalogued family's")
}

// Over HTTP, Neo4j seeds a sharded database only from a full backup of every
// shard ("does not point to a full backup"), and merging a shard's chain does
// not help: live on 2026.08.1 and 2026.09, neo4j-admin's aggregate wrote the
// merged graph shard without its sharding header, and Neo4j refused it
// ("points to a backup without sharding information"). So a run with a
// differential shard is refused before the PVC is exposed.
func TestResolvePVCShardedSeed_RefusesDifferentialShards(t *testing.T) {
	r := shardedSeedReconciler(t, seedTestCluster(), seedTestPVCBackup(
		neo4jv1beta1.ShardArtifact{ShardName: "sdata-g000", Filename: "sdata-g000-2026-10-06T13-48-05.backup", Type: "DIFF"},
		neo4jv1beta1.ShardArtifact{ShardName: "sdata-p000", Filename: "sdata-p000-2026-10-06T13-48-05.backup", Type: "FULL"},
	))

	_, err := r.resolveShardedSeed(context.Background(), seedTestShardedDB())
	require.Error(t, err)
	for _, want := range []string{`Neo4jBackup "sbackup"`, "differential backups of sdata-g000.", "options.backupType: FULL", "cloud storage"} {
		assert.Contains(t, err.Error(), want)
	}
	assert.NotContains(t, err.Error(), "sdata-p000")
	getErr := r.Get(context.Background(), types.NamespacedName{Name: pvcSeedProxyName("sdb2"), Namespace: "default"}, &appsv1.Deployment{})
	assert.True(t, apierrors.IsNotFound(getErr), "the PVC is not exposed for a seed that cannot run")
}

// Full shards, and shards whose type the log did not record (runs from before
// types were recorded), are served as they are — Neo4j still checks them.
func TestResolvePVCShardedSeed_ServesFullAndUntypedShards(t *testing.T) {
	r := shardedSeedReconciler(t, seedTestCluster(), seedTestPVCBackup(
		neo4jv1beta1.ShardArtifact{ShardName: "sdata-g000", Filename: "sdata-g000-2026-10-06T13-46-10.backup", Type: "FULL"},
		neo4jv1beta1.ShardArtifact{ShardName: "sdata-p000", Filename: "sdata-p000-2026-10-06T13-46-09.backup"},
	))

	resolved, err := r.resolveShardedSeed(context.Background(), seedTestShardedDB())
	require.NoError(t, err)
	assert.Len(t, resolved.PerShardURIs, 2)
	assert.Contains(t, resolved.PerShardURIs["sdata2-g000"], "/sbackup/sdata-g000-2026-10-06T13-46-10.backup")
	proxy := &appsv1.Deployment{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: pvcSeedProxyName("sdb2"), Namespace: "default"}, proxy))
	assert.Empty(t, proxy.Spec.Template.Spec.InitContainers, "nothing is merged")
}

// The refusal comes at restore time, when a DR plan needs the backup; the
// warning comes at backup time, while a full backup can still be taken.
func TestWarnShardedPVCDifferential(t *testing.T) {
	diffRun := neo4jv1beta1.BackupRun{RunID: "sbackup-backup-cron-1", Status: "Succeeded",
		ShardArtifacts: []neo4jv1beta1.ShardArtifact{{ShardName: "sdata-g000", Type: "DIFF"}, {ShardName: "sdata-p000", Type: "FULL"}}}
	familyRun := neo4jv1beta1.BackupRun{RunID: "all-1", Status: "Succeeded",
		ShardedFamilies: []neo4jv1beta1.ShardedFamilyArtifacts{{Family: "sdata",
			ShardArtifacts: []neo4jv1beta1.ShardArtifact{{ShardName: "sdata-g000", Type: "DIFF"}}}}}
	fullRun := neo4jv1beta1.BackupRun{RunID: "f", Status: "Succeeded",
		ShardArtifacts: []neo4jv1beta1.ShardArtifact{{ShardName: "sdata-g000", Type: "FULL"}}}
	failedRun := diffRun
	failedRun.Status = "Failed"
	pvc := seedTestPVCBackup()
	cloud := seedTestPVCBackup()
	cloud.Spec.Storage = neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b"}

	cases := []struct {
		name   string
		backup *neo4jv1beta1.Neo4jBackup
		run    neo4jv1beta1.BackupRun
		want   string
	}{
		{"a differential shard on a PVC", pvc, diffRun, "run sbackup-backup-cron-1 wrote differential backups of sdata-g000 to a PVC"},
		{"an all-databases run's family", pvc, familyRun, "differential backups of sdata-g000"},
		{"cloud storage seeds differentials", cloud, diffRun, ""},
		{"every shard full", pvc, fullRun, ""},
		{"a failed run", pvc, failedRun, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := record.NewFakeRecorder(4)
			r := &Neo4jBackupReconciler{Recorder: rec}
			r.warnShardedPVCDifferential(tc.backup, &tc.run)
			if tc.want == "" {
				assert.Empty(t, rec.Events)
				return
			}
			require.Len(t, rec.Events, 1)
			ev := <-rec.Events
			assert.True(t, strings.HasPrefix(ev, "Warning "+EventReasonBackupShardedDifferential), ev)
			assert.Contains(t, ev, tc.want)
			assert.Contains(t, ev, "options.backupType: FULL")
		})
	}
}

// A sharded seed from MinIO needs the endpoint on the server pods too. Live,
// with only the credentials projected, Neo4j's S3 client went to real AWS
// ("must be addressed using the specified endpoint").
func TestEnsureClusterSeedConfig(t *testing.T) {
	minio := &ResolvedShardedSeed{CredsSecretName: "minio-creds",
		Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws", CredentialsSecretRef: "minio-creds", EndpointURL: "http://minio:9000", ForcePathStyle: true}}

	t.Run("PVC seeds need nothing", func(t *testing.T) {
		r := shardedSeedReconciler(t, seedTestShardedDB(), seedTestCluster())
		_, wait := r.ensureClusterSeedConfig(context.Background(), seedTestShardedDB(), seedTestCluster(), &ResolvedShardedSeed{})
		assert.False(t, wait)
	})

	t.Run("without the opt-in: Failed, naming the credentials and the endpoint", func(t *testing.T) {
		sdb := seedTestShardedDB()
		r := shardedSeedReconciler(t, sdb, seedTestCluster())
		_, wait := r.ensureClusterSeedConfig(context.Background(), sdb, seedTestCluster(), minio)
		require.True(t, wait)
		got := &neo4jv1beta1.Neo4jShardedDatabase{}
		require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sdb2", Namespace: "default"}, got))
		assert.Equal(t, "Failed", got.Status.Phase)
		assert.Contains(t, got.Status.Message, `missing credentials Secret "minio-creds"`)
		assert.Contains(t, got.Status.Message, "AWS_ENDPOINT_URL_S3=http://minio:9000")
	})

	t.Run("with the opt-in: both projected in one update, then wait for the rollout", func(t *testing.T) {
		sdb := seedTestShardedDB()
		cluster := seedTestCluster()
		cluster.Annotations = map[string]string{AutoInheritSeedCredsAnnotation: "true"}
		r := shardedSeedReconciler(t, sdb, cluster)
		_, wait := r.ensureClusterSeedConfig(context.Background(), sdb, cluster, minio)
		require.True(t, wait)
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sc", Namespace: "default"}, got))
		assert.True(t, hasSecretEnvFrom(got.Spec.ExtraEnvFrom, "minio-creds"))
		assert.True(t, envHasSeedEndpoint(got.Spec.Env))

		// Projected, but the StatefulSet has not rolled: still waiting.
		_, wait = r.ensureClusterSeedConfig(context.Background(), sdb, got, minio)
		require.True(t, wait)
		status := &neo4jv1beta1.Neo4jShardedDatabase{}
		require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sdb2", Namespace: "default"}, status))
		assert.True(t, strings.HasPrefix(status.Status.Message, "Waiting for cluster \"sc\" pods to roll out"), status.Status.Message)
	})

	t.Run("rolled out: proceed", func(t *testing.T) {
		sdb := seedTestShardedDB()
		cluster := seedTestCluster()
		cluster.Spec.ExtraEnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "minio-creds"}}}}
		cluster.Spec.Env = []corev1.EnvVar{{Name: "AWS_ENDPOINT_URL_S3", Value: "http://minio:9000"}}
		three := int32(3)
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "sc-server", Namespace: "default", Generation: 4},
			Spec: appsv1.StatefulSetSpec{Replicas: &three, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "neo4j", EnvFrom: cluster.Spec.ExtraEnvFrom, Env: cluster.Spec.Env,
			}}}}},
			Status: appsv1.StatefulSetStatus{ObservedGeneration: 4, UpdatedReplicas: 3, ReadyReplicas: 3},
		}
		r := shardedSeedReconciler(t, sdb, cluster, sts)
		_, wait := r.ensureClusterSeedConfig(context.Background(), sdb, cluster, minio)
		assert.False(t, wait)
	})
}
