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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A quantity the user typed reaches resource.MustParse in several places that
// build PVCs. MustParse PANICS on a malformed value, and a panic in a reconciler
// takes the whole manager down, restart-looping every other CR with it. Some of
// these fields are guarded elsewhere (the inline validator rejects a bad
// storage.pvc.size; spec.options.tempStorage.size has a CRD pattern), but a CR
// stored before a pattern was tightened, or any path that skips the validator,
// would still land here. The controller must not depend on the guard: a bad
// value is a spec error to report, never a crash.

var malformedQuantities = []string{"fifty", "5 Gi", "5GiB", "-5Gi", "5Gi;", "1e", ".", "Gi"}

func TestEnsureBackupPVC_MalformedSizeIsAnErrorNotAPanic(t *testing.T) {
	for _, size := range malformedQuantities {
		t.Run(size, func(t *testing.T) {
			backup := &neo4jv1beta1.Neo4jBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
				Spec: neo4jv1beta1.Neo4jBackupSpec{
					InstanceRef:  "ec",
					AllDatabases: true,
					Storage: neo4jv1beta1.StorageLocation{
						Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backup-pvc", Size: size},
					},
				},
			}
			r := newShardedTestReconciler(t, backup)

			var err error
			require.NotPanics(t, func() { err = r.ensureBackupPVC(context.Background(), backup) })
			require.Error(t, err)
			assert.True(t, isInvalidQuantity(err), "must be classified as a spec error: %v", err)
			assert.Contains(t, err.Error(), "spec.storage.pvc.size")
			assert.Contains(t, err.Error(), size, "the message must quote the offending value")

			// And nothing was created from the bad value.
			pvc := &corev1.PersistentVolumeClaim{}
			getErr := r.Get(context.Background(), types.NamespacedName{Name: "backup-pvc", Namespace: "default"}, pvc)
			assert.Error(t, getErr, "no PVC may be created from a malformed size")
		})
	}
}

func TestEnsureBackupPVC_ZeroSizeIsRefused(t *testing.T) {
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:  "ec",
			AllDatabases: true,
			Storage:      neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backup-pvc", Size: "0"}},
		},
	}
	r := newShardedTestReconciler(t, backup)
	err := r.ensureBackupPVC(context.Background(), backup)
	require.Error(t, err)
	assert.True(t, isInvalidQuantity(err))
	assert.Contains(t, err.Error(), "greater than zero")
}

func TestEnsureTempStagingPVC_MalformedSizeIsAnErrorNotAPanic(t *testing.T) {
	for _, size := range malformedQuantities {
		t.Run(size, func(t *testing.T) {
			backup := &neo4jv1beta1.Neo4jBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
				Spec: neo4jv1beta1.Neo4jBackupSpec{
					InstanceRef:  "ec",
					AllDatabases: true,
					Options: &neo4jv1beta1.BackupOptions{
						TempStorage: &neo4jv1beta1.TempStorageSpec{Size: size},
					},
				},
			}
			r := newShardedTestReconciler(t, backup)

			var err error
			require.NotPanics(t, func() { err = r.ensureTempStagingPVC(context.Background(), backup) })
			require.Error(t, err)
			assert.True(t, isInvalidQuantity(err), "must be classified as a spec error: %v", err)
			assert.Contains(t, err.Error(), "spec.options.tempStorage.size")
		})
	}
}

func TestEnsureRestoreTempStagingPVC_MalformedSizeIsAnErrorNotAPanic(t *testing.T) {
	for _, size := range malformedQuantities {
		t.Run(size, func(t *testing.T) {
			restore := &neo4jv1beta1.Neo4jRestore{
				ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "default"},
				Spec: neo4jv1beta1.Neo4jRestoreSpec{
					InstanceRef: "ec",
					Options: &neo4jv1beta1.RestoreOptionsSpec{
						TempStorage: &neo4jv1beta1.TempStorageSpec{Size: size},
					},
				},
			}
			r := newResolvedSourceReconciler(t, restore)

			var err error
			require.NotPanics(t, func() { err = r.ensureRestoreTempStagingPVC(context.Background(), restore) })
			require.Error(t, err)
			assert.True(t, isInvalidQuantity(err), "must be classified as a spec error: %v", err)
			assert.Contains(t, err.Error(), "spec.options.tempStorage.size")
		})
	}
}

// A well-formed size still provisions the PVC, with the requested quantity.
func TestEnsureBackupPVC_ValidSizeStillCreatesThePVC(t *testing.T) {
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:  "ec",
			AllDatabases: true,
			Storage:      neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backup-pvc", Size: "50Gi"}},
		},
	}
	r := newShardedTestReconciler(t, backup)
	require.NoError(t, r.ensureBackupPVC(context.Background(), backup))

	pvc := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "backup-pvc", Namespace: "default"}, pvc))
	got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(t, "50Gi", got.String())
}

// End to end through the one-shot path: a malformed size must leave the CR in
// phase Invalid with a message naming the field — the same terminal-until-edited
// state the inline validator gives — not Failed with a retry loop, and not a crash.
func TestHandleOneTimeBackup_MalformedPVCSizeReportsInvalid(t *testing.T) {
	const ns = "default"
	cluster := minimalClusterForBackup("ec", ns)
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: ns},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:  "ec",
			AllDatabases: true,
			Storage: neo4jv1beta1.StorageLocation{
				Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backup-pvc", Size: "fifty"},
			},
		},
	}
	r := newShardedTestReconciler(t, cluster, backup)

	var err error
	require.NotPanics(t, func() {
		_, err = r.handleOneTimeBackup(context.Background(), backup, cluster)
	})
	require.NoError(t, err, "an invalid spec is not a reconcile error: it must not be retried with backoff")

	got := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "nightly", Namespace: ns}, got))
	assert.Equal(t, neo4jv1beta1.PhaseInvalid, got.Status.Phase)
	assert.Contains(t, got.Status.Message, "spec.storage.pvc.size")
	assert.Contains(t, got.Status.Message, "fifty")
}

// The cluster and standalone storage-expansion checks read spec.storage.size too.
// The inline validator rejects a bad size before they run, but they must not
// depend on that.
func TestStorageExpansionChecks_MalformedSizeIsAnErrorNotAPanic(t *testing.T) {
	ctx := context.Background()

	t.Run("cluster", func(t *testing.T) {
		cluster := minimalClusterForBackup("ec", "default")
		cluster.Spec.Storage = neo4jv1beta1.StorageSpec{Size: "fifty"}
		r := &Neo4jEnterpriseClusterReconciler{Client: newBackupTestReconciler(t, cluster).Client}

		var err error
		require.NotPanics(t, func() { _, err = r.checkStorageExpansionNeeded(ctx, cluster) })
		require.Error(t, err)
		assert.True(t, isInvalidQuantity(err))
		assert.Contains(t, err.Error(), "spec.storage.size")
	})

	t.Run("standalone", func(t *testing.T) {
		sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
			Spec:       neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{Storage: neo4jv1beta1.StorageSpec{Size: "fifty"}},
		}
		r := &Neo4jEnterpriseStandaloneReconciler{Client: newBackupTestReconciler(t, sa).Client}

		var err error
		require.NotPanics(t, func() { _, err = r.reconcileStandaloneStorageExpansion(ctx, sa) })
		require.Error(t, err)
		assert.True(t, isInvalidQuantity(err))
		assert.Contains(t, err.Error(), "spec.storage.size")
	})
}

// The standalone StatefulSet builder takes the same user-supplied size. A
// standalone's size used to be checked only for being non-empty, so a malformed
// one reached resource.MustParse there and panicked the manager.
func TestStandaloneStatefulSet_MalformedStorageSizeDoesNotPanic(t *testing.T) {
	for _, size := range []string{"fifty", "10 Gi", "5K"} {
		t.Run(size, func(t *testing.T) {
			sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
				ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
				Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
					AcceptLicenseAgreement: "eval",
					Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
					Storage:                neo4jv1beta1.StorageSpec{Size: size},
				},
			}
			r := &Neo4jEnterpriseStandaloneReconciler{}

			require.NotPanics(t, func() {
				sts := r.createStatefulSet(context.Background(), sa)
				require.NotNil(t, sts)
				require.NotEmpty(t, sts.Spec.VolumeClaimTemplates)
				q := sts.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
				assert.True(t, q.IsZero(), "a malformed size must produce a zero request, not a crash")
			})
		})
	}
}

func TestParseUserQuantity(t *testing.T) {
	for _, good := range []string{"1", "500Mi", "50Gi", "1Ti", "2G", "1.5Gi"} {
		q, err := parseUserQuantity("spec.x", good)
		require.NoError(t, err, good)
		assert.Equal(t, 1, q.Sign(), good)
	}
	for _, bad := range []string{"", "0", "0Gi", "-1Gi", "abc", "5 Gi"} {
		require.NotPanics(t, func() {
			_, err := parseUserQuantity("spec.x", bad)
			require.Error(t, err, "%q", bad)
			assert.True(t, isInvalidQuantity(err), "%q: %v", bad, err)
			assert.Contains(t, err.Error(), "spec.x")
		}, bad)
	}
}
