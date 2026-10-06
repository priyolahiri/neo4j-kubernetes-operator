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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func pitAt(hour int) *metav1.Time {
	t := metav1.NewTime(time.Date(2026, 10, 6, hour, 0, 0, 0, time.UTC))
	return &t
}

func runAt(name string, hour int, status string) neo4jv1beta1.BackupRun {
	return neo4jv1beta1.BackupRun{RunID: name, Status: status, StartTime: *pitAt(hour), ArtifactFilename: name + ".backup"}
}

// Neo4j seeds to a point in time from "the differential backup containing the
// data up to that date": the earliest run that started at or after it holds
// every transaction before it.
func TestRestoreRunIndex(t *testing.T) {
	// Not in time order on purpose: the choice is by start time.
	history := []neo4jv1beta1.BackupRun{
		runAt("r12", 12, "Succeeded"),
		runAt("r11-failed", 11, "Failed"),
		runAt("r08", 8, "Succeeded"),
		runAt("r10", 10, "Succeeded"),
		{RunID: "unplaced", Status: "Succeeded"},
	}
	name := func(i int) string { return history[i].RunID }

	assert.Equal(t, "r12", name(restoreRunIndex(history, nil)), "no point in time: the most recent run")
	assert.Equal(t, "r10", name(restoreRunIndex(history, pitAt(9))), "the earliest run that started after it")
	assert.Equal(t, "r10", name(restoreRunIndex(history, pitAt(10))), "a run that started exactly then holds it")
	assert.Equal(t, "r12", name(restoreRunIndex(history, pitAt(11))), "a failed run is not a candidate")
	assert.Equal(t, "r08", name(restoreRunIndex(history, pitAt(7))))
	assert.Equal(t, "r12", name(restoreRunIndex(history, pitAt(13))), "none started that late: the most recent")
	assert.Equal(t, -1, restoreRunIndex([]neo4jv1beta1.BackupRun{runAt("f", 9, "Failed")}, nil))
	unordered := []neo4jv1beta1.BackupRun{runAt("r08", 8, "Succeeded"), runAt("r12", 12, "Succeeded")}
	assert.Equal(t, 1, restoreRunIndex(unordered, nil), "the most recent by start time, whatever the order")

	snap := &neo4jv1beta1.ResolvedRestoreSource{BackupStartedAt: pitAt(12)}
	restore := minimalRestore("r", "ns", "c")
	restore.Spec.Source.PointInTime = pitAt(11)
	assert.True(t, pointInTimeCovered(restore, snap))
	restore.Spec.Source.PointInTime = pitAt(13)
	assert.False(t, pointInTimeCovered(restore, snap), "the newest run started before it: transactions after it may be in no backup")
}

func pitRestore(storageType string) *neo4jv1beta1.Neo4jRestore {
	r := pinnedBackupRestore(storageType, neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "neo4j-2026-10-06T12-00-00.backup", ArtifactType: "DIFF", BackupStartedAt: pitAt(12)})
	r.Spec.Database = "asof"
	r.Spec.Source.PointInTime = pitAt(11)
	return r
}

func TestPointInTimeOnlineBlocker(t *testing.T) {
	cases := []struct {
		name    string
		restore func() *neo4jv1beta1.Neo4jRestore
		calver  bool
		exists  bool
		want    string // "" = online
	}{
		{"cloud, CalVer, a new database: online", func() *neo4jv1beta1.Neo4jRestore { return pitRestore("s3") }, true, false, ""},
		{"the database exists", func() *neo4jv1beta1.Neo4jRestore { return pitRestore("s3") }, true, true, "only when creating a database"},
		{"5.26", func() *neo4jv1beta1.Neo4jRestore { return pitRestore("s3") }, false, false, "Neo4j 5.26"},
		{"PVC: HTTP has no restore-until", func() *neo4jv1beta1.Neo4jRestore { return pitRestore("pvc") }, true, false, "PVC"},
		{"no run started after it", func() *neo4jv1beta1.Neo4jRestore {
			r := pitRestore("s3")
			r.Spec.Source.PointInTime = pitAt(13)
			return r
		}, true, false, "started at or after 2026-10-06T13:00:00Z"},
		{"no artifact recorded", func() *neo4jv1beta1.Neo4jRestore {
			r := pitRestore("s3")
			r.Status.ResolvedSource.ArtifactFilename = ""
			return r
		}, true, false, "did not record the artifact"},
		{"all databases", func() *neo4jv1beta1.Neo4jRestore {
			r := pitRestore("s3")
			r.Spec.AllDatabases = true
			return r
		}, true, false, "all-databases"},
		{"source.type pitr", func() *neo4jv1beta1.Neo4jRestore {
			r := pitRestore("s3")
			r.Spec.Source.Type = "pitr"
			return r
		}, true, false, "transaction logs"},
		{"source.type storage", func() *neo4jv1beta1.Neo4jRestore {
			r := pitRestore("s3")
			r.Spec.Source.Type = "storage"
			return r
		}, true, false, "names a path"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.restore()
			why := pointInTimeOnlineBlocker(r, resolvedBackupSnapshot(r), tc.calver, tc.exists)
			if tc.want == "" {
				assert.Empty(t, why)
				return
			}
			assert.Contains(t, why, tc.want)
		})
	}
}

// The decision is made once and recorded: the online restore creates the
// database, after which a fresh existence check would say "offline".
func TestDecidePointInTimePath(t *testing.T) {
	absent := func(context.Context, restoreTarget, string) (bool, error) { return false, nil }
	present := func(context.Context, restoreTarget, string) (bool, error) { return true, nil }
	calverStandalone := func() *neo4jv1beta1.Neo4jEnterpriseStandalone {
		s := minimalStandaloneForRestore("sa", "ns")
		s.Spec.Image.Tag = "2026.08.1-enterprise"
		return s
	}
	get := func(t *testing.T, r *Neo4jRestoreReconciler) *neo4jv1beta1.Neo4jRestore {
		got := &neo4jv1beta1.Neo4jRestore{}
		require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
		return got
	}

	t.Run("online, recorded once", func(t *testing.T) {
		restore := pitRestore("s3")
		r := statusRestoreReconciler(t, restore, calverStandalone())
		_, done, err := r.decidePointInTimePathWith(context.Background(), restore, absent)
		require.NoError(t, err)
		require.False(t, done)
		assert.Equal(t, "online", get(t, r).Annotations[AnnotationPointInTimePath])
		assert.Empty(t, standaloneOfflineReason(restore, nil))

		// The database now exists (the restore created it); the decision stands.
		_, _, err = r.decidePointInTimePathWith(context.Background(), restore, present)
		require.NoError(t, err)
		assert.Equal(t, "online", get(t, r).Annotations[AnnotationPointInTimePath])
	})

	t.Run("a standalone over an existing database: offline, with the reason", func(t *testing.T) {
		restore := pitRestore("s3")
		r := statusRestoreReconciler(t, restore, calverStandalone())
		_, done, err := r.decidePointInTimePathWith(context.Background(), restore, present)
		require.NoError(t, err)
		require.False(t, done, "a standalone goes on to the Job")
		assert.Contains(t, standaloneOfflineReason(restore, nil), `database "asof" exists`)
	})

	t.Run("a cluster that cannot: Failed, never the Job", func(t *testing.T) {
		restore := pitRestore("s3")
		restore.Spec.InstanceRef = "c"
		cluster := minimalClusterForRestore("c", "ns")
		cluster.Spec.Image.Tag = "2026.08.1-enterprise"
		r := statusRestoreReconciler(t, restore, cluster)
		_, done, err := r.decidePointInTimePathWith(context.Background(), restore, present)
		require.NoError(t, err)
		require.True(t, done)
		got := get(t, r)
		assert.Equal(t, StatusFailed, got.Status.Phase)
		assert.Contains(t, got.Status.Message, `database "asof" exists`)
	})

	t.Run("a cluster on 5.26 is told why without asking it", func(t *testing.T) {
		restore := pitRestore("s3")
		restore.Spec.InstanceRef = "c"
		r := statusRestoreReconciler(t, restore, minimalClusterForRestore("c", "ns"))
		asked := false
		_, done, _ := r.decidePointInTimePathWith(context.Background(), restore, func(context.Context, restoreTarget, string) (bool, error) {
			asked = true
			return false, nil
		})
		require.True(t, done)
		assert.False(t, asked, "nothing else allows it: no Bolt round-trip")
		assert.Contains(t, get(t, r).Status.Message, "Neo4j 5.26")
	})

	t.Run("the target cannot be asked: Pending, nothing recorded", func(t *testing.T) {
		restore := pitRestore("s3")
		r := statusRestoreReconciler(t, restore, calverStandalone())
		r.RequeueAfter = 30 * time.Second
		res, done, err := r.decidePointInTimePathWith(context.Background(), restore, func(context.Context, restoreTarget, string) (bool, error) {
			return false, errors.New("connection refused")
		})
		require.NoError(t, err)
		require.True(t, done)
		assert.NotZero(t, res.RequeueAfter)
		got := get(t, r)
		assert.Equal(t, StatusPending, got.Status.Phase)
		assert.NotContains(t, got.Annotations, AnnotationPointInTimePath)
	})

	t.Run("a Job restore it already holds stays offline, without asking", func(t *testing.T) {
		restore := pitRestore("s3")
		held := calverStandalone()
		held.Annotations = map[string]string{RestoreInProgressAnnotation: "r"}
		r := statusRestoreReconciler(t, restore, held)
		_, done, err := r.decidePointInTimePathWith(context.Background(), restore, func(context.Context, restoreTarget, string) (bool, error) {
			t.Fatal("the instance is stopped; nothing to ask")
			return false, nil
		})
		require.NoError(t, err)
		require.False(t, done)
		assert.Contains(t, standaloneOfflineReason(restore, nil), "already holds the standalone stopped")
	})

	t.Run("no point in time: nothing to decide", func(t *testing.T) {
		restore := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "x.backup"})
		r := statusRestoreReconciler(t, restore, calverStandalone())
		_, done, err := r.decidePointInTimePathWith(context.Background(), restore, present)
		require.NoError(t, err)
		require.False(t, done)
		assert.NotContains(t, get(t, r).Annotations, AnnotationPointInTimePath)
	})
}

// A restore that reaches routing before any decision (a direct caller) is
// offline: the online path is only taken once it is known to work.
func TestStandaloneOfflineReason_PointInTimeUndecided(t *testing.T) {
	assert.Contains(t, standaloneOfflineReason(pitRestore("s3"), nil), "only once the operator has checked")
}

// With a point in time the restore pins the run that holds it, and when it
// started, rather than the most recent run.
func TestEnsureResolvedBackupSource_PointInTimePinsTheCoveringRun(t *testing.T) {
	backup := backupCRForRestore("simple-backup", "default", true)
	backup.Status.History = []neo4jv1beta1.BackupRun{
		runAt("neo4j-2026-10-06T12-00-00", 12, "Succeeded"),
		runAt("neo4j-2026-10-06T10-00-00", 10, "Succeeded"),
		runAt("neo4j-2026-10-06T08-00-00", 8, "Succeeded"),
	}
	r := restoreWithBackupRef("simple-restore", "default", "simple-backup")
	r.Spec.Source.PointInTime = pitAt(9)
	rec := newResolvedSourceReconciler(t, backup, r)

	_, done, err := rec.ensureResolvedBackupSource(context.Background(), r)
	require.NoError(t, err)
	require.False(t, done)
	snap := r.Status.ResolvedSource
	assert.Equal(t, "neo4j-2026-10-06T10-00-00.backup", snap.ArtifactFilename)
	require.NotNil(t, snap.BackupStartedAt)
	assert.True(t, snap.BackupStartedAt.Equal(pitAt(10)))
	assert.True(t, pointInTimeCovered(r, snap))
}
