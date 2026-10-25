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

// Round-8 walk: an all-databases restore with replaceExisting recreated
// `neo4j`, and the next pass's connection — whose check uses that database —
// failed with Neo.TransientError.General.DatabaseUnavailable. It was logged
// as an ERROR, the restore went back to Pending, and the next write that made
// it Running announced RestoreStarted a second time. A target that cannot
// serve the restore for a moment is a wait it resumes from (knowledge id 206).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

var errNeo4jRecreating = errors.New("Neo.TransientError.General.DatabaseUnavailable: Database 'neo4j' is unavailable")

// awayRestore is an all-databases restore of neo4j (which exists, so it is
// recreated) and inventory, and a target the test can make unavailable.
type awayRestore struct {
	t       *testing.T
	r       *Neo4jRestoreReconciler
	rec     *record.FakeRecorder
	sink    levelCaptureSink
	ctx     context.Context
	restore *neo4jv1beta1.Neo4jRestore
	away    int // client creations that fail before the target answers again
}

func newAwayRestore(t *testing.T) *awayRestore {
	t.Helper()
	server := &existingServer{
		seedingServer: &seedingServer{pollsToOnline: map[string]int{"neo4j": 3, "inventory": 3}, polls: map[string]int{}},
		exists:        map[string]bool{"neo4j": true},
	}
	snap := neo4jv1beta1.ResolvedRestoreSource{BackupRef: "all", BackupPath: "all/",
		Storage: &neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b", Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws"}}}
	for _, db := range []string{"neo4j", "inventory"} {
		snap.DatabaseArtifacts = append(snap.DatabaseArtifacts,
			neo4jv1beta1.DatabaseArtifact{Database: db, Filename: db + "-2026-10-25T11-00-00.backup", Type: "FULL"})
	}
	restore := &neo4jv1beta1.Neo4jRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "ns", UID: "r-uid", Generation: 1, Finalizers: []string{RestoreFinalizer}},
		Spec: neo4jv1beta1.Neo4jRestoreSpec{
			InstanceRef: "c", AllDatabases: true,
			Source:  neo4jv1beta1.RestoreSource{Type: "backup", BackupRef: "all"},
			Options: &neo4jv1beta1.RestoreOptionsSpec{ReplaceExisting: true},
		},
		Status: neo4jv1beta1.Neo4jRestoreStatus{ResolvedSource: &snap},
	}
	a := &awayRestore{t: t, restore: restore, sink: newLevelCaptureSink(), rec: record.NewFakeRecorder(100)}
	a.ctx = log.IntoContext(context.Background(), logr.New(a.sink))
	a.r = statusRestoreReconciler(t, minimalClusterForRestore("c", "ns"), restore)
	a.r.Recorder, a.r.ProgressRecorder = a.rec, a.rec
	a.r.newAllDatabasesClient = func(restoreTarget) (allDatabasesRestoreClient, error) {
		if a.away > 0 {
			a.away--
			return nil, errNeo4jRecreating
		}
		return server, nil
	}
	return a
}

func (a *awayRestore) reconcile() {
	a.t.Helper()
	_, err := a.r.Reconcile(a.ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(a.restore)})
	require.NoError(a.t, err)
}

func (a *awayRestore) status() neo4jv1beta1.Neo4jRestoreStatus {
	a.t.Helper()
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(a.t, a.r.Get(context.Background(), client.ObjectKeyFromObject(a.restore), got))
	return got.Status
}

func (a *awayRestore) untilDone() {
	a.t.Helper()
	for range 30 {
		if p := a.status().Phase; p == StatusCompleted || p == StatusFailed {
			return
		}
		a.reconcile()
	}
	a.t.Fatalf("the restore did not finish: %+v", a.status())
}

func TestAllDatabasesRestore_ATargetBrieflyUnavailableIsAWait(t *testing.T) {
	a := newAwayRestore(t)
	a.reconcile() // recreates neo4j
	require.Equal(t, StatusRunning, a.status().Phase)

	a.away = 3 // the recreate makes neo4j unavailable to the next connections
	for range 3 {
		a.reconcile()
		st := a.status()
		assert.Equal(t, StatusRunning, st.Phase, "a wait, not a step back to Pending")
		c := meta.FindStatusCondition(st.Conditions, ConditionTypeClusterNotReady)
		require.NotNil(t, c)
		assert.Equal(t, metav1.ConditionTrue, c.Status)
		assert.Equal(t, ConditionReasonClusterUnreachable, c.Reason)
	}

	a.untilDone()
	st := a.status()
	assert.Equal(t, StatusCompleted, st.Phase, "it resumed where it was: %+v", st.DatabaseResults)
	assert.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(st.Conditions, ConditionTypeClusterNotReady).Status)
	events := drainEvents(a.rec)
	assert.Len(t, eventsWithReason(events, EventReasonRestoreStarted), 1, "the start is announced once: %q", events)
	assert.Empty(t, eventsWithReason(events, EventReasonConnectionFailed), "a short wait is not reported")
	assert.Empty(t, a.sink.errors(), "a wait is not an ERROR")
}

// A wait longer than targetUnreachableGrace is reported, once, and the
// restore still waits and resumes.
func TestAllDatabasesRestore_ATargetAwayForLongIsReportedOnce(t *testing.T) {
	a := newAwayRestore(t)
	a.reconcile()
	a.away = 100
	a.reconcile()

	// The wait began longer ago than the grace.
	got := &neo4jv1beta1.Neo4jRestore{}
	require.NoError(t, a.r.Get(context.Background(), client.ObjectKeyFromObject(a.restore), got))
	c := findCondition(got.Status.Conditions, ConditionTypeClusterNotReady)
	require.NotNil(t, c)
	c.LastTransitionTime = metav1.NewTime(time.Now().Add(-targetUnreachableGrace - time.Minute))
	require.NoError(t, a.r.Status().Update(context.Background(), got))

	for range 3 {
		a.reconcile()
	}
	events := drainEvents(a.rec)
	failed := eventsWithReason(events, EventReasonConnectionFailed)
	require.Len(t, failed, 1, "reported once: %q", events)
	assert.Contains(t, failed[0], "has not answered for more than 5m0s")
	assert.Equal(t, StatusRunning, a.status().Phase, "still waiting")

	a.away = 0
	a.untilDone()
	assert.Equal(t, StatusCompleted, a.status().Phase)
	assert.Empty(t, eventsWithReason(drainEvents(a.rec), EventReasonRestoreStarted), "not announced again")
	assert.Empty(t, a.sink.errors())
}

// RestoreStarted is the attempt's start: a write that moves the restore back
// into Running after any wait put it in Pending mid-way announces nothing.
func TestMarkDatabaseResult_AnnouncesTheStartOnlyBeforeAnyDatabaseStarted(t *testing.T) {
	restore := minimalRestore("r", "ns", "c")
	restore.Status = neo4jv1beta1.Neo4jRestoreStatus{Phase: StatusPending, Message: "Waiting for the seed proxy",
		DatabaseResults: []neo4jv1beta1.DatabaseRestoreResult{
			{Database: "neo4j", Phase: StatusRunning}, {Database: "inventory", Phase: StatusPending},
		}}
	r := statusRestoreReconciler(t, restore)
	rec, ok := r.Recorder.(*record.FakeRecorder)
	require.True(t, ok)

	r.markDatabaseResult(context.Background(), restore, "neo4j", StatusCompleted, "online")
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRestoreStarted), "neo4j had started: not a new start")
}
