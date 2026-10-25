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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// State-transition events (DatabaseReady, BackupScheduled) used to be emitted
// on every reconcile pass of an object in that state: about 6 a minute each
// on the v1.21.0 journey. Every event spends the object's event budget —
// client-go's spam filter allows a burst of 25 per object, then one every
// five minutes — so the repeats crowd out the events that report real
// changes. These pin emission on the transition, derived from the persisted
// status, so an operator restart does not announce a steady state either.

func eventsWithReason(events []string, reason string) []string {
	var out []string
	for _, e := range events {
		if strings.Contains(e, " "+reason+" ") {
			out = append(out, e)
		}
	}
	return out
}

func TestReportDatabaseReady_AnnouncesTheTransitionNotEveryPass(t *testing.T) {
	scheme := noEffectTestScheme(t)
	db := &neo4jv1beta1.Neo4jDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default", Generation: 1},
		Spec:       neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "c", Name: "orders"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(db).
		WithStatusSubresource(&neo4jv1beta1.Neo4jDatabase{}).Build()
	rec := record.NewFakeRecorder(20)
	r := &Neo4jDatabaseReconciler{Client: c, Scheme: scheme, Recorder: rec}
	ctx := context.Background()

	for range 3 {
		r.reportDatabaseReady(ctx, db, databaseObservation{})
	}
	assert.Equal(t, []string{"Normal DatabaseReady Database is ready and available"}, drainEvents(rec),
		"announced when the database becomes ready, not on the passes after")

	got := &neo4jv1beta1.Neo4jDatabase{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(db), got))
	assert.Equal(t, "Ready", got.Status.Phase, "the status is still written")

	// Back from a failure is a transition again.
	r.updateDatabaseStatus(ctx, db, metav1.ConditionFalse, EventReasonConnectionFailed, "Failed to connect to Neo4j cluster")
	r.reportDatabaseReady(ctx, db, databaseObservation{})
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonDatabaseReady), 1)

	// A new reconciler — an operator restart — reads the persisted Ready
	// condition and stays quiet about a database that was already ready.
	restarted := &Neo4jDatabaseReconciler{Client: c, Scheme: scheme, Recorder: rec}
	restarted.reportDatabaseReady(ctx, db, databaseObservation{})
	assert.Empty(t, drainEvents(rec))
}

// End to end through Reconcile: a scheduled backup is reconciled again and
// again in its resting phase, and says BackupScheduled once — and again only
// when what it reports changes.
func TestBackupReconcile_AnnouncesTheScheduleOnce(t *testing.T) {
	const ns = "default"
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: ns},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image:    neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
		},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{Phase: neo4jv1beta1.PhaseReady},
	}
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: ns, Finalizers: []string{BackupFinalizer}},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:  "ec",
			AllDatabases: true,
			Schedule:     "0 2 * * *",
			Storage: neo4jv1beta1.StorageLocation{
				Type: "pvc",
				PVC:  &neo4jv1beta1.PVCSpec{Name: "nightly-backups", Size: "1Gi"},
			},
		},
	}
	r := newBackupTestReconcilerWithStatus(t, cluster, backup)
	rec := record.NewFakeRecorder(50)
	r.Recorder = rec
	ctx := context.Background()
	reconcile := func() {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(backup)})
		require.NoError(t, err)
	}

	for range 3 {
		reconcile()
	}
	scheduled := eventsWithReason(drainEvents(rec), EventReasonBackupScheduled)
	require.Len(t, scheduled, 1, "one announcement for an armed schedule, not one per pass")
	assert.Contains(t, scheduled[0], `CronJob nightly-backup-cron (schedule "0 2 * * *")`)

	got := &neo4jv1beta1.Neo4jBackup{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(backup), got))
	require.Equal(t, neo4jv1beta1.PhaseScheduled, got.Status.Phase)

	// Editing the schedule is a change worth announcing.
	require.NoError(t, patchObject(ctx, r.Client, got, func() { got.Spec.Schedule = "30 3 * * *" }))
	reconcile()
	reconcile()
	scheduled = eventsWithReason(drainEvents(rec), EventReasonBackupScheduled)
	require.Len(t, scheduled, 1)
	assert.Contains(t, scheduled[0], `(schedule "30 3 * * *")`)

	// An operator restart finds the schedule already announced in status.
	restarted := newBackupTestReconcilerWithStatus(t)
	restarted.Client = r.Client
	restarted.Recorder = rec
	_, err := restarted.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(backup)})
	require.NoError(t, err)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonBackupScheduled))
}
