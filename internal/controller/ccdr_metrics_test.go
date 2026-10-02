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

// Tests for the cross-cluster replication metrics:
//
//	neo4j_operator_replica_lag_transactions   (gauge, set while replicating,
//	                                           removed on delete / promotion)
//	neo4j_operator_replica_promotions_total   (counter, once per promotion at
//	                                           its terminal outcome)
//
// The Neo4j-facing half of both reconcilers cannot run without a database, so
// these drive the seams the metrics hang off (reportReplicating, markPromoted,
// handleDeletion, complete, fail) and the terminal fast paths of Reconcile.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

const (
	replicaLagMetric        = "neo4j_operator_replica_lag_transactions"
	replicaPromotionsMetric = "neo4j_operator_replica_promotions_total"
)

func replicaLagSeries(t *testing.T, ns, replica string) gatheredSeries {
	t.Helper()
	return gatherSeries(t, replicaLagMetric, map[string]string{"namespace": ns, "replica": replica})
}

func promotionCount(t *testing.T, ns, cluster, result string) float64 {
	t.Helper()
	return gatherSeries(t, replicaPromotionsMetric, map[string]string{
		"namespace": ns, "cluster_name": cluster, "result": result,
	}).value
}

func newReplicaForMetrics(name, ns, clusterRef, dbName, phase string) *neo4jv1beta1.Neo4jReplicaDatabase {
	r := newTestReplicaDatabase(name, ns)
	r.Spec.ClusterRef = clusterRef
	r.Spec.Name = dbName
	r.Finalizers = []string{Neo4jReplicaDatabaseFinalizer}
	r.Status.Phase = phase
	return r
}

func newReplicaReconciler(objs ...*neo4jv1beta1.Neo4jReplicaDatabase) *Neo4jReplicaDatabaseReconciler {
	b := fake.NewClientBuilder().WithScheme(newTestScheme())
	for _, o := range objs {
		b = b.WithObjects(o).WithStatusSubresource(o)
	}
	return &Neo4jReplicaDatabaseReconciler{Client: b.Build(), Recorder: record.NewFakeRecorder(50)}
}

// TestReportReplicating_PublishesLagGauge: the lag SHOW DATABASES reports for a
// live replica becomes the gauge, labelled by the downstream cluster, the CR
// and the database (which may be named differently from the CR).
func TestReportReplicating_PublishesLagGauge(t *testing.T) {
	replica := newReplicaForMetrics("lag-set-cr", "lag-set-ns", "downstream-a", "orders", "")
	r := newReplicaReconciler(replica)

	r.reportReplicating(context.Background(), replica, "orders",
		&neo4jclient.DatabaseInfo{Name: "orders", Type: neo4jclient.DatabaseTypeReplica, ReplicationLag: 42})

	s := gatherSeries(t, replicaLagMetric, map[string]string{
		"namespace": "lag-set-ns", "cluster_name": "downstream-a", "replica": "lag-set-cr", "database": "orders",
	})
	require.True(t, s.found, "the lag series must exist once the replica is observed replicating")
	assert.Equal(t, 42.0, s.value)

	// The next observation overwrites it.
	r.reportReplicating(context.Background(), replica, "orders",
		&neo4jclient.DatabaseInfo{Name: "orders", Type: neo4jclient.DatabaseTypeReplica, ReplicationLag: 3})
	assert.Equal(t, 3.0, replicaLagSeries(t, "lag-set-ns", "lag-set-cr").value)
}

// TestReplicaLagGauge_RemovedWhenReplicationEnds: every way a replica stops
// replicating removes the series, so a dashboard never shows the last lag
// frozen.
func TestReplicaLagGauge_RemovedWhenReplicationEnds(t *testing.T) {
	seed := func(ns, name string) *neo4jv1beta1.Neo4jReplicaDatabase {
		replica := newReplicaForMetrics(name, ns, "downstream-b", "inventory", "")
		r := newReplicaReconciler(replica)
		r.reportReplicating(context.Background(), replica, "inventory",
			&neo4jclient.DatabaseInfo{Name: "inventory", Type: neo4jclient.DatabaseTypeReplica, ReplicationLag: 11})
		require.True(t, replicaLagSeries(t, ns, name).found, "precondition: series published")
		return replica
	}

	t.Run("promotion observed out of band", func(t *testing.T) {
		replica := seed("lag-oob-ns", "lag-oob-cr")
		r := newReplicaReconciler(replica)
		_, err := r.markPromoted(context.Background(), replica,
			&neo4jclient.DatabaseInfo{Name: "inventory", Type: "standard"}, "out-of-band")
		require.NoError(t, err)
		assert.False(t, replicaLagSeries(t, "lag-oob-ns", "lag-oob-cr").found)
	})

	t.Run("already Promoted when the reconcile starts", func(t *testing.T) {
		// The promotion controller drives the replica CR to Promoted itself,
		// so the replica controller never passes through markPromoted: it
		// sees the terminal phase on its fast path.
		replica := seed("lag-fast-ns", "lag-fast-cr")
		replica.Status.Phase = neo4jv1beta1.ReplicaPhasePromoted
		r := newReplicaReconciler(replica)
		res, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: "lag-fast-ns", Name: "lag-fast-cr"}})
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)
		assert.False(t, replicaLagSeries(t, "lag-fast-ns", "lag-fast-cr").found)
	})

	t.Run("CR deleted", func(t *testing.T) {
		replica := seed("lag-del-ns", "lag-del-cr")
		now := metav1.Now()
		replica.DeletionTimestamp = &now
		replica.Spec.DeletionPolicy = "Retain" // release the finalizer without a Neo4j round trip
		r := newReplicaReconciler(replica)
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: "lag-del-ns", Name: "lag-del-cr"}})
		require.NoError(t, err)
		assert.False(t, replicaLagSeries(t, "lag-del-ns", "lag-del-cr").found)
	})

	t.Run("CR already gone", func(t *testing.T) {
		// The finalizer was stripped by hand, or the object vanished between
		// watch events: the reconcile finds nothing and must still clean up.
		replica := seed("lag-gone-ns", "lag-gone-cr")
		r := newReplicaReconciler() // the CR does not exist in this client
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: "lag-gone-ns", Name: replica.Name}})
		require.NoError(t, err)
		assert.False(t, replicaLagSeries(t, "lag-gone-ns", "lag-gone-cr").found)
	})
}

func newPromotionForMetrics(name, ns, replicaRef, phase string) *neo4jv1beta1.Neo4jReplicaPromotion {
	return &neo4jv1beta1.Neo4jReplicaPromotion{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       neo4jv1beta1.Neo4jReplicaPromotionSpec{ReplicaRef: replicaRef},
		Status:     neo4jv1beta1.Neo4jReplicaPromotionStatus{Phase: phase},
	}
}

func newPromotionReconciler(promo *neo4jv1beta1.Neo4jReplicaPromotion, replica *neo4jv1beta1.Neo4jReplicaDatabase) *Neo4jReplicaPromotionReconciler {
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(promo, replica).WithStatusSubresource(promo, replica).Build()
	return &Neo4jReplicaPromotionReconciler{Client: fc, Recorder: record.NewFakeRecorder(50)}
}

// TestPromotionComplete_CountsSuccessOnce: the same promotion reaching its
// terminal outcome repeatedly — a stale-cache reconcile, a retry after a lost
// response, the replica-watch re-enqueue that follows its own status write —
// must count once.
func TestPromotionComplete_CountsSuccessOnce(t *testing.T) {
	const ns, cluster = "promo-ok-ns", "promo-ok-cluster"
	replica := newReplicaForMetrics("promo-ok-replica", ns, cluster, "ledger", neo4jv1beta1.ReplicaPhaseReplicating)
	promo := newPromotionForMetrics("promo-ok", ns, replica.Name, neo4jv1beta1.PromotionPhasePromoting)
	r := newPromotionReconciler(promo, replica)
	ctx := context.Background()

	// A lag series exists while the replica replicates; promotion removes it.
	r2 := newReplicaReconciler(replica)
	r2.reportReplicating(ctx, replica, "ledger",
		&neo4jclient.DatabaseInfo{Name: "ledger", Type: neo4jclient.DatabaseTypeReplica, ReplicationLag: 5})
	require.True(t, replicaLagSeries(t, ns, replica.Name).found)

	info := &neo4jclient.DatabaseInfo{Name: "ledger", Type: "standard"}
	r.complete(ctx, promo, replica, info, "promoted")
	assert.Equal(t, 1.0, promotionCount(t, ns, cluster, "success"))
	assert.False(t, replicaLagSeries(t, ns, replica.Name).found,
		"promotion must remove the replica's lag series without waiting for the replica controller")

	// Replays against the stale in-memory object (still Promoting).
	r.complete(ctx, promo, replica, info, "promoted")
	r.complete(ctx, promo, replica, &neo4jclient.DatabaseInfo{Name: "ledger", Type: "standard"}, "already promoted")
	assert.Equal(t, 1.0, promotionCount(t, ns, cluster, "success"), "terminal outcome counted once")
	assert.Zero(t, promotionCount(t, ns, cluster, "failure"))

	// And the full Reconcile of an already-terminal promotion is inert.
	for i := 0; i < 2; i++ {
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: promo.Name}})
		require.NoError(t, err)
		assert.Equal(t, ctrl.Result{}, res)
	}
	assert.Equal(t, 1.0, promotionCount(t, ns, cluster, "success"))
}

func TestPromotionFail_CountsFailureOnce(t *testing.T) {
	const ns, cluster = "promo-fail-ns", "promo-fail-cluster"
	replica := newReplicaForMetrics("promo-fail-replica", ns, cluster, "ghost", neo4jv1beta1.ReplicaPhaseReplicating)
	promo := newPromotionForMetrics("promo-fail", ns, replica.Name, neo4jv1beta1.PromotionPhasePending)
	r := newPromotionReconciler(promo, replica)
	ctx := context.Background()

	r.fail(ctx, promo, cluster, "database \"ghost\" does not exist")
	r.fail(ctx, promo, cluster, "database \"ghost\" does not exist")
	assert.Equal(t, 1.0, promotionCount(t, ns, cluster, "failure"))
	assert.Zero(t, promotionCount(t, ns, cluster, "success"))

	latest := &neo4jv1beta1.Neo4jReplicaPromotion{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: ns, Name: promo.Name}, latest))
	assert.Equal(t, neo4jv1beta1.PromotionPhaseFailed, latest.Status.Phase)
}

// TestPromotion_OneTerminalOutcomePerPromotion: once a promotion has reached
// one terminal phase, a stale reconcile that reaches the other must neither
// rewrite the status nor count a second outcome.
func TestPromotion_OneTerminalOutcomePerPromotion(t *testing.T) {
	const ns, cluster = "promo-both-ns", "promo-both-cluster"
	replica := newReplicaForMetrics("promo-both-replica", ns, cluster, "mixed", neo4jv1beta1.ReplicaPhaseReplicating)
	promo := newPromotionForMetrics("promo-both", ns, replica.Name, neo4jv1beta1.PromotionPhasePromoting)
	r := newPromotionReconciler(promo, replica)
	ctx := context.Background()

	r.complete(ctx, promo, replica, &neo4jclient.DatabaseInfo{Name: "mixed", Type: "standard"}, "promoted")
	r.fail(ctx, promo, cluster, "late stale failure")

	assert.Equal(t, 1.0, promotionCount(t, ns, cluster, "success"))
	assert.Zero(t, promotionCount(t, ns, cluster, "failure"))
	latest := &neo4jv1beta1.Neo4jReplicaPromotion{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: ns, Name: promo.Name}, latest))
	assert.Equal(t, neo4jv1beta1.PromotionPhaseCompleted, latest.Status.Phase)
}
