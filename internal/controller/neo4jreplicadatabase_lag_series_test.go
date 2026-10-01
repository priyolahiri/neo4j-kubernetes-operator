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

// Tests for when neo4j_operator_replica_lag_transactions must stop being
// published.
//
// A lag series is only worth having while the reconcile that wrote it can still
// vouch for it. A frozen last value is worse than no data: it reads as a replica
// that is exactly N transactions behind, steadily, which is the one reading a
// failover decision must never rest on when nothing is being measured.
//
// ccdr_metrics_test.go covers the ways replication ENDS (deleted, promoted).
// These cover the ways the reading goes UNTRUSTWORTHY while the replica CR is
// still there — the database vanished from the server, the downstream cluster is
// not Ready, or the lag cannot be read — and that a recovery publishes again.
//
// They drive the full Reconcile against a scripted Neo4j server through the
// reconciler's client seam, because the branches under test are exactly the ones
// that sit between "resolve the cluster" and "ask the server".

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// scriptedReplicaServer answers the Neo4j calls Reconcile makes, from whatever
// the test last set.
type scriptedReplicaServer struct {
	info    *neo4jclient.DatabaseInfo
	infoErr error
	created int
}

func (s *scriptedReplicaServer) GetDatabaseInfo(_ context.Context, _ string) (*neo4jclient.DatabaseInfo, error) {
	return s.info, s.infoErr
}

func (s *scriptedReplicaServer) CreateReplicaDatabaseFromBackup(context.Context, string, neo4jclient.ReplicaBackupSource) error {
	s.created++
	return nil
}

func (s *scriptedReplicaServer) CreateReplicaDatabaseFromNetwork(context.Context, string, neo4jclient.ReplicaNetworkSource) error {
	s.created++
	return nil
}

func (s *scriptedReplicaServer) Close() error { return nil }

// replicating makes the server report a live replica with the given lag.
func (s *scriptedReplicaServer) replicating(db string, lag int64) {
	s.info = &neo4jclient.DatabaseInfo{Name: db, Type: neo4jclient.DatabaseTypeReplica, ReplicationLag: lag}
	s.infoErr = nil
}

const lagTestDB = "ledger"

// lagHarness is one replica, its downstream cluster, and the scripted server.
type lagHarness struct {
	t       *testing.T
	ns      string
	name    string
	r       *Neo4jReplicaDatabaseReconciler
	c       client.Client
	server  *scriptedReplicaServer
	connErr error
}

func newLagHarness(t *testing.T, ns string) *lagHarness {
	t.Helper()
	const clusterName = "dr-cluster"

	replica := newReplicaForMetrics("lag-replica", ns, clusterName, lagTestDB, "")
	// A literal address list needs no upstream cluster to resolve if the
	// database has to be re-created.
	replica.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
		Mode:      neo4jv1beta1.ReplicaSourceModeNetwork,
		Addresses: []string{"upstream.example.com:7688"},
	}

	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.0-enterprise"},
		},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			Conditions: []metav1.Condition{{
				Type: ConditionTypeReady, Status: metav1.ConditionTrue, Reason: "ClusterReady",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}

	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica, cluster).WithStatusSubresource(replica, cluster).Build()

	h := &lagHarness{t: t, ns: ns, name: replica.Name, c: fc, server: &scriptedReplicaServer{}}
	h.r = &Neo4jReplicaDatabaseReconciler{
		Client:   fc,
		Recorder: record.NewFakeRecorder(500),
		newNeo4jClient: func(ResolvedTarget, client.Client) (replicaNeo4jClient, error) {
			if h.connErr != nil {
				return nil, h.connErr
			}
			return h.server, nil
		},
	}
	return h
}

func (h *lagHarness) reconcile() error {
	h.t.Helper()
	_, err := h.r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: h.ns, Name: h.name}})
	return err
}

func (h *lagHarness) lag() gatheredSeries {
	h.t.Helper()
	return replicaLagSeries(h.t, h.ns, h.name)
}

func (h *lagHarness) cluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	h.t.Helper()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(h.t, h.c.Get(context.Background(),
		types.NamespacedName{Namespace: h.ns, Name: "dr-cluster"}, cluster))
	return cluster
}

func (h *lagHarness) setClusterReady(ready bool) {
	h.t.Helper()
	cluster := h.cluster()
	status := metav1.ConditionTrue
	if !ready {
		status = metav1.ConditionFalse
	}
	cluster.Status.Conditions = []metav1.Condition{{
		Type: ConditionTypeReady, Status: status, Reason: "Test", LastTransitionTime: metav1.Now(),
	}}
	require.NoError(h.t, h.c.Status().Update(context.Background(), cluster))
}

// publishesFirst reconciles a healthy replica once and checks the series is
// there with the server's lag, so each case starts from "a number is on the
// dashboard".
func (h *lagHarness) publishesFirst(lag int64) {
	h.t.Helper()
	h.server.replicating(lagTestDB, lag)
	require.NoError(h.t, h.reconcile())
	s := h.lag()
	require.True(h.t, s.found, "precondition: a healthy reconcile publishes the lag")
	require.Equal(h.t, float64(lag), s.value)
}

// recovers brings the replica back to health with a new lag and checks that the
// gauge is set again to that new value, not left absent and not the old one.
func (h *lagHarness) recovers(lag int64) {
	h.t.Helper()
	h.server.replicating(lagTestDB, lag)
	require.NoError(h.t, h.reconcile())
	s := h.lag()
	require.True(h.t, s.found, "recovery must publish the lag again")
	assert.Equal(h.t, float64(lag), s.value, "recovery must publish the NEW reading")
}

// TestReplicaLagSeries_PublishedByReconcile is the baseline the cases below
// depart from: a healthy reconcile publishes the server's lag and refreshes it.
func TestReplicaLagSeries_PublishedByReconcile(t *testing.T) {
	h := newLagHarness(t, "lagser-base")

	h.publishesFirst(42)
	h.server.replicating(lagTestDB, 7)
	require.NoError(t, h.reconcile())
	assert.Equal(t, 7.0, h.lag().value)
}

// TestReplicaLagSeries_RemovedWhenDatabaseVanishes: the replica CR is still
// there but the server no longer has the database (dropped out of band). The
// last reading describes a database that does not exist.
func TestReplicaLagSeries_RemovedWhenDatabaseVanishes(t *testing.T) {
	h := newLagHarness(t, "lagser-vanish")
	h.publishesFirst(12)

	h.server.info = nil
	require.NoError(t, h.reconcile())
	assert.False(t, h.lag().found, "no lag may be shown for a database the server no longer has")
	assert.Equal(t, 1, h.server.created,
		"the vanished database is re-created from its source, as before; only the stale reading goes")

	// Re-created and replicating again.
	h.recovers(3)
}

// TestReplicaLagSeries_RemovedWhileDownstreamClusterNotUsable: the cluster that
// hosts the replica cannot answer, so the lag cannot be read.
func TestReplicaLagSeries_RemovedWhileDownstreamClusterNotUsable(t *testing.T) {
	t.Run("cluster not Ready", func(t *testing.T) {
		h := newLagHarness(t, "lagser-notready")
		h.publishesFirst(20)

		h.setClusterReady(false)
		require.NoError(t, h.reconcile())
		assert.False(t, h.lag().found, "a not-Ready downstream cluster cannot vouch for the lag")

		// Still not Ready on the next pass: stays absent, does not reappear.
		require.NoError(t, h.reconcile())
		assert.False(t, h.lag().found)

		h.setClusterReady(true)
		h.recovers(4)
	})

	t.Run("cluster gone", func(t *testing.T) {
		h := newLagHarness(t, "lagser-gone")
		h.publishesFirst(20)

		saved := h.cluster()
		require.NoError(t, h.c.Delete(context.Background(), saved))
		require.NoError(t, h.reconcile())
		assert.False(t, h.lag().found)

		restored := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: saved.Name, Namespace: saved.Namespace},
			Spec:       saved.Spec,
		}
		require.NoError(t, h.c.Create(context.Background(), restored))
		restored.Status = saved.Status
		require.NoError(t, h.c.Status().Update(context.Background(), restored))
		h.recovers(5)
	})

	t.Run("cluster too old to host a replica", func(t *testing.T) {
		h := newLagHarness(t, "lagser-old")
		h.publishesFirst(20)

		cluster := h.cluster()
		cluster.Spec.Image.Tag = "5.26.0-enterprise"
		require.NoError(t, h.c.Update(context.Background(), cluster))
		require.NoError(t, h.reconcile())
		assert.False(t, h.lag().found)

		cluster = h.cluster()
		cluster.Spec.Image.Tag = "2026.08.0-enterprise"
		require.NoError(t, h.c.Update(context.Background(), cluster))
		h.recovers(6)
	})
}

// TestReplicaLagSeries_RemovedWhenLagCannotBeRead: the cluster is Ready but the
// reconcile cannot get a reading from it.
func TestReplicaLagSeries_RemovedWhenLagCannotBeRead(t *testing.T) {
	t.Run("cannot connect to Neo4j", func(t *testing.T) {
		h := newLagHarness(t, "lagser-conn")
		h.publishesFirst(30)

		h.connErr = errors.New("dial tcp: connection refused")
		require.Error(t, h.reconcile())
		assert.False(t, h.lag().found)

		h.connErr = nil
		h.recovers(8)
	})

	t.Run("cannot connect to Neo4j through the real client", func(t *testing.T) {
		// No scripted server: the production client factory runs, and fails
		// because the admin Secret does not exist. Pins that the removal sits on
		// the path the real client takes, not only on the test double's.
		h := newLagHarness(t, "lagser-real")
		h.publishesFirst(30)

		h.r.newNeo4jClient = nil
		require.Error(t, h.reconcile())
		assert.False(t, h.lag().found)
	})

	t.Run("SHOW DATABASES fails", func(t *testing.T) {
		h := newLagHarness(t, "lagser-lookup")
		h.publishesFirst(30)

		h.server.info = nil
		h.server.infoErr = errors.New("bolt: connection reset by peer")
		require.Error(t, h.reconcile())
		assert.False(t, h.lag().found)
		assert.Zero(t, h.server.created,
			"a failed lookup is not evidence the database is gone; nothing may be re-created")

		h.recovers(9)
	})

	t.Run("spec becomes invalid", func(t *testing.T) {
		h := newLagHarness(t, "lagser-invalid")
		h.r.Validator = validation.NewReplicaValidator(h.c)
		h.publishesFirst(30)

		replica := &neo4jv1beta1.Neo4jReplicaDatabase{}
		key := types.NamespacedName{Namespace: h.ns, Name: h.name}
		require.NoError(t, h.c.Get(context.Background(), key, replica))
		replica.Spec.UpstreamDatabase = "bad`name"
		require.NoError(t, h.c.Update(context.Background(), replica))
		require.NoError(t, h.reconcile())
		assert.False(t, h.lag().found, "a reconcile that refuses the spec is not reading the lag")

		require.NoError(t, h.c.Get(context.Background(), key, replica))
		replica.Spec.UpstreamDatabase = "foo"
		require.NoError(t, h.c.Update(context.Background(), replica))
		h.recovers(10)
	})
}

// TestReplicaLagSeries_OtherReplicasUntouched: withdrawing one replica's lag
// must not disturb another replica's, in the same namespace or elsewhere.
func TestReplicaLagSeries_OtherReplicasUntouched(t *testing.T) {
	h := newLagHarness(t, "lagser-iso")
	other := newLagHarness(t, "lagser-iso-other")
	h.publishesFirst(1)
	other.publishesFirst(2)

	h.setClusterReady(false)
	require.NoError(t, h.reconcile())

	assert.False(t, h.lag().found)
	require.True(t, other.lag().found, "a replica elsewhere keeps its series")
	assert.Equal(t, 2.0, other.lag().value)
}
