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

// kubectl get neo4jreplicadatabase showed an empty LAG column at zero lag, and
// kubectl get neo4jreplicapromotion an empty LAGTAKEN: both were int64 fields
// with omitempty, so a 0 was left out of the object the printcolumn reads
// (operations.md id 218).

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// statusJSON returns the status of obj as the API server stores it.
func statusJSON(t *testing.T, status any) map[string]any {
	t.Helper()
	data, err := json.Marshal(status)
	require.NoError(t, err)
	out := map[string]any{}
	require.NoError(t, json.Unmarshal(data, &out))
	return out
}

// A replica not read yet has no lag in status (the column is empty); one at
// zero lag has 0, which the column shows; and the last reading stays while
// the target cannot be read, as before, with the lag metric withdrawn.
func TestReplicaStatus_ZeroLagIsShown(t *testing.T) {
	h := newLagHarness(t, "lagcol")
	replica := &neo4jv1beta1.Neo4jReplicaDatabase{}
	get := func() *neo4jv1beta1.Neo4jReplicaDatabase {
		require.NoError(t, h.c.Get(context.Background(), types.NamespacedName{Namespace: h.ns, Name: h.name}, replica))
		return replica
	}
	assert.NotContains(t, statusJSON(t, get().Status), "replicationLag", "not read yet: no lag")

	h.publishesFirst(0)
	lag, ok := statusJSON(t, get().Status)["replicationLag"]
	require.True(t, ok, "zero lag is in status, so the LAG column shows it")
	assert.Equal(t, 0.0, lag)

	h.setClusterReady(false)
	require.NoError(t, h.reconcile())
	assert.False(t, h.lag().found, "the metric is withdrawn while the target cannot be read")
	assert.Equal(t, 0.0, statusJSON(t, get().Status)["replicationLag"], "status keeps the last reading")
}

// A promotion of a replica that was not behind records 0 as the lag taken.
func TestPromotionStatus_ZeroLagTakenIsShown(t *testing.T) {
	const ns, cluster = "promo-zero-ns", "promo-zero-cluster"
	replica := newReplicaForMetrics("promo-zero-replica", ns, cluster, "ledger", neo4jv1beta1.ReplicaPhaseReplicating)
	promo := newPromotionForMetrics("promo-zero", ns, replica.Name, neo4jv1beta1.PromotionPhasePromoting)
	r := newPromotionReconciler(promo, replica)
	ctx := context.Background()

	r.complete(ctx, promo, replica, &neo4jclient.DatabaseInfo{Name: "ledger", Type: "standard"}, "promoted")

	got := &neo4jv1beta1.Neo4jReplicaPromotion{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: ns, Name: promo.Name}, got))
	lag, ok := statusJSON(t, got.Status)["observedLagTxIds"]
	require.True(t, ok, "a lag of 0 taken is in status, so the LAGTAKEN column shows it")
	assert.Equal(t, 0.0, lag)

	gotReplica := &neo4jv1beta1.Neo4jReplicaDatabase{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: ns, Name: replica.Name}, gotReplica))
	assert.Equal(t, 0.0, statusJSON(t, gotReplica.Status)["replicationLag"])
}
