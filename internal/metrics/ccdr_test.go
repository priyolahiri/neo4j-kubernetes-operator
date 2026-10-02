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

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// TestReplicaMetrics_RegisteredWithDocumentedLabels reads the families back
// from the registry that /metrics serves. A collector that is defined but not
// registered, or registered with different labels than the documentation
// promises, would pass every direct-value test and still be useless to an
// operator scraping it.
func TestReplicaMetrics_RegisteredWithDocumentedLabels(t *testing.T) {
	SetReplicaLagTransactions("reg-ns", "reg-cluster", "reg-replica", "reg-db", 3)
	RecordReplicaPromotion("reg-ns", "reg-cluster", true)
	t.Cleanup(func() { DeleteReplicaLagTransactions("reg-ns", "reg-replica") })

	families, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)

	labelsOf := map[string][]string{}
	for _, fam := range families {
		switch fam.GetName() {
		case "neo4j_operator_replica_lag_transactions", "neo4j_operator_replica_promotions_total":
			for _, m := range fam.GetMetric() {
				var names []string
				for _, lp := range m.GetLabel() {
					names = append(names, lp.GetName())
				}
				labelsOf[fam.GetName()] = names
			}
		}
	}
	assert.ElementsMatch(t, []string{"namespace", "cluster_name", "replica", "database"},
		labelsOf["neo4j_operator_replica_lag_transactions"])
	assert.ElementsMatch(t, []string{"namespace", "cluster_name", "result"},
		labelsOf["neo4j_operator_replica_promotions_total"])
}

func TestReplicaLagTransactions_SetAndRemove(t *testing.T) {
	replicaLagTransactions.Reset()

	SetReplicaLagTransactions("dr", "downstream", "orders-replica", "orders", 42)
	SetReplicaLagTransactions("dr", "downstream", "users-replica", "users", 7)
	SetReplicaLagTransactions("other-ns", "downstream", "orders-replica", "orders", 9)

	assert.Equal(t, 42.0, testutil.ToFloat64(
		replicaLagTransactions.WithLabelValues("dr", "downstream", "orders-replica", "orders")))
	assert.Equal(t, 3, testutil.CollectAndCount(replicaLagTransactions))

	// A later reading overwrites; it does not add a series.
	SetReplicaLagTransactions("dr", "downstream", "orders-replica", "orders", 0)
	assert.Equal(t, 0.0, testutil.ToFloat64(
		replicaLagTransactions.WithLabelValues("dr", "downstream", "orders-replica", "orders")))
	assert.Equal(t, 3, testutil.CollectAndCount(replicaLagTransactions))

	// Removal is by (namespace, replica) alone, because a deleted CR no longer
	// knows its cluster or database, and it touches nothing else, in particular
	// not a same-named replica in another namespace.
	assert.Equal(t, 1, DeleteReplicaLagTransactions("dr", "orders-replica"))
	assert.Equal(t, 2, testutil.CollectAndCount(replicaLagTransactions))

	// Idempotent, and harmless for a replica that never published.
	assert.Equal(t, 0, DeleteReplicaLagTransactions("dr", "orders-replica"))
	assert.Equal(t, 0, DeleteReplicaLagTransactions("dr", "never-existed"))
	assert.Equal(t, 2, testutil.CollectAndCount(replicaLagTransactions))
}

func TestRecordReplicaPromotion_CountsByResult(t *testing.T) {
	replicaPromotionsTotal.Reset()

	RecordReplicaPromotion("dr", "downstream", true)
	RecordReplicaPromotion("dr", "downstream", true)
	RecordReplicaPromotion("dr", "downstream", false)
	RecordReplicaPromotion("dr", "elsewhere", true)

	assert.Equal(t, 2.0, testutil.ToFloat64(replicaPromotionsTotal.WithLabelValues("dr", "downstream", "success")))
	assert.Equal(t, 1.0, testutil.ToFloat64(replicaPromotionsTotal.WithLabelValues("dr", "downstream", "failure")))
	assert.Equal(t, 1.0, testutil.ToFloat64(replicaPromotionsTotal.WithLabelValues("dr", "elsewhere", "success")))
}
