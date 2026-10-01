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

// Metrics for cross-cluster replication (CCDR): Neo4jReplicaDatabase and
// Neo4jReplicaPromotion.
//
// These describe replication as it ships: a downstream cluster hosting a
// read-only replica of an upstream database, promoted exactly once and
// irreversibly. The region-based disaster-recovery families in metrics.go
// described a design that was never built; these are the ones that match the
// feature.
//
// Registered from this file's own init() so the families live beside the code
// that records them.

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// LabelReplica is the label key for the Neo4jReplicaDatabase CR name. It is
	// the CR's metadata.name, which can differ from the database name
	// (spec.name), hence the separate LabelDatabase.
	LabelReplica = "replica"
	// LabelDatabase is the label key for the Neo4j database name.
	LabelDatabase = "database"
)

var (
	// replicaLagTransactions is how many transactions a replica database is
	// behind its upstream, as SHOW DATABASES reports it (replicationLag).
	//
	// It is a TRANSACTION COUNT, not a duration, hence the name. It is the data
	// loss a promotion made right now would make permanent (a promoted replica
	// cannot re-attach to its upstream to catch up), so it is the number to
	// alert on before failing over.
	//
	// The series exists only while the replica is replicating: it is removed when
	// the Neo4jReplicaDatabase is deleted or promoted, so a dashboard never shows
	// the last value frozen after replication has ended.
	replicaLagTransactions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "replica_lag_transactions",
			Help: "Transactions a cross-cluster replica database is behind its upstream " +
				"(a transaction count, not seconds). Removed when the replica is deleted or promoted.",
		},
		[]string{LabelNamespace, LabelClusterName, LabelReplica, LabelDatabase},
	)

	// replicaPromotionsTotal counts Neo4jReplicaPromotion outcomes, incremented
	// once per promotion at its terminal phase: result=success when it reaches
	// Completed, result=failure when it reaches Failed. A promotion that is still
	// retrying is neither.
	replicaPromotionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: subsystem,
			Name:      "replica_promotions_total",
			Help: "Cross-cluster replica promotions by terminal outcome " +
				"(one increment per Neo4jReplicaPromotion).",
		},
		[]string{LabelNamespace, LabelClusterName, LabelResult},
	)
)

func init() {
	metrics.Registry.MustRegister(
		replicaLagTransactions,
		replicaPromotionsTotal,
	)
}

// SetReplicaLagTransactions publishes a replica database's lag, in
// transactions. clusterName is the DOWNSTREAM cluster that hosts the replica.
func SetReplicaLagTransactions(namespace, clusterName, replica, database string, lag int64) {
	replicaLagTransactions.WithLabelValues(namespace, clusterName, replica, database).Set(float64(lag))
}

// DeleteReplicaLagTransactions removes every lag series of one
// Neo4jReplicaDatabase, whichever cluster/database labels it carried: a
// caller handling a deleted CR no longer knows them. It reports how many series
// were removed. Safe to call repeatedly and for a replica that never published.
func DeleteReplicaLagTransactions(namespace, replica string) int {
	return replicaLagTransactions.DeletePartialMatch(prometheus.Labels{
		LabelNamespace: namespace,
		LabelReplica:   replica,
	})
}

// RecordReplicaPromotion counts one terminal promotion outcome against the
// downstream cluster that hosts the replica. Call it once per
// Neo4jReplicaPromotion, after the terminal phase has been persisted.
func RecordReplicaPromotion(namespace, clusterName string, success bool) {
	result := MetricResultSuccess
	if !success {
		result = MetricResultFailure
	}
	replicaPromotionsTotal.WithLabelValues(namespace, clusterName, result).Inc()
}
