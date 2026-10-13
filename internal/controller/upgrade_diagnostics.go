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
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Diagnostics during an upgrade (operations.md id 131).
//
// While an upgrade runs, a pass hands the cluster to the upgrade state machine
// and skips the rest of the normal pass: its StatefulSet apply would undo the
// partition freeze, split-brain detection restarts pods, and the availability
// logic would mark the cluster Degraded for servers that are down on purpose.
// That also froze status.diagnostics, the ServersHealthy and DatabasesHealthy
// conditions and the server_health metric at their values from before the
// upgrade, for an upgrade that can take an hour. Collecting diagnostics only
// reads Neo4j (SHOW SERVERS, SHOW DATABASES, users and roles) and writes those
// fields, which nothing in the upgrade owns, so it keeps running: at most once
// per ready-poll interval, because a status write is a reconcile.

// collectDiagnosticsDuringUpgrade collects the cluster's diagnostics, as a
// Ready cluster's pass does, if the last collection during this upgrade was at
// least a ready-poll interval ago. Never fatal.
func (r *Neo4jEnterpriseClusterReconciler) collectDiagnosticsDuringUpgrade(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	if cluster.Spec.Monitoring != nil && !cluster.Spec.Monitoring.Enabled {
		return
	}
	if !clusterHasFormed(cluster) {
		return
	}
	key := clusterKey(cluster)
	now := time.Now()
	if v, ok := r.upgradeDiagnostics.Load(key); ok {
		if last, isTime := v.(time.Time); isTime && now.Sub(last) < readyPollInterval(r.ReadyPollInterval, r.RequeueAfter) {
			return
		}
	}
	r.upgradeDiagnostics.Store(key, now)

	logger := log.FromContext(ctx)
	if r.UpgradeGates != nil && r.UpgradeGates.diagnostics != nil {
		if err := r.UpgradeGates.diagnostics(ctx, cluster); err != nil {
			logger.V(1).Info("Diagnostics during the upgrade failed (non-fatal)", "error", err.Error())
		}
		return
	}
	neo4jClient, err := r.createNeo4jClient(ctx, cluster)
	if err != nil {
		logger.V(1).Info("Skipping diagnostics during the upgrade: could not create Neo4j client", "error", err.Error())
		return
	}
	defer func() { _ = neo4jClient.Close() }()
	if err := NewQueryMonitor(r.Client, r.Scheme).CollectDiagnostics(ctx, cluster, neo4jClient); err != nil {
		logger.V(1).Info("Diagnostics during the upgrade failed (non-fatal)", "error", err.Error())
	}
}
