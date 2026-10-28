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
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// Diagnostics during an upgrade (operations.md id 131).
//
// While an upgrade runs, a pass hands the cluster to the upgrade state machine
// and skips the rest of the normal pass: its StatefulSet apply would undo the
// partition freeze, split-brain detection restarts pods, and the availability
// logic would turn the cluster's phase Degraded or Forming for servers that
// are down on purpose. That also froze status.diagnostics, the ServersHealthy
// and DatabasesHealthy conditions and the server_health metric at their
// values from before the upgrade, for an upgrade that can take an hour.
// Collecting diagnostics only reads Neo4j (SHOW SERVERS, SHOW DATABASES, users
// and roles) and writes those fields, which nothing in the upgrade owns, so it
// keeps running: at most once per ready-poll interval, because a status write
// is a reconcile.
//
// The Degraded condition follows the same SHOW SERVERS (operations.md id 214):
// it said "All 3 servers are available" for the 30 minutes server-2 sat in
// ImagePullBackOff during a Rolling step, while ServersHealthy said it was not
// (the v1.23.0 journey, #594). It is written as the normal pass writes it for
// a formed cluster short of a server (id 105) — True, reason
// ServersUnavailable, naming the servers, or False once every server is
// available — and nothing else: the phase stays as the upgrade keeps it, and
// no step gate reads the condition. With spec.monitoring.enabled false the
// diagnostics are not collected, but the SHOW SERVERS for the condition is.

// collectDiagnosticsDuringUpgrade collects the cluster's diagnostics, as a
// Ready cluster's pass does, and records from the same SHOW SERVERS whether a
// server is unavailable, if the last collection during this upgrade was at
// least a ready-poll interval ago. Never fatal.
func (r *Neo4jEnterpriseClusterReconciler) collectDiagnosticsDuringUpgrade(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
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
	monitoring := cluster.Spec.Monitoring == nil || cluster.Spec.Monitoring.Enabled

	logger := log.FromContext(ctx)
	if r.UpgradeGates != nil && (r.UpgradeGates.diagnostics != nil || r.UpgradeGates.servers != nil) {
		if monitoring && r.UpgradeGates.diagnostics != nil {
			if err := r.UpgradeGates.diagnostics(ctx, cluster); err != nil {
				logger.V(1).Info("Diagnostics during the upgrade failed (non-fatal)", "error", err.Error())
			}
		}
		if r.UpgradeGates.servers != nil {
			if servers, err := r.UpgradeGates.servers(ctx, cluster); err == nil {
				r.recordShortfallDuringUpgrade(ctx, cluster, servers)
			}
		}
		return
	}
	neo4jClient, err := r.createNeo4jClient(ctx, cluster)
	if err != nil {
		logger.V(1).Info("Skipping diagnostics during the upgrade: could not create Neo4j client", "error", err.Error())
		return
	}
	defer func() { _ = neo4jClient.Close() }()
	if monitoring {
		if err := NewQueryMonitor(r.Client, r.Scheme).CollectDiagnostics(ctx, cluster, neo4jClient); err != nil {
			logger.V(1).Info("Diagnostics during the upgrade failed (non-fatal)", "error", err.Error())
		}
	}
	servers, err := neo4jClient.GetServerList(ctx)
	if err != nil {
		logger.V(1).Info("Could not read SHOW SERVERS for the Degraded condition during the upgrade (non-fatal)", "error", err.Error())
		return
	}
	r.recordShortfallDuringUpgrade(ctx, cluster, servers)
}

// recordShortfallDuringUpgrade writes the Degraded condition from SHOW
// SERVERS during an upgrade: True (ServersUnavailable) naming each member
// (Enabled or Cordoned) that is not Available, False (AllServersAvailable)
// once every one of the StatefulSet's servers is; a server not yet a member
// leaves it as it is.
// Only that condition is written, and only when it changes.
func (r *Neo4jEnterpriseClusterReconciler) recordShortfallDuringUpgrade(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, servers []neo4jclient.ServerInfo) {
	servers = withKnownAddresses(cluster, servers)
	expected := int(cluster.Spec.Topology.Servers)
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name + "-server", Namespace: cluster.Namespace}, sts); err == nil && sts.Spec.Replicas != nil {
		expected = int(*sts.Spec.Replicas) // an upgrade changes no topology (id 163)
	}
	a := shortfallAssessment{Expected: expected}
	enabled := 0
	for _, s := range servers {
		if !serverIsMember(s) { // Enabled or Cordoned, as the normal pass counts (id 224)
			continue
		}
		enabled++
		if s.Health == "Available" {
			a.Available++
		} else {
			a.Unavailable = append(a.Unavailable, describeServer(s))
		}
	}
	var want metav1.Condition
	switch {
	case len(a.Unavailable) > 0:
		want = serversUnavailableCondition(a.summary())
	case enabled >= expected:
		want = allServersAvailableCondition(int32(expected))
	default:
		return
	}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		if have := findCondition(latest.Status.Conditions, ConditionTypeDegraded); have != nil &&
			have.Status == want.Status && have.Reason == want.Reason && have.Message == want.Message {
			return nil
		}
		SetNamedCondition(&latest.Status.Conditions, ConditionTypeDegraded, latest.Generation, want.Status, want.Reason, want.Message)
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		cluster.Status.Conditions = latest.Status.Conditions
		return nil
	})
	if err != nil {
		log.FromContext(ctx).V(1).Info("Could not record the Degraded condition during the upgrade (non-fatal)", "error", fmt.Sprint(err))
	}
}
