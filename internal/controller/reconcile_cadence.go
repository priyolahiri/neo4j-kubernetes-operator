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
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// DefaultSplitBrainCheckInterval is how long a Ready cluster goes between full
// split-brain checks while nothing about its server pods changes. Overridden
// by --split-brain-check-interval; zero checks on every pass.
const DefaultSplitBrainCheckInterval = 5 * time.Minute

// readyPollInterval is how long a Ready deployment waits for its next pass
// when nothing in Kubernetes changes: configured (--ready-poll-interval) when
// set, otherwise the reconciler's RequeueAfter. Every other path — forming,
// upgrading, short of a server — keeps RequeueAfter, so a long poll never
// slows a deployment that is converging.
func readyPollInterval(configured, requeueAfter time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return requeueAfter
}

// splitBrainCheck records the last full split-brain check that found the
// cluster whole: when it ran, and the server pods it ran against.
type splitBrainCheck struct {
	at   time.Time
	pods string
}

// serverPodsFingerprint identifies the server pods as they are now: each
// pod's name, UID, phase, readiness, container restart counts and whether it
// is being deleted. A pod that is deleted or recreated, restarts a container,
// or changes readiness changes it — the moments a server can come back on its
// own and form a separate cluster.
func serverPodsFingerprint(pods []corev1.Pod) string {
	parts := make([]string, 0, len(pods))
	for i := range pods {
		p := &pods[i]
		ready := false
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady {
				ready = c.Status == corev1.ConditionTrue
			}
		}
		restarts := int32(0)
		for _, cs := range p.Status.ContainerStatuses {
			restarts += cs.RestartCount
		}
		parts = append(parts, fmt.Sprintf("%s/%s/%s/%t/%d/%t", p.Name, p.UID, p.Status.Phase, ready, restarts, p.DeletionTimestamp != nil))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// countAvailableServers counts the servers SHOW SERVERS lists as Enabled and
// Available — the same test the split-brain path applies to the largest view.
func countAvailableServers(servers []neo4jclient.ServerInfo) int {
	n := 0
	for _, s := range servers {
		if s.State == "Enabled" && s.Health == "Available" {
			n++
		}
	}
	return n
}

// splitBrainCheckSkippable reports whether a Ready cluster can go without the
// full split-brain check this pass. The full check opens a Bolt connection to
// every server, so on every pass it costs a fleet of large clusters N
// connections per cluster per poll. It is skipped only when all of these hold:
//
//   - an interval is configured (zero checks on every pass);
//   - the cluster is Ready — formation, Degraded and split-brain recovery
//     always check;
//   - SHOW SERVERS through the client Service lists every expected server as
//     Enabled and Available — any shortfall checks;
//   - a full check found the cluster whole within the interval, against
//     exactly the server pods there are now — a pod recreated, restarted or
//     changing readiness since then checks.
func splitBrainCheckSkippable(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, servers []neo4jclient.ServerInfo,
	last *splitBrainCheck, pods string, interval time.Duration, now time.Time) bool {
	if interval <= 0 || last == nil || pods == "" {
		return false
	}
	if cluster.Status.Phase != neo4jv1beta1.PhaseReady {
		return false
	}
	if countAvailableServers(servers) < int(cluster.Spec.Topology.Servers) {
		return false
	}
	return last.pods == pods && now.Sub(last.at) < interval
}

// skipSplitBrainCheck decides, for verifyNeo4jClusterFormation, whether this
// pass can rely on the last clean split-brain check. It returns the current
// server-pod fingerprint, which the caller records if the full check runs and
// finds the cluster whole; an empty fingerprint is never recorded.
func (r *Neo4jEnterpriseClusterReconciler) skipSplitBrainCheck(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	detector *SplitBrainDetector, servers []neo4jclient.ServerInfo) (bool, string) {
	if r.SplitBrainCheckInterval <= 0 {
		return false, ""
	}
	pods, err := detector.getServerPods(ctx, cluster)
	if err != nil {
		return false, ""
	}
	fingerprint := serverPodsFingerprint(pods)
	var last *splitBrainCheck
	if v, ok := r.splitBrainChecks.Load(clusterKey(cluster)); ok {
		if c, ok := v.(splitBrainCheck); ok { // the map only ever holds splitBrainCheck
			last = &c
		}
	}
	return splitBrainCheckSkippable(cluster, servers, last, fingerprint, r.SplitBrainCheckInterval, time.Now()), fingerprint
}

// recordCleanSplitBrainCheck records a full check that compared every
// server's view and found a Ready cluster whole, against the pods it ran on.
// A check on a cluster that is not Ready is not recorded: before formation
// the detector compares no views, and a Degraded cluster checks every pass.
func (r *Neo4jEnterpriseClusterReconciler) recordCleanSplitBrainCheck(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, pods string, now time.Time) {
	if pods == "" || cluster.Status.Phase != neo4jv1beta1.PhaseReady {
		return
	}
	r.splitBrainChecks.Store(clusterKey(cluster), splitBrainCheck{at: now, pods: pods})
}
