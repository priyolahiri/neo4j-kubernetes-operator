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

// Server availability after formation (#444).
//
// A cluster used to report Forming whenever any server was short — a pod
// restart, an OOM kill, a node drain. Forming means "not formed yet": it set
// the Ready condition Unknown, paused every dependent (users, roles,
// databases, backups) until the server returned, froze ServersHealthy, and
// left a cluster whose server never came back Forming forever.
//
// A cluster that has formed once is now judged on whether it is still
// serving. With a majority of its servers available and no rollout in flight
// it stays Ready, with the Degraded condition True, for a grace period that a
// routine restart fits inside; past it, the phase turns Degraded. First
// formation, scale-ups, template rollouts (#262), split-brain and the loss of
// a majority stay Forming, as before.

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// DefaultServerUnavailableGrace is how long a formed cluster may run short of
// a server before its phase turns Degraded. It matches the ConnectivityDegraded
// threshold: long enough for a pod restart to rejoin, short enough to act on.
// --server-unavailable-grace overrides it for servers that restart slower.
const DefaultServerUnavailableGrace = 5 * time.Minute

// availabilityVerdict is what a cluster that is not fully formed means.
type availabilityVerdict string

const (
	// verdictForming: never formed, a rollout or scale-up, a split-brain, or
	// nothing measurable. Phase Forming; the Degraded condition is untouched.
	verdictForming availabilityVerdict = "Forming"
	// verdictQuorumLost: formed, but no majority of servers available. Phase
	// Forming (dependents pause); the Degraded condition is True.
	verdictQuorumLost availabilityVerdict = "QuorumLost"
	// verdictWithinGrace: formed and serving with a majority, short for less
	// than the grace period. Phase stays Ready; the Degraded condition is True.
	verdictWithinGrace availabilityVerdict = "WithinGrace"
	// verdictDegraded: as verdictWithinGrace, for longer than the grace
	// period. Phase Degraded.
	verdictDegraded availabilityVerdict = "Degraded"
)

// shortfallInput is everything assessServerShortfall decides from, so the
// decision is a pure function of it.
type shortfallInput struct {
	// Formed: the cluster has formed at least once (clusterHasFormed).
	Formed bool
	// RolloutInFlight: the server StatefulSet is rolling to a new template,
	// a planned restart of every server in turn.
	RolloutInFlight bool
	// SplitBrain: the split-brain detector found divergent cluster views.
	SplitBrain bool
	// Servers is SHOW SERVERS through the client Service; nil when Neo4j
	// could not be queried.
	Servers []neo4jclient.ServerInfo
	// Expected is spec.topology.servers.
	Expected int
	// UnavailableSince is when the Degraded condition went True for missing
	// servers; nil when it is not True.
	UnavailableSince *time.Time
	Now              time.Time
	Grace            time.Duration
}

// shortfallAssessment is the verdict plus what the status messages need.
type shortfallAssessment struct {
	Verdict   availabilityVerdict
	Available int
	Expected  int
	// Unavailable names each Enabled server that is not Available.
	Unavailable []string
}

// assessServerShortfall decides what a cluster that is not fully formed is:
// still forming, or formed and short of servers — and if short, whether it
// still has a majority and for how long it has been short.
func assessServerShortfall(in shortfallInput) shortfallAssessment {
	a := shortfallAssessment{Verdict: verdictForming, Expected: in.Expected}
	if !in.Formed || in.RolloutInFlight || in.SplitBrain || in.Servers == nil || in.Expected < 1 {
		return a
	}

	enabled := 0
	for _, s := range in.Servers {
		if s.State != "Enabled" {
			continue
		}
		enabled++
		if s.Health == "Available" {
			a.Available++
		} else {
			a.Unavailable = append(a.Unavailable, describeServer(s))
		}
	}
	// A server Neo4j has never enabled is joining — a scale-up — not lost.
	// A server that restarts keeps its identity and stays Enabled throughout.
	if enabled < in.Expected {
		return a
	}

	if a.Available*2 <= in.Expected {
		a.Verdict = verdictQuorumLost
		return a
	}

	since := in.Now
	if in.UnavailableSince != nil {
		since = *in.UnavailableSince
	}
	if in.Now.Sub(since) < in.Grace {
		a.Verdict = verdictWithinGrace
	} else {
		a.Verdict = verdictDegraded
	}
	return a
}

// summary reads e.g. "2 of 3 servers available; unavailable: neo4j-server-2 (Unavailable)".
func (a shortfallAssessment) summary() string {
	s := fmt.Sprintf("%d of %d servers available", a.Available, a.Expected)
	if len(a.Unavailable) > 0 {
		s += "; unavailable: " + strings.Join(a.Unavailable, ", ")
	}
	return s
}

// serverAddressBook remembers, per cluster (key ns/name), the last address
// each server reported. SHOW SERVERS reports no address for a server that is
// down — exactly when the address, the pod's name, is wanted: in the Degraded
// condition, in status.diagnostics, and as the server_address label, where a
// changed value would start a new series. status.diagnostics alone is not
// enough to remember it: a collection that cannot reach Neo4j writes an empty
// server list, and that is the first thing that happens when servers go down.
var serverAddressBook = struct {
	mu        sync.Mutex
	byCluster map[string]map[string]string
}{byCluster: map[string]map[string]string{}}

func clusterKey(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) string {
	return cluster.Namespace + "/" + cluster.Name
}

// forgetServerAddresses drops a deleted cluster's address book.
func forgetServerAddresses(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	serverAddressBook.mu.Lock()
	defer serverAddressBook.mu.Unlock()
	delete(serverAddressBook.byCluster, clusterKey(cluster))
}

// withKnownAddresses returns servers with each missing address filled in: from
// this process's address book first, then from the cluster's last diagnostics
// (which survive an operator restart). Every address a server does report is
// recorded for next time. "<nil>" is what releases before #444 stored for a
// missing address. The input slice is not modified; nil stays nil, because
// nil means Neo4j could not be queried.
func withKnownAddresses(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, servers []neo4jclient.ServerInfo) []neo4jclient.ServerInfo {
	if servers == nil {
		return nil
	}
	usable := func(a string) bool { return a != "" && a != "<nil>" }

	serverAddressBook.mu.Lock()
	defer serverAddressBook.mu.Unlock()
	key := clusterKey(cluster)
	book := serverAddressBook.byCluster[key]
	if book == nil {
		book = map[string]string{}
		serverAddressBook.byCluster[key] = book
	}
	if d := cluster.Status.Diagnostics; d != nil {
		for _, s := range d.Servers {
			if _, ok := book[s.Name]; !ok && usable(s.Address) {
				book[s.Name] = s.Address
			}
		}
	}

	out := make([]neo4jclient.ServerInfo, len(servers))
	copy(out, servers)
	for i := range out {
		if usable(out[i].Address) {
			book[out[i].Name] = out[i].Address
		} else {
			out[i].Address = book[out[i].Name]
		}
	}
	return out
}

// describeServer names a server by the first label of its advertised host —
// the pod name — falling back to the address, then the Neo4j server name,
// with its health.
func describeServer(s neo4jclient.ServerInfo) string {
	name := s.Name
	host := s.Address
	if h, _, err := net.SplitHostPort(s.Address); err == nil {
		host = h
	}
	switch {
	case host == "":
	case net.ParseIP(host) != nil:
		name = host
	default:
		name = strings.SplitN(host, ".", 2)[0]
	}
	return fmt.Sprintf("%s (%s)", name, s.Health)
}

// clusterHasFormed reports whether the cluster has formed at least once: it is
// Ready or Degraded now, or carries the ClusterFormed condition from an
// earlier pass. A formed cluster that is short of servers is degraded, not
// forming.
func clusterHasFormed(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	switch cluster.Status.Phase {
	case neo4jv1beta1.PhaseReady, neo4jv1beta1.PhaseDegraded:
		return true
	}
	c := findCondition(cluster.Status.Conditions, ConditionTypeClusterFormed)
	return c != nil && c.Status == metav1.ConditionTrue
}

// clusterAcceptsWork reports whether resources that act through a cluster —
// users, roles, databases, backups — should proceed against it: it is Ready,
// or Degraded (formed and serving with a majority, one or more servers down
// past the grace period). Pausing them until a lost server returns would stop
// backups exactly when they matter. Whether a particular statement can run
// on fewer servers is Neo4j's call, reported on that resource.
//
// Phase Ready counts on its own: the backup controller hands this a
// standalone dressed as a cluster (standaloneAsCluster), which carries the
// phase but no conditions. Reading only the Ready condition left every
// standalone backup "Waiting: Target cluster is not ready" forever.
func clusterAcceptsWork(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	switch cluster.Status.Phase {
	case neo4jv1beta1.PhaseReady, neo4jv1beta1.PhaseDegraded:
		return true
	}
	return hasReadyCondition(cluster.Status.Conditions)
}

// serversUnavailableSince returns when the Degraded condition went True for
// missing servers, or nil when it is not True. The condition's
// lastTransitionTime is the grace clock, so it survives operator restarts.
func serversUnavailableSince(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *time.Time {
	c := findCondition(cluster.Status.Conditions, ConditionTypeDegraded)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != ConditionReasonServersUnavailable {
		return nil
	}
	t := c.LastTransitionTime.Time
	return &t
}

// statefulSetRolloutInFlight reports a template rollout — every server
// restarting in turn, by design — as distinct from a pod that restarted on its
// own, which leaves the revisions equal.
func statefulSetRolloutInFlight(sts *appsv1.StatefulSet) bool {
	if sts.Status.ObservedGeneration < sts.Generation {
		return true
	}
	return sts.Status.UpdateRevision != "" && sts.Status.CurrentRevision != sts.Status.UpdateRevision
}

// serverRolloutInFlight is statefulSetRolloutInFlight for the cluster's
// server StatefulSet. A StatefulSet it cannot read counts as in flight, which
// keeps the cluster Forming.
func (r *Neo4jEnterpriseClusterReconciler) serverRolloutInFlight(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-server", cluster.Name),
		Namespace: cluster.Namespace,
	}, sts); err != nil {
		return true
	}
	return statefulSetRolloutInFlight(sts)
}

// firstAnnouncement reports whether a Warning for this verdict is due: this
// operator has not announced it since the cluster was last whole, which
// survives a reconcile that reads a stale cache. alreadyInPhase — the cluster
// already shows the phase only this verdict sets — keeps an operator restart
// from announcing it again. Degraded has such a phase; QuorumLost does not,
// since Forming has other causes (a reconcile that cannot reach Neo4j at all
// writes it first), so a restart may announce a lost majority once more.
func (r *Neo4jEnterpriseClusterReconciler) firstAnnouncement(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, verdict availabilityVerdict, alreadyInPhase bool) bool {
	prev, loaded := r.availabilityAnnounced.Swap(clusterKey(cluster), verdict)
	if alreadyInPhase {
		return false
	}
	return !loaded || prev != verdict
}

// serverUnavailableGrace is the configured grace period, or the default.
func (r *Neo4jEnterpriseClusterReconciler) serverUnavailableGrace() time.Duration {
	if r.ServerUnavailableGrace > 0 {
		return r.ServerUnavailableGrace
	}
	return DefaultServerUnavailableGrace
}

// clusterFormedCondition marks a cluster as having formed. It is never set
// back to False: a cluster that formed once has a cluster identity on disk,
// and whatever it is short of later is an outage, not formation.
func clusterFormedCondition() metav1.Condition {
	return metav1.Condition{
		Type:    ConditionTypeClusterFormed,
		Status:  metav1.ConditionTrue,
		Reason:  ConditionReasonFormationComplete,
		Message: "Every server has joined the cluster; a server lost from here on is reported through the Degraded condition",
	}
}

// serversUnavailableCondition is the Degraded condition while servers are
// missing. Its reason must not change between the grace period and the
// Degraded phase, or the lastTransitionTime the grace is measured from resets.
func serversUnavailableCondition(message string) metav1.Condition {
	return metav1.Condition{
		Type:    ConditionTypeDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  ConditionReasonServersUnavailable,
		Message: message,
	}
}

// allServersAvailableCondition clears the Degraded condition.
func allServersAvailableCondition(servers int32) metav1.Condition {
	return metav1.Condition{
		Type:    ConditionTypeDegraded,
		Status:  metav1.ConditionFalse,
		Reason:  ConditionReasonAllServersAvailable,
		Message: fmt.Sprintf("All %d servers are available", servers),
	}
}
