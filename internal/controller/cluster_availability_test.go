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

// Pinning tests for #444: a formed cluster that loses a server stays Ready
// through a grace period, then turns Degraded; it never falls back to Forming
// unless it has lost a majority, is rolling, or never formed.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

func availServer(pod, state, health string) neo4jclient.ServerInfo {
	return neo4jclient.ServerInfo{
		Name:    "id-" + pod,
		Address: pod + ".c-headless.default.svc.cluster.local:7687",
		State:   state,
		Health:  health,
	}
}

// servers returns n Enabled servers, the first `available` of them Available.
func availServers(n, available int) []neo4jclient.ServerInfo {
	out := make([]neo4jclient.ServerInfo, 0, n)
	for i := 0; i < n; i++ {
		health := "Available"
		if i >= available {
			health = "Unavailable"
		}
		out = append(out, availServer(fmt.Sprintf("c-server-%d", i), "Enabled", health))
	}
	return out
}

func TestAssessServerShortfall(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) *time.Time { t := now.Add(-d); return &t }
	grace := 5 * time.Minute

	cases := []struct {
		name  string
		in    shortfallInput
		want  availabilityVerdict
		avail int
	}{
		{"never formed: first formation stays Forming",
			shortfallInput{Servers: availServers(3, 2), Expected: 3}, verdictForming, 0},
		{"template rollout stays Forming (#262)",
			shortfallInput{Formed: true, RolloutInFlight: true, Servers: availServers(3, 2), Expected: 3}, verdictForming, 0},
		{"split-brain stays Forming",
			shortfallInput{Formed: true, SplitBrain: true, Servers: availServers(3, 2), Expected: 3}, verdictForming, 0},
		{"Neo4j unreachable, no streak on record: Forming",
			shortfallInput{Formed: true, Servers: nil, Expected: 3, ReadyServerPods: 3}, verdictForming, 0},
		{"Neo4j unreachable while a majority of pods are Ready: nothing written",
			shortfallInput{Formed: true, Servers: nil, Expected: 3, ReadyServerPods: 2, UnreachableSince: ago(10 * time.Second)}, verdictUnmeasured, 0},
		{"Neo4j unreachable with half the pods Ready: not a majority, Forming",
			shortfallInput{Formed: true, Servers: nil, Expected: 4, ReadyServerPods: 2, UnreachableSince: ago(10 * time.Second)}, verdictForming, 0},
		{"Neo4j unreachable with no majority of pods Ready: Forming",
			shortfallInput{Formed: true, Servers: nil, Expected: 3, ReadyServerPods: 1, UnreachableSince: ago(10 * time.Second)}, verdictForming, 0},
		{"Neo4j unreachable for the whole grace period: Forming",
			shortfallInput{Formed: true, Servers: nil, Expected: 3, ReadyServerPods: 3, UnreachableSince: ago(grace)}, verdictForming, 0},
		{"Neo4j unreachable before it ever formed: Forming",
			shortfallInput{Servers: nil, Expected: 3, ReadyServerPods: 3, UnreachableSince: ago(10 * time.Second)}, verdictForming, 0},
		{"Neo4j unreachable during a rollout: Forming",
			shortfallInput{Formed: true, RolloutInFlight: true, Servers: nil, Expected: 3, ReadyServerPods: 3, UnreachableSince: ago(10 * time.Second)}, verdictForming, 0},
		{"scale-up: servers Neo4j never enabled are joining, not lost",
			shortfallInput{Formed: true, Servers: availServers(3, 3), Expected: 5}, verdictForming, 0},
		{"a Free server is not counted as known",
			shortfallInput{Formed: true, Servers: append(availServers(2, 2), availServer("c-server-2", "Free", "Available")), Expected: 3}, verdictForming, 0},

		{"one of three down, just now: Ready within grace",
			shortfallInput{Formed: true, Servers: availServers(3, 2), Expected: 3}, verdictWithinGrace, 2},
		{"one of three down for 4m: still within grace",
			shortfallInput{Formed: true, Servers: availServers(3, 2), Expected: 3, UnavailableSince: ago(4 * time.Minute)}, verdictWithinGrace, 2},
		{"one of three down for exactly the grace: Degraded",
			shortfallInput{Formed: true, Servers: availServers(3, 2), Expected: 3, UnavailableSince: ago(grace)}, verdictDegraded, 2},
		{"one of three down for an hour: Degraded",
			shortfallInput{Formed: true, Servers: availServers(3, 2), Expected: 3, UnavailableSince: ago(time.Hour)}, verdictDegraded, 2},
		{"pod gone but Neo4j has not noticed yet: still a shortfall",
			shortfallInput{Formed: true, Servers: availServers(3, 3), Expected: 3}, verdictWithinGrace, 3},
		{"three of four is a majority",
			shortfallInput{Formed: true, Servers: availServers(4, 3), Expected: 4}, verdictWithinGrace, 3},

		{"one of three: majority lost",
			shortfallInput{Formed: true, Servers: availServers(3, 1), Expected: 3, UnavailableSince: ago(time.Hour)}, verdictQuorumLost, 1},
		{"one of two: majority lost",
			shortfallInput{Formed: true, Servers: availServers(2, 1), Expected: 2}, verdictQuorumLost, 1},
		{"two of four is not a majority",
			shortfallInput{Formed: true, Servers: availServers(4, 2), Expected: 4}, verdictQuorumLost, 2},
		{"none available",
			shortfallInput{Formed: true, Servers: availServers(3, 0), Expected: 3}, verdictQuorumLost, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.in.Now = now
			if tc.in.Grace == 0 {
				tc.in.Grace = grace
			}
			got := assessServerShortfall(tc.in)
			assert.Equal(t, tc.want, got.Verdict)
			if tc.want != verdictForming {
				assert.Equal(t, tc.avail, got.Available)
				assert.Equal(t, tc.in.Expected, got.Expected)
			}
		})
	}
}

func TestShortfallSummaryNamesThePods(t *testing.T) {
	a := assessServerShortfall(shortfallInput{
		Formed: true, Servers: availServers(3, 2), Expected: 3,
		Now: time.Now(), Grace: time.Minute,
	})
	assert.Equal(t, "2 of 3 servers available; unavailable: c-server-2 (Unavailable)", a.summary())
}

func TestDescribeServer(t *testing.T) {
	assert.Equal(t, "c-server-1 (Unavailable)",
		describeServer(neo4jclient.ServerInfo{Name: "uuid", Address: "c-server-1.c-headless.ns.svc.cluster.local:7687", Health: "Unavailable"}))
	assert.Equal(t, "10.0.0.7 (Unavailable)",
		describeServer(neo4jclient.ServerInfo{Name: "uuid", Address: "10.0.0.7:7687", Health: "Unavailable"}),
		"an IP address must not be cut at its first dot")
	assert.Equal(t, "uuid (Degraded)",
		describeServer(neo4jclient.ServerInfo{Name: "uuid", Health: "Degraded"}))
}

// Live on Kind, a server that was down was reported as "<nil> (Unavailable)":
// SHOW SERVERS has no address for it. The last address it reported is the
// pod's name.
func TestWithKnownAddresses(t *testing.T) {
	cluster := availCluster(neo4jv1beta1.PhaseReady)
	forgetServerAddresses(cluster)
	t.Cleanup(func() { forgetServerAddresses(cluster) })
	cluster.Status.Diagnostics = &neo4jv1beta1.ClusterDiagnosticsStatus{Servers: []neo4jv1beta1.ServerDiagnosticInfo{
		{Name: "id-1", Address: "c-server-1.c-headless.default.svc.cluster.local:7687"},
		{Name: "id-2", Address: "<nil>"},
	}}
	in := []neo4jclient.ServerInfo{
		{Name: "id-0", Address: "c-server-0.c-headless.default.svc.cluster.local:7687", State: "Enabled", Health: "Available"},
		{Name: "id-1", Address: "", State: "Enabled", Health: "Unavailable"},
		{Name: "id-2", Address: "", State: "Enabled", Health: "Unavailable"},
	}
	out := withKnownAddresses(cluster, in)
	assert.Equal(t, "c-server-1.c-headless.default.svc.cluster.local:7687", out[1].Address, "from status.diagnostics")
	assert.Equal(t, "", out[2].Address, `a stored "<nil>" is not an address`)
	assert.Equal(t, "", in[1].Address, "the input is not modified")
	assert.Equal(t, "c-server-1 (Unavailable)", describeServer(out[1]))
	assert.Equal(t, "id-2 (Unavailable)", describeServer(out[2]))
	assert.Nil(t, withKnownAddresses(cluster, nil), "nil means Neo4j could not be queried, and stays nil")

	// Live on Kind: the first reconcile after two servers went could not reach
	// Neo4j, and its diagnostics pass wrote an empty server list — so the
	// next one found nothing to carry over. An address seen once is kept.
	cluster.Status.Diagnostics = &neo4jv1beta1.ClusterDiagnosticsStatus{}
	out = withKnownAddresses(cluster, []neo4jclient.ServerInfo{{Name: "id-0", State: "Enabled", Health: "Unavailable"}})
	assert.Equal(t, "c-server-0 (Unavailable)", describeServer(out[0]))
}

func availCluster(phase string, conds ...metav1.Condition) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := gateTestCluster("5.26.0-enterprise", phase)
	c.Status.Conditions = conds
	return c
}

func cond(condType string, status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: condType, Status: status, Reason: reason, LastTransitionTime: metav1.Now()}
}

func TestClusterHasFormed(t *testing.T) {
	assert.True(t, clusterHasFormed(availCluster(neo4jv1beta1.PhaseReady)))
	assert.True(t, clusterHasFormed(availCluster(neo4jv1beta1.PhaseDegraded)))
	assert.False(t, clusterHasFormed(availCluster(neo4jv1beta1.PhaseForming)), "first formation")
	assert.False(t, clusterHasFormed(availCluster("")))
	assert.True(t, clusterHasFormed(availCluster(neo4jv1beta1.PhaseForming,
		cond(ConditionTypeClusterFormed, metav1.ConditionTrue, ConditionReasonFormationComplete))),
		"a formed cluster mid-rollout or below a majority is still formed")
}

func TestClusterAcceptsWork(t *testing.T) {
	ready := cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady)
	assert.True(t, clusterAcceptsWork(availCluster(neo4jv1beta1.PhaseReady, ready)))
	assert.True(t, clusterAcceptsWork(availCluster(neo4jv1beta1.PhaseReady, ready,
		cond(ConditionTypeDegraded, metav1.ConditionTrue, ConditionReasonServersUnavailable))),
		"within the grace period the cluster is Ready and dependents carry on")
	assert.True(t, clusterAcceptsWork(availCluster(neo4jv1beta1.PhaseDegraded,
		cond(ConditionTypeReady, metav1.ConditionFalse, ConditionReasonDegraded))),
		"a Degraded cluster is serving; dependents must not pause")
	assert.False(t, clusterAcceptsWork(availCluster(neo4jv1beta1.PhaseForming,
		cond(ConditionTypeReady, metav1.ConditionUnknown, ConditionReasonForming))))
	assert.False(t, clusterAcceptsWork(availCluster(neo4jv1beta1.PhaseFailed,
		cond(ConditionTypeReady, metav1.ConditionFalse, ConditionReasonFailed))))

	// The resolver every auth/alias/composite/replica controller goes through.
	assert.True(t, ResolvedTarget{Found: true, Cluster: availCluster(neo4jv1beta1.PhaseDegraded)}.IsReady())
	// The backup gate, which used to compare the phase with Ready.
	assert.True(t, (&Neo4jBackupReconciler{}).isClusterReady(availCluster(neo4jv1beta1.PhaseDegraded)))
	assert.True(t, (&Neo4jDatabaseReconciler{}).isClusterReady(availCluster(neo4jv1beta1.PhaseDegraded)))
}

// The backup controller backs up a standalone through standaloneAsCluster,
// which copies the phase and not the conditions. Live on Kind after #447, a
// Ready standalone's backup sat in "Waiting: Target cluster is not ready".
func TestBackupGateAcceptsAReadyStandalone(t *testing.T) {
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		Status: neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{
			Phase: neo4jv1beta1.PhaseReady,
			Ready: true,
			Conditions: []metav1.Condition{
				cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady),
			},
		},
	}
	assert.True(t, (&Neo4jBackupReconciler{}).isClusterReady(standaloneAsCluster(standalone)))

	standalone.Status.Phase = neo4jv1beta1.PhasePending
	assert.False(t, (&Neo4jBackupReconciler{}).isClusterReady(standaloneAsCluster(standalone)))
}

func TestStatefulSetRolloutInFlight(t *testing.T) {
	podRestart := gateTestSTS("neo4j:x", 3, "rev1", "rev1", 3, 2)
	assert.False(t, statefulSetRolloutInFlight(podRestart), "a pod that restarted on its own leaves the revisions equal")

	assert.True(t, statefulSetRolloutInFlight(gateTestSTS("neo4j:x", 3, "rev1", "rev2", 1, 3)))

	unobserved := gateTestSTS("neo4j:x", 3, "rev1", "rev1", 3, 3)
	unobserved.Generation = 4
	unobserved.Status.ObservedGeneration = 3
	assert.True(t, statefulSetRolloutInFlight(unobserved), "a spec change the StatefulSet controller has not seen yet")
}

func TestPhaseDegradedReadyConditionIsNotAFailure(t *testing.T) {
	status, reason := PhaseToConditionStatus(neo4jv1beta1.PhaseDegraded)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, "ClusterDegraded", reason, "the reason is user-visible in `kubectl describe`")
}

// --- the status the reconciler writes, through a grace period and back ---

func shortfallReconciler(t *testing.T, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (*Neo4jEnterpriseClusterReconciler, *record.FakeRecorder) {
	t.Helper()
	r := gateTestReconciler(t, gateTestSTS("neo4j:5.26.0-enterprise", 3, "rev1", "rev1", 3, 2), cluster)
	rec := record.NewFakeRecorder(32)
	r.Recorder = rec
	return r, rec
}

func refetch(t *testing.T, r *Neo4jEnterpriseClusterReconciler) *neo4jv1beta1.Neo4jEnterpriseCluster {
	t.Helper()
	c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "c"}, c))
	return c
}

// backdateDegraded moves the Degraded condition's lastTransitionTime into the
// past, which is what the grace period is measured from.
func backdateDegraded(t *testing.T, r *Neo4jEnterpriseClusterReconciler, by time.Duration) {
	t.Helper()
	c := refetch(t, r)
	d := findCondition(c.Status.Conditions, ConditionTypeDegraded)
	require.NotNil(t, d)
	d.LastTransitionTime = metav1.NewTime(d.LastTransitionTime.Add(-by))
	require.NoError(t, r.Status().Update(context.Background(), c))
}

func TestReconcileServerShortfall_GraceDegradedQuorumLost(t *testing.T) {
	ctx := context.Background()
	start := availCluster(neo4jv1beta1.PhaseReady, cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady))
	r, rec := shortfallReconciler(t, start)
	oneDown := formationCheck{message: "Cluster forming: 2/3 servers available", servers: availServers(3, 2)}

	// 1. A server goes: the cluster stays Ready and dependents carry on.
	r.reconcileServerShortfall(ctx, refetch(t, r), oneDown, oneDown.message)
	c := refetch(t, r)
	assert.Equal(t, neo4jv1beta1.PhaseReady, c.Status.Phase)
	assert.True(t, c.Status.Ready)
	assert.True(t, clusterAcceptsWork(c))
	degraded := findCondition(c.Status.Conditions, ConditionTypeDegraded)
	require.NotNil(t, degraded)
	assert.Equal(t, metav1.ConditionTrue, degraded.Status)
	assert.Equal(t, ConditionReasonServersUnavailable, degraded.Reason)
	assert.Contains(t, degraded.Message, "c-server-2 (Unavailable)")
	assert.Equal(t, metav1.ConditionTrue, findCondition(c.Status.Conditions, ConditionTypeClusterFormed).Status)
	assert.Empty(t, drainEvents(rec), "a routine restart inside the grace period raises no events")

	// 2. Past the grace period: Degraded, announced once.
	backdateDegraded(t, r, 6*time.Minute)
	since := findCondition(refetch(t, r).Status.Conditions, ConditionTypeDegraded).LastTransitionTime
	r.reconcileServerShortfall(ctx, refetch(t, r), oneDown, oneDown.message)
	c = refetch(t, r)
	assert.Equal(t, neo4jv1beta1.PhaseDegraded, c.Status.Phase)
	assert.False(t, c.Status.Ready)
	assert.True(t, clusterAcceptsWork(c), "dependents keep working against a Degraded cluster")
	ready := findCondition(c.Status.Conditions, ConditionTypeReady)
	assert.Equal(t, metav1.ConditionFalse, ready.Status)
	assert.Equal(t, ConditionReasonDegraded, ready.Reason)
	assert.True(t, since.Equal(&findCondition(c.Status.Conditions, ConditionTypeDegraded).LastTransitionTime),
		"the grace clock must not reset when the phase turns Degraded")
	events := drainEvents(rec)
	require.Len(t, events, 1)
	assert.True(t, strings.HasPrefix(events[0], "Warning ClusterDegraded "), events[0])

	r.reconcileServerShortfall(ctx, refetch(t, r), oneDown, oneDown.message)
	assert.Empty(t, drainEvents(rec), "ClusterDegraded is emitted once per transition")

	// 3. Below a majority: Forming, dependents pause, still remembered as formed.
	twoDown := formationCheck{message: "Cluster forming: 1/3 servers available", servers: availServers(3, 1)}
	r.reconcileServerShortfall(ctx, refetch(t, r), twoDown, twoDown.message)
	c = refetch(t, r)
	assert.Equal(t, neo4jv1beta1.PhaseForming, c.Status.Phase)
	assert.False(t, clusterAcceptsWork(c))
	assert.True(t, clusterHasFormed(c), "ClusterFormed carries the memory through a Forming phase")
	events = drainEvents(rec)
	require.Len(t, events, 1)
	assert.True(t, strings.HasPrefix(events[0], "Warning ClusterQuorumLost "), events[0])

	// 4. A majority back, one server still missing well past the grace:
	// straight to Degraded, not a fresh grace period and not Forming.
	r.reconcileServerShortfall(ctx, refetch(t, r), oneDown, oneDown.message)
	assert.Equal(t, neo4jv1beta1.PhaseDegraded, refetch(t, r).Status.Phase)
}

// A Ready cluster that loses a majority in one step — and that carries no
// ClusterFormed condition yet, as after an operator upgrade — must be
// remembered as formed once its phase reads Forming, or a later partial
// recovery would be mistaken for first formation.
func TestReconcileServerShortfall_QuorumLostFromReadyRecordsFormation(t *testing.T) {
	ctx := context.Background()
	r, _ := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseReady, cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady)))
	f := formationCheck{message: "Cluster forming: 1/3 servers available", servers: availServers(3, 1)}

	r.reconcileServerShortfall(ctx, refetch(t, r), f, f.message)
	c := refetch(t, r)
	assert.Equal(t, neo4jv1beta1.PhaseForming, c.Status.Phase)
	assert.True(t, clusterHasFormed(c))
}

// The informer cache can hand the next reconcile a cluster that does not show
// the phase just written yet. That must not announce the transition again —
// observed live as ClusterDegraded with count 2.
func TestReconcileServerShortfall_StaleCacheDoesNotAnnounceTwice(t *testing.T) {
	ctx := context.Background()
	r, rec := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseReady, cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady)))
	oneDown := formationCheck{message: "Cluster forming: 2/3 servers available", servers: availServers(3, 2)}
	r.reconcileServerShortfall(ctx, refetch(t, r), oneDown, oneDown.message)
	backdateDegraded(t, r, 6*time.Minute)

	stale := refetch(t, r) // phase still Ready
	r.reconcileServerShortfall(ctx, stale.DeepCopy(), oneDown, oneDown.message)
	r.reconcileServerShortfall(ctx, stale.DeepCopy(), oneDown, oneDown.message)
	require.Len(t, drainEvents(rec), 1, "one ClusterDegraded per transition")

	// Live on Kind: the first reconcile after two servers went could not reach
	// Neo4j and wrote a plain Forming; the next found the lost majority with
	// the phase already Forming. It must still be announced.
	r.availabilityAnnounced.Delete("default/c")
	forming := refetch(t, r)
	forming.Status.Phase = neo4jv1beta1.PhaseForming
	twoDown := formationCheck{message: "Cluster forming: 1/3 servers available", servers: availServers(3, 1)}
	r.reconcileServerShortfall(ctx, forming, twoDown, twoDown.message)
	events := drainEvents(rec)
	require.Len(t, events, 1)
	assert.True(t, strings.HasPrefix(events[0], "Warning ClusterQuorumLost "), events[0])

	// Recovery clears the memory, so the next outage is announced again.
	r.availabilityAnnounced.Delete("default/c")
	r.reconcileServerShortfall(ctx, refetch(t, r), twoDown, twoDown.message)
	r.reconcileServerShortfall(ctx, stale.DeepCopy(), twoDown, twoDown.message)
	events = drainEvents(rec)
	require.Len(t, events, 1)
	assert.True(t, strings.HasPrefix(events[0], "Warning ClusterQuorumLost "), events[0])
}

func TestReconcileServerShortfall_FirstFormationStaysForming(t *testing.T) {
	ctx := context.Background()
	r, rec := shortfallReconciler(t, availCluster(""))
	f := formationCheck{message: "Cluster forming: 2/3 servers available", servers: availServers(3, 2)}

	r.reconcileServerShortfall(ctx, refetch(t, r), f, f.message)
	c := refetch(t, r)
	assert.Equal(t, neo4jv1beta1.PhaseForming, c.Status.Phase)
	assert.Nil(t, findCondition(c.Status.Conditions, ConditionTypeDegraded), "nothing is degraded before the cluster has formed")
	events := drainEvents(rec)
	require.Len(t, events, 1)
	assert.True(t, strings.HasPrefix(events[0], "Normal ClusterFormationStarted "), events[0])
}

func TestReconcileServerShortfall_RolloutStaysFormingWithoutFormationEvent(t *testing.T) {
	ctx := context.Background()
	cluster := availCluster(neo4jv1beta1.PhaseReady, cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady))
	r := gateTestReconciler(t, gateTestSTS("neo4j:5.26.0-enterprise", 3, "rev1", "rev2", 1, 2), cluster)
	rec := record.NewFakeRecorder(8)
	r.Recorder = rec
	f := formationCheck{message: "Cluster forming: 2/3 servers available", servers: availServers(3, 2)}

	r.reconcileServerShortfall(ctx, refetch(t, r), f, f.message)
	assert.Equal(t, neo4jv1beta1.PhaseForming, refetch(t, r).Status.Phase, "#262: never Ready mid-rollout")
	assert.Empty(t, drainEvents(rec), "a planned rollout of a formed cluster is not formation starting")
}

// The extra conditions must reach the API server even when nothing else
// changed, without reading as a phase change (which callers announce).
func TestUpdateClusterStatusWritesExtraConditionsSilently(t *testing.T) {
	ctx := context.Background()
	r, _ := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseReady))
	msg := "Neo4j cluster is fully formed and ready"

	require.True(t, r.updateClusterStatusWithVersion(ctx, refetch(t, r), neo4jv1beta1.PhaseReady, msg, ""))
	changed := r.updateClusterStatusWithVersion(ctx, refetch(t, r), neo4jv1beta1.PhaseReady, msg, "",
		clusterFormedCondition(), allServersAvailableCondition(3))
	assert.False(t, changed, "only the conditions moved")
	c := refetch(t, r)
	require.NotNil(t, findCondition(c.Status.Conditions, ConditionTypeClusterFormed))
	assert.Equal(t, metav1.ConditionFalse, findCondition(c.Status.Conditions, ConditionTypeDegraded).Status)
}

// A rolling upgrade must not start while a server is down, even though the
// phase still reads Ready during the grace period.
func TestIsUpgradeRequired_NotWhileAServerIsDown(t *testing.T) {
	sts := gateTestSTS("neo4j:5.26.0-enterprise", 3, "rev1", "rev1", 3, 3)
	cluster := availCluster(neo4jv1beta1.PhaseReady,
		cond(ConditionTypeDegraded, metav1.ConditionTrue, ConditionReasonServersUnavailable))
	cluster.Spec.Image.Tag = "2026.08.1-enterprise"
	r := gateTestReconciler(t, sts, cluster)
	assert.False(t, r.isUpgradeRequired(context.Background(), cluster))

	cluster.Status.Conditions = nil
	assert.True(t, r.isUpgradeRequired(context.Background(), cluster), "control: the same drift on a whole cluster starts the upgrade")
}

// Installing a plugin rolls every server, so it waits for the whole cluster.
func TestPluginWaitsForEveryServer(t *testing.T) {
	cluster := availCluster(neo4jv1beta1.PhaseReady,
		cond(ConditionTypeDegraded, metav1.ConditionTrue, ConditionReasonServersUnavailable))
	r := gateTestReconciler(t, cluster)
	pr := &Neo4jPluginReconciler{Client: r.Client}
	info, err := pr.getTargetDeployment(context.Background(), &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec:       neo4jv1beta1.Neo4jPluginSpec{ClusterRef: "c"},
	})
	require.NoError(t, err)
	assert.False(t, info.IsReady)
}

// A server's container dies: until Kubernetes takes it out of the client
// Service's endpoints the Bolt check can land on it and fail, so the pass has
// no server list. The journey for v1.19.1 saw that write phase Forming once on
// a formed cluster; it must write nothing while a majority of server pods is
// Ready, and still turn Forming once the cluster has been unreachable for the
// grace period.
func TestReconcileServerShortfall_UnreachableWithReadyMajorityWritesNothing(t *testing.T) {
	ctx := context.Background()
	r, rec := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseReady, cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady)))
	for i, ready := range []bool{true, true, false} {
		pod := serverPod(fmt.Sprintf("c-server-%d", i), fmt.Sprintf("u%d", i), ready, 0)
		require.NoError(t, r.Create(ctx, &pod))
	}
	require.Equal(t, 2, r.readyServerPods(ctx, refetch(t, r)))

	unreachable := formationCheck{message: "Waiting for Neo4j to accept connections"}
	r.recordConnectivityFailure(refetch(t, r))
	before := refetch(t, r)
	r.reconcileServerShortfall(ctx, before, unreachable, unreachable.message)
	after := refetch(t, r)
	assert.Equal(t, neo4jv1beta1.PhaseReady, after.Status.Phase)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion, "nothing is written")
	assert.Empty(t, drainEvents(rec))

	// Unreachable for the whole grace period: Forming, as before.
	v, _ := r.connectivityFailures.Load(clusterKey(after))
	streak, ok := v.(*connectivityFailureStreak)
	require.True(t, ok)
	streak.since = time.Now().Add(-DefaultServerUnavailableGrace)
	r.reconcileServerShortfall(ctx, refetch(t, r), unreachable, unreachable.message)
	assert.Equal(t, neo4jv1beta1.PhaseForming, refetch(t, r).Status.Phase)
}
