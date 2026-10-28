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

// The Degraded condition and its message agree (operations.md id 225). The
// walk of the #606 fixes (#608): for about ten seconds after a completed
// scale-down drain, Degraded read True (ServersUnavailable) "3 of 3 servers
// available". The formation check did not pass while the removed servers'
// pods were terminating, and the shortfall assessment, finding every server
// the spec keeps available and every one of their pods ready, still wrote a
// shortfall.

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

func TestReconcileServerShortfall_AfterADrainNothingIsShort(t *testing.T) {
	ctx := context.Background()
	whole := allServersAvailableCondition(3)
	whole.LastTransitionTime = metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	start := availCluster(neo4jv1beta1.PhaseReady, cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady), whole)
	start.Status.Message = "Cluster is ready"
	r, rec := shortfallReconciler(t, start)

	// The scale-down 5→3 just ended: servers 0–2 ready, the removed servers'
	// pods terminating, and SHOW SERVERS still listing them Dropped.
	for i := range 5 {
		pod := serverPod("c-server-"+strconv.Itoa(i), "u"+strconv.Itoa(i), true, 0)
		if i >= 3 {
			pod.Finalizers = []string{"test/hold"}
		}
		require.NoError(t, r.Create(ctx, &pod))
		if i >= 3 {
			require.NoError(t, r.Delete(ctx, &pod))
		}
	}
	servers := append(availServers(3, 3),
		availServer("c-server-3", "Dropped", "Available"), availServer("c-server-4", "Dropped", "Available"))

	for _, detail := range []string{
		"Too many connection failures (2/5) to determine cluster status",
		"Cluster formation in progress: 3/5 servers have full view",
		"Waiting for every server pod to be updated and ready: 3/3 pods updated, 3/3 ready",
	} {
		r.reconcileServerShortfall(ctx, refetch(t, r), formationCheck{message: detail, servers: servers}, detail)
		c := refetch(t, r)
		assert.Equal(t, neo4jv1beta1.PhaseReady, c.Status.Phase, detail)
		assert.Equal(t, "Cluster is ready", c.Status.Message, detail)
		degraded := findCondition(c.Status.Conditions, ConditionTypeDegraded)
		require.NotNil(t, degraded)
		assert.Equal(t, metav1.ConditionFalse, degraded.Status, "no server is unavailable: %s", detail)
		assert.Equal(t, "All 3 servers are available", degraded.Message)
		assert.True(t, whole.LastTransitionTime.Equal(&degraded.LastTransitionTime), "nothing written: %s", detail)
	}
	assert.Empty(t, drainEvents(rec))

	// A server the spec keeps that is down still is a shortfall, named.
	servers[1].Health = "Unavailable"
	r.reconcileServerShortfall(ctx, refetch(t, r), formationCheck{message: "x", servers: servers}, "x")
	degraded := findCondition(refetch(t, r).Status.Conditions, ConditionTypeDegraded)
	assert.Equal(t, metav1.ConditionTrue, degraded.Status)
	assert.Equal(t, "2 of 3 servers available; unavailable: c-server-1 (Unavailable)", degraded.Message)
}

func TestAssessServerShortfall_NoneShortAndCordonedMembers(t *testing.T) {
	now := time.Now()
	in := func(servers []neo4jclient.ServerInfo, expected int, notReady ...string) shortfallInput {
		return shortfallInput{Formed: true, Servers: servers, Expected: expected, NotReadyPods: notReady, Now: now, Grace: time.Minute}
	}
	cordoned := func(pod, health string) neo4jclient.ServerInfo { return availServer(pod, "Cordoned", health) }

	assert.Equal(t, verdictNoneShort, assessServerShortfall(in(availServers(3, 3), 3)).Verdict,
		"every server available and every pod ready: nothing short")
	assert.Equal(t, verdictWithinGrace, assessServerShortfall(in(availServers(3, 3), 3, "c-server-2 (terminating)")).Verdict,
		"a pod the spec keeps terminating is a shortfall its message names")

	withCordoned := append(availServers(4, 4), cordoned("c-server-4", "Available"))
	assert.Equal(t, verdictNoneShort, assessServerShortfall(in(withCordoned, 5)).Verdict,
		"a cordoned server is a member: five of five, not a scale-up")
	down := append(availServers(4, 4), cordoned("c-server-4", "Unavailable"))
	a := assessServerShortfall(in(down, 5))
	assert.Equal(t, verdictWithinGrace, a.Verdict, "a cordoned server that is down is a server short, not one joining")
	assert.Equal(t, []string{"c-server-4 (Unavailable)"}, a.Unavailable)
	deallocating := append(availServers(4, 4), availServer("c-server-4", "Deallocating", "Available"))
	assert.Equal(t, verdictForming, assessServerShortfall(in(deallocating, 5)).Verdict,
		"a deallocating server is no member: four of five")
}

// During an upgrade the Degraded condition counts a cordoned server as the
// normal pass does.
func TestRecordShortfallDuringUpgrade_CountsACordonedServer(t *testing.T) {
	ctx := context.Background()
	cluster := availCluster(neo4jv1beta1.PhaseReady)
	r := gateTestReconciler(t, gateTestSTS("neo4j:5.26.0-enterprise", 3, "rev1", "rev1", 3, 3), cluster)

	servers := append(availServers(2, 2), availServer("c-server-2", "Cordoned", "Unavailable"))
	r.recordShortfallDuringUpgrade(ctx, refetch(t, r), servers)
	d := findCondition(refetch(t, r).Status.Conditions, ConditionTypeDegraded)
	require.NotNil(t, d)
	assert.Equal(t, metav1.ConditionTrue, d.Status)
	assert.Equal(t, "2 of 3 servers available; unavailable: c-server-2 (Unavailable)", d.Message)

	servers[2].Health = "Available"
	r.recordShortfallDuringUpgrade(ctx, refetch(t, r), servers)
	d = findCondition(refetch(t, r).Status.Conditions, ConditionTypeDegraded)
	assert.Equal(t, metav1.ConditionFalse, d.Status)
	assert.Equal(t, "All 3 servers are available", d.Message)
}
