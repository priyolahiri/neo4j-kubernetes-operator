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

// A scale-down set back before its drain ended gives back what the drain took
// (operations.md id 224). The walk of the #606 fixes (#608, 3 × 2026.08.1):
// 3→5, then 5→3 and at once back to 5 — ServersPendingDrain read "Scale-down
// not in progress" and the replicas stayed 5, but server-3 and server-4, which
// the drain had cordoned, stayed Cordoned, and the cluster sat in Forming with
// "Cluster formation verification incomplete" until a later scale-down
// dropped them.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// setServers patches spec.topology.servers, as kubectl does.
func setServers(t *testing.T, fc client.Client, c *neo4jv1beta1.Neo4jEnterpriseCluster, n int32) {
	t.Helper()
	latest := latestCluster(t, fc, c)
	require.NoError(t, patchObject(context.Background(), fc, latest, func() { latest.Spec.Topology.Servers = n }))
}

// drainPass runs one drain pass on the cluster as stored.
func drainPass(t *testing.T, r *Neo4jEnterpriseClusterReconciler, fc client.Client, c *neo4jv1beta1.Neo4jEnterpriseCluster) {
	t.Helper()
	require.NoError(t, r.reconcileScaleDownDrain(context.Background(), latestCluster(t, fc, c)))
}

// opsOf returns the drain's statements of one kind ("enable", "cordon"…),
// sorted: the drain takes a step's servers in no fixed order.
func opsOf(neo *drainNeo4j, kind string) []string {
	var out []string
	for _, op := range neo.ops {
		if strings.HasPrefix(op, kind+" ") {
			out = append(out, op)
		}
	}
	sort.Strings(out)
	return out
}

func events(rec record.EventRecorder) []string {
	fr, ok := rec.(*record.FakeRecorder)
	if !ok {
		return nil
	}
	var out []string
	for {
		select {
		case e := <-fr.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// statesOf is each server's state in SHOW SERVERS, by id.
func statesOf(neo *drainNeo4j) map[string]string {
	out := map[string]string{}
	for id, s := range neo.servers {
		out[id] = s.State
	}
	return out
}

func TestScaleDownDrain_ASetBackEnablesWhatTheDrainCordoned(t *testing.T) {
	neo := newDrainNeo4j("sb", 5)
	r, fc, c := scalingDown(t, "sb", neo, nil)

	// 5→3: the first pass cordons server-3 and server-4, and records them
	// before it does.
	drainPass(t, r, fc, c)
	require.Equal(t, []string{"cordon uuid-3", "cordon uuid-4"}, opsOf(neo, "cordon"))
	assert.Equal(t, []string{"uuid-3", "uuid-4"}, cordonedByDrain(latestCluster(t, fc, c)))
	_ = events(r.Recorder)
	beforeSetBack := latestCluster(t, fc, c)

	// …and at once back to 5.
	setServers(t, fc, c, 5)
	drainPass(t, r, fc, c)

	assert.Equal(t, []string{"enable uuid-3", "enable uuid-4"}, opsOf(neo, "enable"), "the drain gives back what it cordoned")
	for i := 0; i < 5; i++ {
		assert.Equal(t, "Enabled", neo.servers[fmt.Sprintf("uuid-%d", i)].State, "server-%d", i)
	}
	stored := latestCluster(t, fc, c)
	assert.False(t, scaleDownDrainInProgress(stored), "the hold is released")
	assert.Empty(t, cordonedByDrain(stored), "the record goes")
	pending := pendingDrain(t, fc, c)
	require.NotNil(t, pending)
	assert.Equal(t, metav1.ConditionFalse, pending.Status)
	assert.Equal(t, "Scale-down not in progress", pending.Message)
	got := events(r.Recorder)
	require.Len(t, got, 1)
	assert.Equal(t, "Normal ScaleDownSetBack Scale-down set back: enabled 2 server(s) the drain had cordoned: "+
		"sb-server-3 (uuid-3), sb-server-4 (uuid-4)", got[0])

	// Idempotent: the passes after it, and one on a copy of the cluster from
	// before the set-back's writes (its records still there), enable nothing
	// again and announce nothing.
	for range repeatPasses {
		drainPass(t, r, fc, c)
	}
	stale := beforeSetBack.DeepCopy()
	stale.Spec.Topology.Servers = 5
	require.NoError(t, r.reconcileScaleDownDrain(context.Background(), stale))
	assert.Len(t, opsOf(neo, "enable"), 2, "each server is enabled once")
	assert.Len(t, opsOf(neo, "cordon"), 2)
	assert.Empty(t, events(r.Recorder))
	assert.Empty(t, cordonedByDrain(latestCluster(t, fc, c)))

	// The cluster is formed again: every server the spec asks for is a member.
	rows, err := neo.ListServersFromLeader(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 5, countAvailableServers(rows))
}

// A server a user cordoned by hand — before the scale-down, a removed one or a
// kept one — is never in the record, and a set-back leaves it cordoned. The
// cluster is formed with it: a cordoned server is a member.
func TestScaleDownDrain_ASetBackLeavesAServerCordonedByHand(t *testing.T) {
	neo := newDrainNeo4j("hand", 5)
	neo.servers["uuid-4"].State = "Cordoned" // removed by the scale-down, cordoned by hand
	neo.servers["uuid-1"].State = "Cordoned" // kept, cordoned by hand
	r, fc, c := scalingDown(t, "hand", neo, nil)

	drainPass(t, r, fc, c)
	require.Equal(t, []string{"cordon uuid-3"}, opsOf(neo, "cordon"), "a cordoned server is not cordoned again")
	assert.Equal(t, []string{"uuid-3"}, cordonedByDrain(latestCluster(t, fc, c)))

	setServers(t, fc, c, 5)
	for range repeatPasses {
		drainPass(t, r, fc, c)
	}
	assert.Equal(t, []string{"enable uuid-3"}, opsOf(neo, "enable"))
	assert.Equal(t, "Enabled", neo.servers["uuid-3"].State)
	assert.Equal(t, "Cordoned", neo.servers["uuid-4"].State, "cordoned by hand: left as it is")
	assert.Equal(t, "Cordoned", neo.servers["uuid-1"].State, "cordoned by hand: left as it is")
	assert.False(t, scaleDownDrainInProgress(latestCluster(t, fc, c)))
	assert.Equal(t, "Scale-down not in progress", pendingDrain(t, fc, c).Message)

	// Formed: the formation check counts five of five, the split-brain views
	// agree, and the shortfall assessment finds nothing short.
	rows, err := neo.ListServersFromLeader(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 5, countAvailableServers(rows))
	views := make([]ClusterView, 0, 5)
	for i := 0; i < 5; i++ {
		views = append(views, ClusterView{ServerPodName: fmt.Sprintf("hand-server-%d", i), Servers: rows})
	}
	analysis := (&SplitBrainDetector{}).analyzeClusterViews(views, 5)
	assert.Equal(t, RepairActionNone, analysis.RepairAction, analysis.ErrorMessage)
	a := assessServerShortfall(shortfallInput{Formed: true, Servers: rows, Expected: 5})
	assert.Equal(t, verdictNoneShort, a.Verdict, "not a scale-up, not a shortfall")
}

// A set-back that comes after the drain took a server past what Neo4j can
// undo — deallocating, or dropped and still running in a pod the StatefulSet
// keeps — is refused: nothing is enabled, the replicas stay held, and
// ServersPendingDrain says what lets the drain finish. Lowering the spec again
// lets it.
func TestScaleDownDrain_ASetBackTooLateIsRefused(t *testing.T) {
	t.Run("deallocating, with no address in SHOW SERVERS", func(t *testing.T) {
		neo := newDrainNeo4j("late", 5)
		neo.noDeallocatingAddress = true
		r, fc, c := scalingDown(t, "late", neo, nil)
		drainPass(t, r, fc, c) // cordon
		drainPass(t, r, fc, c) // deallocate
		require.Equal(t, "Deallocating", neo.servers["uuid-3"].State)
		_ = events(r.Recorder)

		setServers(t, fc, c, 5)
		for range repeatPasses {
			drainPass(t, r, fc, c)
		}
		assert.Empty(t, opsOf(neo, "enable"))
		assert.Empty(t, opsOf(neo, "drop"), "nothing more is drained while the spec keeps them")
		stored := latestCluster(t, fc, c)
		assert.True(t, scaleDownDrainInProgress(stored), "the replicas stay held")
		pending := pendingDrain(t, fc, c)
		require.NotNil(t, pending)
		assert.Equal(t, metav1.ConditionTrue, pending.Status)
		assert.Equal(t, ConditionReasonScaleDownBlocked, pending.Reason)
		assert.Equal(t, "Scale-down set back too late for late-server-3 (uuid-3) (Deallocating), late-server-4 (uuid-4) (Deallocating): "+
			"Neo4j cannot enable a server again once it deallocates, and a dropped server cannot rejoin. "+
			"Set spec.topology.servers to 3 so the drain can finish them, then raise it to add new servers in their place. "+
			"Replicas are held at 5.", pending.Message)
		got := events(r.Recorder)
		require.Len(t, got, 1, "announced once")
		assert.True(t, strings.HasPrefix(got[0], "Warning ScaleDownBlocked Scale-down set back too late"), got[0])

		// Lowered again: the drain finishes.
		setServers(t, fc, c, 3)
		require.True(t, drainUntilReleased(t, r, fc, c))
		assert.Equal(t, "Dropped", neo.servers["uuid-3"].State)
		assert.Equal(t, "Dropped", neo.servers["uuid-4"].State)
		assert.Empty(t, cordonedByDrain(latestCluster(t, fc, c)))
		assert.Equal(t, "Scale-down drain complete", pendingDrain(t, fc, c).Message)
	})

	t.Run("a server the drain never took is not the set-back's to refuse", func(t *testing.T) {
		neo := newDrainNeo4j("own", 5)
		r, fc, c := scalingDown(t, "own", neo, nil)
		drainPass(t, r, fc, c)                                                                          // cordons 3 and 4
		neo.servers["uuid-2"].State, neo.servers["uuid-2"].Hosting = "Deallocating", []string{"system"} // by hand

		setServers(t, fc, c, 5)
		drainPass(t, r, fc, c)
		assert.Equal(t, []string{"enable uuid-3", "enable uuid-4"}, opsOf(neo, "enable"))
		assert.False(t, scaleDownDrainInProgress(latestCluster(t, fc, c)))
		assert.Equal(t, "Scale-down not in progress", pendingDrain(t, fc, c).Message)
	})

	t.Run("one dropped and still running, one deallocating", func(t *testing.T) {
		neo := newDrainNeo4j("gone", 5)
		r, fc, c := scalingDown(t, "gone", neo, nil)
		drainPass(t, r, fc, c)
		drainPass(t, r, fc, c)
		neo.servers["uuid-4"].State = "Dropped" // the drain dropped it; its pod still runs it

		setServers(t, fc, c, 5)
		drainPass(t, r, fc, c)
		assert.Empty(t, opsOf(neo, "enable"))
		msg := pendingDrain(t, fc, c).Message
		assert.Contains(t, msg, "gone-server-4 (uuid-4) (Dropped)")
		assert.Contains(t, msg, "gone-server-3 (uuid-3) (Deallocating)")
		assert.Contains(t, msg, "Set spec.topology.servers to 3")
		assert.True(t, scaleDownDrainInProgress(latestCluster(t, fc, c)))
	})
}

// A scale-down set back part of the way (5→3, then 4) gives back the server
// the spec keeps again and drains the one it still removes; too late for the
// kept one, it is refused.
func TestScaleDownDrain_APartialSetBack(t *testing.T) {
	t.Run("in time: server-3 given back, server-4 drained", func(t *testing.T) {
		neo := newDrainNeo4j("part", 5)
		r, fc, c := scalingDown(t, "part", neo, nil)
		drainPass(t, r, fc, c) // cordons 3 and 4

		setServers(t, fc, c, 4)
		require.True(t, drainUntilReleased(t, r, fc, c))
		assert.Equal(t, []string{"enable uuid-3"}, opsOf(neo, "enable"))
		assert.Equal(t, "Enabled", neo.servers["uuid-3"].State, "kept by the spec, given back")
		assert.Equal(t, "Dropped", neo.servers["uuid-4"].State)
		assert.Empty(t, cordonedByDrain(latestCluster(t, fc, c)))
		assert.Equal(t, "Scale-down drain complete", pendingDrain(t, fc, c).Message)
	})

	t.Run("too late: server-3 deallocating", func(t *testing.T) {
		neo := newDrainNeo4j("part2", 5)
		r, fc, c := scalingDown(t, "part2", neo, nil)
		drainPass(t, r, fc, c)
		drainPass(t, r, fc, c) // both deallocating

		setServers(t, fc, c, 4)
		for range repeatPasses {
			drainPass(t, r, fc, c)
		}
		assert.Empty(t, opsOf(neo, "drop"), "nothing is dropped while the spec keeps server-3")
		assert.True(t, scaleDownDrainInProgress(latestCluster(t, fc, c)))
		msg := pendingDrain(t, fc, c).Message
		assert.Contains(t, msg, "part2-server-3 (uuid-3) (Deallocating)")
		assert.NotContains(t, msg, "server-4", "server-4 is still removed: the drain may finish it")
		assert.Contains(t, msg, "Set spec.topology.servers to 3")
	})
}

// While Neo4j cannot be asked, a set-back keeps the drain's records, and gives
// back what the drain took once it can.
func TestScaleDownDrain_ASetBackWaitsUntilNeo4jCanBeAsked(t *testing.T) {
	neo := newDrainNeo4j("wait", 5)
	r, fc, c := scalingDown(t, "wait", neo, nil)
	drainPass(t, r, fc, c)

	setServers(t, fc, c, 5)
	r.drainClient = func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) (scaleDownClient, error) {
		return nil, errors.New("connection refused")
	}
	drainPass(t, r, fc, c)
	stored := latestCluster(t, fc, c)
	assert.Equal(t, []string{"uuid-3", "uuid-4"}, cordonedByDrain(stored), "the record stays")
	assert.True(t, scaleDownDrainInProgress(stored))
	assert.Equal(t, metav1.ConditionTrue, pendingDrain(t, fc, c).Status, "nothing is settled before Neo4j answers")

	r.drainClient = func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) (scaleDownClient, error) { return neo, nil }
	drainPass(t, r, fc, c)
	assert.Equal(t, map[string]string{"uuid-0": "Enabled", "uuid-1": "Enabled", "uuid-2": "Enabled", "uuid-3": "Enabled", "uuid-4": "Enabled"}, statesOf(neo))
	assert.False(t, scaleDownDrainInProgress(latestCluster(t, fc, c)))
	assert.Equal(t, "Scale-down not in progress", pendingDrain(t, fc, c).Message)
}

// A cluster that is not scaling down, with nothing on record, asks Neo4j
// nothing on its passes.
func TestScaleDownDrain_NoDrainOnRecordAsksNeo4jNothing(t *testing.T) {
	neo := newDrainNeo4j("idle", 5)
	r, fc, c := scalingDown(t, "idle", neo, nil)
	setServers(t, fc, c, 5)
	asked := 0
	r.drainClient = func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) (scaleDownClient, error) {
		asked++
		return neo, nil
	}
	for range repeatPasses {
		drainPass(t, r, fc, c)
	}
	assert.Zero(t, asked)
	assert.Nil(t, pendingDrain(t, fc, c))
}

func TestServersPastUndoing(t *testing.T) {
	row := func(id string, ord int, state string) neo4jclient.ServerInfo {
		addr := ""
		if ord >= 0 {
			addr = fmt.Sprintf("c-server-%d.c-headless.default.svc.cluster.local:7687", ord)
		}
		return neo4jclient.ServerInfo{ID: id, Name: id, Address: addr, State: state, Health: "Available"}
	}
	servers := []neo4jclient.ServerInfo{
		row("u0", 0, "Enabled"), row("u3", 3, "Deallocating"), row("u4", 4, "Dropped"),
		row("old1", 1, "Deallocated"), row("u1", 1, "Enabled"), // an earlier identity of server-1
		row("ghost", -1, "Deallocating"),
		row("u2", 2, "Deallocating"), // deallocated by hand: the drain never took it
	}
	reported := []neo4jclient.ServerInfo{servers[0], servers[1], servers[2], row("old1", -1, "Deallocated"), servers[4], servers[5], servers[6]}
	touched := map[string]bool{"u3": true, "u4": true, "old1": true, "ghost": true}

	ids := func(past []neo4jclient.ServerInfo) []string {
		var out []string
		for _, s := range past {
			out = append(out, s.ID)
		}
		return out
	}
	assert.Equal(t, []string{"u3", "u4"}, ids(serversPastUndoing(servers, reported, "c", 5, touched, false)),
		"kept pods: the one in server-1's place is an earlier identity, not its server; server-2 the drain never took")
	assert.Empty(t, ids(serversPastUndoing(servers, reported, "c", 3, touched, false)), "server-3 and server-4 are removed: progress, not a refusal")
	assert.Equal(t, []string{"u3", "u4", "ghost"}, ids(serversPastUndoing(servers, reported, "c", 5, touched, true)),
		"a server of the drain with no ordinal known, every pod kept")
}
