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

// A scale-down drains every server it removes, also one that is down when it
// starts (operations.md id 213). The v1.23.0 journey (#594): a 5→3
// scale-down issued while c3-server-3's pod was being replaced drained and
// dropped only server-4. SHOW SERVERS listed server-3 with no address and its
// id for a name, so the drain could not tell its ordinal; it released the
// replica hold once server-4 was dropped, the StatefulSet removed server-3's
// pod, and server-3 stayed Enabled/Unavailable until it was dropped by hand.

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// drainNeo4j plays the system database the drain talks to: servers by id,
// each reporting its address only while it is available — and, with
// noDeallocatingAddress, not while it deallocates either.
type drainNeo4j struct {
	servers               map[string]*neo4jclient.ServerInfo
	address               map[string]string
	down                  map[string]bool
	noDeallocatingAddress bool
	ops                   []string
	// refuseDryRun, when set, is Neo4j's refusal of a DRYRUN DEALLOCATE of
	// these ids, in the order the drain gave them.
	refuseDryRun func(ids []string) error
}

func newDrainNeo4j(cluster string, n int, down ...int) *drainNeo4j {
	d := &drainNeo4j{servers: map[string]*neo4jclient.ServerInfo{}, address: map[string]string{}, down: map[string]bool{}}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("uuid-%d", i)
		d.servers[id] = &neo4jclient.ServerInfo{ID: id, Name: id, State: "Enabled", Health: "Available", Hosting: []string{"system", "neo4j"}}
		d.address[id] = fmt.Sprintf("%s-server-%d.%s-headless.default.svc.cluster.local:7687", cluster, i, cluster)
	}
	for _, i := range down {
		d.down[fmt.Sprintf("uuid-%d", i)] = true
	}
	return d
}

func (d *drainNeo4j) ListServersFromLeader(context.Context) ([]neo4jclient.ServerInfo, error) {
	ids := make([]string, 0, len(d.servers))
	for id := range d.servers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]neo4jclient.ServerInfo, 0, len(ids))
	for _, id := range ids {
		s := *d.servers[id]
		switch {
		case d.down[id]:
			s.Health, s.Address = "Unavailable", "" // SHOW SERVERS: NULL
		case d.noDeallocatingAddress && (s.State == "Deallocating" || s.State == "Deallocated"):
			s.Address = ""
		default:
			s.Address = d.address[id]
		}
		out = append(out, s)
	}
	return out, nil
}
func (d *drainNeo4j) MinimumSystemPrimaries(context.Context) int { return 3 }
func (d *drainNeo4j) CordonServer(_ context.Context, id string) error {
	d.ops = append(d.ops, "cordon "+id)
	d.servers[id].State = "Cordoned"
	return nil
}

// EnableServer is ENABLE SERVER: a cordoned (or enabled) server is Enabled
// afterwards; Neo4j refuses it for one deallocating or past it.
func (d *drainNeo4j) EnableServer(_ context.Context, id string) error {
	d.ops = append(d.ops, "enable "+id)
	switch d.servers[id].State {
	case "Enabled", "Cordoned":
		d.servers[id].State = "Enabled"
		return nil
	}
	return fmt.Errorf("server %s is %s and cannot be enabled", id, d.servers[id].State)
}
func (d *drainNeo4j) DeallocateServers(_ context.Context, ids []string, dryRun bool) error {
	if dryRun {
		if d.refuseDryRun != nil {
			return d.refuseDryRun(ids)
		}
		return nil
	}
	for _, id := range ids {
		d.ops = append(d.ops, "deallocate "+id)
		d.servers[id].State, d.servers[id].Hosting = "Deallocating", []string{"system"}
	}
	return nil
}
func (d *drainNeo4j) DropServer(_ context.Context, id string) error {
	d.ops = append(d.ops, "drop "+id)
	d.servers[id].State = "Dropped"
	return nil
}
func (d *drainNeo4j) Close() error { return nil }

// scalingDown is a 5-server cluster scaled to 3, the drain's reconciler and
// its Neo4j.
func scalingDown(t *testing.T, name string, neo *drainNeo4j, diagnostics []neo4jv1beta1.ServerDiagnosticInfo) (*Neo4jEnterpriseClusterReconciler, client.Client, *neo4jv1beta1.Neo4jEnterpriseCluster) {
	t.Helper()
	c := clusterForUpgrade(name, "default", holdRunning, holdRunning, 3)
	c.Status.Phase = neo4jv1beta1.PhaseReady
	if diagnostics != nil {
		c.Status.Diagnostics = &neo4jv1beta1.ClusterDiagnosticsStatus{Servers: diagnostics}
	}
	sts := serverSTSForUpgrade(name, "default", "neo4j:"+holdRunning, 5)
	r, fc, _ := repairReconciler(t, c, sts)
	r.Recorder = record.NewFakeRecorder(50)
	r.drainClient = func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) (scaleDownClient, error) { return neo, nil }
	forgetServerAddresses(c)
	t.Cleanup(func() { forgetServerAddresses(c) })
	return r, fc, c
}

// drainUntilReleased runs the drain until it releases the replica hold, or
// passes run out; it reports whether it released.
func drainUntilReleased(t *testing.T, r *Neo4jEnterpriseClusterReconciler, fc client.Client, c *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	t.Helper()
	for pass := 0; pass < 12; pass++ {
		require.NoError(t, r.reconcileScaleDownDrain(context.Background(), latestCluster(t, fc, c)))
		if !scaleDownDrainInProgress(latestCluster(t, fc, c)) {
			return true
		}
	}
	return false
}

func TestScaleDownDrain_AServerDownWhenItStartsIsDrainedToo(t *testing.T) {
	t.Run("its address from the diagnostics", func(t *testing.T) {
		// Server-1, which the scale-down keeps, is down too: only the addresses
		// they last reported tell the two apart.
		neo := newDrainNeo4j("ghost-a", 5, 1, 3)
		r, fc, c := scalingDown(t, "ghost-a", neo, []neo4jv1beta1.ServerDiagnosticInfo{
			{Name: "uuid-1", Address: neo.address["uuid-1"], State: "Enabled", Health: "Available"},
			{Name: "uuid-3", Address: neo.address["uuid-3"], State: "Enabled", Health: "Available"},
		})
		require.True(t, drainUntilReleased(t, r, fc, c))
		assert.Equal(t, "Dropped", neo.servers["uuid-3"].State, "server-3 is drained although it was down")
		assert.Equal(t, "Dropped", neo.servers["uuid-4"].State)
		for i := 0; i < 3; i++ {
			assert.Equal(t, "Enabled", neo.servers[fmt.Sprintf("uuid-%d", i)].State, "server-%d is kept", i)
		}
	})

	t.Run("by elimination: every kept server reports its address", func(t *testing.T) {
		neo := newDrainNeo4j("ghost-b", 5, 3) // no diagnostics: an operator restart, monitoring off
		r, fc, c := scalingDown(t, "ghost-b", neo, nil)
		require.True(t, drainUntilReleased(t, r, fc, c))
		assert.Equal(t, "Dropped", neo.servers["uuid-3"].State)
		assert.Equal(t, "Dropped", neo.servers["uuid-4"].State)
	})

	t.Run("a kept server down too: it cannot tell, and holds", func(t *testing.T) {
		neo := newDrainNeo4j("ghost-c", 5, 1, 3)
		r, fc, c := scalingDown(t, "ghost-c", neo, nil)
		assert.False(t, drainUntilReleased(t, r, fc, c), "the replicas stay held")
		assert.Equal(t, "Dropped", neo.servers["uuid-4"].State, "the server it can name is drained")
		assert.Equal(t, "Enabled", neo.servers["uuid-1"].State, "nothing it cannot name is touched")
		assert.Equal(t, "Enabled", neo.servers["uuid-3"].State)
		pending := findCondition(latestCluster(t, fc, c).Status.Conditions, ConditionTypeServersPendingDrain)
		require.NotNil(t, pending)
		assert.Equal(t, metav1.ConditionTrue, pending.Status)
		assert.Contains(t, pending.Message, "no server reports being ghost-c-server-1")

		// server-1 comes back: the rest is named, and drained.
		delete(neo.down, "uuid-1")
		require.True(t, drainUntilReleased(t, r, fc, c))
		assert.Equal(t, "Dropped", neo.servers["uuid-3"].State)
		assert.Equal(t, "Enabled", neo.servers["uuid-1"].State)
	})
}

func TestUnidentifiedRemovedServerIDs(t *testing.T) {
	row := func(id, addr, state string) neo4jclient.ServerInfo {
		return neo4jclient.ServerInfo{ID: id, Name: id, Address: addr, State: state}
	}
	a := func(n int) string { return fmt.Sprintf("c-server-%d.c-headless.default.svc.cluster.local:7687", n) }
	kept := []neo4jclient.ServerInfo{row("u0", a(0), "Enabled"), row("u1", a(1), "Enabled"), row("u2", a(2), "Enabled")}

	ids, why := unidentifiedRemovedServerIDs(append(kept, row("u3", "", "Enabled"), row("u4", a(4), "Enabled")), "c", 3)
	assert.Equal(t, []string{"u3"}, ids)
	assert.Empty(t, why)

	ids, why = unidentifiedRemovedServerIDs(append(kept, row("u4", a(4), "Enabled")), "c", 3)
	assert.Empty(t, ids, "every row named")
	assert.Empty(t, why)

	ids, why = unidentifiedRemovedServerIDs(append(kept, row("old", "", "Dropped")), "c", 3)
	assert.Empty(t, ids, "a dropped server is not drained again")
	assert.Empty(t, why)

	ids, why = unidentifiedRemovedServerIDs([]neo4jclient.ServerInfo{row("u0", a(0), "Enabled"), row("u1", "", "Enabled"),
		row("u2", a(2), "Enabled"), row("u3", "", "Enabled")}, "c", 3)
	assert.Empty(t, ids, "server-1 could be either")
	assert.Contains(t, why, "u1, u3")
	assert.Contains(t, why, "c-server-1")
}
