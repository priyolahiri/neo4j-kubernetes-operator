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

// Tests for the shard-level status of a Neo4jShardedDatabase: graphShard,
// propertyShards, virtualDatabase and creationTime, derived from the SHOW
// DATABASES rows updateShardStatus already fetches.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// row is one SHOW DATABASES row: a database has one per server hosting a copy.
func row(name, status, requested string) neo4j.DatabaseInfo {
	return neo4j.DatabaseInfo{Name: name, Status: status, RequestedStatus: requested}
}

func TestObserveShardFamily_ReadsTheFamilyAndIgnoresLookalikes(t *testing.T) {
	dbs := []neo4j.DatabaseInfo{
		// the family of "orders": parent + graph shard + 2 property shards,
		// each with one row per hosting server
		row("orders", "online", "online"), row("orders", "online", "online"), row("orders", "online", "online"),
		row("orders-g000", "online", "online"), row("orders-g000", "online", "online"), row("orders-g000", "online", "online"),
		row("orders-p001", "online", "online"), row("orders-p001", "online", "online"),
		row("orders-p000", "online", "online"), row("orders-p000", "online", "online"),
		// things that merely share a prefix or look like shards of another
		// database -- none may leak into the status
		row("orders2", "online", "online"),
		row("orders2-p000", "online", "online"),
		row("orders-p000-backup", "online", "online"),
		row("orders-archive", "online", "online"),
		row("neo4j", "online", "online"),
		row("system", "online", "online"),
	}

	obs := observeShardFamily("orders", dbs)

	require.NotNil(t, obs.GraphShard)
	assert.Equal(t, neo4jv1beta1.ShardStatus{
		Name: "orders-g000", Type: "graph", State: "online", Ready: true,
	}, *obs.GraphShard)

	require.Len(t, obs.PropertyShards, 2)
	// ordered by index regardless of the order SHOW DATABASES returned them in
	assert.Equal(t, "orders-p000", obs.PropertyShards[0].Name)
	assert.Equal(t, "orders-p001", obs.PropertyShards[1].Name)
	for i, ps := range obs.PropertyShards {
		assert.Equal(t, "property", ps.Type)
		assert.Equal(t, "online", ps.State)
		assert.True(t, ps.Ready)
		require.NotNil(t, ps.PropertyShardIndex)
		assert.Equal(t, int32(i), *ps.PropertyShardIndex)
	}

	require.NotNil(t, obs.VirtualDatabase)
	assert.Equal(t, neo4jv1beta1.VirtualDatabaseStatus{Name: "orders", Ready: true}, *obs.VirtualDatabase)
	assert.True(t, obs.Observed)
}

func TestObserveShardFamily_ReadinessAcrossCopies(t *testing.T) {
	t.Run("a copy that should be online but is not makes the shard not ready", func(t *testing.T) {
		obs := observeShardFamily("db", []neo4j.DatabaseInfo{
			row("db-g000", "online", "online"),
			row("db-p000", "online", "online"),
			row("db-p000", "store copying", "online"),
		})
		require.Len(t, obs.PropertyShards, 1)
		assert.False(t, obs.PropertyShards[0].Ready)
		assert.Equal(t, "store copying", obs.PropertyShards[0].State,
			"the non-online state is reported rather than hidden behind the healthy copy")
		assert.True(t, obs.GraphShard.Ready, "the other shards are unaffected")
	})

	t.Run("a deliberately stopped copy does not, while another copy serves", func(t *testing.T) {
		obs := observeShardFamily("db", []neo4j.DatabaseInfo{
			row("db-g000", "online", "online"),
			row("db-g000", "offline", "offline"),
		})
		assert.True(t, obs.GraphShard.Ready)
		assert.Equal(t, "offline", obs.GraphShard.State)
	})

	t.Run("no copy online means not ready even if none is meant to be", func(t *testing.T) {
		obs := observeShardFamily("db", []neo4j.DatabaseInfo{
			row("db-g000", "offline", "offline"),
		})
		assert.False(t, obs.GraphShard.Ready)
		assert.Equal(t, "offline", obs.GraphShard.State)
	})

	t.Run("state is deterministic when several copies are off", func(t *testing.T) {
		a := observeShardFamily("db", []neo4j.DatabaseInfo{
			row("db-g000", "starting", "online"), row("db-g000", "initial", "online"),
		})
		b := observeShardFamily("db", []neo4j.DatabaseInfo{
			row("db-g000", "initial", "online"), row("db-g000", "starting", "online"),
		})
		assert.Equal(t, a.GraphShard.State, b.GraphShard.State,
			"row order from SHOW DATABASES must not make the status flap")
	})
}

func TestObserveShardFamily_NothingObserved(t *testing.T) {
	obs := observeShardFamily("orders", []neo4j.DatabaseInfo{row("neo4j", "online", "online"), row("system", "online", "online")})
	assert.Nil(t, obs.GraphShard)
	assert.Empty(t, obs.PropertyShards)
	assert.Nil(t, obs.VirtualDatabase)
	assert.False(t, obs.Observed, "no graph shard yet: nothing created that the operator can see")
}

func newShardedForStatus(name string) *neo4jv1beta1.Neo4jShardedDatabase {
	return &neo4jv1beta1.Neo4jShardedDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec:       neo4jv1beta1.Neo4jShardedDatabaseSpec{Name: "orders"},
	}
}

func newShardedReconciler(sd *neo4jv1beta1.Neo4jShardedDatabase) *Neo4jShardedDatabaseReconciler {
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(sd).WithStatusSubresource(sd).Build()
	return &Neo4jShardedDatabaseReconciler{Client: fc, Recorder: record.NewFakeRecorder(10)}
}

func getSharded(t *testing.T, r *Neo4jShardedDatabaseReconciler, name string) *neo4jv1beta1.Neo4jShardedDatabase {
	t.Helper()
	latest := &neo4jv1beta1.Neo4jShardedDatabase{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, latest))
	return latest
}

func familyRows(shardState string) []neo4j.DatabaseInfo {
	return []neo4j.DatabaseInfo{
		row("orders", "online", "online"),
		row("orders-g000", "online", "online"),
		row("orders-p000", shardState, "online"),
	}
}

// TestApplyShardStatus_PopulatesAndStaysQuiet: the fields are written from the
// observation, and an unchanged observation writes NOTHING -- a status write
// is a watch event, which reconciles, which (with no guard) writes again.
func TestApplyShardStatus_PopulatesAndStaysQuiet(t *testing.T) {
	sd := newShardedForStatus("populate")
	r := newShardedReconciler(sd)
	ctx := context.Background()

	obs := observeShardFamily("orders", familyRows("online"))
	require.NoError(t, r.applyShardStatus(ctx, sd, obs, false))

	got := getSharded(t, r, "populate")
	require.NotNil(t, got.Status.GraphShard)
	assert.Equal(t, "orders-g000", got.Status.GraphShard.Name)
	require.Len(t, got.Status.PropertyShards, 1)
	assert.True(t, got.Status.PropertyShards[0].Ready)
	require.NotNil(t, got.Status.VirtualDatabase)
	assert.Equal(t, "orders", got.Status.VirtualDatabase.Name)
	require.NotNil(t, got.Status.CreationTime)
	assert.WithinDuration(t, time.Now(), got.Status.CreationTime.Time, 10*time.Second)
	assert.Empty(t, got.Status.TotalSize, "totalSize stays reserved: SHOW DATABASES carries no store size")

	// The very same observation again: no write.
	before := got.ResourceVersion
	require.NoError(t, r.applyShardStatus(ctx, sd, observeShardFamily("orders", familyRows("online")), false))
	assert.Equal(t, before, getSharded(t, r, "populate").ResourceVersion, "unchanged observation must not write status")

	// A real change is written, and creationTime does not move with it.
	created := got.Status.CreationTime.DeepCopy()
	require.NoError(t, r.applyShardStatus(ctx, sd, observeShardFamily("orders", familyRows("offline")), false))
	changed := getSharded(t, r, "populate")
	assert.NotEqual(t, before, changed.ResourceVersion)
	assert.Equal(t, "offline", changed.Status.PropertyShards[0].State)
	assert.False(t, changed.Status.PropertyShards[0].Ready)
	assert.True(t, created.Equal(changed.Status.CreationTime), "creationTime is set once")
}

func TestApplyShardStatus_CreationTime(t *testing.T) {
	ctx := context.Background()

	t.Run("not stamped until the family is actually seen", func(t *testing.T) {
		sd := newShardedForStatus("early")
		r := newShardedReconciler(sd)
		require.NoError(t, r.applyShardStatus(ctx, sd, observeShardFamily("orders", nil), false))
		assert.Nil(t, getSharded(t, r, "early").Status.CreationTime)
	})

	t.Run("a destructive recreate restarts the clock", func(t *testing.T) {
		sd := newShardedForStatus("recreate")
		old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
		sd.Status.CreationTime = &old
		r := newShardedReconciler(sd)
		require.NoError(t, r.applyShardStatus(ctx, sd, observeShardFamily("orders", familyRows("online")), true))
		got := getSharded(t, r, "recreate").Status.CreationTime
		require.NotNil(t, got)
		assert.WithinDuration(t, time.Now(), got.Time, 10*time.Second, "the database was just created again")
	})

	t.Run("recreate before the family is visible clears the stale stamp", func(t *testing.T) {
		sd := newShardedForStatus("recreate-late")
		old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
		sd.Status.CreationTime = &old
		r := newShardedReconciler(sd)
		require.NoError(t, r.applyShardStatus(ctx, sd, observeShardFamily("orders", nil), true))
		assert.Nil(t, getSharded(t, r, "recreate-late").Status.CreationTime,
			"keeping the old stamp would report the dropped database's age")
	})

	t.Run("a shard vanishing does not erase when the database was created", func(t *testing.T) {
		sd := newShardedForStatus("vanish")
		r := newShardedReconciler(sd)
		require.NoError(t, r.applyShardStatus(ctx, sd, observeShardFamily("orders", familyRows("online")), false))
		created := getSharded(t, r, "vanish").Status.CreationTime.DeepCopy()

		require.NoError(t, r.applyShardStatus(ctx, sd, observeShardFamily("orders", nil), false))
		got := getSharded(t, r, "vanish")
		assert.True(t, created.Equal(got.Status.CreationTime))
		assert.Nil(t, got.Status.GraphShard, "but the observed shard state follows the observation")
		assert.Empty(t, got.Status.PropertyShards)
		assert.Nil(t, got.Status.VirtualDatabase)
	})
}

// The shard status is written from the SHOW DATABASES pass right after CREATE
// returns, when Neo4j can still report a shard as "starting". Before the fix
// nothing re-read it until the five-minute periodic reconcile, so a Ready
// sharded database showed a starting graph shard for minutes (v1.18.0
// journey). The reconcile now re-reads soon while the family settles, bounded
// to a window after creation.
func TestShardFamilySettling(t *testing.T) {
	now := time.Date(2026, 10, 2, 13, 15, 0, 0, time.UTC)
	created := func(ago time.Duration) *metav1.Time { c := metav1.NewTime(now.Add(-ago)); return &c }
	ready := neo4jv1beta1.ShardStatus{Name: "testdata-g000", State: "online", Ready: true}
	starting := neo4jv1beta1.ShardStatus{Name: "testdata-g000", State: "starting"}

	cases := []struct {
		name   string
		status neo4jv1beta1.Neo4jShardedDatabaseStatus
		want   bool
	}{
		{"nothing observed yet: no creation time to bound the window", neo4jv1beta1.Neo4jShardedDatabaseStatus{}, false},
		{"graph shard still starting", neo4jv1beta1.Neo4jShardedDatabaseStatus{CreationTime: created(time.Minute), GraphShard: &starting}, true},
		{"graph shard not observed yet", neo4jv1beta1.Neo4jShardedDatabaseStatus{CreationTime: created(time.Minute)}, true},
		{"a property shard still starting", neo4jv1beta1.Neo4jShardedDatabaseStatus{CreationTime: created(time.Minute), GraphShard: &ready,
			PropertyShards: []neo4jv1beta1.ShardStatus{{Name: "testdata-p000", State: "online", Ready: true}, {Name: "testdata-p001", State: "starting"}}}, true},
		{"every shard settled", neo4jv1beta1.Neo4jShardedDatabaseStatus{CreationTime: created(time.Minute), GraphShard: &ready,
			PropertyShards: []neo4jv1beta1.ShardStatus{{Name: "testdata-p000", State: "online", Ready: true}}}, false},
		{"past the window: a deliberately stopped family is not re-read every few seconds",
			neo4jv1beta1.Neo4jShardedDatabaseStatus{CreationTime: created(shardSettleWindow + time.Minute), GraphShard: &starting}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shardFamilySettling(&tc.status, now))
		})
	}
}

// mirrorShardObservation is what lets the end of the reconcile see the shard
// state applyShardStatus wrote (to a freshly read copy the caller never sees).
func TestMirrorShardObservation_FeedsTheSettlingDecision(t *testing.T) {
	now := time.Now()
	sd := &neo4jv1beta1.Neo4jShardedDatabase{}

	mirrorShardObservation(sd, shardStatusObservation{
		GraphShard: &neo4jv1beta1.ShardStatus{Name: "testdata-g000", State: "starting"},
		Observed:   true,
	}, false, now)
	require.NotNil(t, sd.Status.CreationTime, "an observed family gets a creation time")
	assert.True(t, shardFamilySettling(&sd.Status, now), "a starting graph shard must be re-read soon")

	mirrorShardObservation(sd, shardStatusObservation{
		GraphShard: &neo4jv1beta1.ShardStatus{Name: "testdata-g000", State: "online", Ready: true},
		Observed:   true,
	}, false, now)
	assert.False(t, shardFamilySettling(&sd.Status, now), "once online, back to the periodic reconcile")

	before := sd.Status.CreationTime
	mirrorShardObservation(sd, shardStatusObservation{GraphShard: sd.Status.GraphShard, Observed: true}, true, now.Add(time.Minute))
	require.NotNil(t, sd.Status.CreationTime)
	assert.NotEqual(t, before.Time, sd.Status.CreationTime.Time, "a recreated family restarts its creation time")
}
