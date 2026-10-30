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

// The drain waits ScaleDownSettle after its cordon before it deallocates, so
// a scale-down set back in that time is given back (operations.md id 231).
// On the v1.24.0 journey (#619) the cordon and the DEALLOCATE came within
// about a second of each other — the cordon's own status write starts the
// pass that deallocates — and three set-backs by hand were all refused: the
// give-back of #617 (id 224) was not reached once.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// settling is a 5 → 3 drain with the default settle on a clock the test
// moves.
func settling(t *testing.T, name string, neo *drainNeo4j) (*Neo4jEnterpriseClusterReconciler, func(time.Duration), func() *neo4jv1beta1.Neo4jEnterpriseCluster, func()) {
	t.Helper()
	r, fc, c := scalingDown(t, name, neo, nil)
	r.ScaleDownSettle = DefaultScaleDownSettle
	now := time.Date(2026, 10, 29, 16, 28, 18, 0, time.UTC)
	r.clock = func() time.Time { return now }
	advance := func(d time.Duration) { now = now.Add(d) }
	stored := func() *neo4jv1beta1.Neo4jEnterpriseCluster { return latestCluster(t, fc, c) }
	pass := func() { drainPass(t, r, fc, c) }
	return r, advance, stored, pass
}

// noted is the wake the drain asked for, if any.
func noted(r *Neo4jEnterpriseClusterReconciler, c *neo4jv1beta1.Neo4jEnterpriseCluster) time.Duration {
	v, ok := r.wakes.m.LoadAndDelete(clusterKey(c))
	if !ok {
		return 0
	}
	d, _ := v.(time.Duration)
	return d
}

func TestScaleDownDrain_TheSettleKeepsASetBackRecoverable(t *testing.T) {
	neo := newDrainNeo4j("settle", 5)
	r, advance, stored, pass := settling(t, "settle", neo)

	// 5 → 3: the cordon, its time recorded with it, and the message says
	// when the deallocation starts.
	pass()
	require.Equal(t, []string{"cordon uuid-3", "cordon uuid-4"}, opsOf(neo, "cordon"))
	assert.Equal(t, "2026-10-29T16:28:18.000000000Z", stored().Annotations[ScaleDownCordonedAtAnnotation])
	pending := pendingDrain(t, r.Client, stored())
	require.NotNil(t, pending)
	assert.Equal(t, "Scale-down to 3 server(s) in progress: cordoned 2 server(s); their databases are deallocated from "+
		"2026-10-29T16:28:38Z unless spec.topology.servers is raised again before then", pending.Message)
	assert.Equal(t, 20*time.Second, noted(r, stored()), "the next pass comes when the settle ends")

	// The pass the cordon's status write starts, and one a little later:
	// nothing is deallocated yet.
	pass()
	advance(9 * time.Second)
	pass()
	assert.Empty(t, opsOf(neo, "deallocate"), "nothing is deallocated during the settle")
	assert.Equal(t, 11*time.Second, noted(r, stored()))
	assert.Equal(t, "Cordoned", neo.servers["uuid-3"].State)

	// Set back ten seconds after the scale-down: given back.
	advance(time.Second)
	setServers(t, r.Client, stored(), 5)
	_ = events(r.Recorder)
	pass()
	assert.Equal(t, []string{"enable uuid-3", "enable uuid-4"}, opsOf(neo, "enable"))
	assert.Equal(t, "Enabled", neo.servers["uuid-3"].State)
	assert.Equal(t, "Enabled", neo.servers["uuid-4"].State)
	after := stored()
	assert.Empty(t, after.Annotations[ScaleDownCordonedAtAnnotation], "the time goes with the record")
	assert.Empty(t, cordonedByDrain(after))
	assert.False(t, scaleDownDrainInProgress(after))
	assert.Len(t, eventsWithReason(events(r.Recorder), EventReasonScaleDownSetBack), 1)
	pending = pendingDrain(t, r.Client, after)
	require.NotNil(t, pending)
	assert.Equal(t, metav1.ConditionFalse, pending.Status)
	assert.Equal(t, "Scale-down not in progress", pending.Message)
}

func TestScaleDownDrain_TheSettleEndsAndTheDrainGoesOn(t *testing.T) {
	neo := newDrainNeo4j("settle-end", 5)
	r, advance, stored, pass := settling(t, "settle-end", neo)

	pass()
	require.Equal(t, 20*time.Second, noted(r, stored()))
	advance(DefaultScaleDownSettle)
	pass()
	assert.Equal(t, []string{"deallocate uuid-3", "deallocate uuid-4"}, opsOf(neo, "deallocate"), "deallocated once the settle is over")
	assert.Zero(t, noted(r, stored()), "nothing to wait for")
	pending := pendingDrain(t, r.Client, stored())
	require.NotNil(t, pending)
	assert.Equal(t, "Scale-down to 3 server(s) in progress: draining 2 server(s)", pending.Message)

	// A set-back now is too late, as before (id 224).
	setServers(t, r.Client, stored(), 5)
	pass()
	pending = pendingDrain(t, r.Client, stored())
	require.NotNil(t, pending)
	assert.Equal(t, ConditionReasonScaleDownBlocked, pending.Reason)

	// Lowered again, the drain finishes; the records go.
	setServers(t, r.Client, stored(), 3)
	for range 4 {
		pass()
	}
	assert.False(t, scaleDownDrainInProgress(stored()))
	assert.Empty(t, stored().Annotations[ScaleDownCordonedAtAnnotation])
}

// The settle is never shorter than ScaleDownSettle. The walk of the #619
// fixes measured 19.3 s: the cordon's time was truncated to the second.
func TestScaleDownDrain_TheSettleIsNeverShort(t *testing.T) {
	t.Run("a cordon part-way through a second", func(t *testing.T) {
		neo := newDrainNeo4j("settle-ns", 5)
		r, advance, stored, pass := settling(t, "settle-ns", neo)
		advance(700 * time.Millisecond) // 16:28:18.7
		pass()
		assert.Equal(t, "2026-10-29T16:28:18.700000000Z", stored().Annotations[ScaleDownCordonedAtAnnotation])
		pending := pendingDrain(t, r.Client, stored())
		require.NotNil(t, pending)
		assert.Contains(t, pending.Message, "deallocated from 2026-10-29T16:28:39Z", "the end shown rounded up")
		assert.Equal(t, 20*time.Second, noted(r, stored()))

		advance(19*time.Second + 500*time.Millisecond) // 19.5 s after the cordon
		pass()
		assert.Empty(t, opsOf(neo, "deallocate"), "half a second of the settle is left")
		assert.Equal(t, 500*time.Millisecond, noted(r, stored()))
		advance(500 * time.Millisecond)
		pass()
		assert.Equal(t, []string{"deallocate uuid-3", "deallocate uuid-4"}, opsOf(neo, "deallocate"))
	})

	t.Run("a cordon time an earlier build truncated to the second", func(t *testing.T) {
		neo := newDrainNeo4j("settle-old", 5)
		r, advance, stored, pass := settling(t, "settle-old", neo)
		advance(700 * time.Millisecond)
		pass() // cordons at 16:28:18.7
		c := stored()
		require.NoError(t, patchObject(context.Background(), r.Client, c, func() {
			c.Annotations[ScaleDownCordonedAtAnnotation] = "2026-10-29T16:28:18Z"
		}))

		advance(19*time.Second + 500*time.Millisecond) // 16:28:38.2, 19.5 s after the cordon
		pass()
		assert.Empty(t, opsOf(neo, "deallocate"), "read as the second after: never short")
		advance(800 * time.Millisecond) // 16:28:39.0
		pass()
		assert.Equal(t, []string{"deallocate uuid-3", "deallocate uuid-4"}, opsOf(neo, "deallocate"))
	})
}

// A server cordoned by hand is deallocated without a cordon of the drain's,
// and without a settle: there is nothing a set-back would give back.
func TestScaleDownDrain_NoSettleForServersCordonedByHand(t *testing.T) {
	neo := newDrainNeo4j("settle-hand", 5)
	neo.servers["uuid-3"].State = "Cordoned"
	neo.servers["uuid-4"].State = "Cordoned"
	r, _, stored, pass := settling(t, "settle-hand", neo)

	pass()
	assert.Empty(t, opsOf(neo, "cordon"))
	assert.Equal(t, []string{"deallocate uuid-3", "deallocate uuid-4"}, opsOf(neo, "deallocate"))
	assert.Zero(t, noted(r, stored()))
	assert.Empty(t, stored().Annotations[ScaleDownCordonedAtAnnotation])
}

func TestPassWakes_BringTheNextPassForward(t *testing.T) {
	var w passWakes
	assert.Equal(t, ctrl.Result{RequeueAfter: 30 * time.Second}, w.apply("ns/c", ctrl.Result{RequeueAfter: 30 * time.Second}, nil), "nothing noted")

	w.note("ns/c", 20*time.Second)
	w.note("ns/c", 5*time.Second)
	w.note("ns/c", 10*time.Second)
	assert.Equal(t, ctrl.Result{RequeueAfter: 5 * time.Second}, w.apply("ns/c", ctrl.Result{RequeueAfter: 30 * time.Second}, nil), "the soonest wins")
	assert.Equal(t, ctrl.Result{RequeueAfter: 30 * time.Second}, w.apply("ns/c", ctrl.Result{RequeueAfter: 30 * time.Second}, nil), "taken once")

	w.note("ns/c", 5*time.Second)
	assert.Equal(t, ctrl.Result{RequeueAfter: 2 * time.Second}, w.apply("ns/c", ctrl.Result{RequeueAfter: 2 * time.Second}, nil), "a sooner pass stays")
	w.note("ns/c", 5*time.Second)
	assert.Equal(t, ctrl.Result{RequeueAfter: 5 * time.Second}, w.apply("ns/c", ctrl.Result{}, nil), "a pass that asked for none")
	w.note("ns/c", 5*time.Second)
	assert.Equal(t, ctrl.Result{}, w.apply("ns/c", ctrl.Result{}, errors.New("boom")), "a failed pass backs off")
	w.note("ns/other", 5*time.Second)
	assert.Equal(t, ctrl.Result{}, w.apply("ns/c", ctrl.Result{}, nil), "per cluster")
}
