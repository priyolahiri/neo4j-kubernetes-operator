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

// Right after a pass wrote a new server template, the StatefulSet's status
// still described the template before it — every replica updated and ready —
// so serverStatefulSetFullyRolled said rolled, and the cluster was reported
// Ready just before its servers restarted. Users, roles, databases and an
// upgrade read that Ready. And status.observedGeneration was the generation
// the status write re-read, not the one the pass acted on (operations.md id
// 153).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestServerStatefulSetFullyRolled_ATemplateWriteNotYetObservedIsInFlight(t *testing.T) {
	ctx := context.Background()

	t.Run("the StatefulSet controller has not seen the new generation", func(t *testing.T) {
		sts := gateTestSTS("neo4j:x", 3, "rev1", "rev1", 3, 3)
		sts.Generation, sts.Status.ObservedGeneration = 2, 1
		cluster := gateTestCluster("x", "Ready")
		r := gateTestReconciler(t, sts, cluster)
		rolled, detail := r.serverStatefulSetFullyRolled(ctx, cluster)
		assert.False(t, rolled)
		assert.Contains(t, detail, "has not seen the current template")
		assert.True(t, r.serverRolloutInFlight(ctx, cluster))
	})

	t.Run("a cached read older than the pass's own write", func(t *testing.T) {
		sts := gateTestSTS("neo4j:x", 3, "rev1", "rev1", 3, 3)
		sts.UID, sts.Generation, sts.Status.ObservedGeneration = "sts-1", 2, 2
		cluster := gateTestCluster("x", "Ready")
		r := gateTestReconciler(t, sts, cluster)
		written := sts.DeepCopy()
		written.Generation = 3 // what the write returned; the cache still has 2
		r.noteServerStatefulSetWrite(cluster, written)

		rolled, _ := r.serverStatefulSetFullyRolled(ctx, cluster)
		assert.False(t, rolled)
		assert.True(t, r.serverRolloutInFlight(ctx, cluster))

		// The same generation read back, and observed: rolled.
		written.Generation = 2
		r.noteServerStatefulSetWrite(cluster, written)
		rolled, detail := r.serverStatefulSetFullyRolled(ctx, cluster)
		assert.True(t, rolled, detail)
		assert.False(t, r.serverRolloutInFlight(ctx, cluster))
	})

	t.Run("a StatefulSet created again starts at generation 1", func(t *testing.T) {
		sts := gateTestSTS("neo4j:x", 3, "rev1", "rev1", 3, 3)
		sts.UID, sts.Generation, sts.Status.ObservedGeneration = "sts-2", 1, 1
		cluster := gateTestCluster("x", "Ready")
		r := gateTestReconciler(t, sts, cluster)
		old := sts.DeepCopy()
		old.UID, old.Generation = "sts-1", 7
		r.noteServerStatefulSetWrite(cluster, old)
		rolled, detail := r.serverStatefulSetFullyRolled(ctx, cluster)
		assert.True(t, rolled, "another StatefulSet's generation says nothing: %s", detail)
	})

	t.Run("a held upgrade's servers below the partition are still not a rollout", func(t *testing.T) {
		sts := heldSTS() // generation 5, observed
		sts.UID = "sts-1"
		c := heldCluster(upgradePhasePaused, true)
		r := gateTestReconciler(t, sts, c)
		r.noteServerStatefulSetWrite(c, sts)
		rolled, detail := r.serverStatefulSetFullyRolled(ctx, c)
		assert.True(t, rolled, detail)
		assert.False(t, r.serverRolloutInFlight(ctx, c))
	})
}

// status.observedGeneration says which spec the status describes: the one
// the pass read and acted on. A spec edited while the pass ran has not been
// acted on.
func TestUpdateClusterStatus_RecordsTheGenerationThePassActedOn(t *testing.T) {
	ctx := context.Background()
	stored := gateTestCluster("2026.08.1-enterprise", "Forming")
	stored.Generation = 3 // the spec was edited after the pass read generation 2
	r := gateTestReconciler(t, stored)

	actedOn := stored.DeepCopy()
	actedOn.Generation = 2
	require.True(t, r.updateClusterStatus(ctx, actedOn, "Ready", "Neo4j cluster is fully formed and ready"))

	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "c", Namespace: "default"}, got))
	assert.Equal(t, int64(2), got.Status.ObservedGeneration)
	ready := findCondition(got.Status.Conditions, ConditionTypeReady)
	require.NotNil(t, ready)
	assert.Equal(t, int64(2), ready.ObservedGeneration)
	assert.Equal(t, metav1.ConditionTrue, ready.Status)

	// The pass that acts on generation 3 records it.
	actedOn.Generation = 3
	r.updateClusterStatus(ctx, actedOn, "Ready", "Neo4j cluster is fully formed and ready")
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "c", Namespace: "default"}, got))
	assert.Equal(t, int64(3), got.Status.ObservedGeneration)
	assert.Equal(t, int64(3), ptr.Deref(findCondition(got.Status.Conditions, ConditionTypeReady), metav1.Condition{}).ObservedGeneration)
}

// The standalone's status.observedGeneration is the generation its pass acted
// on too: updateStatus stamped the one its write re-read (#550).
func TestStandaloneUpdateStatus_RecordsTheGenerationThePassActedOn(t *testing.T) {
	ctx := context.Background()
	key := types.NamespacedName{Name: "sa", Namespace: "default"}
	sa := standaloneForSTS("2026.08.1-enterprise")
	sa.Status.Version = "2026.08.1-enterprise"
	sa.Generation = 3 // the spec was edited after the pass read generation 2
	r := standaloneVersionTestReconciler(t, sa, standaloneVersionTestSTS("neo4j:2026.08.1-enterprise", "rev1", "rev1", 1, 1))

	stored := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, r.Get(ctx, key, stored))
	require.Equal(t, int64(3), stored.Generation)
	actedOn := stored.DeepCopy()
	actedOn.Generation = 2

	require.NoError(t, r.updateStatus(ctx, actedOn))
	got := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, actedOn.Generation, got.Status.ObservedGeneration)
	ready := findCondition(got.Status.Conditions, ConditionTypeReady)
	require.NotNil(t, ready)
	assert.Equal(t, actedOn.Generation, ready.ObservedGeneration)

	// The pass that acts on the new generation records it.
	require.NoError(t, r.updateStatus(ctx, got))
	require.NoError(t, r.Get(ctx, key, got))
	assert.Equal(t, stored.Generation, got.Status.ObservedGeneration)
}

// noteServerStatefulSetWrite ignores what it cannot identify.
func TestNoteServerStatefulSetWrite_NeedsAUID(t *testing.T) {
	r := &Neo4jEnterpriseClusterReconciler{}
	cluster := gateTestCluster("x", "Ready")
	r.noteServerStatefulSetWrite(cluster, &appsv1.StatefulSet{})
	r.noteServerStatefulSetWrite(cluster, nil)
	_, ok := r.serverStatefulSetWrites.Load(clusterKey(cluster))
	assert.False(t, ok)
}
