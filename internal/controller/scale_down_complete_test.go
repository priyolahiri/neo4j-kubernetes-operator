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

// A scale-down whose drain ran to its end leaves ServersPendingDrain False
// with "Scale-down drain complete", and the passes after it leave it so; one
// stopped before its end (scaled back up) says "Scale-down not in progress"
// (#606). The walk of the #594 fixes saw "Scale-down not in progress" after a
// completed drain: the completion's message sat on a branch no pass could
// reach, and the pass that found nothing left to drain reset the condition as
// for an aborted scale-down, judged from its own copy.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// pendingDrain is the cluster's ServersPendingDrain condition as stored.
func pendingDrain(t *testing.T, fc client.Client, c *neo4jv1beta1.Neo4jEnterpriseCluster) *metav1.Condition {
	t.Helper()
	return findCondition(latestCluster(t, fc, c).Status.Conditions, ConditionTypeServersPendingDrain)
}

// setServerReplicas sets the server StatefulSet's replicas, as the apply after
// the drain released its hold does.
func setServerReplicas(t *testing.T, fc client.Client, cluster string, replicas int32) {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, fc.Get(context.Background(), types.NamespacedName{Name: cluster + "-server", Namespace: "default"}, sts))
	sts.Spec.Replicas = ptr.To(replicas)
	require.NoError(t, fc.Update(context.Background(), sts))
}

func TestScaleDownDrain_ACompletedDrainSaysSoOnEveryPassAfter(t *testing.T) {
	ctx := context.Background()
	neo := newDrainNeo4j("done", 5)
	r, fc, c := scalingDown(t, "done", neo, nil)

	// The drain, pass by pass, keeping the copy the releasing pass read: the
	// condition True, the hold in place.
	var beforeRelease *neo4jv1beta1.Neo4jEnterpriseCluster
	for pass := 0; pass < 12; pass++ {
		read := latestCluster(t, fc, c)
		require.NoError(t, r.reconcileScaleDownDrain(ctx, read.DeepCopy()))
		if !scaleDownDrainInProgress(latestCluster(t, fc, c)) {
			beforeRelease = read
			break
		}
	}
	require.NotNil(t, beforeRelease, "the hold is released")
	require.Equal(t, "Dropped", neo.servers["uuid-3"].State)
	require.Equal(t, "Dropped", neo.servers["uuid-4"].State)
	require.Equal(t, metav1.ConditionTrue,
		findCondition(beforeRelease.Status.Conditions, ConditionTypeServersPendingDrain).Status)

	done := pendingDrain(t, fc, c)
	require.NotNil(t, done)
	assert.Equal(t, metav1.ConditionFalse, done.Status)
	assert.Equal(t, ConditionReasonNoServersPendingDrain, done.Reason)
	assert.Equal(t, "Scale-down drain complete", done.Message)
	want := *done

	same := func(why string) {
		t.Helper()
		got := pendingDrain(t, fc, c)
		require.NotNil(t, got)
		assert.Equal(t, want.Status, got.Status, why)
		assert.Equal(t, want.Reason, got.Reason, why)
		assert.Equal(t, want.Message, got.Message, why)
		assert.True(t, want.LastTransitionTime.Equal(&got.LastTransitionTime), why)
		assert.False(t, scaleDownDrainInProgress(latestCluster(t, fc, c)), why)
	}

	// The StatefulSet as the cache can still serve it, five replicas.
	require.NoError(t, r.reconcileScaleDownDrain(ctx, latestCluster(t, fc, c)))
	same("a pass that reads the StatefulSet before the apply lowered it")

	// The apply lowered the replicas: the passes after it.
	setServerReplicas(t, fc, "done", 3)
	for range repeatPasses {
		require.NoError(t, r.reconcileScaleDownDrain(ctx, latestCluster(t, fc, c)))
	}
	same("the passes after the drain")
	require.NoError(t, r.reconcileScaleDownDrain(ctx, beforeRelease.DeepCopy()))
	same("a pass on a copy from before the completion: the condition still True there")
}

// A scale-down stopped before its drain ended — the servers set back — is not
// a completed one.
func TestScaleDownDrain_AScaleDownSetBackIsNotInProgress(t *testing.T) {
	ctx := context.Background()
	neo := newDrainNeo4j("back", 5)
	r, fc, c := scalingDown(t, "back", neo, nil)

	require.NoError(t, r.reconcileScaleDownDrain(ctx, latestCluster(t, fc, c))) // cordons servers 3 and 4
	require.Equal(t, metav1.ConditionTrue, pendingDrain(t, fc, c).Status)
	stale := latestCluster(t, fc, c)

	back := latestCluster(t, fc, c)
	require.NoError(t, patchObject(ctx, fc, back, func() { back.Spec.Topology.Servers = 5 }))
	require.NoError(t, r.reconcileScaleDownDrain(ctx, latestCluster(t, fc, c)))
	got := pendingDrain(t, fc, c)
	assert.Equal(t, metav1.ConditionFalse, got.Status)
	assert.Equal(t, ConditionReasonNoServersPendingDrain, got.Reason)
	assert.Equal(t, "Scale-down not in progress", got.Message)
	assert.False(t, scaleDownDrainInProgress(latestCluster(t, fc, c)))

	stale.Spec.Topology.Servers = 5
	require.NoError(t, r.reconcileScaleDownDrain(ctx, stale))
	assert.Equal(t, "Scale-down not in progress", pendingDrain(t, fc, c).Message, "said once, and kept")
}

// A scale-down that finds nothing to drain — the servers it removes never
// joined, or were dropped by hand — writes no condition.
func TestScaleDownDrain_NothingToDrainWritesNoCondition(t *testing.T) {
	ctx := context.Background()
	neo := newDrainNeo4j("none", 3) // servers 3 and 4 never registered
	r, fc, c := scalingDown(t, "none", neo, nil)
	require.NoError(t, r.reconcileScaleDownDrain(ctx, latestCluster(t, fc, c)))
	assert.Nil(t, pendingDrain(t, fc, c))
	assert.False(t, scaleDownDrainInProgress(latestCluster(t, fc, c)), "the hold it seeded is released")
	assert.Empty(t, neo.ops)
}
