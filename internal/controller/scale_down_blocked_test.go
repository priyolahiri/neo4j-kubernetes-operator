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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// #565: ScaleDownBlocked was announced unless the pass's own copy of the
// cluster already carried the blocked ServersPendingDrain condition. The
// informer cache can hand a pass a copy from before the last pass's write, and
// two such passes announced the same block twice. It is announced from the
// write that records the block: once per cause, and not again by a restarted
// operator.
func TestBlockScaleDown_IsAnnouncedOncePerCause(t *testing.T) {
	ctx := context.Background()
	cluster := clusterForUpgrade("shrink", "default", "5.26.0-enterprise", "5.26.0-enterprise", 2)
	fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(20)
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: rec}
	blocked := func() []string { return eventsWithReason(drainEvents(rec), EventReasonScaleDownBlocked) }
	const floor = "Refusing to scale this cluster to 2 server(s): a Neo4j cluster cannot drop below 3 servers"

	stale := cluster.DeepCopy() // no condition yet, as two passes on a lagging cache read it
	r.blockScaleDown(ctx, stale.DeepCopy(), floor)
	r.blockScaleDown(ctx, stale.DeepCopy(), floor)
	require.Len(t, blocked(), 1, "the same block from a stale copy twice: one event")

	live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, fc.Get(ctx, client.ObjectKeyFromObject(cluster), live))
	c := findCondition(live.Status.Conditions, ConditionTypeServersPendingDrain)
	require.NotNil(t, c)
	assert.Equal(t, metav1.ConditionTrue, c.Status)
	assert.Equal(t, ConditionReasonScaleDownBlocked, c.Reason)
	assert.Equal(t, floor, c.Message)

	const dryRun = "Scale-down to 2 server(s) is blocked: DEALLOCATE dry-run failed: database orders has one primary"
	r.blockScaleDown(ctx, live.DeepCopy(), dryRun)
	got := blocked()
	require.Len(t, got, 1, "another cause is announced")
	assert.Contains(t, got[0], "DEALLOCATE dry-run failed")

	restarted := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: rec}
	restarted.blockScaleDown(ctx, stale.DeepCopy(), dryRun)
	assert.Empty(t, blocked(), "a restarted operator reads the block from status")
}
