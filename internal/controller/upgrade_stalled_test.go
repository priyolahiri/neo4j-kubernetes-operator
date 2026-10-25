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

// A cluster whose upgrade stopped stays Ready, so the Stalled condition is
// what tells kstatus readers (Flux) that the image change did not land
// (upgrade_stalled.go, operations.md id 149).

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func stalledOf(c *neo4jv1beta1.Neo4jEnterpriseCluster) *metav1.Condition {
	return findCondition(c.Status.Conditions, ConditionTypeStalled)
}

func TestStalled_FollowsTheUpgradeRecordInTheSameWrite(t *testing.T) {
	ctx := context.Background()
	for _, autoPause := range []bool{false, true} {
		c := clusterForUpgrade("st", "default", holdRunning, holdTarget, 3)
		c.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: autoPause}
		c.Status.Phase = neo4jv1beta1.PhaseReady
		SetReadyCondition(&c.Status.Conditions, 0, metav1.ConditionTrue, ConditionReasonReady, "ready")
		now := metav1.Now()
		c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{Phase: upgradePhaseRolling, StartTime: &now,
			PreviousVersion: holdRunning, TargetVersion: holdTarget}
		writes := 0
		fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(c).WithStatusSubresource(c).
			WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					writes++
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				},
			}).Build()
		r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: record.NewFakeRecorder(20)}
		latest := func() *neo4jv1beta1.Neo4jEnterpriseCluster { return latestCluster(t, fc, c) }

		// An upgrade in progress is not stalled, and carries no condition.
		require.NoError(t, r.patchUpgradeStatus(ctx, latest(), func(us *neo4jv1beta1.UpgradeStatus) { us.CurrentStep = "rolling" }))
		assert.Nil(t, stalledOf(latest()), "a cluster whose upgrade never stopped has no Stalled condition")

		writes = 0
		_, err := r.failUpgrade(ctx, latest(), errors.New("timed out waiting for server ordinal 2 to roll and rejoin the cluster"))
		require.NoError(t, err)
		got := latest()
		stalled := stalledOf(got)
		require.NotNil(t, stalled, "autoPauseOnFailure %v", autoPause)
		assert.Equal(t, metav1.ConditionTrue, stalled.Status)
		wantReason := ConditionReasonUpgradeFailed
		if autoPause {
			wantReason = ConditionReasonUpgradePaused
		}
		assert.Equal(t, wantReason, stalled.Reason)
		assert.Contains(t, stalled.Message, "timed out waiting for server ordinal 2")
		assert.Contains(t, stalled.Message, holdTarget)
		ready := findCondition(got.Status.Conditions, ConditionTypeReady)
		require.NotNil(t, ready)
		assert.Equal(t, metav1.ConditionTrue, ready.Status, "Ready stays as it is: the cluster serves")
		assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
		assert.Equal(t, 2, writes, "the record's write and the cluster's message: no write of its own")

		// Taken up again: not stalled any more.
		held := latest().Status.UpgradeStatus.DeepCopy()
		require.NoError(t, r.patchUpgradeStatus(ctx, latest(), func(us *neo4jv1beta1.UpgradeStatus) {
			*us = newUpgradeRecord(held, holdRunning, holdTarget, 3, metav1.Now())
		}))
		stalled = stalledOf(latest())
		require.NotNil(t, stalled)
		assert.Equal(t, metav1.ConditionFalse, stalled.Status)
		assert.Equal(t, ConditionReasonUpgradeNotStopped, stalled.Reason)

		// Stopped again, then completed (the write Verifying makes): False.
		_, err = r.failUpgrade(ctx, latest(), errors.New("cluster failed to stabilize after the rolling upgrade"))
		require.NoError(t, err)
		require.Equal(t, metav1.ConditionTrue, stalledOf(latest()).Status)
		orch := NewRollingUpgradeOrchestrator(fc, c.Name, c.Namespace)
		require.NoError(t, orch.updateUpgradeStatus(ctx, latest(), upgradePhaseCompleted, "done", "", ""))
		assert.Equal(t, metav1.ConditionFalse, stalledOf(latest()).Status)
	}
}

// The record of an upgrade nothing started on, dropped once the tag no longer
// names it, takes its Stalled condition with it.
func TestStalled_ClearedWhenAnAbandonedUpgradeIsDropped(t *testing.T) {
	ctx := context.Background()
	c := heldCluster(upgradePhaseFailed, false)
	c.Spec.Image.Tag = holdRunning
	c.Status.UpgradeStatus.StartedTargetVersion = holdRunning
	c.Status.UpgradeStatus.Progress = &neo4jv1beta1.UpgradeProgress{Total: 3, Pending: 3}
	c.Status.UpgradeStatus.CurrentPartition = nil
	c.Status.UpgradeStatus.LastError = "pre-upgrade check: a server is unavailable"
	syncStalledCondition(c)
	require.Equal(t, metav1.ConditionTrue, stalledOf(c).Status)
	sts := heldSTS()
	sts.Spec.Template.Spec.Containers[0].Image = "neo4j:" + holdRunning
	r, fc, _ := repairReconciler(t, c, sts)

	r.forgetAbandonedUpgrade(ctx, latestCluster(t, fc, c))
	got := latestCluster(t, fc, c)
	require.Nil(t, got.Status.UpgradeStatus)
	assert.Equal(t, metav1.ConditionFalse, stalledOf(got).Status)
}
