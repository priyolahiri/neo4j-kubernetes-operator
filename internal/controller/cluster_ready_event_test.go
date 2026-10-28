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

// ClusterReady is announced by the status write that takes a cluster into
// Ready from another phase, and only then (operations.md id 136). The walk of
// the #594 fixes (#606, d35c5aa9, 2026-10-27) counted one more ClusterReady
// over an upgrade of a 3-server cluster retargeted from 2026.09.9, a tag never
// published, to 2026.09.0, although the phase read Ready the whole time: the
// normal pass after the upgrade wrote Ready with the message of a cluster with
// no upgrade over the completion's "Rolling upgrade completed successfully",
// and Reconcile announced every Ready write that moved the phase, the
// message, the Ready condition or the generation.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// clusterReadyEvents returns the ClusterReady events the recorder holds, and
// takes every event off it.
func clusterReadyEvents(rec *record.FakeRecorder) []string {
	return eventsWithReason(drainEvents(rec), EventReasonClusterReady)
}

// The walk: the upgrade to 2026.09.9 Rolling with server-2 in ImagePullBackOff
// (Degraded=True from the diagnostics' SHOW SERVERS), retargeted to 2026.09.0,
// walked to the end with server-2 replaced first, completed, then the normal
// pass. The cluster is Ready throughout: no ClusterReady. recordClusterReady
// returns what the normal pass announced.
func TestClusterReady_ARetargetedUpgradeOfAReadyClusterAnnouncesNothing(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := heldOnNeverPublished(t)
	rec, ok := r.Recorder.(*record.FakeRecorder)
	require.True(t, ok)
	latest := func() *neo4jv1beta1.Neo4jEnterpriseCluster { return latestCluster(t, fc, c) }
	forgetServerAddresses(c)
	t.Cleanup(func() { forgetServerAddresses(c) })
	ready := func(step string) {
		t.Helper()
		assert.Equal(t, neo4jv1beta1.PhaseReady, latest().Status.Phase, "%s: the phase stays Ready", step)
	}
	rows := func(server2Down bool) []neo4jclient.ServerInfo {
		var out []neo4jclient.ServerInfo
		for i := 0; i < 3; i++ {
			s := neo4jclient.ServerInfo{Name: fmt.Sprintf("uuid-%d", i), State: "Enabled", Health: "Available",
				Address: fmt.Sprintf("c-server-%d.c-headless.default.svc.cluster.local:7687", i)}
			if i == 2 && server2Down {
				s.Health, s.Address = "Unavailable", "" // SHOW SERVERS: NULL
			}
			out = append(out, s)
		}
		return out
	}

	// The upgrade to the tag never published is Rolling, its message written
	// when it started; every server was available then.
	stored := latest()
	now := metav1.Now()
	stored.Status.UpgradeStatus.Phase = upgradePhaseRolling
	stored.Status.UpgradeStatus.StepStartTime = &now
	stored.Status.Conditions = []metav1.Condition{clusterFormedCondition(), allServersAvailableCondition(3)}
	require.NoError(t, fc.Status().Update(ctx, stored))
	r.noteUpgradeInClusterMessage(ctx, latest())
	require.Contains(t, latest().Status.Message, "its upgrade to "+neverPublished+" is in progress")
	r.recordShortfallDuringUpgrade(ctx, latest(), rows(false)) // the addresses, known from here on

	// Server-2 cannot pull the image: the diagnostics write Degraded=True.
	r.recordShortfallDuringUpgrade(ctx, latest(), rows(true))
	degraded := findCondition(latest().Status.Conditions, ConditionTypeDegraded)
	require.NotNil(t, degraded)
	require.Equal(t, metav1.ConditionTrue, degraded.Status)
	ready("server-2 down")

	// The retarget to 2026.09.0.
	setTag(t, fc, c, holdTarget)
	_, err := r.handleRollingUpgrade(ctx, latest())
	require.NoError(t, err)
	require.Equal(t, upgradePhaseStaging, latest().Status.UpgradeStatus.Phase)
	require.Contains(t, latest().Status.Message, "upgrade to "+holdTarget+" is in progress")
	ready("retargeted")

	// The walk to the end; server-2 is back.
	got := rollToTheEnd(t, r, fc, emu, c)
	require.Equal(t, upgradePhaseStabilizing, got.Status.UpgradeStatus.Phase, got.Status.UpgradeStatus.LastError)
	r.recordShortfallDuringUpgrade(ctx, latest(), rows(false))
	require.Equal(t, metav1.ConditionFalse, findCondition(latest().Status.Conditions, ConditionTypeDegraded).Status)
	ready("rolled")

	// Stabilizing (the post-upgrade health check is off), then the completion
	// Verifying writes once every server reports the target.
	_, err = r.handleRollingUpgrade(ctx, latest())
	require.NoError(t, err)
	require.Equal(t, upgradePhaseVerifying, latest().Status.UpgradeStatus.Phase)
	require.NoError(t, r.completeUpgrade(ctx, latest(), NewRollingUpgradeOrchestrator(fc, "c", "default"), ""))
	require.Equal(t, upgradePhaseCompleted, latest().Status.UpgradeStatus.Phase)
	require.Equal(t, "Rolling upgrade completed successfully", latest().Status.Message)
	ready("completed")

	// The normal pass writes the message of a cluster with no upgrade: a
	// change of message, not of phase.
	for pass := 0; pass < repeatPasses; pass++ {
		assert.False(t, r.recordClusterReady(ctx, latest(), rows(false), clusterFormedCondition(), allServersAvailableCondition(3)),
			"pass %d: the cluster stayed Ready through its upgrade, nothing to announce", pass)
	}
	assert.Equal(t, "Neo4j cluster is fully formed and ready", latest().Status.Message)
	assert.Equal(t, holdTarget, latest().Status.Version)
	assert.Empty(t, clusterReadyEvents(rec), "no ClusterReady over the upgrade")
}

// A write that takes the cluster into Ready from another phase announces it,
// once; a write that keeps it Ready announces nothing, whatever else it moves.
func TestClusterReady_IsAnnouncedByTheWriteThatEntersReady(t *testing.T) {
	ctx := context.Background()
	readyCond := cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady)
	formed := cond(ConditionTypeClusterFormed, metav1.ConditionTrue, ConditionReasonFormationComplete)
	allUp := func() []metav1.Condition {
		return []metav1.Condition{clusterFormedCondition(), allServersAvailableCondition(3)}
	}
	normalPass := func(r *Neo4jEnterpriseClusterReconciler) bool {
		t.Helper()
		return r.recordClusterReady(ctx, refetch(t, r), availServers(3, 3), allUp()...)
	}

	for _, tc := range []struct {
		name string
		from *neo4jv1beta1.Neo4jEnterpriseCluster
	}{
		{"first formation", availCluster(neo4jv1beta1.PhaseForming)},
		{"back from Degraded", availCluster(neo4jv1beta1.PhaseDegraded, formed,
			cond(ConditionTypeDegraded, metav1.ConditionTrue, ConditionReasonServersUnavailable))},
		{"a majority back, after losing it", availCluster(neo4jv1beta1.PhaseForming, formed,
			cond(ConditionTypeDegraded, metav1.ConditionTrue, ConditionReasonServersUnavailable))},
		{"a spec fixed after a failure", availCluster(neo4jv1beta1.PhaseFailed, formed)},
	} {
		t.Run("enters Ready: "+tc.name, func(t *testing.T) {
			r, rec := shortfallReconciler(t, tc.from)
			assert.True(t, normalPass(r))
			assert.Len(t, clusterReadyEvents(rec), 1)
			for range repeatPasses {
				assert.False(t, normalPass(r))
			}
			assert.Empty(t, clusterReadyEvents(rec), "announced once")
		})
	}

	t.Run("stays Ready: a server back within the grace", func(t *testing.T) {
		r, rec := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseReady, readyCond, formed))
		oneDown := formationCheck{message: "Cluster forming: 2/3 servers available", servers: availServers(3, 2)}
		r.reconcileServerShortfall(ctx, refetch(t, r), oneDown, oneDown.message)
		require.Equal(t, neo4jv1beta1.PhaseReady, refetch(t, r).Status.Phase)
		require.Contains(t, refetch(t, r).Status.Message, "Neo4j cluster is serving.")
		assert.False(t, normalPass(r), "a new message and Degraded=False, not a new phase")
		assert.Equal(t, "Neo4j cluster is fully formed and ready", refetch(t, r).Status.Message)
		assert.Empty(t, clusterReadyEvents(rec))
	})

	t.Run("stays Ready: a spec edit", func(t *testing.T) {
		r, rec := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseReady, readyCond, formed))
		normalPass(r)
		edited := refetch(t, r)
		edited.Generation++ // the generation the pass acted on
		assert.False(t, r.recordClusterReady(ctx, edited, availServers(3, 3), allUp()...))
		assert.Equal(t, edited.Generation, refetch(t, r).Status.ObservedGeneration)
		assert.Empty(t, clusterReadyEvents(rec))
	})

	t.Run("enters Ready within the grace: a majority back, one server still down", func(t *testing.T) {
		r, rec := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseForming, formed,
			cond(ConditionTypeDegraded, metav1.ConditionTrue, ConditionReasonServersUnavailable)))
		oneDown := formationCheck{message: "Cluster forming: 2/3 servers available", servers: availServers(3, 2)}
		r.reconcileServerShortfall(ctx, refetch(t, r), oneDown, oneDown.message)
		require.Equal(t, neo4jv1beta1.PhaseReady, refetch(t, r).Status.Phase)
		assert.Len(t, clusterReadyEvents(rec), 1, "Forming to Ready: dependents resume")
		assert.False(t, normalPass(r), "the last server back: Ready already")
		assert.Empty(t, clusterReadyEvents(rec))
	})

	t.Run("a stale copy: judged from the status as stored", func(t *testing.T) {
		r, rec := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseForming))
		stale := refetch(t, r)
		assert.True(t, normalPass(r))
		assert.False(t, r.recordClusterReady(ctx, stale, availServers(3, 3), allUp()...),
			"a copy from before the Ready write still reads Forming")
		assert.Len(t, clusterReadyEvents(rec), 1)
	})

	t.Run("an upgrade that completes on a cluster it repaired", func(t *testing.T) {
		// A walk that replaced servers which could not start (id 162) runs on
		// a cluster short of them; its completion is the write into Ready.
		r, rec := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseDegraded, formed,
			cond(ConditionTypeDegraded, metav1.ConditionTrue, ConditionReasonServersUnavailable)))
		upgrading := refetch(t, r)
		upgrading.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{Phase: upgradePhaseVerifying,
			PreviousVersion: "5.26.0-enterprise", TargetVersion: upgrading.Spec.Image.Tag}
		require.NoError(t, r.Status().Update(ctx, upgrading))
		require.NoError(t, r.completeUpgrade(ctx, refetch(t, r), NewRollingUpgradeOrchestrator(r.Client, "c", "default"), ""))
		assert.Len(t, clusterReadyEvents(rec), 1)
		assert.False(t, normalPass(r))
		assert.Empty(t, clusterReadyEvents(rec))
	})

	t.Run("an operator restart announces a Ready cluster again: no", func(t *testing.T) {
		r, rec := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseForming))
		normalPass(r)
		require.Len(t, clusterReadyEvents(rec), 1)
		restarted := &Neo4jEnterpriseClusterReconciler{Client: r.Client, Scheme: r.Scheme, Recorder: rec}
		assert.False(t, normalPass(restarted))
		assert.Empty(t, clusterReadyEvents(rec))
	})
}
