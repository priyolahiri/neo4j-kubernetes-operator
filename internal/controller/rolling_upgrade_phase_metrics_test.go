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

// Tests for neo4j_operator_upgrade_duration_seconds: one observation per ended
// upgrade phase, derived from the PERSISTED status.upgradeStatus.phaseStartTime
// (never from in-memory state) so it survives an operator restart, and recorded
// exactly once however often the reconcile that drives the transition repeats.

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const upgradeDurationMetric = "neo4j_operator_upgrade_duration_seconds"

// upgradePhaseObservations reads the number of observations and their sum for
// one (cluster, namespace, phase) series of the upgrade-duration histogram
// from the controller-runtime registry the operator actually serves. Each test
// uses its own cluster name, so the process-global registry never leaks
// between tests.
func upgradePhaseObservations(t *testing.T, cluster, namespace, phase string) (count uint64, sum float64) {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() != upgradeDurationMetric {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchesLabels(m, map[string]string{
				"cluster_name": cluster, "namespace": namespace, "phase": phase,
			}) {
				return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
			}
		}
	}
	return 0, 0
}

func matchesLabels(m *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, lp := range m.GetLabel() {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func clusterInPhase(name, phase string, phaseAge time.Duration) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := clusterForUpgrade(name, "default", "5.26.0-enterprise", "5.26.1-enterprise", 3)
	started := metav1.NewTime(time.Now().Add(-phaseAge))
	// stepStartTime is deliberately RECENT: inside Rolling it restarts on every
	// partition advance, so it is not the phase start. The metric must not be
	// derived from it.
	recent := metav1.NewTime(time.Now().Add(-time.Second))
	c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
		Phase:          phase,
		TargetVersion:  "5.26.1-enterprise",
		StartTime:      &started,
		PhaseStartTime: &started,
		StepStartTime:  &recent,
	}
	return c
}

func newUpgradeReconciler(c *neo4jv1beta1.Neo4jEnterpriseCluster) *Neo4jEnterpriseClusterReconciler {
	fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(c).WithStatusSubresource(c).Build()
	return &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: record.NewFakeRecorder(20)}
}

func getCluster(t *testing.T, r *Neo4jEnterpriseClusterReconciler, name string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	t.Helper()
	latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, latest))
	return latest
}

// TestPatchUpgradeStatus_ObservesEndedPhaseExactlyOnce pins the core contract:
// the phase that just ended is observed once, with its persisted duration, and
// repeating the same transition — from the same stale in-memory object, from a
// brand-new reconciler (an operator restart), or as a no-phase-change patch —
// adds nothing.
func TestPatchUpgradeStatus_ObservesEndedPhaseExactlyOnce(t *testing.T) {
	const name = "phase-once"
	cluster := clusterInPhase(name, upgradePhaseRolling, 90*time.Second)
	r := newUpgradeReconciler(cluster)
	ctx := context.Background()

	toStabilizing := func(us *neo4jv1beta1.UpgradeStatus) {
		us.Phase = upgradePhaseStabilizing
		us.CurrentStep = "All servers rolled; waiting for cluster stabilization"
	}

	require.NoError(t, r.patchUpgradeStatus(ctx, cluster, toStabilizing))

	count, sum := upgradePhaseObservations(t, name, "default", upgradePhaseRolling)
	assert.Equal(t, uint64(1), count, "the Rolling phase ended exactly once")
	// 90s in the status, persisted at second resolution: allow a few seconds.
	assert.InDelta(t, 90.0, sum, 5.0,
		"duration must be measured from the persisted phaseStartTime, not stepStartTime (1s old)")

	// The new phase is stamped so the NEXT transition can be measured.
	persisted := getCluster(t, r, name).Status.UpgradeStatus
	require.NotNil(t, persisted.PhaseStartTime)
	assert.WithinDuration(t, time.Now(), persisted.PhaseStartTime.Time, 5*time.Second)

	// Repeat 1: a reconcile re-driving the same transition from a STALE
	// in-memory object (still Rolling). The persisted phase is already
	// Stabilizing, so this must be a no-op for the metric.
	stale := clusterInPhase(name, upgradePhaseRolling, 90*time.Second)
	stale.ResourceVersion = cluster.ResourceVersion
	require.NoError(t, r.patchUpgradeStatus(ctx, stale, toStabilizing))

	// Repeat 2: an operator restart — a fresh reconciler with no memory of
	// the first observation — re-driving the same transition.
	restarted := &Neo4jEnterpriseClusterReconciler{Client: r.Client, Recorder: record.NewFakeRecorder(20)}
	require.NoError(t, restarted.patchUpgradeStatus(ctx, cluster, toStabilizing))

	// Repeat 3: a patch that does not change the phase (a partition advance,
	// a message refresh) is not a transition.
	require.NoError(t, restarted.patchUpgradeStatus(ctx, cluster, func(us *neo4jv1beta1.UpgradeStatus) {
		us.CurrentStep = "still stabilizing"
	}))

	count, _ = upgradePhaseObservations(t, name, "default", upgradePhaseRolling)
	assert.Equal(t, uint64(1), count, "repeats and restarts must not double-count")
	count, _ = upgradePhaseObservations(t, name, "default", upgradePhaseStabilizing)
	assert.Zero(t, count, "Stabilizing has not ended yet")
}

// TestPatchUpgradeStatus_WalksEveryPhase drives the full non-terminal chain and
// checks each phase is observed once, labelled by its own name, ending with the
// last step that moves to a terminal phase.
func TestPatchUpgradeStatus_WalksEveryPhase(t *testing.T) {
	const name = "phase-walk"
	cluster := clusterInPhase(name, upgradePhaseStaging, 10*time.Second)
	r := newUpgradeReconciler(cluster)
	ctx := context.Background()

	steps := []struct {
		to      string
		ended   string
		backdat time.Duration // age the CURRENT phase before the transition
	}{
		{upgradePhaseRolling, upgradePhaseStaging, 10 * time.Second},
		{upgradePhaseStabilizing, upgradePhaseRolling, 300 * time.Second},
		{upgradePhaseVerifying, upgradePhaseStabilizing, 20 * time.Second},
		{upgradePhaseFailed, upgradePhaseVerifying, 40 * time.Second},
	}
	for _, s := range steps {
		// Age the persisted phaseStartTime so the expected duration is known.
		require.NoError(t, r.patchUpgradeStatus(ctx, cluster, func(us *neo4jv1beta1.UpgradeStatus) {
			ago := metav1.NewTime(time.Now().Add(-s.backdat))
			us.PhaseStartTime = &ago
		}))
		to := s.to
		require.NoError(t, r.patchUpgradeStatus(ctx, cluster, func(us *neo4jv1beta1.UpgradeStatus) {
			us.Phase = to
		}))
		count, sum := upgradePhaseObservations(t, name, "default", s.ended)
		assert.Equal(t, uint64(1), count, "%s observed once when it ended", s.ended)
		assert.InDelta(t, s.backdat.Seconds(), sum, 5.0, "%s duration", s.ended)
	}

	// The terminal phase has no duration: nothing is observed for Failed, nor
	// for leaving it into the next attempt's Staging.
	require.NoError(t, r.patchUpgradeStatus(ctx, cluster, func(us *neo4jv1beta1.UpgradeStatus) {
		us.Phase = upgradePhaseStaging
	}))
	count, _ := upgradePhaseObservations(t, name, "default", upgradePhaseFailed)
	assert.Zero(t, count, "terminal phases are not timed")
}

// TestPatchUpgradeStatus_NoPhaseStartTimeIsNotObserved: an upgrade that began
// under an operator which did not persist phaseStartTime has no reliable phase
// start. Observing "time since stepStartTime" would silently under-report
// Rolling, so the transition is skipped — but the NEW phase is stamped, so the
// rest of the upgrade is measured.
func TestPatchUpgradeStatus_NoPhaseStartTimeIsNotObserved(t *testing.T) {
	const name = "phase-legacy"
	cluster := clusterInPhase(name, upgradePhaseRolling, time.Minute)
	cluster.Status.UpgradeStatus.PhaseStartTime = nil
	r := newUpgradeReconciler(cluster)
	ctx := context.Background()

	require.NoError(t, r.patchUpgradeStatus(ctx, cluster, func(us *neo4jv1beta1.UpgradeStatus) {
		us.Phase = upgradePhaseStabilizing
	}))

	count, _ := upgradePhaseObservations(t, name, "default", upgradePhaseRolling)
	assert.Zero(t, count)
	require.NotNil(t, getCluster(t, r, name).Status.UpgradeStatus.PhaseStartTime,
		"the new phase must still be stamped")
}

// TestUpdateUpgradeStatus_ObservesVerifyingOnCompleted covers the second write
// path: Completed is persisted by the orchestrator, not patchUpgradeStatus, and
// ends the Verifying phase.
func TestUpdateUpgradeStatus_ObservesVerifyingOnCompleted(t *testing.T) {
	const name = "phase-complete"
	cluster := clusterInPhase(name, upgradePhaseVerifying, 45*time.Second)
	fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	orch := NewRollingUpgradeOrchestrator(fc, name, "default")
	ctx := context.Background()

	require.NoError(t, orch.updateUpgradeStatus(ctx, cluster, upgradePhaseCompleted, "done", ""))
	// A retried Completed persist (the caller retries on the next reconcile
	// when a later step failed) must not observe again.
	require.NoError(t, orch.updateUpgradeStatus(ctx, cluster, upgradePhaseCompleted, "done", ""))

	count, sum := upgradePhaseObservations(t, name, "default", upgradePhaseVerifying)
	assert.Equal(t, uint64(1), count)
	assert.InDelta(t, 45.0, sum, 5.0)
	count, _ = upgradePhaseObservations(t, name, "default", upgradePhaseCompleted)
	assert.Zero(t, count, "Completed is terminal and not timed")
}

// TestFailUpgrade_ObservesTheFailedPhaseOnce: a timeout/failure ends the phase
// it happened in, and the failure routing is retried on persist failure, so it
// must not double-observe.
func TestFailUpgrade_ObservesTheFailedPhaseOnce(t *testing.T) {
	const name = "phase-fail"
	cluster := clusterInPhase(name, upgradePhaseRolling, 600*time.Second)
	r := newUpgradeReconciler(cluster)
	ctx := context.Background()

	_, _ = r.failUpgrade(ctx, cluster, assert.AnError)
	_, _ = r.failUpgrade(ctx, cluster, assert.AnError) // routed again

	count, sum := upgradePhaseObservations(t, name, "default", upgradePhaseRolling)
	assert.Equal(t, uint64(1), count)
	assert.InDelta(t, 600.0, sum, 5.0)
}

// TestHandleRollingUpgrade_RestageObservesInterruptedPhase: retargeting
// mid-flight ends the active phase (here Rolling) and restarts at Staging.
func TestHandleRollingUpgrade_RestageObservesInterruptedPhase(t *testing.T) {
	const name = "phase-restage"
	cluster := clusterInPhase(name, upgradePhaseRolling, 120*time.Second)
	cluster.Status.UpgradeStatus.TargetVersion = "5.26.1-enterprise"
	cluster.Spec.Image.Tag = "5.26.2-enterprise" // retarget
	r := newUpgradeReconciler(cluster)

	_, err := r.handleRollingUpgrade(context.Background(), cluster)
	require.NoError(t, err)

	count, sum := upgradePhaseObservations(t, name, "default", upgradePhaseRolling)
	assert.Equal(t, uint64(1), count)
	assert.InDelta(t, 120.0, sum, 5.0)
	assert.Equal(t, upgradePhaseStaging, getCluster(t, r, name).Status.UpgradeStatus.Phase)
}
