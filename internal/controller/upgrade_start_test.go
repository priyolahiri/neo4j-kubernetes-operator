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

// Starting an upgrade (upgrade_start.go): a refusal by a check before
// anything is written leaves the cluster as it was and is announced once; the
// check holds the target to what the servers report; the cluster's message
// follows the upgrade; a wait for a drain is announced once.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// startFixture is a Ready cluster on holdRunning whose spec asks for
// holdTarget, its server StatefulSet on holdRunning with every server ready,
// and a reconciler whose pre-upgrade check is preUpgrade.
func startFixture(t *testing.T, autoPause bool, preUpgrade func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) ([]neo4jclient.ServerInfo, error)) (*Neo4jEnterpriseClusterReconciler, client.Client, *record.FakeRecorder, *neo4jv1beta1.Neo4jEnterpriseCluster) {
	t.Helper()
	c := clusterForUpgrade("c", "default", holdRunning, holdTarget, 3)
	c.Status.Phase = "Ready"
	c.Status.Message = clusterReadyMessage(c)
	c.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{PreUpgradeHealthCheck: true, AutoPauseOnFailure: autoPause}
	sts := serverSTSForUpgrade("c", "default", "neo4j:"+holdRunning, 3)
	sts.Status.ReadyReplicas = 3
	fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(c, sts).WithStatusSubresource(c).Build()
	rec := record.NewFakeRecorder(50)
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: rec, RequeueAfter: 10 * time.Second,
		UpgradeGates: &upgradeGates{preUpgrade: preUpgrade}}
	return r, fc, rec, c
}

func latestCluster(t *testing.T, c client.Client, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *neo4jv1beta1.Neo4jEnterpriseCluster {
	t.Helper()
	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cluster), got))
	return got
}

func eventsWith(rec *record.FakeRecorder, reason string) []string {
	var out []string
	for len(rec.Events) > 0 {
		if ev := <-rec.Events; strings.Contains(ev, reason) {
			out = append(out, ev)
		}
	}
	return out
}

// A pre-upgrade check that keeps refusing flipped the cluster between Failed
// and Ready, with an UpgradeFailed and a ClusterReady on every cycle (#538,
// #531). Nothing was rolled: the cluster stays Ready, the refusal is
// announced once, and it is tried again after upgradeStartRetry, with the
// normal pass in between.
func TestStartRollingUpgrade_ARefusedStartLeavesTheClusterReadyAndIsAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	refusal := errors.New("cluster is not healthy, upgrade not safe")
	r, fc, rec, c := startFixture(t, false, func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) ([]neo4jclient.ServerInfo, error) {
		return nil, refusal
	})

	attempts := 0
	for pass := 0; pass < 4; pass++ {
		got := latestCluster(t, fc, c)
		if !r.isUpgradeRequired(ctx, got) {
			// The normal pass runs. Let the retry interval pass for the next.
			past := metav1.NewTime(time.Now().Add(-2 * upgradeStartRetry))
			got.Status.UpgradeStatus.StepStartTime = &past
			require.NoError(t, fc.Status().Update(ctx, got))
			continue
		}
		attempts++
		res, err := r.startRollingUpgrade(ctx, got)
		require.NoError(t, err, "a refused start is recorded, not a reconcile error")
		assert.Greater(t, res.RequeueAfter, upgradeStartRetry, "the retry is due when the pass runs")

		after := latestCluster(t, fc, c)
		assert.Equal(t, "Ready", after.Status.Phase, "nothing was rolled: the cluster stays Ready")
		assert.Contains(t, after.Status.Message, "its upgrade to "+holdTarget+" failed (status.upgradeStatus)")
		assert.Equal(t, upgradePhaseFailed, after.Status.UpgradeStatus.Phase)
		assert.Equal(t, holdTarget, after.Status.UpgradeStatus.TargetVersion)
		assert.Contains(t, after.Status.UpgradeStatus.LastError, refusal.Error())
		assert.False(t, r.isUpgradeRequired(ctx, after), "tried again only after upgradeStartRetry")
	}
	assert.Equal(t, 2, attempts, "every other pass is the normal pass")
	assert.Len(t, eventsWith(rec, EventReasonUpgradeFailed), 1, "the same refusal is announced once")

	// A new cause is a new announcement.
	r.UpgradeGates.preUpgrade = func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) ([]neo4jclient.ServerInfo, error) {
		return nil, errors.New("no cluster leader found")
	}
	_, err := r.startRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Len(t, eventsWith(rec, EventReasonUpgradeFailed), 1)
}

// With autoPauseOnFailure the refused upgrade is Paused and waits for the
// user; the cluster stays Ready and says so.
func TestStartRollingUpgrade_ARefusedStartPausesWithTheClusterReady(t *testing.T) {
	ctx := context.Background()
	r, fc, rec, c := startFixture(t, true, func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) ([]neo4jclient.ServerInfo, error) {
		return nil, errors.New("cluster does not have consensus, upgrade not safe")
	})
	_, err := r.startRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)

	after := latestCluster(t, fc, c)
	assert.Equal(t, "Ready", after.Status.Phase)
	assert.Contains(t, after.Status.Message, "its upgrade to "+holdTarget+" is paused")
	assert.Equal(t, upgradePhasePaused, after.Status.UpgradeStatus.Phase)
	assert.Len(t, eventsWith(rec, EventReasonUpgradePaused), 1)
	assert.False(t, r.isUpgradeRequired(ctx, after), "Paused waits for the user")
}

// The pre-upgrade check holds the target to what the servers report now: the
// operator may not have recorded it yet (a floating tag pulled again can run a
// newer patch), so the version rule let this tag through (#550).
func TestStartRollingUpgrade_RefusesATargetBelowWhatAServerReports(t *testing.T) {
	ctx := context.Background()
	servers := []neo4jclient.ServerInfo{
		{Name: "a", Address: "c-server-0.c-headless:7687", State: "Enabled", Health: "Available", Version: "5.26.31"},
		{Name: "b", Address: "c-server-1.c-headless:7687", State: "Enabled", Health: "Available", Version: "5.26.32"},
	}
	r, fc, rec, c := startFixture(t, false, func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster) ([]neo4jclient.ServerInfo, error) {
		return servers, nil
	})
	got := latestCluster(t, fc, c)
	got.Status.Version = "5.26-enterprise" // nothing recorded in status.runningVersion yet
	require.NoError(t, fc.Status().Update(ctx, got))
	got = latestCluster(t, fc, c)
	got.Spec.Image.Tag = "5.26.31-enterprise"
	require.Empty(t, r.validateTagOnly(got), "the version rule, with nothing recorded, lets it through")

	_, err := r.startRollingUpgrade(ctx, got)
	require.NoError(t, err)
	us := latestCluster(t, fc, c).Status.UpgradeStatus
	assert.Equal(t, upgradePhaseFailed, us.Phase)
	assert.Contains(t, us.LastError, "a server runs 5.26.32 (SHOW SERVERS), and spec.image.tag 5.26.31-enterprise is below it")
	assert.Len(t, eventsWith(rec, EventReasonUpgradeFailed), 1)

	// The patch the servers run, or a later one, starts.
	got = latestCluster(t, fc, c)
	got.Spec.Image.Tag = "5.26.32-enterprise"
	_, err = r.startRollingUpgrade(ctx, got)
	require.NoError(t, err)
	assert.Equal(t, upgradePhaseStaging, latestCluster(t, fc, c).Status.UpgradeStatus.Phase)
}

// validateTagOnly is the version rule a pass runs before the machine starts.
func (r *Neo4jEnterpriseClusterReconciler) validateTagOnly(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	return NewRollingUpgradeOrchestrator(r.Client, cluster.Name, cluster.Namespace).validateVersionCompatibility(cluster)
}

// Only the normal pass wrote the cluster's message, and it does not run while
// an upgrade is active: a resumed upgrade said "is paused" for the whole of
// its walk (#553).
func TestStartRollingUpgrade_AResumedUpgradeIsNoLongerCalledPaused(t *testing.T) {
	ctx := context.Background()
	c := heldCluster(upgradePhasePaused, true)
	c.Status.Message = clusterReadyMessage(c)
	require.Contains(t, c.Status.Message, "is paused")
	c.Spec.Image.Tag = "2026.10.0-enterprise"
	c.Spec.UpgradeStrategy.PreUpgradeHealthCheck = false
	fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(c, heldSTS()).WithStatusSubresource(c).Build()
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: record.NewFakeRecorder(10)}

	_, err := r.startRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)

	got := latestCluster(t, fc, c)
	assert.Equal(t, upgradePhaseStaging, got.Status.UpgradeStatus.Phase)
	assert.Equal(t, "Ready", got.Status.Phase)
	assert.NotContains(t, got.Status.Message, "paused")
	assert.Contains(t, got.Status.Message, "its upgrade to 2026.10.0-enterprise is in progress (status.upgradeStatus)")
}

// An upgrade that waits for a scale-down drain is retried every
// upgradeStepRequeue; it emitted UpgradeDeferred on each of them (rule 145).
func TestStartRollingUpgrade_AWaitForADrainIsAnnouncedOnce(t *testing.T) {
	cluster := clusterForUpgrade("defer", "default", "5.26.0-enterprise", "5.26.1-enterprise", 2)
	cluster.Annotations = map[string]string{ScaleDownDrainingServersAnnotation: "id-a,id-b"}
	fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(10)
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: rec}

	for i := 0; i < 3; i++ {
		_, err := r.startRollingUpgrade(context.Background(), cluster)
		require.NoError(t, err)
	}
	assert.Len(t, eventsWith(rec, EventReasonUpgradeDeferred), 1)
}
