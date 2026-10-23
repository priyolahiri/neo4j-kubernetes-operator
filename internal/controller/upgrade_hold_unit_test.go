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

// A Paused or Failed upgrade holds the servers where the upgrade state
// machine left them (operations.md id 149). The envtest spec "Neo4jEnterpriseCluster
// whose upgrade is Paused or Failed" drives the normal pass against the API
// server; these pin the pieces it cannot reach without Neo4j.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const (
	holdRunning = "2026.08.1-enterprise"
	holdTarget  = "2026.09.0-enterprise"
)

// heldCluster is a cluster whose upgrade to holdTarget stopped in phase after
// rolling server-2 (partition 2).
func heldCluster(phase string, autoPause bool) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := clusterForUpgrade("c", "default", holdRunning, holdTarget, 3)
	c.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: autoPause}
	c.Status.Phase = "Ready"
	start := metav1.NewTime(time.Now().Add(-time.Hour))
	c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
		Phase: phase, PreviousVersion: holdRunning, TargetVersion: holdTarget,
		StartTime: &start, CurrentPartition: ptr.To(int32(2)),
		Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2},
	}
	return c
}

// heldSTS is the server StatefulSet such an upgrade leaves: the target
// template, server-2 on it, every server ready.
func heldSTS() *appsv1.StatefulSet {
	sts := gateTestSTS("neo4j:"+holdTarget, 3, "rev-before", "rev-target", 1, 3)
	sts.Generation, sts.Status.ObservedGeneration = 5, 5
	sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
		Type:          appsv1.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(int32(2))},
	}
	return sts
}

// The normal pass's StatefulSet apply keeps the partition of a held upgrade,
// and only of one that is held or active.
func TestCreateOrUpdateResource_KeepsTheHeldUpgradesPartition(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))

	// What the builder renders: RollingUpdate with no partition.
	desired := func() *appsv1.StatefulSet {
		sts := heldSTS()
		sts.Status = appsv1.StatefulSetStatus{}
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
		return sts
	}
	// The API server gives objects a UID and defaults a missing partition to 0.
	newClient := func(objs ...client.Object) client.Client {
		defaulted := func(obj client.Object) {
			if sts, ok := obj.(*appsv1.StatefulSet); ok && sts.Spec.UpdateStrategy.RollingUpdate != nil &&
				sts.Spec.UpdateStrategy.RollingUpdate.Partition == nil {
				sts.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To(int32(0))
			}
		}
		for _, o := range objs {
			o.SetUID(types.UID(o.GetName() + "-uid"))
		}
		return interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(), interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				obj.SetUID(types.UID(obj.GetName() + "-uid"))
				defaulted(obj)
				return cl.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				defaulted(obj)
				return cl.Update(ctx, obj, opts...)
			},
		})
	}
	partition := func(t *testing.T, c client.Client) int32 {
		t.Helper()
		live := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKey{Name: "c-server", Namespace: "default"}, live))
		require.NotNil(t, live.Spec.UpdateStrategy.RollingUpdate.Partition)
		return *live.Spec.UpdateStrategy.RollingUpdate.Partition
	}

	for _, tc := range []struct {
		phase string
		want  int32
	}{
		{upgradePhasePaused, 2},
		{upgradePhaseFailed, 2},
		{upgradePhaseRolling, 2},
		{upgradePhaseCompleted, 0},
	} {
		t.Run("update while "+tc.phase, func(t *testing.T) {
			owner := heldCluster(tc.phase, tc.phase == upgradePhasePaused)
			owner.UID = "cluster-uid"
			c := newClient(heldSTS())
			r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
			require.NoError(t, r.createOrUpdateResource(context.Background(), desired(), owner))
			assert.Equal(t, tc.want, partition(t, c))
		})
	}

	// A storage expansion deletes the StatefulSet; the next pass creates it.
	for _, tc := range []struct {
		name   string
		mutate func(*neo4jv1beta1.Neo4jEnterpriseCluster)
		want   int32
	}{
		{"created again while Paused: where the machine left it", nil, 2},
		{"created again before the upgrade staged: every server frozen",
			func(c *neo4jv1beta1.Neo4jEnterpriseCluster) { c.Status.UpgradeStatus.CurrentPartition = nil }, 3},
		{"created again for a tag the held upgrade does not target",
			func(c *neo4jv1beta1.Neo4jEnterpriseCluster) { c.Spec.Image.Tag = "2026.10.0-enterprise" }, 0},
		{"created with no upgrade", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) { c.Status.UpgradeStatus = nil }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := heldCluster(upgradePhasePaused, true)
			owner.UID = "cluster-uid"
			if tc.mutate != nil {
				tc.mutate(owner)
			}
			c := newClient()
			r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
			require.NoError(t, r.createOrUpdateResource(context.Background(), desired(), owner))
			assert.Equal(t, tc.want, partition(t, c))
		})
	}
}

// Paused waits for the user; Failed is retried; the user's acts resume a
// Paused one.
func TestIsUpgradeRequired_HeldUpgrade(t *testing.T) {
	scheme := makeUpgradeScheme()
	cases := []struct {
		name   string
		mutate func(*neo4jv1beta1.Neo4jEnterpriseCluster)
		want   bool
	}{
		{"Paused waits", nil, false},
		{"Paused, tag moved on", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) { c.Spec.Image.Tag = "2026.10.0-enterprise" }, true},
		{"Paused, tag moved back", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) { c.Spec.Image.Tag = holdRunning }, true},
		{"Paused, autoPauseOnFailure turned off", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Spec.UpgradeStrategy.AutoPauseOnFailure = false
		}, true},
		{"Paused by an older operator that recorded no target", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.TargetVersion = ""
			c.Spec.Image.Tag = "2026.10.0-enterprise"
		}, false},
		{"Failed is retried", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.Phase = upgradePhaseFailed
			c.Spec.UpgradeStrategy.AutoPauseOnFailure = false
		}, true},
		{"Failed, but a server is down", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.Phase = upgradePhaseFailed
			c.Status.Conditions = []metav1.Condition{{
				Type: ConditionTypeDegraded, Status: metav1.ConditionTrue, Reason: ConditionReasonServersUnavailable,
				LastTransitionTime: metav1.Now(),
			}}
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := heldCluster(upgradePhasePaused, true)
			if tc.mutate != nil {
				tc.mutate(c)
			}
			r := &Neo4jEnterpriseClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(c, heldSTS()).Build()}
			assert.Equal(t, tc.want, r.isUpgradeRequired(context.Background(), c))
		})
	}
}

// Taking up a held upgrade keeps its start and its progress: the progress
// says a server runs the target, which the version rule reads. A new upgrade
// after a completed one starts afresh.
func TestStartRollingUpgrade_TakesUpAHeldUpgrade(t *testing.T) {
	scheme := makeUpgradeScheme()
	run := func(t *testing.T, c *neo4jv1beta1.Neo4jEnterpriseCluster) (*neo4jv1beta1.UpgradeStatus, []string) {
		t.Helper()
		c.Spec.UpgradeStrategy.PreUpgradeHealthCheck = false
		fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(c, heldSTS()).WithStatusSubresource(c).Build()
		rec := record.NewFakeRecorder(10)
		r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: rec}
		_, err := r.startRollingUpgrade(context.Background(), c)
		require.NoError(t, err)
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, fc.Get(context.Background(), client.ObjectKeyFromObject(c), got))
		var events []string
		for len(rec.Events) > 0 {
			events = append(events, <-rec.Events)
		}
		return got.Status.UpgradeStatus, events
	}

	t.Run("a Failed upgrade, retried", func(t *testing.T) {
		c := heldCluster(upgradePhaseFailed, false)
		start := c.Status.UpgradeStatus.StartTime.Time
		us, events := run(t, c)
		assert.Equal(t, upgradePhaseStaging, us.Phase)
		assert.Equal(t, holdTarget, us.TargetVersion)
		assert.Equal(t, int32(1), us.Progress.InProgress, "a server runs the target: keep saying so")
		assert.WithinDuration(t, start, us.StartTime.Time, time.Second, "one upgrade, taken up again")
		require.Len(t, events, 1)
		assert.Contains(t, events[0], "Rolling upgrade resumed after it was Failed")
	})

	t.Run("a Paused upgrade, retargeted", func(t *testing.T) {
		c := heldCluster(upgradePhasePaused, true)
		c.Spec.Image.Tag = "2026.10.0-enterprise"
		us, _ := run(t, c)
		assert.Equal(t, "2026.10.0-enterprise", us.TargetVersion)
		assert.Equal(t, int32(1), us.Progress.InProgress)
	})

	t.Run("a new upgrade after a completed one", func(t *testing.T) {
		c := heldCluster(upgradePhaseCompleted, true)
		c.Status.UpgradeStatus.PreviousVersion = "2026.07.0-enterprise"
		c.Status.UpgradeStatus.Progress = &neo4jv1beta1.UpgradeProgress{Total: 3, Upgraded: 3}
		us, events := run(t, c)
		assert.Equal(t, int32(0), us.Progress.Upgraded)
		assert.Equal(t, int32(0), us.Progress.InProgress)
		assert.WithinDuration(t, time.Now(), us.StartTime.Time, time.Minute)
		require.Len(t, events, 1)
		assert.Contains(t, events[0], "Rolling upgrade started: "+holdRunning+" -> "+holdTarget)
	})
}

// An attempt a gate refuses before Staging records the target it was for, so
// a Paused one waits for that tag to change — the record used to keep the
// previous upgrade's target (or none), and a changed tag could not be told.
func TestStartRollingUpgrade_ARefusedAttemptNamesItsTarget(t *testing.T) {
	scheme := makeUpgradeScheme()
	c := heldCluster(upgradePhaseCompleted, true)
	c.Status.UpgradeStatus.TargetVersion = holdRunning // the completed upgrade's
	c.Status.UpgradeStatus.PreviousVersion = "2026.07.0-enterprise"
	c.Status.UpgradeStatus.Progress = &neo4jv1beta1.UpgradeProgress{Total: 3, Upgraded: 3}
	c.Spec.UpgradeStrategy.PreUpgradeHealthCheck = true // and there is no admin Secret to connect with
	sts := heldSTS()
	sts.Spec.Template.Spec.Containers[0].Image = "neo4j:" + holdRunning
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(c, sts).WithStatusSubresource(c).Build()
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: record.NewFakeRecorder(10)}

	_, err := r.startRollingUpgrade(context.Background(), c)
	require.NoError(t, err, "paused, not an error")

	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, fc.Get(context.Background(), client.ObjectKeyFromObject(c), got))
	us := got.Status.UpgradeStatus
	assert.Equal(t, upgradePhasePaused, us.Phase)
	assert.Equal(t, holdTarget, us.TargetVersion)
	assert.Equal(t, holdRunning, us.PreviousVersion)
	assert.Zero(t, us.Progress.Upgraded, "the completed upgrade's progress is not this one's")
	assert.True(t, strings.Contains(us.LastError, "failed to create Neo4j client"), us.LastError)

	got.Status.Phase = "Ready"
	assert.False(t, r.isUpgradeRequired(context.Background(), got), "Paused waits for the user")
}

// A held upgrade's servers below the partition are not a rollout in flight:
// the cluster can be Ready, and a server down is a server short.
func TestServerStatefulSetFullyRolled_HeldUpgrade(t *testing.T) {
	cases := []struct {
		name        string
		phase       string
		mutate      func(*appsv1.StatefulSet)
		wantRolled  bool
		wantRolling bool
	}{
		{"Paused, what it rolled is rolled", upgradePhasePaused, nil, true, false},
		{"Failed, what it rolled is rolled", upgradePhaseFailed, nil, true, false},
		{"Paused, staged but nothing rolled", upgradePhasePaused, func(s *appsv1.StatefulSet) {
			s.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To(int32(3))
			s.Status.UpdatedReplicas = 0
		}, true, false},
		{"Paused, a server it rolled is not on the template", upgradePhasePaused,
			func(s *appsv1.StatefulSet) { s.Status.UpdatedReplicas = 0 }, false, true},
		{"Paused, the controller has not seen the template", upgradePhasePaused,
			func(s *appsv1.StatefulSet) { s.Status.ObservedGeneration = 4 }, false, true},
		{"Paused, a server down", upgradePhasePaused,
			func(s *appsv1.StatefulSet) { s.Status.ReadyReplicas = 2 }, false, false},
		{"no upgrade held: a rollout", upgradePhaseCompleted, nil, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sts := heldSTS()
			if tc.mutate != nil {
				tc.mutate(sts)
			}
			c := heldCluster(tc.phase, true)
			c.Name = "c"
			r := gateTestReconciler(t, sts, c)
			rolled, detail := r.serverStatefulSetFullyRolled(context.Background(), c)
			assert.Equal(t, tc.wantRolled, rolled, detail)
			assert.Equal(t, tc.wantRolling, r.serverRolloutInFlight(context.Background(), c))
		})
	}
}

// A Ready cluster whose upgrade is held says so.
func TestClusterReadyMessage(t *testing.T) {
	paused := heldCluster(upgradePhasePaused, true)
	assert.Contains(t, clusterReadyMessage(paused), "its upgrade to "+holdTarget+" is paused (status.upgradeStatus)")
	assert.Contains(t, clusterReadyMessage(paused), "autoPauseOnFailure is set to false")

	failed := heldCluster(upgradePhaseFailed, false)
	assert.Contains(t, clusterReadyMessage(failed), "its upgrade to "+holdTarget+" failed (status.upgradeStatus)")

	for _, c := range []*neo4jv1beta1.Neo4jEnterpriseCluster{
		heldCluster(upgradePhaseCompleted, true),
		{Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{}},
	} {
		assert.Equal(t, "Neo4j cluster is fully formed and ready", clusterReadyMessage(c))
	}
	stale := heldCluster(upgradePhaseFailed, false)
	stale.Status.UpgradeStatus.TargetVersion = holdRunning // an older record of the upgrade that brought it here
	assert.Equal(t, "Neo4j cluster is fully formed and ready", clusterReadyMessage(stale))
}

// The servers a held upgrade has not rolled restart only through the machine:
// the retry of a Failed upgrade walks from a full freeze and lowers the
// partition past the servers already on the target, one gated step at a time.
func TestStepUpgradeRolling_ARetriedUpgradeRollsTheRestThroughTheGates(t *testing.T) {
	scheme := makeUpgradeScheme()
	c := heldCluster(upgradePhaseRolling, false)
	c.Status.UpgradeStatus.CurrentPartition = ptr.To(int32(3)) // Staging re-froze every server
	sts := heldSTS()
	sts.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To(int32(3))
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(c, sts).WithStatusSubresource(c).Build()
	var asked []string
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: record.NewFakeRecorder(10), UpgradeGates: &upgradeGates{
		serverAvailable: func(_ context.Context, _ *neo4jv1beta1.Neo4jEnterpriseCluster, pod string) (bool, string, error) {
			asked = append(asked, pod)
			return true, "", nil
		},
		databasesCaughtUp: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, []string, error) {
			return true, nil, nil
		},
	}}
	latest := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, fc.Get(context.Background(), client.ObjectKeyFromObject(c), got))
		return got
	}
	livePartition := func() int32 {
		got := &appsv1.StatefulSet{}
		require.NoError(t, fc.Get(context.Background(), client.ObjectKeyFromObject(sts), got))
		return *got.Spec.UpdateStrategy.RollingUpdate.Partition
	}

	// Nothing released yet: lower to server-2, which already runs the target.
	_, err := r.stepUpgradeRolling(context.Background(), latest())
	require.NoError(t, err)
	assert.Equal(t, int32(2), livePartition())
	// server-2 is on the template and passes its gates: release server-1.
	_, err = r.stepUpgradeRolling(context.Background(), latest())
	require.NoError(t, err)
	assert.Equal(t, int32(1), livePartition())
	assert.Equal(t, []string{"c-server-2"}, asked)
	// server-1 has not restarted yet (one server on the template): it waits.
	_, err = r.stepUpgradeRolling(context.Background(), latest())
	require.NoError(t, err)
	assert.Equal(t, int32(1), livePartition(), "server-0 waits for server-1's gates")
}
