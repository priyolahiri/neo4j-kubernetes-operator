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

// A retarget never puts a server the upgrade moved on back below the
// partition, where a recreated pod comes back on the version before the
// upgrade (upgrade_retarget.go, operations.md id 180). Played against
// statefulSetEmulator: version 0 is holdRunning, the first target A holdTarget,
// the new target B repairTag.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// walkingTo is a 3-server cluster whose upgrade from holdRunning to
// holdTarget has rolled the servers at and above partition, in phase. The
// StatefulSet's current revision is holdRunning's; downAt names a server
// whose pod is not Ready.
func walkingTo(t *testing.T, phase string, partition int32, downAt ...int) (*Neo4jEnterpriseClusterReconciler, client.Client, *statefulSetEmulator, *neo4jv1beta1.Neo4jEnterpriseCluster) {
	t.Helper()
	c := clusterForUpgrade("c", "default", holdRunning, holdTarget, 3)
	c.UID = "c-uid"
	c.Finalizers = []string{ClusterFinalizer}
	c.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: true}
	c.Status.Phase = neo4jv1beta1.PhaseReady
	c.Status.Conditions = []metav1.Condition{clusterFormedCondition()}
	now := metav1.Now()
	start := metav1.NewTime(time.Now().Add(-time.Hour))
	c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
		Phase: phase, PreviousVersion: holdRunning, TargetVersion: holdTarget, StartedTargetVersion: holdTarget,
		StartTime: &start, StepStartTime: &now, CurrentPartition: ptr.To(partition),
		Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, Upgraded: 3 - partition - 1, InProgress: 1, Pending: partition},
	}

	scheme := makeUpgradeScheme()
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(c).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				obj.SetUID(types.UID(obj.GetName() + "-uid"))
				return cl.Create(ctx, obj, opts...)
			},
		}).Build()
	emu := newStatefulSetEmulator(t, fc)
	before := emu.register(resources.BuildServerStatefulSetForEnterprise(clusterForUpgrade("c", "default", holdRunning, holdRunning, 3)).Spec.Template)
	sts := resources.BuildServerStatefulSetForEnterprise(c)
	sts.OwnerReferences = []metav1.OwnerReference{{APIVersion: "neo4j.neo4j.com/v1beta1", Kind: "Neo4jEnterpriseCluster",
		Name: "c", UID: c.UID, Controller: ptr.To(true)}}
	sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(partition)}
	require.NoError(t, fc.Create(context.Background(), sts))
	target := emu.register(sts.Spec.Template)
	stored := emu.sts()
	stored.Status.CurrentRevision = before
	require.NoError(t, fc.Status().Update(context.Background(), stored))
	for ordinal := 0; ordinal < 3; ordinal++ {
		revision := before
		if int32(ordinal) >= partition {
			revision = target
		}
		emu.createPod(ordinal, revision)
	}
	for _, ordinal := range downAt {
		pod := emu.pods()[ordinal]
		pod.Status.Conditions[0].Status = corev1.ConditionFalse
		pod.Status.ContainerStatuses[0].Ready = false
		require.NoError(t, fc.Status().Update(context.Background(), pod))
	}
	stored = emu.sts()
	stored.Status.UpdateRevision = target
	stored.Status.ObservedGeneration = stored.Generation
	stored.Status.Replicas = 3
	stored.Status.UpdatedReplicas = 3 - partition
	stored.Status.ReadyReplicas = int32(3 - len(downAt))
	require.NoError(t, fc.Status().Update(context.Background(), stored))
	emu.created = map[int][]string{}

	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: scheme, Recorder: record.NewFakeRecorder(100),
		ConfigMapManager: NewConfigMapManager(fc), UpgradeGates: &upgradeGates{
			serverAvailable: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, string, error) {
				return true, "", nil
			},
			databasesCaughtUp: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, []string, error) {
				return true, nil, nil
			},
		}}
	return r, fc, emu, c
}

// setTag sets the cluster's spec.image.tag as the user does.
func setTag(t *testing.T, fc client.Client, c *neo4jv1beta1.Neo4jEnterpriseCluster, tag string) {
	t.Helper()
	got := latestCluster(t, fc, c)
	got.Spec.Image.Tag = tag
	require.NoError(t, fc.Update(context.Background(), got))
}

// drain deletes server pod ordinal and lets the StatefulSet controller create
// it again, as a node drain does.
func (e *statefulSetEmulator) drain(ordinal int) {
	e.t.Helper()
	pod := e.pods()[ordinal]
	require.NotNil(e.t, pod)
	require.NoError(e.t, e.c.Delete(context.Background(), pod))
	e.recreateMissing()
	e.observe()
}

// rollToTheEnd runs the walk with the StatefulSet controller in between until
// it leaves Rolling.
func rollToTheEnd(t *testing.T, r *Neo4jEnterpriseClusterReconciler, fc client.Client, emu *statefulSetEmulator, c *neo4jv1beta1.Neo4jEnterpriseCluster) *neo4jv1beta1.Neo4jEnterpriseCluster {
	t.Helper()
	for step := 0; step < 15; step++ {
		emu.observe()
		emu.deleteForUpdate()
		emu.recreateMissing()
		emu.observe()
		got := latestCluster(t, fc, c)
		if got.Status.UpgradeStatus.Phase != upgradePhaseRolling && got.Status.UpgradeStatus.Phase != upgradePhaseStaging {
			return got
		}
		_, err := r.handleRollingUpgrade(context.Background(), got)
		require.NoError(t, err)
	}
	return latestCluster(t, fc, c)
}

func TestRetarget_MidWalk_AServerTheWalkRolledNeverGoesBack(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := walkingTo(t, upgradePhaseRolling, 1) // servers 1 and 2 run the first target

	setTag(t, fc, c, repairTag)
	_, err := r.handleRollingUpgrade(ctx, latestCluster(t, fc, c)) // the retarget: back to Staging
	require.NoError(t, err)
	require.Equal(t, upgradePhaseStaging, latestCluster(t, fc, c).Status.UpgradeStatus.Phase)
	_, err = r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Equal(t, int32(1), *statefulSetPartition(emu.sts()), "staged at the lowest server on the first target")
	assert.Equal(t, int32(1), *latestCluster(t, fc, c).Status.UpgradeStatus.CurrentPartition)
	emu.observe()

	// A node drain takes server-1 before anything else moves.
	emu.drain(1)
	assert.Equal(t, []string{"neo4j:" + repairTag}, emu.created[1], "server-1 comes back on the new target, not on the version before the upgrade")

	got := rollToTheEnd(t, r, fc, emu, c)
	require.Equal(t, upgradePhaseStabilizing, got.Status.UpgradeStatus.Phase, got.Status.UpgradeStatus.LastError)
	for ordinal, images := range emu.created {
		for _, image := range images {
			assert.Equal(t, "neo4j:"+repairTag, image, "server-%d was created on %s", ordinal, image)
		}
	}
}

func TestRetarget_HeldUpgrade_AServerTheWalkRolledNeverGoesBack(t *testing.T) {
	ctx := context.Background()
	// Paused with servers 1 and 2 on the first target, every server up (a
	// database that did not catch up in time).
	r, fc, emu, c := walkingTo(t, upgradePhasePaused, 1)
	stored := latestCluster(t, fc, c)
	stored.Spec.UpgradeStrategy.PreUpgradeHealthCheck = false
	require.NoError(t, fc.Update(ctx, stored))

	setTag(t, fc, c, repairTag)
	require.True(t, r.isUpgradeRequired(ctx, latestCluster(t, fc, c)))
	_, err := r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	require.Equal(t, upgradePhaseStaging, latestCluster(t, fc, c).Status.UpgradeStatus.Phase, latestCluster(t, fc, c).Status.UpgradeStatus.LastError)
	_, err = r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Equal(t, int32(1), *statefulSetPartition(emu.sts()))
	emu.observe()

	emu.drain(2)
	assert.Equal(t, []string{"neo4j:" + repairTag}, emu.created[2])
	got := rollToTheEnd(t, r, fc, emu, c)
	require.Equal(t, upgradePhaseStabilizing, got.Status.UpgradeStatus.Phase, got.Status.UpgradeStatus.LastError)
	for ordinal, images := range emu.created {
		for _, image := range images {
			assert.Equal(t, "neo4j:"+repairTag, image, "server-%d was created on %s", ordinal, image)
		}
	}
}

// The write that stages the new target at such a partition restarts servers at
// once: it waits while a server below the partition is down.
func TestRetarget_MidWalk_WaitsWhileAServerBelowIsDown(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := walkingTo(t, upgradePhaseRolling, 2, 0) // server-0, on the version before, is down

	setTag(t, fc, c, repairTag)
	_, err := r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	_, err = r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Equal(t, "neo4j:"+holdTarget, serverImage(emu.sts()), "nothing staged while server-0 is down")
	assert.Equal(t, upgradePhaseStaging, latestCluster(t, fc, c).Status.UpgradeStatus.Phase)

	pod := emu.pods()[0]
	pod.Status.Conditions[0].Status = corev1.ConditionTrue
	pod.Status.ContainerStatuses[0].Ready = true
	require.NoError(t, fc.Status().Update(ctx, pod))
	emu.observe()
	_, err = r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Equal(t, "neo4j:"+repairTag, serverImage(emu.sts()))
	assert.Equal(t, int32(2), *statefulSetPartition(emu.sts()))
}

// Every server runs the first target and Kubernetes has not recorded it as
// current yet (the walk had just finished): Staging waits for that, then
// freezes every server and the walk checks each one.
func TestRetarget_AfterEveryServerRolled_WaitsForTheCurrentRevision(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := walkingTo(t, upgradePhaseVerifying, 0)
	setTag(t, fc, c, repairTag)
	_, err := r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	_, err = r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Equal(t, "neo4j:"+holdTarget, serverImage(emu.sts()), "waits for the StatefulSet controller")

	emu.observe() // every pod updated and Ready: the first target is current now
	_, err = r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	assert.Equal(t, "neo4j:"+repairTag, serverImage(emu.sts()))
	assert.Equal(t, int32(3), *statefulSetPartition(emu.sts()), "every server frozen: the walk checks each")
	emu.observe()
	emu.drain(0)
	assert.Equal(t, []string{"neo4j:" + holdTarget}, emu.created[0], "a recreated server comes back on the version it ran")

	got := rollToTheEnd(t, r, fc, emu, c)
	require.Equal(t, upgradePhaseStabilizing, got.Status.UpgradeStatus.Phase, got.Status.UpgradeStatus.LastError)
}

func TestRetargetPartition(t *testing.T) {
	sts := gateTestSTS("neo4j:"+holdTarget, 3, "rev-0", "rev-a", 1, 3)
	pods := func(revisions ...string) []corev1.Pod {
		var out []corev1.Pod
		for i, rev := range revisions {
			pod := heldServerPod(i, "x", true)
			pod.Labels[appsv1.ControllerRevisionHashLabelKey] = rev
			out = append(out, *pod)
		}
		return out
	}
	for _, tc := range []struct {
		name      string
		pods      []corev1.Pod
		partition int32
		repairing bool
		want      int32
		waits     bool
	}{
		{"every server on the current revision", pods("rev-0", "rev-0", "rev-0"), 3, false, 3, false},
		{"the walk rolled server-2", pods("rev-0", "rev-0", "rev-a"), 3, false, 2, false},
		{"servers 1 and 2", pods("rev-0", "rev-a", "rev-a"), 3, false, 1, false},
		{"a repair at 2 with server-1 on the first target", pods("rev-0", "rev-a", "rev-a"), 2, true, 1, false},
		{"every server on the update revision", pods("rev-a", "rev-a", "rev-a"), 3, false, 3, true},
		{"every server moved on, a repair", pods("rev-a", "rev-a", "rev-a"), 2, true, 0, false},
		{"every server moved on, not all to the update revision", pods("rev-b", "rev-a", "rev-a"), 3, false, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, wait := retargetPartition(sts, tc.pods, tc.partition, tc.repairing)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.waits, wait != "")
		})
	}
	sts.Status.CurrentRevision = ""
	got, wait := retargetPartition(sts, pods("rev-0", "rev-a", "rev-a"), 3, false)
	assert.Equal(t, int32(3), got, "no current revision recorded: nothing to compare with")
	assert.Empty(t, wait)
}
