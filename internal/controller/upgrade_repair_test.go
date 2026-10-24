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

// A held upgrade whose restarted servers cannot start on its target goes on
// towards a new tag (upgrade_repair.go, operations.md id 162). Paused after
// server-2 restarted on a target it cannot run, the cluster is short of
// server-2, so the machine never started again, the image hold pinned the new
// tag, and Staging would have frozen server-2 with the rest: changing the tag
// did nothing, towards any tag (#538).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const repairTag = "2026.10.0-enterprise"

// downServerPod is heldServerPod not Ready, its neo4j container in state.
func downServerPod(ordinal int, image string, state corev1.ContainerState, last corev1.ContainerState) *corev1.Pod {
	pod := heldServerPod(ordinal, image, false)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "neo4j", State: state, LastTerminationState: last}}
	return pod
}

func pullBackOff() corev1.ContainerState {
	return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}
}

func crashLoop() (corev1.ContainerState, corev1.ContainerState) {
	return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
		corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}
}

// repairFixture: a Paused upgrade to holdTarget that restarted server-2
// (partition 2), the cluster short of server-2 (Ready, Degraded condition),
// spec.image.tag moved on to repairTag.
func repairFixture() (*neo4jv1beta1.Neo4jEnterpriseCluster, *appsv1.StatefulSet) {
	c := heldCluster(upgradePhasePaused, true)
	c.Spec.Image.Tag = repairTag
	c.Status.Conditions = []metav1.Condition{
		clusterFormedCondition(),
		serversUnavailableCondition("2 of 3 servers available; unavailable: c-server-2 (Unavailable)"),
	}
	sts := heldSTS()
	sts.Status.ReadyReplicas = 2
	return c, sts
}

func repairReconciler(t *testing.T, objs ...client.Object) (*Neo4jEnterpriseClusterReconciler, client.Client, *record.FakeRecorder) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()
	rec := record.NewFakeRecorder(20)
	return &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: scheme, Recorder: rec, ConfigMapManager: NewConfigMapManager(fc)}, fc, rec
}

func TestPodCannotStart(t *testing.T) {
	waiting, last := crashLoop()
	running := corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"its image cannot be pulled", downServerPod(2, "x", pullBackOff(), corev1.ContainerState{}), true},
		{"crash-looping", downServerPod(2, "x", waiting, last), true},
		{"running again after a failure: may be starting fine", downServerPod(2, "x", running, last), false},
		{"starting", downServerPod(2, "x", corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}, corev1.ContainerState{}), false},
		{"an init container cannot pull its image", func() *corev1.Pod {
			p := downServerPod(2, "x", corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}, corev1.ContainerState{})
			p.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "truststore-init", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull"}}}}
			return p
		}(), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			described, ok := podCannotStart(tc.pod)
			assert.Equal(t, tc.want, ok, described)
			if ok {
				assert.Contains(t, described, "c-server-2")
			}
		})
	}
}

// The machine takes the held upgrade up although the cluster is short of a
// server only when the servers down are the ones it restarted, failing to
// start, and the tag is a new one.
func TestIsUpgradeRequired_RepairsAHeldUpgradesServersThatCannotStart(t *testing.T) {
	up0, up1 := heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true)
	waiting, last := crashLoop()
	cases := []struct {
		name   string
		mutate func(*neo4jv1beta1.Neo4jEnterpriseCluster)
		pods   []client.Object
		want   bool
	}{
		{"server-2 cannot pull the target, the tag moved on", nil,
			[]client.Object{up0, up1, downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{})}, true},
		{"server-2 crash-loops on the target, the tag moved on", nil,
			[]client.Object{up0, up1, downServerPod(2, "neo4j:"+holdTarget, waiting, last)}, true},
		{"Failed, the tag moved on", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.Phase = upgradePhaseFailed
		}, []client.Object{up0, up1, downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{})}, true},
		{"the same target: it cannot start there either", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Spec.Image.Tag = holdTarget
			c.Spec.UpgradeStrategy.AutoPauseOnFailure = false
		}, []client.Object{up0, up1, downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{})}, false},
		{"server-2 still starting", nil,
			[]client.Object{up0, up1, downServerPod(2, "neo4j:"+holdTarget, corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}, corev1.ContainerState{})}, false},
		{"server-1, below the partition, is down too", nil,
			[]client.Object{up0, downServerPod(1, "neo4j:"+holdRunning, waiting, last), downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{})}, false},
		{"server-2 missing", nil, []client.Object{up0, up1}, false},
		{"Neo4j reports another server unavailable", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.Diagnostics = &neo4jv1beta1.ClusterDiagnosticsStatus{Servers: []neo4jv1beta1.ServerDiagnosticInfo{
				{Name: "s0", Address: "c-server-0.c-headless.default.svc.cluster.local:7687", Health: "Unavailable"},
			}}
		}, []client.Object{up0, up1, downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{})}, false},
		{"no upgrade held", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.Phase = upgradePhaseCompleted
		}, []client.Object{up0, up1, downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{})}, false},
		{"a cluster that never formed", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.Phase, c.Status.Conditions = "Forming", nil
		}, []client.Object{up0, up1, downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{})}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, sts := repairFixture()
			if tc.mutate != nil {
				tc.mutate(c)
			}
			objs := []client.Object{c, sts}
			for _, pod := range tc.pods {
				p, ok := pod.(*corev1.Pod)
				require.True(t, ok)
				objs = append(objs, p.DeepCopy())
			}
			r, _, _ := repairReconciler(t, objs...)
			assert.Equal(t, tc.want, r.isUpgradeRequired(context.Background(), c))
		})
	}
}

// Taken up, the upgrade skips the checks that need every server, and Staging
// freezes at server-2 instead of at the replica count, so the StatefulSet
// replaces server-2 on the new template first; server-0 and server-1 wait
// for the walk's gates.
func TestStartRollingUpgrade_TheRepairReplacesTheServersThatCannotStartFirst(t *testing.T) {
	ctx := context.Background()
	c, sts := repairFixture()
	c.Spec.UpgradeStrategy.PreUpgradeHealthCheck = true // it would refuse: no admin Secret to connect with
	r, fc, rec := repairReconciler(t, c, sts,
		heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true),
		downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{}))

	_, err := r.startRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	got := latestCluster(t, fc, c)
	require.Equal(t, upgradePhaseStaging, got.Status.UpgradeStatus.Phase, got.Status.UpgradeStatus.LastError)
	assert.Equal(t, repairTag, got.Status.UpgradeStatus.TargetVersion)
	assert.Equal(t, int32(1), got.Status.UpgradeStatus.Progress.InProgress, "the held upgrade's progress is kept")
	started := eventsWith(rec, EventReasonUpgradeStarted)
	require.Len(t, started, 1)
	assert.Contains(t, started[0], "c-server-2 (container neo4j: ImagePullBackOff) cannot start on "+holdTarget+" and is replaced first")

	_, err = r.stepUpgradeStaging(ctx, got)
	require.NoError(t, err)
	live := &appsv1.StatefulSet{}
	require.NoError(t, fc.Get(ctx, client.ObjectKeyFromObject(sts), live))
	assert.Equal(t, "neo4j:"+repairTag, live.Spec.Template.Spec.Containers[0].Image)
	require.NotNil(t, live.Spec.UpdateStrategy.RollingUpdate.Partition)
	assert.Equal(t, int32(2), *live.Spec.UpdateStrategy.RollingUpdate.Partition, "server-2 is replaced first; the others wait")
	assert.Equal(t, int32(3), *live.Spec.Replicas)
	got = latestCluster(t, fc, c)
	assert.Equal(t, upgradePhaseRolling, got.Status.UpgradeStatus.Phase)
	assert.Equal(t, int32(2), *got.Status.UpgradeStatus.CurrentPartition)
}

// Staging freezes every server when none is down, as before, and keeps the
// replica count whatever spec.topology.servers says (operations.md id 163).
func TestStepUpgradeStaging_FreezesEveryServerAndKeepsTheReplicaCount(t *testing.T) {
	ctx := context.Background()
	for _, servers := range []int32{2, 3, 4} {
		c := heldCluster(upgradePhaseStaging, true)
		c.Spec.Image.Tag = repairTag
		c.Spec.Topology.Servers = servers
		now := metav1.Now()
		c.Status.UpgradeStatus.StepStartTime = &now
		sts := heldSTS()
		r, fc, _ := repairReconciler(t, c, sts,
			heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true), heldServerPod(2, "neo4j:"+holdTarget, true))
		_, err := r.stepUpgradeStaging(ctx, latestCluster(t, fc, c))
		require.NoError(t, err)
		live := &appsv1.StatefulSet{}
		require.NoError(t, fc.Get(ctx, client.ObjectKeyFromObject(sts), live))
		assert.Equal(t, int32(3), *live.Spec.UpdateStrategy.RollingUpdate.Partition, "spec.topology.servers %d", servers)
		assert.Equal(t, int32(3), *live.Spec.Replicas, "an upgrade changes no topology (spec.topology.servers %d)", servers)
	}
}
