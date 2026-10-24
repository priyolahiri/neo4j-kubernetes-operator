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

// What an upgrade's servers started on (upgrade_started_target.go), the
// message of a cluster short of servers while the machine walks it, and the
// record of an upgrade nothing will take up (#562 walk).

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestContainerStartedOn(t *testing.T) {
	const tag = "2026.09.9-enterprise"
	waiting := func(reason string) corev1.ContainerState {
		return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}
	}
	exited := corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}
	running := corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	for _, tc := range []struct {
		name  string
		image string
		state corev1.ContainerState
		last  corev1.ContainerState
		want  bool
	}{
		{"its image cannot be pulled", tag, waiting("ErrImagePull"), corev1.ContainerState{}, false},
		{"pull back-off", tag, waiting("ImagePullBackOff"), corev1.ContainerState{}, false},
		{"an invalid image name", tag, waiting("InvalidImageName"), corev1.ContainerState{}, false},
		{"its container could not be created", tag, waiting("CreateContainerConfigError"), corev1.ContainerState{}, false},
		{"still being created", tag, waiting("ContainerCreating"), corev1.ContainerState{}, false},
		{"running", tag, running, corev1.ContainerState{}, true},
		{"crash-looping: it ran", tag, waiting("CrashLoopBackOff"), exited, true},
		{"exited", tag, exited, corev1.ContainerState{}, true},
		{"running another image", "2026.08.1-enterprise", running, corev1.ContainerState{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := downServerPod(2, "neo4j:"+tc.image, tc.state, tc.last)
			assert.Equal(t, tc.want, containerStartedOn(pod, tag))
		})
	}
}

// A new upgrade starts with nothing started on its target; one that takes a
// held upgrade up keeps what that upgrade's servers started on; an earlier
// operator's record is taken as having run its target once the walk released
// a server, as its own rule did.
func TestNewUpgradeRecord_StartedTargetVersion(t *testing.T) {
	now := metav1.Now()
	fresh := newUpgradeRecord(nil, holdRunning, holdTarget, 3, now)
	assert.Equal(t, holdRunning, fresh.StartedTargetVersion)

	held := heldCluster(upgradePhasePaused, true).Status.UpgradeStatus
	held.StartedTargetVersion = holdRunning // server-2 never started on holdTarget
	assert.Equal(t, holdRunning, newUpgradeRecord(held, holdRunning, "2026.09.1-enterprise", 3, now).StartedTargetVersion)
	held.StartedTargetVersion = holdTarget
	assert.Equal(t, holdTarget, newUpgradeRecord(held, holdRunning, "2026.10.0-enterprise", 3, now).StartedTargetVersion)

	legacy := heldCluster(upgradePhasePaused, true).Status.UpgradeStatus // progress: one released, no field
	assert.Equal(t, holdTarget, newUpgradeRecord(legacy, holdRunning, "2026.10.0-enterprise", 3, now).StartedTargetVersion)
	legacy.Progress = &neo4jv1beta1.UpgradeProgress{Total: 3, Pending: 3}
	assert.Equal(t, holdRunning, newUpgradeRecord(legacy, holdRunning, "2026.10.0-enterprise", 3, now).StartedTargetVersion)
}

// The record follows the containers: nothing while server-2 cannot pull the
// target, the target once its container has started.
func TestRecordStartedTarget(t *testing.T) {
	ctx := context.Background()
	c := heldCluster(upgradePhaseRolling, false)
	c.Status.UpgradeStatus.StartedTargetVersion = holdRunning
	pulling := downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{})
	r, fc, _ := repairReconciler(t, c, heldSTS(),
		heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true), pulling)

	got := latestCluster(t, fc, c)
	r.recordStartedTarget(ctx, got)
	assert.Equal(t, holdRunning, latestCluster(t, fc, c).Status.UpgradeStatus.StartedTargetVersion,
		"server-2 has not run the target")

	started := &corev1.Pod{}
	require.NoError(t, fc.Get(ctx, client.ObjectKeyFromObject(pulling), started))
	started.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	require.NoError(t, fc.Status().Update(ctx, started))
	got = latestCluster(t, fc, c)
	r.recordStartedTarget(ctx, got)
	assert.Equal(t, holdTarget, latestCluster(t, fc, c).Status.UpgradeStatus.StartedTargetVersion)
	assert.Equal(t, holdTarget, got.Status.UpgradeStatus.StartedTargetVersion, "the pass's copy follows")
}

// With an earlier release's CRDs the API server drops the field, so every pass
// sends it again; that write changes nothing and is not an info line on every
// pass. Once the CRDs are applied it is recorded, and said once.
func TestRecordStartedTarget_ADroppedFieldIsNotAnInfoLineOnEveryPass(t *testing.T) {
	sink := newLevelCaptureSink()
	ctx := log.IntoContext(context.Background(), logr.New(sink))
	c := heldCluster(upgradePhaseRolling, false)
	c.Status.UpgradeStatus.StartedTargetVersion = holdRunning
	oldCRD := true
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(c, heldSTS(), heldServerPod(0, "neo4j:"+holdRunning, true),
			heldServerPod(1, "neo4j:"+holdRunning, true), heldServerPod(2, "neo4j:"+holdTarget, true)).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if oldCRD {
					return nil // pruned to what is stored: a no-op, the resourceVersion unchanged
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: newTestScheme(), Recorder: record.NewFakeRecorder(8)}
	const said = "has started on the upgrade's target"

	for pass := 0; pass < 3; pass++ {
		r.recordStartedTarget(ctx, latestCluster(t, fc, c))
	}
	assert.Equal(t, holdRunning, latestCluster(t, fc, c).Status.UpgradeStatus.StartedTargetVersion)
	assert.Empty(t, sink.mentioning(0, said), "a write the API server drops is not an info line on every pass")
	assert.Len(t, sink.mentioning(1, said), 3)

	oldCRD = false
	r.recordStartedTarget(ctx, latestCluster(t, fc, c))
	r.recordStartedTarget(ctx, latestCluster(t, fc, c))
	assert.Equal(t, holdTarget, latestCluster(t, fc, c).Status.UpgradeStatus.StartedTargetVersion)
	assert.Len(t, sink.mentioning(0, said), 1)
}

// The record is read where the tag rule acts on it: on every pass of the walk,
// when a held upgrade is taken up, and on the normal passes that hold one.
func TestRecordStartedTarget_IsReadWhereTheTagRuleActs(t *testing.T) {
	ctx := context.Background()
	servers := func() []client.Object {
		return []client.Object{heldSTS(),
			heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true),
			heldServerPod(2, "neo4j:"+holdTarget, true)} // server-2 has started on the target
	}
	nothingRecorded := func(c *neo4jv1beta1.Neo4jEnterpriseCluster) *neo4jv1beta1.Neo4jEnterpriseCluster {
		c.Status.UpgradeStatus.StartedTargetVersion = holdRunning
		return c
	}

	t.Run("a pass of the walk", func(t *testing.T) {
		c := nothingRecorded(heldCluster(upgradePhaseRolling, false))
		r, fc, _ := repairReconciler(t, append(servers(), c)...)
		_, _ = r.stepUpgradeRolling(ctx, latestCluster(t, fc, c))
		assert.Equal(t, holdTarget, latestCluster(t, fc, c).Status.UpgradeStatus.StartedTargetVersion)
	})

	t.Run("a held upgrade taken up towards a new tag", func(t *testing.T) {
		c := nothingRecorded(heldCluster(upgradePhasePaused, true))
		c.Spec.Image.Tag = "2026.10.0-enterprise"
		c.Spec.UpgradeStrategy.PreUpgradeHealthCheck = false
		r, fc, _ := repairReconciler(t, append(servers(), c)...)
		_, err := r.startRollingUpgrade(ctx, latestCluster(t, fc, c))
		require.NoError(t, err)
		us := latestCluster(t, fc, c).Status.UpgradeStatus
		assert.Equal(t, "2026.10.0-enterprise", us.TargetVersion)
		assert.Equal(t, holdTarget, us.StartedTargetVersion, "the new record keeps what server-2 started on")
	})

	t.Run("a normal pass that holds the upgrade", func(t *testing.T) {
		c := nothingRecorded(heldCluster(upgradePhasePaused, true))
		c.Finalizers = []string{ClusterFinalizer}
		c.Spec.Storage.ClassName = ""
		r, fc, _ := repairReconciler(t, append(servers(), c)...)
		r.RequeueAfter = time.Second
		_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)})
		assert.Equal(t, holdTarget, latestCluster(t, fc, c).Status.UpgradeStatus.StartedTargetVersion)
	})
}

// A target changed mid-walk to below what a server has meanwhile started on
// is not staged: validation read the record before that server started.
func TestHandleRollingUpgrade_ARetargetBelowWhatAServerStartedOnWaits(t *testing.T) {
	ctx := context.Background()
	c := heldCluster(upgradePhaseRolling, false)
	c.Status.UpgradeStatus.StartedTargetVersion = holdRunning
	c.Spec.Image.Tag = "2026.08.5-enterprise" // below holdTarget, which server-2 now runs
	r, fc, _ := repairReconciler(t, c, heldSTS(),
		heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true),
		heldServerPod(2, "neo4j:"+holdTarget, true))

	_, err := r.handleRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	us := latestCluster(t, fc, c).Status.UpgradeStatus
	assert.Equal(t, upgradePhaseRolling, us.Phase, "not re-staged")
	assert.Equal(t, holdTarget, us.TargetVersion)
	assert.Equal(t, holdTarget, us.StartedTargetVersion)
}

// The #562 walk: a repair walk on a Degraded cluster kept the message the
// normal pass wrote before it — naming the server it then replaced as
// unavailable.
func TestStartRollingUpgrade_ARepairOnADegradedClusterSaysTheUpgradeIsInProgress(t *testing.T) {
	ctx := context.Background()
	c, sts := repairFixture()
	c.Status.Phase = neo4jv1beta1.PhaseDegraded
	c.Status.Message = "Neo4j cluster is serving but degraded for longer than 5m0s. 2 of 3 servers available; unavailable: c-server-2 (Unavailable)"
	r, fc, _ := repairReconciler(t, c, sts,
		heldServerPod(0, "neo4j:"+holdRunning, true), heldServerPod(1, "neo4j:"+holdRunning, true),
		downServerPod(2, "neo4j:"+holdTarget, pullBackOff(), corev1.ContainerState{}))

	_, err := r.startRollingUpgrade(ctx, latestCluster(t, fc, c))
	require.NoError(t, err)
	got := latestCluster(t, fc, c)
	require.Equal(t, upgradePhaseStaging, got.Status.UpgradeStatus.Phase)
	assert.Equal(t, neo4jv1beta1.PhaseDegraded, got.Status.Phase, "the next normal pass judges the phase")
	assert.NotContains(t, got.Status.Message, "Unavailable")
	assert.Contains(t, got.Status.Message, "upgrade to "+repairTag+" is in progress (status.upgradeStatus)")
}

// An upgrade a check refused before Staging, whose tag the user then moved
// back to status.version: nothing will take it up, and its message said "failed
// …, and the operator takes it up again" for as long as the record stayed
// (#562 walk, still there after 5 minutes). The record is dropped.
func TestForgetAbandonedUpgrade(t *testing.T) {
	ctx := context.Background()
	abandoned := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		c := heldCluster(upgradePhaseFailed, false)
		c.Spec.Image.Tag = holdRunning // moved back
		c.Status.UpgradeStatus.StartedTargetVersion = holdRunning
		c.Status.UpgradeStatus.Progress = &neo4jv1beta1.UpgradeProgress{Total: 3, Pending: 3}
		c.Status.UpgradeStatus.CurrentPartition = nil
		return c
	}
	unstaged := func() client.Object {
		sts := heldSTS()
		sts.Spec.Template.Spec.Containers[0].Image = "neo4j:" + holdRunning
		sts.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To(int32(0))
		return sts
	}
	for _, tc := range []struct {
		name   string
		mutate func(*neo4jv1beta1.Neo4jEnterpriseCluster)
		sts    client.Object
		forget bool
	}{
		{"refused before Staging, tag moved back", nil, unstaged(), true},
		{"an earlier operator's record that released no server", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.StartedTargetVersion = ""
		}, unstaged(), true},
		{"the spec still asks for it (retried)", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) { c.Spec.Image.Tag = holdTarget }, unstaged(), false},
		{"its template carries the target: the machine's to finish", nil, heldSTS(), false},
		{"a server started on it", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.StartedTargetVersion = holdTarget
		}, unstaged(), false},
		{"an earlier operator's record that released a server", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.StartedTargetVersion = ""
			c.Status.UpgradeStatus.Progress = &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1}
		}, unstaged(), false},
		{"no target: an earlier operator's pause waits for the user", func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Status.UpgradeStatus.TargetVersion = ""
		}, unstaged(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := abandoned()
			if tc.mutate != nil {
				tc.mutate(c)
			}
			r, fc, _ := repairReconciler(t, c, tc.sts)
			got := latestCluster(t, fc, c)
			r.forgetAbandonedUpgrade(ctx, got)
			stored := latestCluster(t, fc, c).Status.UpgradeStatus
			if !tc.forget {
				require.NotNil(t, stored)
				return
			}
			assert.Nil(t, stored)
			assert.Nil(t, got.Status.UpgradeStatus)
			assert.Equal(t, "Neo4j cluster is fully formed and ready", clusterReadyMessage(got))
		})
	}

	// Through Reconcile: the stale message goes on the next Ready pass.
	t.Run("the pass drops it before anything reads it", func(t *testing.T) {
		c := abandoned()
		c.Finalizers = []string{ClusterFinalizer}
		c.Spec.Storage.ClassName = "" // the default class: nothing to look up
		c.Status.Message = clusterReadyMessage(heldCluster(upgradePhaseFailed, false))
		r, fc, _ := repairReconciler(t, c, unstaged())
		r.RequeueAfter = time.Second
		_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)})
		assert.Nil(t, latestCluster(t, fc, c).Status.UpgradeStatus)
	})
}
