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

// A server pod that cannot start and is due for the update is replaced by the
// operator, where the StatefulSet controller does not replace it
// (stuck_server_pods.go, operations.md id 204): Kubernetes 1.35.0–1.35.3 run
// MaxUnavailableStatefulSet on, and its rolling update deletes nothing while a
// pod is unavailable — the pod that cannot start is one.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// pluginRepairSTS is a 3-server StatefulSet whose template a plugin's repair
// has just written (update revision rev-fixed), seen by the StatefulSet
// controller, servers 0 and 1 up on rev-broken.
func pluginRepairSTS() *appsv1.StatefulSet {
	sts := gateTestSTS("neo4j:"+holdRunning, 3, "rev-broken", "rev-fixed", 0, 2)
	sts.Generation, sts.Status.ObservedGeneration = 4, 4
	return sts
}

func revisionPod(ordinal int, revision string, ready bool) *corev1.Pod {
	pod := heldServerPod(ordinal, "neo4j:"+holdRunning, ready)
	pod.UID = types.UID(pod.Name + "-uid")
	pod.Labels[appsv1.ControllerRevisionHashLabelKey] = revision
	return pod
}

// initFailing is server pod ordinal on revision, its plugin's init container
// failing.
func initFailing(ordinal int, revision string) *corev1.Pod {
	pod := revisionPod(ordinal, revision, false)
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "plugin-init-apoc",
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"}},
		State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}
	return pod
}

func TestStuckServerPods(t *testing.T) {
	names := func(pods []*corev1.Pod) []string {
		var out []string
		for _, p := range pods {
			out = append(out, p.Name)
		}
		return out
	}
	up := func(ordinal int) corev1.Pod { return *revisionPod(ordinal, "rev-broken", true) }
	for _, tc := range []struct {
		name string
		sts  func(*appsv1.StatefulSet)
		pods []corev1.Pod
		want []string
	}{
		{"the pod a plugin's repair is for", nil, []corev1.Pod{up(0), up(1), *initFailing(2, "rev-broken")}, []string{"c-server-2"}},
		{"an image it cannot pull", nil, []corev1.Pod{up(0), up(1),
			func() corev1.Pod {
				p := downServerPod(2, "neo4j:"+neverPublished, pullBackOff(), corev1.ContainerState{})
				p.Labels[appsv1.ControllerRevisionHashLabelKey] = "rev-broken"
				return *p
			}()}, []string{"c-server-2"}},
		{"already on the update revision: nothing a delete changes", nil, []corev1.Pod{up(0), up(1), *initFailing(2, "rev-fixed")}, nil},
		{"still starting, not failing", nil, []corev1.Pod{up(0), up(1), *revisionPod(2, "rev-broken", false)}, nil},
		{"not yet seen by the StatefulSet controller", func(s *appsv1.StatefulSet) { s.Generation = 5 },
			[]corev1.Pod{up(0), up(1), *initFailing(2, "rev-broken")}, nil},
		{"below the partition: it would come back on the current revision", func(s *appsv1.StatefulSet) {
			s.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(int32(1))}
		}, []corev1.Pod{*initFailing(0, "rev-broken"), up(1), up(2)}, nil},
		{"two that cannot start", nil, []corev1.Pod{up(0), *initFailing(1, "rev-broken"), *initFailing(2, "rev-broken")},
			[]string{"c-server-1", "c-server-2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := pluginRepairSTS()
			if tc.sts != nil {
				tc.sts(sts)
			}
			got, _ := stuckServerPods(sts, tc.pods)
			assert.Equal(t, tc.want, names(got))
		})
	}
}

// The normal pass, after a plugin's repair on a cluster short of server-2: the
// pod is deleted, announced, and nothing else is touched.
func TestReconcileStuckServerPods_ReplacesThePodTheRepairIsFor(t *testing.T) {
	ctx := context.Background()
	cluster := availCluster(neo4jv1beta1.PhaseReady)
	r := gateTestReconciler(t, pluginRepairSTS(), cluster,
		revisionPod(0, "rev-broken", true), revisionPod(1, "rev-broken", true), initFailing(2, "rev-broken"))
	rec := record.NewFakeRecorder(8)
	r.Recorder = rec

	r.reconcileStuckServerPods(ctx, cluster)

	err := r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c-server-2"}, &corev1.Pod{})
	assert.True(t, apierrors.IsNotFound(err), "server-2 is deleted, for the StatefulSet to create it again from rev-fixed")
	for _, name := range []string{"c-server-0", "c-server-1"} {
		require.NoError(t, r.Get(ctx, client.ObjectKey{Namespace: "default", Name: name}, &corev1.Pod{}), "%s is up: untouched", name)
	}
	events := drainEvents(rec)
	require.Len(t, events, 1)
	assert.True(t, strings.HasPrefix(events[0], "Normal "+EventReasonServerPodReplaced+" Deleted pod c-server-2, which cannot start on revision rev-broken (init container plugin-init-apoc exited with code 1), so that StatefulSet c-server creates it again from its update revision rev-fixed"), events[0])

	// Nothing to do on a StatefulSet whose update has reached every pod.
	whole := pluginRepairSTS()
	whole.Status.CurrentRevision, whole.Status.ReadyReplicas = "rev-fixed", 3
	r = gateTestReconciler(t, whole, cluster, initFailing(2, "rev-broken"))
	r.reconcileStuckServerPods(ctx, cluster)
	require.NoError(t, r.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c-server-2"}, &corev1.Pod{}))
}

// Through Reconcile: the normal pass replaces the pod after it applies the
// StatefulSet.
func TestReconcile_ReplacesAServerThatCannotStartOnTheRevisionBeingLeft(t *testing.T) {
	ctx := context.Background()
	c := expansionCluster("1Gi")
	c.Status.UpgradeStatus = nil
	c.Finalizers = []string{ClusterFinalizer}
	c.Spec.Storage.ClassName = "" // the default class: nothing to look up
	sts := resources.BuildServerStatefulSetForEnterprise(c)
	sts.Status = pluginRepairSTS().Status
	r, fc, rec := repairReconciler(t, c, sts,
		revisionPod(0, "rev-broken", true), revisionPod(1, "rev-broken", true), initFailing(2, "rev-broken"))
	r.RequeueAfter = time.Second
	stored := storedServerSTS(t, fc)
	stored.Status = pluginRepairSTS().Status
	stored.Status.ObservedGeneration = stored.Generation
	require.NoError(t, fc.Status().Update(ctx, stored))

	_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(c)})

	err := fc.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c-server-2"}, &corev1.Pod{})
	assert.True(t, apierrors.IsNotFound(err), "server-2 is deleted")
	require.NoError(t, fc.Get(ctx, client.ObjectKey{Namespace: "default", Name: "c-server-1"}, &corev1.Pod{}))
	var replaced int
	for _, e := range drainEvents(rec) {
		if strings.HasPrefix(e, "Normal "+EventReasonServerPodReplaced+" ") {
			replaced++
		}
	}
	assert.Equal(t, 1, replaced)
}

// maxUnavailable: 1 is said by the StatefulSet the operator creates. One an
// earlier operator created without it is left as it is: the StatefulSet
// controller reads it unset as 1, and with MaxUnavailableStatefulSet off the
// API server drops it on every write, so writing it again on every pass was
// an Update each time (operations.md id 235). A write the strategy needs for
// another reason (a partition left over) carries it, and the pod template,
// and so its revision, stay as they were: no server restarts.
func TestServerStatefulSet_MaxUnavailableRestartsNothing(t *testing.T) {
	ctx := context.Background()
	cluster := expansionCluster("1Gi")
	cluster.Status.UpgradeStatus = nil
	created := resources.BuildServerStatefulSetForEnterprise(cluster)
	require.Equal(t, intstr.FromInt32(1), *created.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable, "said on create")

	earlier := resources.BuildServerStatefulSetForEnterprise(cluster)
	earlier.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable = nil
	c := recreateClient(t)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme()}
	require.NoError(t, c.Create(ctx, earlier))
	before := storedServerSTS(t, c)
	require.Nil(t, before.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable)

	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(cluster), cluster))
	after := storedServerSTS(t, c)
	assert.Equal(t, before.ResourceVersion, after.ResourceVersion, "nothing to write: unset is 1")

	// A partition left over is a change the strategy needs: written with
	// maxUnavailable 1, the template untouched.
	leftover := after.DeepCopy()
	leftover.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To[int32](2)
	require.NoError(t, c.Update(ctx, leftover))
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(cluster), cluster))
	after = storedServerSTS(t, c)
	require.NotNil(t, after.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable)
	assert.Equal(t, intstr.FromInt32(1), *after.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable)
	assert.Nil(t, after.Spec.UpdateStrategy.RollingUpdate.Partition, "the partition the rendering leaves unset")
	assert.True(t, equality.Semantic.DeepEqual(before.Spec.Template, after.Spec.Template), "the pod template is unchanged")
	assert.Equal(t, revisionOf(before.Spec.Template), revisionOf(after.Spec.Template), "the same revision: no server restarts")
}

// The repair of an upgrade held on a tag that was never published (id 162), on
// Kubernetes 1.35.0–1.35.3: the StatefulSet controller does not replace
// server-2, which cannot pull the image, because it is unavailable; the walk
// does, and goes on through its gates.
func TestRepair_OnKubernetes135_TheWalkReplacesTheServerThatCannotStart(t *testing.T) {
	ctx := context.Background()
	r, fc, emu, c := heldOnNeverPublished(t)
	emu.maxUnavailable135 = true
	rec := record.NewFakeRecorder(100)
	r.Recorder = rec

	fixed := latestCluster(t, fc, c)
	require.NoError(t, patchObject(ctx, fc, fixed, func() { fixed.Spec.Image.Tag = holdTarget }))
	require.True(t, r.isUpgradeRequired(ctx, latestCluster(t, fc, c)), "the repair takes it up")
	for step := 0; step < 16; step++ {
		got := latestCluster(t, fc, c)
		if got.Status.UpgradeStatus.Phase == upgradePhaseStabilizing {
			break
		}
		_, err := r.handleRollingUpgrade(ctx, got)
		require.NoError(t, err)
		emu.observe()
		emu.deleteForUpdate()
		emu.recreateMissing()
		emu.observe()
	}
	got := latestCluster(t, fc, c)
	require.Equal(t, upgradePhaseStabilizing, got.Status.UpgradeStatus.Phase,
		"server-2 replaced and the walk at its end: %s", got.Status.UpgradeStatus.Message)
	for ordinal := 0; ordinal < 3; ordinal++ {
		assert.Equal(t, []string{"neo4j:" + holdTarget}, emu.created[ordinal], "server-%d restarts once, onto the fixed tag", ordinal)
	}
	var replaced []string
	for _, e := range drainEvents(rec) {
		if strings.HasPrefix(e, "Normal "+EventReasonServerPodReplaced+" ") {
			replaced = append(replaced, e)
		}
	}
	require.Len(t, replaced, 1)
	assert.Contains(t, replaced[0], "Deleted pod c-server-2, which cannot start")
	assert.Contains(t, replaced[0], "ImagePullBackOff")
}
