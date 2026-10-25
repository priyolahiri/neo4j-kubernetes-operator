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

// The order gap of operations.md id 187, end to end against a fake API server
// with the StatefulSet controller's revision rules played by
// statefulSetEmulator. A 3-server cluster on 5.26.21 runs a Managed n10s; the
// tag is set to 2026.08.1 and refused; the plugin is then changed — to
// VerifiedDownload, or disabled — with the new tag still set. The refusal
// lifts at once, while the plugin's own write waits for a Ready target, so the
// upgrade starts first: server-2 restarts on 2026.08.1 with n10s still in
// NEO4J_PLUGINS and does not start. What follows needs no one: the plugin
// writes its change although the cluster is not Ready (every server down
// fails on what the write changes; ids 152, 155), the StatefulSet replaces
// server-2 from that template, and the upgrade walks on.

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

const (
	orderRunning = "5.26.21-enterprise"
	orderTarget  = "2026.08.1-enterprise"
)

// orderPod is server pod ordinal created from revision: it starts unless its
// NEO4J_PLUGINS lists n10s on the CalVer image (the entrypoint exits on a
// name the image does not list).
func orderPod(t *testing.T, e *statefulSetEmulator, ordinal int, revision string) {
	t.Helper()
	template := e.revisions[revision]
	neo4j := template.Spec.Containers[0]
	var names []string
	if v, ok := findEnv(&neo4j, "NEO4J_PLUGINS"); ok {
		_ = json.Unmarshal([]byte(v), &names)
	}
	starts := neo4j.Image != "neo4j:"+orderTarget || !slices.Contains(names, "n10s")
	pod := heldServerPod(ordinal, neo4j.Image, starts)
	pod.Labels["app.kubernetes.io/name"], pod.Labels["app.kubernetes.io/instance"] = "neo4j", "c"
	pod.Labels[appsv1.ControllerRevisionHashLabelKey] = revision
	pod.Spec.Containers[0].Env = neo4j.Env
	if !starts {
		crashLooping(pod, 1, neo4j.Env...)
		pod.Spec.Containers[0].Image = neo4j.Image
	}
	require.NoError(t, e.c.Create(context.Background(), pod))
	e.created[ordinal] = append(e.created[ordinal], fmt.Sprintf("%s %v", neo4j.Image, names))
}

func orderRecreate(t *testing.T, e *statefulSetEmulator) {
	t.Helper()
	pods := e.pods()
	for ordinal := 0; ordinal < int(*e.sts().Spec.Replicas); ordinal++ {
		if pods[ordinal] == nil {
			orderPod(t, e, ordinal, e.revisionFor(ordinal))
		}
	}
}

// upgradeWithN10s is what the refusal's lifting leaves: the upgrade to
// orderTarget staged (partition 2), server-2 restarted on it and crash-looping
// on n10s, servers 0 and 1 on orderRunning; the plugin changed by edit.
func upgradeWithN10s(t *testing.T, edit func(*neo4jv1beta1.Neo4jPlugin)) (*Neo4jEnterpriseClusterReconciler, *Neo4jPluginReconciler, *record.FakeRecorder, client.Client, *statefulSetEmulator, *neo4jv1beta1.Neo4jEnterpriseCluster) {
	t.Helper()
	c := clusterForUpgrade("c", "default", orderRunning, orderTarget, 3)
	c.UID = "c-uid"
	c.Finalizers = []string{ClusterFinalizer}
	c.Spec.UpgradeStrategy = &neo4jv1beta1.UpgradeStrategySpec{AutoPauseOnFailure: true}
	c.Status.Phase = neo4jv1beta1.PhaseUpgrading
	c.Status.Conditions = []metav1.Condition{clusterFormedCondition()}
	now := metav1.Now()
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	c.Status.UpgradeStatus = &neo4jv1beta1.UpgradeStatus{
		Phase: upgradePhaseRolling, PreviousVersion: orderRunning, TargetVersion: orderTarget,
		StartedTargetVersion: orderTarget, StartTime: &start, StepStartTime: &now, CurrentPartition: ptr.To(int32(2)),
		Progress: &neo4jv1beta1.UpgradeProgress{Total: 3, InProgress: 1, Pending: 2},
	}

	plugin := &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{Name: "semantics", Namespace: "default", Generation: 2, Finalizers: []string{PluginFinalizer}},
		Spec:       neo4jv1beta1.Neo4jPluginSpec{ClusterRef: "c", Name: "n10s", Version: "5.26.0", Enabled: true},
		Status: neo4jv1beta1.Neo4jPluginStatus{Phase: neo4jv1beta1.PhaseReady, ObservedGeneration: 1,
			Message: "Plugin installed and configured successfully"},
	}
	edit(plugin)

	scheme := makeUpgradeScheme()
	fc := fake.NewClientBuilder().WithScheme(scheme).WithObjects(c, plugin).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}, &neo4jv1beta1.Neo4jPlugin{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				obj.SetUID(types.UID(obj.GetName() + "-uid"))
				return cl.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); ok {
					obj.SetGeneration(obj.GetGeneration() + 1)
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).Build()
	emu := newStatefulSetEmulator(t, fc)

	withN10s := func(sts *appsv1.StatefulSet) *appsv1.StatefulSet {
		sts.Spec.Template.Spec.Containers[0].Env = append(sts.Spec.Template.Spec.Containers[0].Env,
			corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["n10s"]`})
		return sts
	}
	running := withN10s(resources.BuildServerStatefulSetForEnterprise(clusterForUpgrade("c", "default", orderRunning, orderRunning, 3)))
	before := emu.register(running.Spec.Template)
	sts := withN10s(resources.BuildServerStatefulSetForEnterprise(c))
	sts.OwnerReferences = []metav1.OwnerReference{{APIVersion: "neo4j.neo4j.com/v1beta1", Kind: "Neo4jEnterpriseCluster",
		Name: "c", UID: c.UID, Controller: ptr.To(true)}}
	sts.Annotations = map[string]string{pluginEntriesAnnotation("semantics"): "n10s"}
	sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(int32(2))}
	require.NoError(t, fc.Create(context.Background(), sts))
	staged := emu.register(sts.Spec.Template)
	stored := emu.sts()
	stored.Status.CurrentRevision = before
	require.NoError(t, fc.Status().Update(context.Background(), stored))
	emu.observe()
	orderPod(t, emu, 0, before)
	orderPod(t, emu, 1, before)
	orderPod(t, emu, 2, staged)
	emu.observe()
	require.False(t, isPodReady(emu.pods()[2]), "server-2 does not start on %s with n10s", orderTarget)
	emu.created = map[int][]string{}

	cr := &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: scheme, Recorder: record.NewFakeRecorder(100),
		ConfigMapManager: NewConfigMapManager(fc), UpgradeGates: &upgradeGates{
			serverAvailable: func(_ context.Context, _ *neo4jv1beta1.Neo4jEnterpriseCluster, pod string) (bool, string, error) {
				p := &corev1.Pod{}
				err := fc.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: pod}, p)
				return err == nil && isPodReady(p), "", client.IgnoreNotFound(err)
			},
			databasesCaughtUp: func(context.Context, *neo4jv1beta1.Neo4jEnterpriseCluster, string) (bool, []string, error) {
				return true, nil, nil
			},
		}}
	rec := record.NewFakeRecorder(64)
	pr := &Neo4jPluginReconciler{Client: fc, Scheme: scheme, Recorder: rec, RequeueAfter: 30 * time.Second,
		bolt: &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}}
	return cr, pr, rec, fc, emu, c
}

func TestImageChange_APluginChangedAfterTheRefusalLiftsRecoversByItself(t *testing.T) {
	for name, edit := range map[string]func(*neo4jv1beta1.Neo4jPlugin){
		"switched to VerifiedDownload": func(p *neo4jv1beta1.Neo4jPlugin) {
			p.Spec.InstallMode = PluginInstallModeVerifiedDownload
			p.Spec.Version = "2025.06.1"
			p.Spec.Source = &neo4jv1beta1.PluginSource{Type: "url",
				URL:      "https://github.com/neo4j-labs/neosemantics/releases/download/2025.06.1/neosemantics-2025.06.1.jar",
				Checksum: "sha256:0000000000000000000000000000000000000000000000000000000000000000"}
		},
		"disabled": func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Enabled = false },
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cr, pr, rec, fc, emu, c := upgradeWithN10s(t, edit)

			// The plugin's pass: the cluster is upgrading, but every server down
			// keeps failing on NEO4J_PLUGINS, which its write changes.
			_, err := pr.Reconcile(ctx, reconcileRequest("semantics"))
			require.NoError(t, err)
			sts := emu.sts()
			names := neo4jPluginsList(neo4jContainerEnv(sts.Spec.Template.Spec))
			assert.NotContains(t, names, "n10s", "the plugin's change is written although the cluster is not Ready")
			assert.Equal(t, "neo4j:"+orderTarget, serverImage(sts), "on the upgrade's template")
			assert.Equal(t, int32(2), *statefulSetPartition(sts), "and its partition")
			assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginSettingsFailing), 1)

			// The StatefulSet controller replaces server-2 from that template;
			// it starts. The upgrade walks on through its gates.
			emu.observe()
			emu.deleteForUpdate()
			orderRecreate(t, emu)
			emu.observe()
			require.True(t, isPodReady(emu.pods()[2]), "server-2 starts on %s without n10s", orderTarget)
			for step := 0; step < 12; step++ {
				got := latestCluster(t, fc, c)
				if got.Status.UpgradeStatus.Phase != upgradePhaseRolling {
					break
				}
				_, err := cr.handleRollingUpgrade(ctx, got)
				require.NoError(t, err)
				emu.observe()
				emu.deleteForUpdate()
				orderRecreate(t, emu)
				emu.observe()
			}
			got := latestCluster(t, fc, c)
			require.Equal(t, upgradePhaseStabilizing, got.Status.UpgradeStatus.Phase, got.Status.UpgradeStatus.LastError)
			for ordinal := 0; ordinal < 3; ordinal++ {
				require.Len(t, emu.created[ordinal], 1, "server-%d restarts once", ordinal)
				assert.Equal(t, "neo4j:"+orderTarget+" []", emu.created[ordinal][0], "server-%d, on the target without n10s", ordinal)
				assert.True(t, isPodReady(emu.pods()[ordinal]))
			}
		})
	}
}

// reconcileRequest is the request for the Neo4jPlugin name in "default".
func reconcileRequest(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: name}}
}
