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

// A Neo4jPlugin's write waits for a target that has acted on its latest spec;
// the plugin is Ready once the roll its write caused is done; and the write
// that repairs the plugin's own failing init container does not wait
// (operations.md id 152).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// gateReconciler is a plugin reconciler on a fake API server that defaults
// StatefulSets and moves their generation on every write, as the real one
// does; writes counts the StatefulSet updates and messages records every
// status message written.
func gateReconciler(t *testing.T, bolt *pluginBolt, objs ...client.Object) (*Neo4jPluginReconciler, *record.FakeRecorder, *int, *[]string) {
	t.Helper()
	writes := 0
	var messages []string
	c := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jPlugin{}, &appsv1.StatefulSet{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); ok {
					writes++
					apiServerDefaults(obj)
					obj.SetGeneration(obj.GetGeneration() + 1)
				}
				return cl.Update(ctx, obj, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if p, ok := obj.(*neo4jv1beta1.Neo4jPlugin); ok {
					messages = append(messages, p.Status.Message)
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).
		Build()
	rec := record.NewFakeRecorder(64)
	if bolt == nil {
		bolt = &pluginBolt{
			functional: func(context.Context, *DeploymentInfo) bool { return true },
		}
	}
	return &Neo4jPluginReconciler{
		Client: c, Scheme: newTestScheme(), Recorder: rec,
		RequeueAfter: 30 * time.Second, bolt: bolt,
	}, rec, &writes, &messages
}

// liveSTS reads the StatefulSet name.
func liveSTS(t *testing.T, c client.Client, name string) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: name}, sts))
	return sts
}

// rollOut reports the StatefulSet name rolled out at its generation, as its
// controller does once every pod runs the template and is ready.
func rollOut(t *testing.T, c client.Client, name string) {
	t.Helper()
	sts := liveSTS(t, c, name)
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	sts.Status = rolledOutStatus(replicas, sts.Generation)
	require.NoError(t, c.Status().Update(context.Background(), sts))
}

// gatePod is pod {sts}-{ordinal} of cluster "c" (or, with labels, of a
// standalone), Running and Ready.
func gatePod(name string, labels map[string]string) *corev1.Pod {
	if labels == nil {
		labels = map[string]string{"app.kubernetes.io/name": "neo4j", "app.kubernetes.io/instance": "c"}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: selfHealNS, Labels: labels},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

// failingIn makes pod fail in its init container name: it exited with code,
// and waits in CrashLoopBackOff to run again.
func failingIn(pod *corev1.Pod, name string, code int32) *corev1.Pod {
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodPending,
		InitContainerStatuses: []corev1.ContainerStatus{{
			Name:                 name,
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: "Error"}},
			RestartCount:         6,
		}},
	}
	return pod
}

// The defect (v1.22.0 journey, 2026-10-17 09:33 CEST): the trusted CAs changed,
// the cluster wrote its truststore to the StatefulSet, and the plugin, woken by
// that write, read the cluster's Ready from the pass before — the cluster had
// not acted on its new generation yet — and wrote its CA update in the same
// second: the servers rolled twice at once, and the plugin never waited. A
// Ready the target has not acted on is not Ready: its controller has to have
// recorded its latest spec, and its StatefulSet has to have rolled out.
func TestReconcile_AReadyTheTargetHasNotActedOnIsNotReady(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stale func(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet)
	}{
		{"the cluster has not recorded its latest spec", func(c *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet) {
			c.Generation, c.Status.ObservedGeneration = 2, 1
		}},
		{"the StatefulSet controller has not seen the new template", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet) {
			sts.Generation = 3
		}},
		{"the servers are rolling", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet) {
			sts.Status.UpdateRevision, sts.Status.UpdatedReplicas = "rev-next", 0
		}},
		{"a server is not ready", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet) {
			sts.Status.ReadyReplicas = 0
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
			_, before := caTarget(neo4jv1beta1.PhaseReady, "corp-ca")
			cluster, _ := caTarget(neo4jv1beta1.PhaseReady, "corp-ca", "mirror-ca")
			sts := installedWith(t, selfHealSTS(), before, plugin)
			tc.stale(cluster, sts)
			r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), plugin, cluster, sts)

			res := reconcilePlugin(t, r, "custom")

			assert.Positive(t, res.RequeueAfter)
			assert.Zero(t, *writes, "the plugin waits for the target to act on its spec")
			got := getPlugin(t, r, "custom")
			assert.Equal(t, neo4jv1beta1.PhaseWaiting, got.Status.Phase)
			assert.Equal(t, "Waiting until Neo4jEnterpriseCluster c is Ready with every server available: "+
				"updating the plugin's trusted CAs restarts every server", got.Status.Message)
			events := drainEvents(rec)
			require.Len(t, eventsWithReason(events, EventReasonPluginInstallDeferred), 1)
			assert.Len(t, events, 1)

			// The passes that go on waiting say nothing more.
			rv := got.ResourceVersion
			reconcilePlugin(t, r, "custom")
			assert.Equal(t, rv, getPlugin(t, r, "custom").ResourceVersion)
			assert.Empty(t, drainEvents(rec))
		})
	}
}

// Once the target has acted on its spec, the CA update is written, and says
// so: a CA update that waited is still one.
func TestReconcile_ACAUpdateThatWaitedIsWrittenAsOne(t *testing.T) {
	plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	_, before := caTarget(neo4jv1beta1.PhaseReady, "corp-ca")
	cluster, _ := caTarget(neo4jv1beta1.PhaseReady, "corp-ca", "mirror-ca")
	cluster.Generation, cluster.Status.ObservedGeneration = 2, 1
	r, rec, writes, messages := gateReconciler(t, nil, plugin, cluster, installedWith(t, selfHealSTS(), before, plugin))

	reconcilePlugin(t, r, "custom")
	require.Zero(t, *writes)
	require.Equal(t, neo4jv1beta1.PhaseWaiting, getPlugin(t, r, "custom").Status.Phase)
	drainEvents(rec)

	latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: "c"}, latest))
	require.NoError(t, patchObject(context.Background(), r.Client, latest, func() { latest.Status.ObservedGeneration = latest.Generation }))
	reconcilePlugin(t, r, "custom")

	assert.Equal(t, 1, *writes)
	assert.Equal(t, []string{"corp-ca (optional)", "mirror-ca (optional)"}, projectedCAs(liveTemplate(t, r.Client, "c-server")))
	assert.Contains(t, *messages, "Updating the plugin's trusted CAs: spec.trustedCASecrets of Neo4jEnterpriseCluster c changed "+
		"(volume plugin-ca of StatefulSet c-server)")
	assert.NotContains(t, *messages, "Installing plugin")
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallDeferred), "the wait was announced once")
}

// The defect (v1.22.0 journey): a cluster VerifiedDownload install read Ready
// at 09:04:09 CEST, and the roll it caused finished at 09:07:43; a standalone
// PluginMissing repair read Ready mid-restart. The write is what restarts the
// servers, and the old pods report Ready until they stop. The plugin is Ready
// only once its target's StatefulSet has rolled the new template out.
func TestReconcile_ReadyOnlyOnceItsOwnRollIsDone(t *testing.T) {
	standaloneSTS := pluginTestSTS("sa", selfHealNS)
	standaloneSTS.Name = "sa"
	for _, tc := range []struct {
		name   string
		plugin *neo4jv1beta1.Neo4jPlugin
		objs   []client.Object
		sts    string
	}{
		{"a cluster's VerifiedDownload install", verifiedDownloadSelfHeal("", ""),
			append([]client.Object{readyCluster(), selfHealSTS()}, readyClusterPods()...), "c-server"},
		{"a standalone's PluginMissing repair",
			selfHealPlugin("apoc", "sa", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully"),
			[]client.Object{
				&neo4jv1beta1.Neo4jEnterpriseStandalone{
					ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: selfHealNS},
					Status:     neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: neo4jv1beta1.PhaseReady},
				},
				standaloneSTS, gatePod("sa-0", map[string]string{"app": "sa"}),
			}, "sa"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{tc.plugin}, tc.objs...)...)

			reconcilePlugin(t, r, tc.plugin.Name)
			require.Equal(t, 1, *writes, "the write")

			// Every pod still reports Ready: they have not stopped yet.
			for i := 0; i < 2; i++ {
				res := reconcilePlugin(t, r, tc.plugin.Name)
				assert.Positive(t, res.RequeueAfter)
				got := getPlugin(t, r, tc.plugin.Name)
				assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase, "not Ready while the servers roll")
				assert.Equal(t, "Waiting for pods to be ready after plugin installation", got.Status.Message)
			}
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstalled))

			rollOut(t, r.Client, tc.sts)
			reconcilePlugin(t, r, tc.plugin.Name)
			assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, tc.plugin.Name).Status.Phase)
			assert.Equal(t, 1, *writes)
			assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstalled), 1)
		})
	}
}

// The defect (found walking this fix live on Kind, 2026-10-17 13:46 CEST): a
// plugin edit (a wrong checksum) was written, and the same pass read the
// StatefulSet back from the informer cache, which had not seen the write yet
// and still read fully rolled out; the plugin went Ready in the second it
// wrote and stayed Ready while the server its write broke crash-looped. The
// pass that writes waits for the generation it wrote, whatever the cache says.
func TestReconcile_ThePassThatWritesWaitsForTheGenerationItWrote(t *testing.T) {
	plugin := verifiedDownloadSelfHeal("", "")
	stale := map[string]*appsv1.StatefulSet{}
	c := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(append([]client.Object{plugin, readyCluster(), selfHealSTS()}, readyClusterPods()...)...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jPlugin{}, &appsv1.StatefulSet{}).
		WithInterceptorFuncs(interceptor.Funcs{
			// A cache that has not seen the write: reads of a StatefulSet
			// return it as it was before.
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if s, ok := obj.(*appsv1.StatefulSet); ok && stale[key.Name] != nil {
					stale[key.Name].DeepCopyInto(s)
					return nil
				}
				return cl.Get(ctx, key, obj, opts...)
			},
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if s, ok := obj.(*appsv1.StatefulSet); ok {
					before := &appsv1.StatefulSet{}
					if err := cl.Get(ctx, client.ObjectKeyFromObject(s), before); err != nil {
						return err
					}
					stale[s.Name] = before
					s.SetGeneration(s.GetGeneration() + 1)
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).
		Build()
	rec := record.NewFakeRecorder(16)
	r := &Neo4jPluginReconciler{Client: c, Scheme: newTestScheme(), Recorder: rec, RequeueAfter: 30 * time.Second,
		bolt: &pluginBolt{
			functional: func(context.Context, *DeploymentInfo) bool { return true },
		}}

	res := reconcilePlugin(t, r, "custom")

	require.NotNil(t, stale["c-server"], "the pass wrote the StatefulSet")
	assert.Positive(t, res.RequeueAfter)
	got := getPlugin(t, r, "custom")
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase, "not Ready in the second it wrote")
	assert.Equal(t, "Waiting for pods to be ready after plugin installation", got.Status.Message)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstalled))
}

// deadlockFixture is the journey's deadlock: cluster "c" with three servers is
// Forming while its server StatefulSet's roll is stuck, because server 2 fails
// in the VerifiedDownload plugin's init container (curl exit 60), which still
// reads the CA list liveCAs; the cluster now trusts clusterCAs.
func deadlockFixture(t *testing.T, plugin *neo4jv1beta1.Neo4jPlugin, liveCAs, clusterCAs []string) (*neo4jv1beta1.Neo4jEnterpriseCluster, *appsv1.StatefulSet, []*corev1.Pod) {
	t.Helper()
	_, before := caTarget(neo4jv1beta1.PhaseReady, liveCAs...)
	installed := plugin.DeepCopy()
	installed.Spec.Source.Checksum = verifiedDownloadPlugin().Spec.Source.Checksum
	installed.Generation = 1
	sts := installedWith(t, selfHealSTS(), before, installed)
	sts.Spec.Replicas = ptr.To[int32](3)
	sts.Status = rolledOutStatus(3, 0)
	sts.Status.UpdateRevision, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = "rev-next", 1, 2
	cluster, _ := caTarget(neo4jv1beta1.PhaseForming, clusterCAs...)
	cluster.Spec.Topology.Servers = 3
	pods := []*corev1.Pod{
		gatePod("c-server-0", nil), gatePod("c-server-1", nil),
		failingIn(gatePod("c-server-2", nil), "plugin-init-custom", 60),
	}
	return cluster, sts, pods
}

func objects(plugin *neo4jv1beta1.Neo4jPlugin, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet, pods []*corev1.Pod) []client.Object {
	objs := []client.Object{plugin, cluster, sts}
	for _, p := range pods {
		objs = append(objs, p)
	}
	return objs
}

// boltDown is a target whose Bolt answers nothing; a pass that writes a repair
// must not need it.
func boltDown() *pluginBolt {
	return &pluginBolt{
		functional: func(context.Context, *DeploymentInfo) bool { return false },
	}
}

// The defect (v1.22.0 journey, 09:53 CEST): removing the trusted CAs DEADLOCKED.
// The plugin waited for its target to be Ready with every server available,
// and the target could not be: server-2 failed in the plugin's own init
// container, whose CA volume still named the removed CA, and only the plugin's
// write would change that. The same holds for an install whose checksum the
// user fixes while the server it broke crash-loops. A write that repairs the
// plugin's own init container, failing on every server that is down, goes
// ahead, and says so once.
func TestReconcile_ARepairOfItsOwnInitContainerDoesNotWait(t *testing.T) {
	for _, tc := range []struct {
		name               string
		phase, message     string
		edit               func(p *neo4jv1beta1.Neo4jPlugin)
		liveCAs, targetCAs []string
		check              func(t *testing.T, ic corev1.Container)
	}{
		{"trusted CAs removed from a Ready plugin's target", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully",
			nil, []string{"corp-ca"}, nil, func(t *testing.T, ic corev1.Container) {
				assert.False(t, containerMountsVolume(ic, "plugin-ca"), "the CA is gone from the download")
			}},
		{"a checksum fixed during the install that broke the server", neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation",
			func(p *neo4jv1beta1.Neo4jPlugin) {
				p.Generation = 2
				p.Spec.Source.Checksum = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
			}, []string{"corp-ca"}, []string{"corp-ca"}, func(t *testing.T, ic corev1.Container) {
				v, _ := findEnv(&ic, "PLUGIN_CHECKSUM")
				assert.Equal(t, "sha256:1111111111111111111111111111111111111111111111111111111111111111", v)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := verifiedDownloadSelfHeal(tc.phase, tc.message)
			if tc.edit != nil {
				tc.edit(plugin)
			}
			cluster, sts, pods := deadlockFixture(t, plugin, tc.liveCAs, tc.targetCAs)
			r, rec, writes, _ := gateReconciler(t, boltDown(), objects(plugin, cluster, sts, pods)...)

			reconcilePlugin(t, r, "custom")

			require.Equal(t, 1, *writes, "the repair is written although the cluster is Forming")
			tc.check(t, initContainer(t, liveTemplate(t, r.Client, "c-server"), "plugin-init-custom"))
			got := getPlugin(t, r, "custom")
			assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase)
			events := drainEvents(rec)
			assert.Empty(t, eventsWithReason(events, EventReasonPluginInstallDeferred))
			repairs := eventsWithReason(events, EventReasonPluginInitContainerFailing)
			require.Len(t, repairs, 1)
			assert.Contains(t, repairs[0], "Warning")
			assert.Contains(t, repairs[0], "c-server-2 (init container plugin-init-custom exited with code 60)")
			assert.Contains(t, repairs[0], "Neo4jEnterpriseCluster c")

			// Written once: the next passes wait for the roll and say nothing more.
			reconcilePlugin(t, r, "custom")
			reconcilePlugin(t, r, "custom")
			assert.Equal(t, 1, *writes)
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInitContainerFailing))
		})
	}
}

// The exception is narrow: it never rolls a cluster short of a server for any
// other reason. Each case below is the deadlock fixture with one thing
// changed, and waits; the fixture as it is goes ahead (above).
func TestReconcile_ARepairWaitsWhenSomethingElseIsDown(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod
	}{
		{"another server is down for another reason", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			pods[0].Status.Conditions[0].Status = corev1.ConditionFalse
			return pods
		}},
		{"the failing server is below one that is up", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			// The roll would replace the server that is up first.
			return []*corev1.Pod{pods[0], failingIn(gatePod("c-server-1", nil), "plugin-init-custom", 60), gatePod("c-server-2", nil)}
		}},
		{"a server pod is missing", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			return pods[1:]
		}},
		{"a server pod is being deleted", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			now := metav1.Now()
			pods[1].DeletionTimestamp, pods[1].Finalizers = &now, []string{"test/hold"}
			return pods
		}},
		{"the server fails in another init container", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			pods[2] = failingIn(gatePod("c-server-2", nil), "truststore-init", 1)
			return pods
		}},
		{"the init container is still starting", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			pods[2].Status.InitContainerStatuses[0].LastTerminationState = corev1.ContainerState{}
			return pods
		}},
		{"Neo4j reports another server unavailable", func(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			cluster.Status.Diagnostics = &neo4jv1beta1.ClusterDiagnosticsStatus{Servers: []neo4jv1beta1.ServerDiagnosticInfo{
				{Name: "s0", Address: "c-server-0.c-headless.default.svc.cluster.local:7687", State: "Enabled", Health: "Unavailable"},
				{Name: "s2", Address: "c-server-2.c-headless.default.svc.cluster.local:7687", State: "Enabled", Health: "Unavailable"},
			}}
			return pods
		}},
		{"Neo4j reports a server unavailable at no known address", func(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			cluster.Status.Diagnostics = &neo4jv1beta1.ClusterDiagnosticsStatus{Servers: []neo4jv1beta1.ServerDiagnosticInfo{
				{Name: "s2", State: "Enabled", Health: "Unavailable"},
			}}
			return pods
		}},
		{"the failing server is below the partition", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			sts.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To[int32](3)
			return pods
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
			cluster, sts, pods := deadlockFixture(t, plugin, []string{"corp-ca"}, nil)
			pods = tc.change(cluster, sts, pods)
			r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), objects(plugin, cluster, sts, pods)...)

			reconcilePlugin(t, r, "custom")

			assert.Zero(t, *writes, "not written")
			assert.Equal(t, neo4jv1beta1.PhaseWaiting, getPlugin(t, r, "custom").Status.Phase)
			events := drainEvents(rec)
			assert.Len(t, eventsWithReason(events, EventReasonPluginInstallDeferred), 1)
			assert.Empty(t, eventsWithReason(events, EventReasonPluginInitContainerFailing))
		})
	}

	// A write that leaves the failing container as it is repairs nothing.
	t.Run("the write does not change the init container", func(t *testing.T) {
		plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
		cluster, sts, pods := deadlockFixture(t, plugin, []string{"corp-ca"}, []string{"corp-ca"})
		plugin.Generation = 2
		plugin.Spec.Config = map[string]string{"custom.setting": "off"}
		r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), objects(plugin, cluster, sts, pods)...)

		reconcilePlugin(t, r, "custom")

		assert.Zero(t, *writes)
		assert.Equal(t, neo4jv1beta1.PhaseWaiting, getPlugin(t, r, "custom").Status.Phase)
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInitContainerFailing))
	})
}

// While the roll its write caused is stuck on a pod its init container keeps
// from starting, the plugin says which, and why — it used to say Ready.
func TestReconcile_TheWaitNamesThePodsItsInitContainerKeepsFromStarting(t *testing.T) {
	plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation")
	cluster, sts, pods := deadlockFixture(t, plugin, []string{"corp-ca"}, []string{"corp-ca"})
	r, rec, writes, _ := gateReconciler(t, nil, objects(plugin, cluster, sts, pods)...)

	reconcilePlugin(t, r, "custom")

	assert.Zero(t, *writes, "nothing to repair: the plugin is as it was written")
	got := getPlugin(t, r, "custom")
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase)
	assert.Equal(t, "Waiting for pods to be ready after plugin installation; "+
		"cannot start: c-server-2 (init container plugin-init-custom exited with code 60)", got.Status.Message)
	assert.Empty(t, drainEvents(rec))
}

// A standalone's StatefulSet uses OrderedReady pod management, which does not
// replace a pod that is not Ready when its template changes (checked on Kind
// v1.34: the pod stayed on the broken revision). Once the repair is written and
// the StatefulSet has seen it, the plugin deletes the stuck pod, so it comes
// back on the repaired template.
func TestReconcile_AStandalonesStuckPodIsReplacedAfterTheRepair(t *testing.T) {
	plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation")
	plugin.Spec.ClusterRef = "sa"
	plugin.Generation = 2
	plugin.Spec.Source.Checksum = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: selfHealNS},
		Status:     neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: neo4jv1beta1.PhasePending},
	}
	installed := plugin.DeepCopy()
	installed.Spec.Source.Checksum = verifiedDownloadPlugin().Spec.Source.Checksum
	sts := pluginTestSTS("sa", selfHealNS)
	sts.Name = "sa"
	sts = installedWith(t, sts, &DeploymentInfo{Type: "standalone", Name: "sa", Namespace: selfHealNS, Object: standalone}, installed)
	sts.Status.ReadyReplicas = 0
	pod := failingIn(gatePod("sa-0", map[string]string{"app": "sa"}), "plugin-init-custom", 1)
	pod.Labels[appsv1.ControllerRevisionHashLabelKey] = sts.Status.UpdateRevision
	r, rec, writes, _ := gateReconciler(t, boltDown(), plugin, standalone, sts, pod)

	reconcilePlugin(t, r, "custom")
	require.Equal(t, 1, *writes, "the repair is written to the standalone that is down")
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInitContainerFailing), 1)

	// Not before the StatefulSet has seen the new template: the pod would come
	// back on the broken one.
	reconcilePlugin(t, r, "custom")
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}), "the pod is kept")

	live := liveSTS(t, r.Client, "sa")
	live.Status.ObservedGeneration = live.Generation
	live.Status.UpdateRevision = fmt.Sprintf("rev-%d", live.Generation)
	require.NoError(t, r.Status().Update(context.Background(), live))
	reconcilePlugin(t, r, "custom")

	err := r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
	assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "the stuck pod is deleted: %v", err)
	replaced := eventsWithReason(drainEvents(rec), EventReasonPluginInitContainerFailing)
	require.Len(t, replaced, 1)
	assert.Contains(t, replaced[0], "Deleted pod sa-0")
}

// replaceStuckPods deletes only a standalone's pod on an older revision, stuck
// in this plugin's init container, once its StatefulSet has seen its template.
func TestReplaceStuckPods(t *testing.T) {
	for _, tc := range []struct {
		name    string
		target  string
		change  func(sts *appsv1.StatefulSet, pod *corev1.Pod)
		deleted bool
	}{
		{"a standalone's pod on an older revision", "sa", nil, true},
		{"the pod is on the latest revision", "sa", func(sts *appsv1.StatefulSet, pod *corev1.Pod) {
			pod.Labels[appsv1.ControllerRevisionHashLabelKey] = sts.Status.UpdateRevision
		}, false},
		{"the StatefulSet has not seen its template", "sa", func(sts *appsv1.StatefulSet, _ *corev1.Pod) {
			sts.Generation++
		}, false},
		{"a Parallel StatefulSet replaces it itself", "sa", func(sts *appsv1.StatefulSet, _ *corev1.Pod) {
			sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		}, false},
		{"the pod fails in another init container", "sa", func(_ *appsv1.StatefulSet, pod *corev1.Pod) {
			pod.Status.InitContainerStatuses[0].Name = "truststore-init"
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation")
			plugin.Spec.ClusterRef = "sa"
			sts := pluginTestSTS("sa", selfHealNS)
			sts.Name = "sa"
			sts.Generation = 2
			sts.Status = rolledOutStatus(1, 2)
			sts.Status.CurrentRevision, sts.Status.ReadyReplicas = "rev-1", 0
			pod := failingIn(gatePod("sa-0", map[string]string{"app": "sa", appsv1.ControllerRevisionHashLabelKey: "rev-1"}), "plugin-init-custom", 60)
			if tc.change != nil {
				tc.change(sts, pod)
			}
			r, rec, _, _ := gateReconciler(t, nil, plugin, sts, pod)
			target := &DeploymentInfo{Type: "standalone", Name: "sa", Namespace: selfHealNS}

			r.replaceStuckPods(context.Background(), plugin, target)

			err := r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
			assert.Equal(t, tc.deleted, err != nil, "deleted: %v", err)
			assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInitContainerFailing), map[bool]int{true: 1, false: 0}[tc.deleted])
		})
	}

	t.Run("a cluster's pod is the StatefulSet's to replace", func(t *testing.T) {
		plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseInstalling, "")
		cluster, sts, pods := deadlockFixture(t, plugin, []string{"corp-ca"}, nil)
		sts.Spec.PodManagementPolicy = appsv1.OrderedReadyPodManagement
		r, _, _, _ := gateReconciler(t, nil, objects(plugin, cluster, sts, pods)...)
		r.replaceStuckPods(context.Background(), plugin, &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: cluster})
		assert.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(pods[2]), &corev1.Pod{}))
	})
}

func TestStatefulSetRolloutPending(t *testing.T) {
	rolled := func() *appsv1.StatefulSet {
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "c-server", Generation: 4},
			Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To[int32](3)},
			Status:     rolledOutStatus(3, 4),
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*appsv1.StatefulSet)
		want   string
	}{
		{"rolled out", nil, ""},
		{"a template its controller has not seen", func(s *appsv1.StatefulSet) { s.Generation = 5 },
			"StatefulSet c-server has not rolled out its latest pod template yet"},
		{"rolling", func(s *appsv1.StatefulSet) { s.Status.UpdateRevision, s.Status.UpdatedReplicas = "rev-5", 1 },
			"StatefulSet c-server is rolling out its pod template (1 of 3 pods updated)"},
		{"held below a partition", func(s *appsv1.StatefulSet) { s.Status.UpdateRevision, s.Status.UpdatedReplicas = "rev-5", 2 },
			"StatefulSet c-server is rolling out its pod template (2 of 3 pods updated)"},
		{"a pod not ready", func(s *appsv1.StatefulSet) { s.Status.ReadyReplicas = 2 },
			"StatefulSet c-server has 2 of 3 pods ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := rolled()
			if tc.change != nil {
				tc.change(sts)
			}
			assert.Equal(t, tc.want, statefulSetRolloutPending(sts))
		})
	}
}

func TestInitContainerFailure(t *testing.T) {
	status := func(s corev1.ContainerStatus) *corev1.Pod {
		s.Name = "plugin-init-custom"
		return &corev1.Pod{Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{s}}}
	}
	exited := func(code int32) *corev1.ContainerStateTerminated {
		return &corev1.ContainerStateTerminated{ExitCode: code}
	}
	for _, tc := range []struct {
		name    string
		pod     *corev1.Pod
		code    int32
		failing bool
	}{
		{"Init:Error", status(corev1.ContainerStatus{State: corev1.ContainerState{Terminated: exited(60)}}), 60, true},
		{"Init:CrashLoopBackOff", status(corev1.ContainerStatus{
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: exited(1)},
		}), 1, true},
		{"running again after a failure", status(corev1.ContainerStatus{
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			LastTerminationState: corev1.ContainerState{Terminated: exited(60)},
		}), 60, true},
		{"succeeded after a failure", status(corev1.ContainerStatus{
			State:                corev1.ContainerState{Terminated: exited(0)},
			LastTerminationState: corev1.ContainerState{Terminated: exited(60)},
		}), 0, false},
		{"not started yet", status(corev1.ContainerStatus{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
		}), 0, false},
		{"pulling its image", status(corev1.ContainerStatus{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
		}), 0, false},
		{"no such init container", &corev1.Pod{}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, failing := initContainerFailure(tc.pod, "plugin-init-custom")
			assert.Equal(t, tc.failing, failing)
			assert.Equal(t, tc.code, code)
		})
	}
}

func TestStatefulSetOrdinal(t *testing.T) {
	for pod, want := range map[string]int{"c-server-0": 0, "c-server-12": 12, "c-server-01": -1, "c-server-x": -1, "c-server": -1, "c-servers-1": -1, "other-0": -1} {
		got, ok := statefulSetOrdinal("c-server", pod)
		if want < 0 {
			assert.False(t, ok, pod)
			continue
		}
		assert.True(t, ok, pod)
		assert.Equal(t, want, got, pod)
	}
}

// simulateStatefulSetController stands in, on envtest, for the StatefulSet
// controller envtest does not run: it reports every StatefulSet rolled out at
// its generation as soon as it sees it.
func simulateStatefulSetController(t *testing.T, mgr ctrl.Manager) {
	t.Helper()
	c := mgr.GetClient()
	require.NoError(t, ctrl.NewControllerManagedBy(mgr).
		Named("statefulset-rollout-stand-in").
		For(&appsv1.StatefulSet{}).
		Complete(reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
			sts := &appsv1.StatefulSet{}
			if err := c.Get(ctx, req.NamespacedName, sts); err != nil {
				return reconcile.Result{}, client.IgnoreNotFound(err)
			}
			replicas := int32(1)
			if sts.Spec.Replicas != nil {
				replicas = *sts.Spec.Replicas
			}
			want := rolledOutStatus(replicas, sts.Generation)
			if sts.Status.ObservedGeneration == want.ObservedGeneration && sts.Status.UpdateRevision == want.UpdateRevision {
				return reconcile.Result{}, nil
			}
			sts.Status = want
			return reconcile.Result{}, c.Status().Update(ctx, sts)
		})))
}

// Against an API server, which moves a StatefulSet's and a cluster's
// generation on every spec write and keeps their status apart, the gate end to
// end: an install is Ready only once its roll is done; a trusted-CA change the
// cluster has not acted on yet waits; and the journey's deadlock — server-2
// failing in the plugin's init container while the cluster is Forming, the
// trusted CAs then removed — is repaired by the plugin's write, though not
// while another server is down.
func TestPluginWritesFollowTheTargetsRollout_APIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test-unit`")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	defer func() { _ = env.Stop() }()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := context.Background()
	const ns = "default"

	cluster := minimalCluster("c", ns)
	cluster.Spec.Topology.Servers = 3
	cluster.Spec.TrustedCASecrets = trustedCAs("corp-ca")
	require.NoError(t, c.Create(ctx, cluster))
	editCluster := func(edit func(*neo4jv1beta1.Neo4jEnterpriseCluster) error) {
		t.Helper()
		require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
				return err
			}
			return edit(latest)
		}))
	}
	// clusterIs records the cluster's phase as its controller does once it
	// has acted on its spec.
	clusterIs := func(phase string) {
		editCluster(func(l *neo4jv1beta1.Neo4jEnterpriseCluster) error {
			l.Status.Phase, l.Status.ObservedGeneration = phase, l.Generation
			return c.Status().Update(ctx, l)
		})
	}
	// trust is the user's edit of spec.trustedCASecrets.
	trust := func(cas ...string) {
		editCluster(func(l *neo4jv1beta1.Neo4jEnterpriseCluster) error {
			return patchObject(ctx, c, l, func() { l.Spec.TrustedCASecrets = trustedCAs(cas...) })
		})
	}
	// clusterWrites is the cluster controller's apply of its server
	// StatefulSet, truststore included, from its current spec.
	clusters := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
	clusterWrites := func() {
		t.Helper()
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cluster), latest))
		require.NoError(t, clusters.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(latest), latest))
	}
	live := func() *appsv1.StatefulSet {
		t.Helper()
		sts := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "c-server"}, sts))
		return sts
	}
	// stsStatus reports the StatefulSet rolled out at its generation, then
	// changed by change.
	stsStatus := func(change func(*appsv1.StatefulSetStatus)) {
		t.Helper()
		sts := live()
		sts.Status = rolledOutStatus(*sts.Spec.Replicas, sts.Generation)
		if change != nil {
			change(&sts.Status)
		}
		require.NoError(t, c.Status().Update(ctx, sts))
	}
	pod := func(i int, status corev1.PodStatus) {
		t.Helper()
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("c-server-%d", i), Namespace: ns,
				Labels: map[string]string{"app.kubernetes.io/name": "neo4j", "app.kubernetes.io/instance": "c"}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: "neo4j:2026.08.1-enterprise"}}},
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(p), p); err != nil {
			require.NoError(t, c.Create(ctx, p))
		}
		p.Status = status
		require.NoError(t, c.Status().Update(ctx, p))
	}
	ready := gatePod("", nil).Status
	notReady := gatePod("", nil).Status
	notReady.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	failing := failingIn(&corev1.Pod{}, "plugin-init-custom", 60).Status

	clusterIs(neo4jv1beta1.PhaseReady)
	clusterWrites()
	stsStatus(nil)
	for i := 0; i < 3; i++ {
		pod(i, ready)
	}

	rec := record.NewFakeRecorder(64)
	plugins := &Neo4jPluginReconciler{Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Second,
		bolt: &pluginBolt{
			functional: func(context.Context, *DeploymentInfo) bool { return true },
		}}
	plugin := verifiedDownloadPlugin()
	plugin.Namespace, plugin.Generation = ns, 0
	require.NoError(t, c.Create(ctx, plugin))
	pass := func() {
		t.Helper()
		_, err := plugins.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(plugin)})
		require.NoError(t, err)
	}
	phase := func() string {
		t.Helper()
		p := &neo4jv1beta1.Neo4jPlugin{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(plugin), p))
		return p.Status.Phase
	}

	t.Run("an install is Ready once its roll is done", func(t *testing.T) {
		pass() // the finalizer
		before := live().Generation
		pass()
		require.Greater(t, live().Generation, before, "the install is written")
		require.Contains(t, initContainerNames(live()), "plugin-init-custom")
		pass()
		assert.Equal(t, neo4jv1beta1.PhaseInstalling, phase(), "every pod still reports Ready, but nothing has rolled")
		stsStatus(nil)
		pass()
		assert.Equal(t, neo4jv1beta1.PhaseReady, phase())
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstalled), 1)
	})

	t.Run("a trusted-CA change waits until the cluster has acted on it", func(t *testing.T) {
		trust("corp-ca", "mirror-ca")
		// Its truststore, written before the cluster records the spec: the
		// cluster still reads Ready from the pass before.
		clusterWrites()
		written := live().Generation
		pass()
		assert.Equal(t, written, live().Generation, "the plugin does not write in the same second")
		assert.Equal(t, neo4jv1beta1.PhaseWaiting, phase())
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallDeferred), 1)

		clusterIs(neo4jv1beta1.PhaseForming)
		stsStatus(nil)
		clusterIs(neo4jv1beta1.PhaseReady)
		pass()
		assert.Greater(t, live().Generation, written, "written once the cluster has rolled its truststore")
		assert.Equal(t, []string{"corp-ca (optional)", "mirror-ca (optional)"}, projectedCAs(live().Spec.Template))
		pass()
		assert.Equal(t, neo4jv1beta1.PhaseInstalling, phase())
		stsStatus(nil)
		pass()
		assert.Equal(t, neo4jv1beta1.PhaseReady, phase())
		drainEvents(rec)
	})

	t.Run("its own failing init container is repaired, not waited for", func(t *testing.T) {
		// The plugin's CA write rolled server-2 first, and it fails in the
		// plugin's init container; the user removes the CAs.
		trust()
		clusterIs(neo4jv1beta1.PhaseForming)
		clusterWrites()
		stsStatus(func(s *appsv1.StatefulSetStatus) {
			s.CurrentRevision, s.UpdatedReplicas, s.ReadyReplicas = "rev-before", 1, 2
		})
		pod(2, failing)
		pod(0, notReady)
		stuck := live().Generation
		pass()
		assert.Equal(t, stuck, live().Generation, "not while another server is down")
		assert.Equal(t, neo4jv1beta1.PhaseWaiting, phase())
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInitContainerFailing))

		pod(0, ready)
		pass()
		repaired := live()
		assert.Greater(t, repaired.Generation, stuck, "the repair is written while the cluster is Forming")
		assert.Nil(t, projectedCAs(repaired.Spec.Template))
		assert.False(t, containerMountsVolume(initContainer(t, repaired.Spec.Template, "plugin-init-custom"), resources.PluginCAVolumeName))
		events := eventsWithReason(drainEvents(rec), EventReasonPluginInitContainerFailing)
		require.Len(t, events, 1)
		assert.Contains(t, events[0], "c-server-2 (init container plugin-init-custom exited with code 60)")

		pass()
		assert.Equal(t, repaired.Generation, live().Generation, "written once")
		assert.Empty(t, drainEvents(rec))
	})
}
