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

// A target whose StatefulSet does not exist is waited for quietly
// (operations.md id 177), and a deleted plugin's uninstall replaces the
// standalone pod that keeps failing on it (id 155).

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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// reconcileLogged runs one pass with a logger that records what it logs.
func reconcileLogged(t *testing.T, r *Neo4jPluginReconciler, name string) (ctrl.Result, []logEntry) {
	t.Helper()
	logger, entries := newCaptureLogger()
	res, err := r.Reconcile(log.IntoContext(context.Background(), logger),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: selfHealNS, Name: name}})
	require.NoError(t, err)
	return res, entries()
}

// The defect (v1.22.0 re-walk of the combined build): a storage expansion
// deletes the server StatefulSet and creates it again, as it was, about ten
// seconds later. Each plugin read the missing StatefulSet as drift:
// PluginMissing and PluginInstallFailed Warnings, ERROR logs ("Cannot tell
// whether installing the plugin would change its target", "Failed to install
// plugin"), Installing for the whole expansion (14 minutes), and
// PluginInstalled again when the StatefulSet came back. Nothing was lost; the
// plugin waits, quietly, and a Ready one stays Ready.
func TestReconcile_AMissingStatefulSetIsWaitedForQuietly(t *testing.T) {
	t.Run("a Ready plugin while a storage expansion recreates the StatefulSet", func(t *testing.T) {
		plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
		r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), append([]client.Object{plugin, readyCluster()}, readyClusterPods()...)...)
		rv := getPlugin(t, r, "apoc").ResourceVersion

		for i := 0; i < 3; i++ {
			res, logged := reconcileLogged(t, r, "apoc")
			assert.Equal(t, ctrl.Result{}, res, "the StatefulSet's creation wakes a Ready plugin")
			assert.Empty(t, errorEntries(logged), "%+v", logged)
		}
		got := getPlugin(t, r, "apoc")
		assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
		assert.Equal(t, rv, got.ResourceVersion, "no status write")
		assert.Empty(t, drainEvents(rec))

		// Created again as it was: nothing to do, nothing announced again.
		back := installedSelfHealSTS()
		back.Annotations = map[string]string{
			pluginEnvVarsAnnotation("apoc"): "NEO4J_apoc_export_file_enabled", pluginEntriesAnnotation("apoc"): "apoc",
		}
		require.NoError(t, r.Create(context.Background(), back))
		_, logged := reconcileLogged(t, r, "apoc")
		assert.Empty(t, errorEntries(logged))
		assert.Zero(t, *writes)
		assert.Equal(t, rv, getPlugin(t, r, "apoc").ResourceVersion)
		assert.Empty(t, drainEvents(rec), "no PluginMissing, no PluginInstalled")
	})

	t.Run("a plugin created before its target's StatefulSet", func(t *testing.T) {
		plugin := selfHealPlugin("apoc", "c", "", "")
		cluster := readyCluster()
		cluster.Status.Phase = neo4jv1beta1.PhaseForming
		r, rec, writes, _ := gateReconciler(t, nil, plugin, cluster)

		res, logged := reconcileLogged(t, r, "apoc")

		assert.Positive(t, res.RequeueAfter)
		assert.Empty(t, errorEntries(logged), "%+v", logged)
		got := getPlugin(t, r, "apoc")
		assert.Equal(t, neo4jv1beta1.PhaseWaiting, got.Status.Phase)
		assert.Equal(t, "Waiting for StatefulSet c-server of Neo4jEnterpriseCluster c to be created", got.Status.Message)
		assert.Empty(t, drainEvents(rec))

		// Its target comes up: installed.
		require.NoError(t, r.Create(context.Background(), selfHealSTS()))
		for _, pod := range readyClusterPods() {
			require.NoError(t, r.Create(context.Background(), pod))
		}
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(cluster), latest))
		latest.Status.Phase = neo4jv1beta1.PhaseReady
		require.NoError(t, r.Update(context.Background(), latest))
		installAndRoll(t, r, "apoc", "c-server")
		assert.Equal(t, 1, *writes)
	})

	for _, tc := range []struct {
		name   string
		plugin func() *neo4jv1beta1.Neo4jPlugin
	}{
		{"an install in flight", func() *neo4jv1beta1.Neo4jPlugin {
			return selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation")
		}},
		{"a disabled plugin's removal in flight", func() *neo4jv1beta1.Neo4jPlugin {
			p := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseInstalling, "Removing the plugin (spec.enabled: false): waiting for the servers to restart without it")
			p.Spec.Enabled = false
			return p
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := tc.plugin()
			r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), append([]client.Object{plugin, readyCluster()}, readyClusterPods()...)...)
			rv := getPlugin(t, r, "apoc").ResourceVersion

			res, logged := reconcileLogged(t, r, "apoc")

			assert.Positive(t, res.RequeueAfter, "it looks again")
			assert.Empty(t, errorEntries(logged), "%+v", logged)
			assert.Equal(t, rv, getPlugin(t, r, "apoc").ResourceVersion, "its status stays")
			assert.Zero(t, *writes)
			assert.Empty(t, drainEvents(rec))
		})
	}
}

// The ProceduresHidden condition stays while the StatefulSet it is read from
// is away: a missing StatefulSet tells nothing about the allowlist.
func TestReconcile_AMissingStatefulSetKeepsProceduresHidden(t *testing.T) {
	apoc, genai, _ := hiddenFixture()
	SetNamedCondition(&apoc.Status.Conditions, ConditionTypeProceduresHidden, 1, metav1.ConditionTrue, ConditionReasonNotInAllowlist, "hidden")
	r, rec, _, _ := gateReconciler(t, boltMustNotRun(t), apoc, genai, readyCluster())

	reconcilePlugin(t, r, "apoc")

	assert.NotNil(t, findCondition(getPlugin(t, r, "apoc").Status.Conditions, ConditionTypeProceduresHidden))
	assert.Empty(t, drainEvents(rec))
}

// crashLoopingStandalone is standalone "sa" with the GenAI plugin of
// genaiPlugin written to its StatefulSet and rolled out to revision rev-1,
// whose one pod keeps exiting on genai.vector.enabled.
func crashLoopingStandalone() (*neo4jv1beta1.Neo4jEnterpriseStandalone, *appsv1.StatefulSet, *corev1.Pod) {
	standalone := readyStandalone()
	standalone.Status.Phase = neo4jv1beta1.PhasePending
	sts := standaloneSTS(genaiEnv()...)
	sts.Annotations = map[string]string{
		pluginEnvVarsAnnotation("genai"): baseURLVar + "," + vectorVar, pluginEntriesAnnotation("genai"): "genai",
	}
	sts.Generation = 1
	sts.Status = rolledOutStatus(1, 1)
	sts.Status.ReadyReplicas = 0
	pod := crashLooping(gatePod("sa-0", map[string]string{"app": "sa"}), 1, genaiEnv()...)
	pod.Labels[appsv1.ControllerRevisionHashLabelKey] = sts.Status.UpdateRevision
	return standalone, sts, pod
}

// The defect (v1.22.0 re-walk of the combined build): deleting a plugin whose
// setting kept a standalone's pod crash-looping wrote the uninstall —
// NEO4J_PLUGINS back to what the others need — and released the finalizer at
// once. An OrderedReady StatefulSet does not replace a pod that is not Ready,
// and the plugin had no pass left to delete it: the pod stayed in
// CrashLoopBackOff until deleted by hand. The finalizer now waits for the
// StatefulSet's controller to see the uninstall and deletes the pod first.
func TestUninstall_ReplacesTheStandalonePodThatKeepsFailingOnThePlugin(t *testing.T) {
	plugin := genaiPlugin("sa", neo4jv1beta1.PhaseReady)
	standalone, sts, pod := crashLoopingStandalone()
	r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), plugin, standalone, sts, pod)
	require.NoError(t, r.Delete(context.Background(), plugin))

	res := reconcilePlugin(t, r, "genai")

	require.Equal(t, 1, *writes, "the uninstall")
	assert.NotContains(t, envNames(liveSTSEnv(t, r.Client, "sa")), vectorVar)
	assert.Positive(t, res.RequeueAfter, "the finalizer waits for the pod")
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(plugin), &neo4jv1beta1.Neo4jPlugin{}), "still there")
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}), "not before the StatefulSet has seen the uninstall")

	// The StatefulSet's controller sees the uninstall.
	live := liveSTS(t, r.Client, "sa")
	live.Status.ObservedGeneration, live.Status.UpdateRevision = live.Generation, "rev-next"
	require.NoError(t, r.Status().Update(context.Background(), live))
	res = reconcilePlugin(t, r, "genai")

	assert.Zero(t, res.RequeueAfter)
	err := r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
	assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "the stuck pod is deleted: %v", err)
	err = r.Get(context.Background(), client.ObjectKeyFromObject(plugin), &neo4jv1beta1.Neo4jPlugin{})
	assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "and the plugin goes: %v", err)
	replaced := eventsWithReason(drainEvents(rec), EventReasonPluginSettingsFailing)
	require.Len(t, replaced, 1)
	assert.Contains(t, replaced[0], "Deleted pod sa-0")
	assert.Contains(t, replaced[0], "from its current template")
	assert.NotContains(t, replaced[0], "revision", "the pod comes back from the update revision at that moment, which may have moved on")
	assert.Equal(t, 1, *writes)
}

// A deletion never wedges: past pluginUninstallPodWait the finalizer goes,
// replaced pod or not.
func TestUninstall_TheWaitForAStuckPodIsBounded(t *testing.T) {
	plugin := genaiPlugin("sa", neo4jv1beta1.PhaseReady)
	standalone, sts, pod := crashLoopingStandalone()
	r, _, _, _ := gateReconciler(t, boltMustNotRun(t), plugin, standalone, sts, pod)
	require.NoError(t, r.Delete(context.Background(), plugin))
	deleting := getPlugin(t, r, "genai")
	deleting.DeletionTimestamp = &metav1.Time{Time: time.Now().Add(-pluginUninstallPodWait - time.Minute)}

	assert.False(t, r.uninstallWaitsForStuckPod(context.Background(), deleting))
}

// A disabled plugin's removal on a standalone replaces the pod that keeps
// failing on it with its own later passes: its CR stays (id 160).
func TestReconcile_DisablingReplacesTheStandalonePodThatKeepsFailingOnThePlugin(t *testing.T) {
	plugin := genaiPlugin("sa", neo4jv1beta1.PhaseReady)
	plugin.Spec.Enabled, plugin.Generation = false, 2
	standalone, sts, pod := crashLoopingStandalone()
	r, _, writes, _ := gateReconciler(t, boltMustNotRun(t), plugin, standalone, sts, pod)

	reconcilePlugin(t, r, "genai")
	require.Equal(t, 1, *writes, "the removal is written to the standalone that is down")
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}))

	live := liveSTS(t, r.Client, "sa")
	live.Status.ObservedGeneration, live.Status.UpdateRevision = live.Generation, "rev-next"
	require.NoError(t, r.Status().Update(context.Background(), live))
	reconcilePlugin(t, r, "genai")

	err := r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
	assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "the stuck pod is deleted: %v", err)
}

// The defect (#573, from the code; it predates the v1.22.0 work): a plugin
// reconciled before its target existed — one kubectl apply of a directory, a
// GitOps sync — went Failed ("Target deployment not found") with no requeue,
// and stayed Failed after the target came, until its CR was edited. It waits:
// Pending, one ClusterNotFound Warning, a requeue; then it installs.
func TestReconcile_APluginWaitsForATargetNotCreatedYet(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", "", "")
	r, rec, writes, _ := gateReconciler(t, nil, plugin)

	res, logs := reconcileLogged(t, r, "apoc")

	assert.Positive(t, res.RequeueAfter, "looked at again: nothing watches for the target")
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhasePending, got.Status.Phase)
	assert.Contains(t, got.Status.Message, `clusterRef "c" not found`)
	warnings := eventsWithReason(drainEvents(rec), EventReasonClusterNotFound)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "Warning")
	assert.Empty(t, errorEntries(logs), "a target to come is not an error")

	// The wait goes on quietly.
	rv := got.ResourceVersion
	reconcilePlugin(t, r, "apoc")
	assert.Equal(t, rv, getPlugin(t, r, "apoc").ResourceVersion)
	assert.Empty(t, drainEvents(rec))

	// The target arrives: the plugin installs.
	for _, obj := range append([]client.Object{readyCluster(), selfHealSTS()}, readyClusterPods()...) {
		require.NoError(t, r.Create(context.Background(), obj))
	}
	installAndRoll(t, r, "apoc", "c-server")
	assert.Equal(t, 1, *writes)
}
