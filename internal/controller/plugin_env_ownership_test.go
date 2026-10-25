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

// A Neo4jPlugin takes the variables it wrote off its target's servers when it
// no longer sets them: a key removed from spec.config, and everything it wrote
// when the CR is deleted; and a removal that repairs servers crash-looping on
// the plugin's settings does not wait for them (operations.md id 155).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// genaiPlugin is the GenAI Neo4jPlugin of target ref with two settings: one
// Neo4j knows (genai.openai.baseurl, CalVer) and one it rejects at startup
// (genai.vector.enabled, from the example the v1.22.0 journey applied).
func genaiPlugin(ref, phase string) *neo4jv1beta1.Neo4jPlugin {
	plugin := selfHealPlugin("genai", ref, phase, "")
	if phase == neo4jv1beta1.PhaseReady {
		plugin.Status.Message = "Plugin installed and configured successfully"
	}
	plugin.Spec.Name = "genai"
	plugin.Spec.Version = "2026.08.1"
	plugin.Spec.Config = map[string]string{
		"genai.openai.baseurl": "https://llm-proxy.internal",
		"genai.vector.enabled": "true",
	}
	return plugin
}

const (
	baseURLVar = "NEO4J_genai_openai_baseurl"
	vectorVar  = "NEO4J_genai_vector_enabled"
)

// genaiEnv is what genaiPlugin wrote to its target: NEO4J_PLUGINS and both
// settings.
func genaiEnv(extra ...corev1.EnvVar) []corev1.EnvVar {
	return append([]corev1.EnvVar{
		{Name: "NEO4J_PLUGINS", Value: `["genai"]`},
		{Name: baseURLVar, Value: "https://llm-proxy.internal"},
		{Name: vectorVar, Value: "true"},
	}, extra...)
}

// envTarget is a Ready target of a plugin, cluster "c" or standalone "sa",
// with its server StatefulSet (carrying env) and its Ready pods.
type envTarget struct {
	name, sts string
	objects   func(env ...corev1.EnvVar) []client.Object
}

func envTargets() []envTarget {
	return []envTarget{
		{"cluster", "c-server", func(env ...corev1.EnvVar) []client.Object {
			return append([]client.Object{readyCluster(), selfHealSTS(env...)}, readyClusterPods()...)
		}},
		{"standalone", "sa", func(env ...corev1.EnvVar) []client.Object {
			return []client.Object{readyStandalone(), standaloneSTS(env...), gatePod("sa-0", map[string]string{"app": "sa"})}
		}},
	}
}

func (tg envTarget) ref() string {
	if tg.name == "cluster" {
		return "c"
	}
	return "sa"
}

func readyStandalone() *neo4jv1beta1.Neo4jEnterpriseStandalone {
	return &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: selfHealNS},
		Status:     neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: neo4jv1beta1.PhaseReady},
	}
}

// standaloneSTS is standalone "sa"'s StatefulSet with env on its neo4j
// container.
func standaloneSTS(env ...corev1.EnvVar) *appsv1.StatefulSet {
	sts := pluginTestSTS("sa", selfHealNS)
	sts.Name = "sa"
	sts.Spec.Template.Spec.Containers[0].Env = env
	return sts
}

// editPlugin applies edit to the stored plugin as a user's edit does: a new
// generation the status has not caught up with.
func editPlugin(t *testing.T, c client.Client, name string, edit func(*neo4jv1beta1.Neo4jPlugin)) {
	t.Helper()
	p := &neo4jv1beta1.Neo4jPlugin{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: selfHealNS, Name: name}, p))
	require.NoError(t, patchObject(context.Background(), c, p, func() {
		edit(p)
		p.Generation++
	}))
}

func envNames(env []corev1.EnvVar) []string {
	var names []string
	for _, e := range env {
		names = append(names, e.Name)
	}
	return names
}

// recordOf is the plugin's record on the StatefulSet name ("" when none).
func recordOf(t *testing.T, c client.Client, name, plugin string) string {
	t.Helper()
	return liveSTS(t, c, name).Annotations[pluginEnvVarsAnnotation(plugin)]
}

// crashLooping makes pod's neo4j container run env and exit with code over
// and over: it waits in CrashLoopBackOff to start again.
func crashLooping(pod *corev1.Pod, code int32, env ...corev1.EnvVar) *corev1.Pod {
	pod.Spec.Containers = []corev1.Container{{Name: "neo4j", Env: env}}
	pod.Status = corev1.PodStatus{
		Phase:      corev1.PodRunning,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}},
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:                 "neo4j",
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: "Error"}},
			RestartCount:         5,
		}},
	}
	return pod
}

// running gives pod a neo4j container that runs env.
func running(pod *corev1.Pod, env ...corev1.EnvVar) *corev1.Pod {
	pod.Spec.Containers = []corev1.Container{{Name: "neo4j", Env: env}}
	return pod
}

// The record's key is a valid annotation key for any CR name, and two names
// never share one.
func TestPluginEnvVarsAnnotation(t *testing.T) {
	long := strings.Repeat("a", 60) + ".example-plugin"
	other := strings.Repeat("a", 60) + ".example-plugin2"
	assert.Equal(t, "neo4j.com/plugin-env-vars.genai", pluginEnvVarsAnnotation("genai"))
	for _, name := range []string{"genai", "a", strings.Repeat("b", 47), strings.Repeat("b", 48), long, strings.Repeat("c", 253)} {
		key := pluginEnvVarsAnnotation(name)
		assert.Empty(t, validation.IsQualifiedName(key), "%q", key)
		assert.True(t, strings.HasPrefix(key, pluginEnvVarsAnnotationPrefix))
	}
	assert.NotEqual(t, pluginEnvVarsAnnotation(long), pluginEnvVarsAnnotation(other))
}

// The defect (v1.22.0 journey, CalVer 2026.08.1): the GenAI example's
// genai.vector.enabled is not a Neo4j setting, server-2 crash-looped on it, and
// removing the key from the plugin left NEO4J_genai_vector_enabled on the
// StatefulSet: the StatefulSet had to be edited by hand. The key's variable
// goes with the key, in one write; the plugin's other variables stay.
func TestReconcile_ARemovedConfigKeyLeavesTheServers(t *testing.T) {
	for _, tg := range envTargets() {
		t.Run(tg.name, func(t *testing.T) {
			plugin := genaiPlugin(tg.ref(), "")
			r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin}, tg.objects()...)...)

			reconcilePlugin(t, r, "genai")
			require.Equal(t, 1, *writes, "the install")
			require.Subset(t, envNames(liveSTSEnv(t, r.Client, tg.sts)), []string{baseURLVar, vectorVar})
			assert.Equal(t, baseURLVar+","+vectorVar, recordOf(t, r.Client, tg.sts, "genai"), "the install records what it sets")
			rollOut(t, r.Client, tg.sts)
			reconcilePlugin(t, r, "genai")
			require.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "genai").Status.Phase)
			drainEvents(rec)

			editPlugin(t, r.Client, "genai", func(p *neo4jv1beta1.Neo4jPlugin) { delete(p.Spec.Config, "genai.vector.enabled") })
			reconcilePlugin(t, r, "genai")

			assert.Equal(t, 2, *writes, "one write takes it away")
			env := envNames(liveSTSEnv(t, r.Client, tg.sts))
			assert.NotContains(t, env, vectorVar)
			assert.Contains(t, env, baseURLVar)
			assert.Equal(t, baseURLVar, recordOf(t, r.Client, tg.sts, "genai"))
			assert.Equal(t, neo4jv1beta1.PhaseInstalling, getPlugin(t, r, "genai").Status.Phase, "Ready once the roll is done")
			rollOut(t, r.Client, tg.sts)
			reconcilePlugin(t, r, "genai")
			assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "genai").Status.Phase)
			assert.Equal(t, 2, *writes)
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing), "a removal is not a loss")

			// The last key goes: so does its variable, and the record.
			editPlugin(t, r.Client, "genai", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Config = nil })
			reconcilePlugin(t, r, "genai")
			assert.NotContains(t, envNames(liveSTSEnv(t, r.Client, tg.sts)), baseURLVar)
			assert.NotContains(t, liveSTS(t, r.Client, tg.sts).Annotations, pluginEnvVarsAnnotation("genai"))
		})
	}
}

// Plugins installed by an earlier operator have no record. The first pass
// records what the plugin sets now and takes nothing away: a variable a key
// removal left behind under that operator cannot be told from one someone else
// set. A settled plugin's record is a metadata write — no restart, no status
// write, no event — and from then on a removed key's variable goes.
func TestReconcile_TheFirstPassAfterAnUpgradeRecordsAndRemovesNothing(t *testing.T) {
	for _, tg := range envTargets() {
		t.Run(tg.name, func(t *testing.T) {
			plugin := genaiPlugin(tg.ref(), neo4jv1beta1.PhaseReady)
			delete(plugin.Spec.Config, "genai.vector.enabled") // removed under the earlier operator
			r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin}, tg.objects(genaiEnv()...)...)...)
			rv := getPlugin(t, r, "genai").ResourceVersion
			generation := liveSTS(t, r.Client, tg.sts).Generation

			res := reconcilePlugin(t, r, "genai")

			assert.Equal(t, ctrl.Result{}, res)
			assert.Zero(t, *writes, "no template write")
			assert.Equal(t, generation, liveSTS(t, r.Client, tg.sts).Generation, "nothing restarts")
			assert.Equal(t, baseURLVar, recordOf(t, r.Client, tg.sts, "genai"), "what it sets now is recorded")
			assert.Contains(t, envNames(liveSTSEnv(t, r.Client, tg.sts)), vectorVar, "what it cannot attribute stays")
			assert.Equal(t, rv, getPlugin(t, r, "genai").ResourceVersion, "no status write")
			assert.Empty(t, drainEvents(rec))

			editPlugin(t, r.Client, "genai", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Config = nil })
			reconcilePlugin(t, r, "genai")
			assert.Equal(t, 1, *writes)
			env := envNames(liveSTSEnv(t, r.Client, tg.sts))
			assert.NotContains(t, env, baseURLVar, "a recorded variable goes")
			assert.Contains(t, env, vectorVar)
		})
	}
}

// A variable the plugin stops setting stays when someone else owns it: the
// target renders it (spec.env), another plugin records it, or another plugin
// of the target sets it without a record yet. A procedure list is never the
// plugin's to remove.
func TestApplyPlugin_LeavesWhatSomeoneElseOwns(t *testing.T) {
	const unrestricted = "NEO4J_dbms_security_procedures_unrestricted"
	mine := genaiPlugin("c", neo4jv1beta1.PhaseReady)
	mine.Spec.Config = map[string]string{"genai.openai.baseurl": "https://llm-proxy.internal"}
	other := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "")
	// A shared list stays while a plugin of the target puts tokens in it,
	// whatever a record says (setClusterPluginLists).
	other.Spec.Config = map[string]string{"genai.azure.openai.baseurl": "https://azure.internal", "dbms.security.procedures.unrestricted": "genai.*"}
	deleting := selfHealPlugin("gds", "c", neo4jv1beta1.PhaseReady, "")
	deleting.Spec.Name = "graph-data-science"
	deleting.Spec.Config = map[string]string{"genai.mine.only": "x"}
	deleting.DeletionTimestamp = ptr.To(metav1.Now())
	sts := selfHealSTS(
		corev1.EnvVar{Name: "NEO4J_genai_cluster_owned", Value: "a"},
		corev1.EnvVar{Name: "NEO4J_genai_recorded__elsewhere", Value: "b"},
		corev1.EnvVar{Name: "NEO4J_genai_azure_openai_baseurl", Value: "https://azure.internal"},
		corev1.EnvVar{Name: "NEO4J_genai_mine_only", Value: "x"},
		corev1.EnvVar{Name: unrestricted, Value: "genai.*"},
		corev1.EnvVar{Name: baseURLVar, Value: "https://llm-proxy.internal"},
	)
	sts.Annotations = map[string]string{
		ownedEnvVarsAnnotation:           "NEO4J_AUTH,NEO4J_genai_cluster_owned",
		pluginEnvVarsAnnotation("other"): "NEO4J_genai_recorded__elsewhere",
		pluginEnvVarsAnnotation("genai"): strings.Join([]string{baseURLVar, "NEO4J_genai_azure_openai_baseurl",
			"NEO4J_genai_cluster_owned", "NEO4J_genai_mine_only", "NEO4J_genai_recorded__elsewhere", unrestricted}, ","),
	}
	r, _, _, _ := gateReconciler(t, nil, mine, other, deleting, readyCluster(), sts)
	target := &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: readyCluster()}

	edited := liveSTS(t, r.Client, "c-server")
	removed, err := r.applyPlugin(context.Background(), edited, mine, target)

	require.NoError(t, err)
	assert.Equal(t, []string{"NEO4J_genai_mine_only"}, removed, "only what is the plugin's alone")
	assert.ElementsMatch(t, []string{"NEO4J_genai_cluster_owned", "NEO4J_genai_recorded__elsewhere",
		"NEO4J_genai_azure_openai_baseurl", unrestricted, baseURLVar, "NEO4J_PLUGINS"}, envNames(neo4jContainerEnv(edited.Spec.Template.Spec)))
	assert.Equal(t, baseURLVar, edited.Annotations[pluginEnvVarsAnnotation("genai")], "the record is what it sets")
	assert.Equal(t, "NEO4J_genai_recorded__elsewhere", edited.Annotations[pluginEnvVarsAnnotation("other")], "another's record is its own")
}

// A removal restarts every server, like an install, and waits for a target
// that is Ready with every server available: the cluster here is short of a
// server for a reason the removal does not touch.
func TestReconcile_ARemovalWaitsForAFullyAvailableTarget(t *testing.T) {
	plugin := genaiPlugin("c", neo4jv1beta1.PhaseReady)
	plugin.Generation = 2
	delete(plugin.Spec.Config, "genai.vector.enabled")
	sts := selfHealSTS(genaiEnv()...)
	sts.Annotations = map[string]string{pluginEnvVarsAnnotation("genai"): baseURLVar + "," + vectorVar}
	r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), plugin, degradedCluster(neo4jv1beta1.PhaseDegraded), sts)

	reconcilePlugin(t, r, "genai")

	assert.Zero(t, *writes)
	assert.Contains(t, envNames(liveSTSEnv(t, r.Client, "c-server")), vectorVar)
	assert.Equal(t, neo4jv1beta1.PhaseWaiting, getPlugin(t, r, "genai").Status.Phase)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallDeferred), 1)
}

// settingsDeadlock is the journey's other deadlock: cluster "c" with three
// servers is Forming while its server StatefulSet's roll is stuck, because
// server 2 runs the GenAI plugin's genai.vector.enabled, which Neo4j refuses
// at startup, and its neo4j container keeps exiting; servers 0 and 1 still run
// the template from before the plugin. The user has removed the key.
func settingsDeadlock() (*neo4jv1beta1.Neo4jPlugin, *neo4jv1beta1.Neo4jEnterpriseCluster, *appsv1.StatefulSet, []*corev1.Pod) {
	plugin := genaiPlugin("c", neo4jv1beta1.PhaseInstalling)
	plugin.Status.Message = "Waiting for pods to be ready after plugin installation"
	plugin.Generation = 2
	delete(plugin.Spec.Config, "genai.vector.enabled")
	sts := selfHealSTS(genaiEnv()...)
	sts.Annotations = map[string]string{pluginEnvVarsAnnotation("genai"): baseURLVar + "," + vectorVar}
	sts.Spec.Replicas = ptr.To[int32](3)
	sts.Status = rolledOutStatus(3, 0)
	sts.Status.UpdateRevision, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = "rev-next", 1, 2
	cluster := readyCluster()
	cluster.Status.Phase = neo4jv1beta1.PhaseForming
	cluster.Spec.Topology.Servers = 3
	pods := []*corev1.Pod{
		running(gatePod("c-server-0", nil)), running(gatePod("c-server-1", nil)),
		crashLooping(gatePod("c-server-2", nil), 1, genaiEnv()...),
	}
	return plugin, cluster, sts, pods
}

// The defect (v1.22.0 journey): with server-2 crash-looping on the plugin's
// genai.vector.enabled, removing the key waited for a Ready cluster that only
// the removal could bring back — the deadlock id 152 solved for the plugin's
// init container. A write that changes the settings every down server keeps
// failing with goes ahead, and says so once.
func TestReconcile_ARemovalThatRepairsCrashingServersDoesNotWait(t *testing.T) {
	plugin, cluster, sts, pods := settingsDeadlock()
	r, rec, writes, _ := gateReconciler(t, boltDown(), objects(plugin, cluster, sts, pods)...)

	reconcilePlugin(t, r, "genai")

	require.Equal(t, 1, *writes, "the removal is written although the cluster is Forming")
	assert.NotContains(t, envNames(liveSTSEnv(t, r.Client, "c-server")), vectorVar)
	got := getPlugin(t, r, "genai")
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase)
	events := drainEvents(rec)
	assert.Empty(t, eventsWithReason(events, EventReasonPluginInstallDeferred))
	repairs := eventsWithReason(events, EventReasonPluginSettingsFailing)
	require.Len(t, repairs, 1)
	assert.Contains(t, repairs[0], "Warning")
	assert.Contains(t, repairs[0], "c-server-2 (container neo4j exited with code 1)")
	assert.Contains(t, repairs[0], "Neo4jEnterpriseCluster c")

	reconcilePlugin(t, r, "genai")
	assert.Equal(t, 1, *writes, "written once")
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginSettingsFailing))
}

// The exception is as narrow as id 152's: it never rolls a cluster short of a
// server for any other reason. Each case is the deadlock with one thing
// changed, and waits.
func TestReconcile_ASettingsRepairWaitsWhenSomethingElseIsDown(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod
	}{
		{"another server is down for another reason", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			pods[0].Status.Conditions[0].Status = corev1.ConditionFalse
			return pods
		}},
		{"the failing server does not run what the write changes", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			pods[2] = crashLooping(gatePod("c-server-2", nil), 1)
			return pods
		}},
		{"the failing server is below one that is up", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			return []*corev1.Pod{pods[0], crashLooping(gatePod("c-server-1", nil), 1, genaiEnv()...), running(gatePod("c-server-2", nil))}
		}},
		{"the server is starting again", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			pods[2].Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
			return pods
		}},
		{"a server pod is missing", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			return pods[1:]
		}},
		{"Neo4j reports another server unavailable", func(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, _ *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			cluster.Status.Diagnostics = &neo4jv1beta1.ClusterDiagnosticsStatus{Servers: []neo4jv1beta1.ServerDiagnosticInfo{
				{Name: "s0", Address: "c-server-0.c-headless.default.svc.cluster.local:7687", State: "Enabled", Health: "Unavailable"},
			}}
			return pods
		}},
		{"the failing server is below the partition", func(_ *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet, pods []*corev1.Pod) []*corev1.Pod {
			sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To[int32](3)}
			return pods
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin, cluster, sts, pods := settingsDeadlock()
			pods = tc.change(cluster, sts, pods)
			r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), objects(plugin, cluster, sts, pods)...)

			reconcilePlugin(t, r, "genai")

			assert.Zero(t, *writes, "not written")
			assert.Equal(t, neo4jv1beta1.PhaseWaiting, getPlugin(t, r, "genai").Status.Phase)
			events := drainEvents(rec)
			assert.Len(t, eventsWithReason(events, EventReasonPluginInstallDeferred), 1)
			assert.Empty(t, eventsWithReason(events, EventReasonPluginSettingsFailing))
		})
	}
}

// While its roll is stuck on servers whose neo4j container keeps exiting, the
// plugin names them: it used to say only that it waited.
func TestReconcile_TheWaitNamesServersThatKeepExiting(t *testing.T) {
	plugin, cluster, sts, pods := settingsDeadlock()
	plugin.Generation = 1
	plugin.Spec.Config["genai.vector.enabled"] = "true"
	r, rec, writes, _ := gateReconciler(t, nil, objects(plugin, cluster, sts, pods)...)

	reconcilePlugin(t, r, "genai")

	assert.Zero(t, *writes, "nothing to write: the plugin is as it was written")
	got := getPlugin(t, r, "genai")
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase)
	assert.Equal(t, "Waiting for pods to be ready after plugin installation; "+
		"cannot start: c-server-2 (container neo4j exited with code 1)", got.Status.Message)
	assert.Empty(t, drainEvents(rec))
}

// A standalone's StatefulSet uses OrderedReady pod management and does not
// replace a pod that is not Ready (id 152). Once the removal is written and the
// StatefulSet has seen it, the plugin deletes the pod that keeps exiting with
// the removed setting, so it comes back without it.
func TestReconcile_AStandalonesCrashingPodIsReplacedAfterTheRepair(t *testing.T) {
	plugin := genaiPlugin("sa", neo4jv1beta1.PhaseInstalling)
	plugin.Status.Message = "Waiting for pods to be ready after plugin installation"
	plugin.Generation = 2
	delete(plugin.Spec.Config, "genai.vector.enabled")
	standalone := readyStandalone()
	standalone.Status.Phase = neo4jv1beta1.PhasePending
	sts := standaloneSTS(genaiEnv()...)
	sts.Annotations = map[string]string{pluginEnvVarsAnnotation("genai"): baseURLVar + "," + vectorVar}
	sts.Status.ReadyReplicas = 0
	pod := crashLooping(gatePod("sa-0", map[string]string{"app": "sa"}), 1, genaiEnv()...)
	pod.Labels[appsv1.ControllerRevisionHashLabelKey] = sts.Status.UpdateRevision
	r, rec, writes, _ := gateReconciler(t, boltDown(), plugin, standalone, sts, pod)

	reconcilePlugin(t, r, "genai")
	require.Equal(t, 1, *writes, "the removal is written to the standalone that is down")
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginSettingsFailing), 1)

	// Not before the StatefulSet has seen the new template.
	reconcilePlugin(t, r, "genai")
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}), "the pod is kept")

	live := liveSTS(t, r.Client, "sa")
	live.Status.ObservedGeneration = live.Generation
	live.Status.UpdateRevision = fmt.Sprintf("rev-%d", live.Generation)
	require.NoError(t, r.Status().Update(context.Background(), live))
	reconcilePlugin(t, r, "genai")

	err := r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
	assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "the stuck pod is deleted: %v", err)
	replaced := eventsWithReason(drainEvents(rec), EventReasonPluginSettingsFailing)
	require.Len(t, replaced, 1)
	assert.Contains(t, replaced[0], "Deleted pod sa-0")
	assert.Contains(t, replaced[0], "container neo4j exited with code 1")
}

// A standalone's pod that keeps exiting is the plugin's to delete only while
// the plugin's own write is what the pod lacks.
func TestReplaceStuckPods_Settings(t *testing.T) {
	for _, tc := range []struct {
		name    string
		change  func(plugin *neo4jv1beta1.Neo4jPlugin, sts *appsv1.StatefulSet, pod *corev1.Pod)
		deleted bool
	}{
		{"the template changes what the pod runs", nil, true},
		{"the plugin is settled", func(p *neo4jv1beta1.Neo4jPlugin, _ *appsv1.StatefulSet, _ *corev1.Pod) {
			p.Status.Phase = neo4jv1beta1.PhaseReady
		}, false},
		{"the template changes only what the standalone renders", func(_ *neo4jv1beta1.Neo4jPlugin, sts *appsv1.StatefulSet, pod *corev1.Pod) {
			sts.Annotations = map[string]string{standaloneOwnedEnvVarsAnnotation: "NEO4J_AUTH"}
			sts.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "NEO4J_AUTH", Value: "neo4j/new"}}
			pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "NEO4J_AUTH", Value: "neo4j/old"}}
		}, false},
		{"the container is starting again", func(_ *neo4jv1beta1.Neo4jPlugin, _ *appsv1.StatefulSet, pod *corev1.Pod) {
			pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := genaiPlugin("sa", neo4jv1beta1.PhaseInstalling)
			sts := standaloneSTS(corev1.EnvVar{Name: baseURLVar, Value: "https://llm-proxy.internal"})
			sts.Generation = 2
			sts.Status = rolledOutStatus(1, 2)
			sts.Status.CurrentRevision, sts.Status.ReadyReplicas = "rev-1", 0
			pod := crashLooping(gatePod("sa-0", map[string]string{"app": "sa", appsv1.ControllerRevisionHashLabelKey: "rev-1"}), 1, genaiEnv()...)
			if tc.change != nil {
				tc.change(plugin, sts, pod)
			}
			r, rec, _, _ := gateReconciler(t, nil, plugin, sts, pod)

			r.replaceStuckPods(context.Background(), plugin, &DeploymentInfo{Type: "standalone", Name: "sa", Namespace: selfHealNS})

			err := r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
			assert.Equal(t, tc.deleted, err != nil, "deleted: %v", err)
			assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginSettingsFailing), map[bool]int{true: 1, false: 0}[tc.deleted])
		})
	}
}

func TestContainerFailure(t *testing.T) {
	status := func(s corev1.ContainerStatus) *corev1.Pod {
		s.Name = "neo4j"
		return &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{s}}}
	}
	exited := func(code int32) *corev1.ContainerStateTerminated {
		return &corev1.ContainerStateTerminated{ExitCode: code}
	}
	for _, tc := range []struct {
		name                  string
		pod                   *corev1.Pod
		code                  int32
		failing, crashLooping bool
	}{
		{"exited", status(corev1.ContainerStatus{State: corev1.ContainerState{Terminated: exited(1)}}), 1, true, true},
		{"CrashLoopBackOff", status(corev1.ContainerStatus{
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: exited(1)},
		}), 1, true, true},
		{"running again after a failure", status(corev1.ContainerStatus{
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			LastTerminationState: corev1.ContainerState{Terminated: exited(137)},
		}), 137, true, false},
		{"ready after a failure", status(corev1.ContainerStatus{
			Ready:                true,
			State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			LastTerminationState: corev1.ContainerState{Terminated: exited(1)},
		}), 0, false, false},
		{"starting", status(corev1.ContainerStatus{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}), 0, false, false},
		{"waiting to start", status(corev1.ContainerStatus{
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
		}), 0, false, false},
		{"exited cleanly", status(corev1.ContainerStatus{State: corev1.ContainerState{Terminated: exited(0)}}), 0, false, false},
		{"no such container", &corev1.Pod{}, 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, failing := containerFailure(tc.pod, "neo4j")
			assert.Equal(t, tc.failing, failing, "failing")
			if failing {
				assert.Equal(t, tc.code, code)
			}
			_, looping := containerCrashLooping(tc.pod, "neo4j")
			assert.Equal(t, tc.crashLooping, looping, "crash-looping")
		})
	}
}

// The defect: deleting the plugin left its settings on the servers (the
// uninstall removed only its NEO4J_PLUGINS entry, and not even that for an
// alias such as "gds"). Everything the plugin wrote goes, in one write; what the
// target renders itself and what another plugin needs stay.
func TestUninstall_TakesAwayWhatThePluginWrote(t *testing.T) {
	for _, tg := range envTargets() {
		t.Run(tg.name, func(t *testing.T) {
			plugin := genaiPlugin(tg.ref(), neo4jv1beta1.PhaseReady)
			// Two keys it shares: one the target renders from its own spec,
			// one another plugin sets.
			plugin.Spec.Config["server.memory.pagecache.size"] = "256m"
			plugin.Spec.Config["apoc.export.file.enabled"] = "true"
			apoc := selfHealPlugin("apoc", tg.ref(), neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
			env := []corev1.EnvVar{
				{Name: "NEO4J_AUTH", Value: "neo4j/secret"},
				{Name: "NEO4J_server_memory_pagecache_size", Value: "256m"},
				{Name: "NEO4J_PLUGINS", Value: `["apoc","genai"]`},
				{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
				{Name: baseURLVar, Value: "https://llm-proxy.internal"},
				{Name: vectorVar, Value: "true"},
			}
			objs := tg.objects(env...)
			for _, o := range objs {
				if sts, ok := o.(*appsv1.StatefulSet); ok {
					sts.Annotations = map[string]string{
						pluginEnvVarsAnnotation("genai"): baseURLVar + ",NEO4J_server_memory_pagecache_size",
						ownedEnvVarsAnnotation:           "NEO4J_AUTH,NEO4J_server_memory_pagecache_size",
						standaloneOwnedEnvVarsAnnotation: "NEO4J_AUTH,NEO4J_server_memory_pagecache_size",
					}
				}
			}
			r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin, apoc}, objs...)...)
			require.NoError(t, r.Delete(context.Background(), plugin))

			reconcilePlugin(t, r, "genai")

			assert.Equal(t, 1, *writes, "one write")
			live := liveSTSEnv(t, r.Client, tg.sts)
			assert.Equal(t, []string{"NEO4J_AUTH", "NEO4J_server_memory_pagecache_size", "NEO4J_PLUGINS", "NEO4J_apoc_export_file_enabled"}, envNames(live),
				"its recorded variable and the one its spec still sets go; the target's own and another plugin's stay")
			v, _ := findEnv(&corev1.Container{Env: live}, "NEO4J_PLUGINS")
			assert.Equal(t, `["apoc"]`, v)
			assert.NotContains(t, liveSTS(t, r.Client, tg.sts).Annotations, pluginEnvVarsAnnotation("genai"))
			err := r.Get(context.Background(), client.ObjectKeyFromObject(plugin), &neo4jv1beta1.Neo4jPlugin{})
			assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "the finalizer is released: %v", err)
		})
	}
}

// What a plugin shares with others on the target stays when it goes: a
// NEO4J_PLUGINS name another plugin installs, procedure tokens another plugin
// puts in a list, fleet-management while the cluster installs it, a list the
// cluster renders itself; and a PreBaked plugin never touches NEO4J_PLUGINS.
func TestUninstall_LeavesWhatOthersNeed(t *testing.T) {
	const unrestricted = "NEO4J_dbms_security_procedures_unrestricted"
	gds := func(mode string) *neo4jv1beta1.Neo4jPlugin {
		p := selfHealPlugin("gds", "c", neo4jv1beta1.PhaseReady, "")
		p.Spec.Name, p.Spec.Config, p.Spec.InstallMode = "gds", nil, mode
		p.Spec.Dependencies = []neo4jv1beta1.PluginDependency{{Name: "apoc"}}
		return p
	}
	apoc := func() *neo4jv1beta1.Neo4jPlugin {
		p := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "")
		p.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"apoc.load.*"}}
		return p
	}
	for _, tc := range []struct {
		name       string
		plugin     *neo4jv1beta1.Neo4jPlugin
		others     []client.Object
		cluster    func(*neo4jv1beta1.Neo4jEnterpriseCluster)
		sts        func(*appsv1.StatefulSet)
		plugins    string
		procedures string
	}{
		{"its alias and its dependency, alone", gds(""), nil, nil, nil, `["fleet-management"]`, ""},
		{"a dependency another plugin installs, and its tokens", gds(""), []client.Object{apoc()}, nil, nil,
			`["fleet-management","apoc"]`, "apoc.load.*"},
		{"fleet-management while the cluster installs it", func() *neo4jv1beta1.Neo4jPlugin {
			p := gds("")
			p.Spec.Name = "fleet-management"
			return p
		}(), nil, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
		}, nil, `["fleet-management","graph-data-science"]`, ""}, // no plugin of the target puts gds.*,apoc.load.* there
		{"NEO4J_PLUGINS and a list the cluster renders", gds(""), nil, nil, func(s *appsv1.StatefulSet) {
			s.Annotations = map[string]string{ownedEnvVarsAnnotation: "NEO4J_PLUGINS," + unrestricted}
		}, `["fleet-management","graph-data-science","apoc"]`, "gds.*,apoc.load.*"},
		{"PreBaked", gds(PluginInstallModePreBaked), nil, nil, nil, `["fleet-management","graph-data-science","apoc"]`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := readyCluster()
			if tc.cluster != nil {
				tc.cluster(cluster)
			}
			sts := selfHealSTS(
				corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["fleet-management","graph-data-science","apoc"]`},
				corev1.EnvVar{Name: unrestricted, Value: "gds.*,apoc.load.*"},
			)
			if tc.sts != nil {
				tc.sts(sts)
			}
			r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{tc.plugin, cluster, sts}, tc.others...)...)
			require.NoError(t, r.Delete(context.Background(), tc.plugin))

			reconcilePlugin(t, r, "gds")

			assert.LessOrEqual(t, *writes, 1)
			env := &corev1.Container{Env: liveSTSEnv(t, r.Client, "c-server")}
			v, _ := findEnv(env, "NEO4J_PLUGINS")
			assert.Equal(t, tc.plugins, v)
			v, _ = findEnv(env, unrestricted)
			assert.Equal(t, tc.procedures, v)
		})
	}
}

// A VerifiedDownload plugin's uninstall takes its settings off in the same
// write as its init container and volumes: one restart, where there were two.
func TestUninstall_VerifiedDownloadIsOneWrite(t *testing.T) {
	plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	r, _, writes, _ := gateReconciler(t, nil, plugin, readyCluster(), selfHealSTS(corev1.EnvVar{Name: "NEO4J_AUTH", Value: "neo4j/x"}))
	target := &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: readyCluster()}
	require.NoError(t, r.installPluginViaEnvironment(context.Background(), plugin, target))
	require.Equal(t, 1, *writes)
	require.Equal(t, "NEO4J_custom_setting", recordOf(t, r.Client, "c-server", "custom"))
	require.NoError(t, r.Delete(context.Background(), plugin))

	reconcilePlugin(t, r, "custom")

	assert.Equal(t, 2, *writes, "one write")
	live := liveSTS(t, r.Client, "c-server")
	assert.Equal(t, []string{"NEO4J_AUTH"}, envNames(neo4jContainerEnv(live.Spec.Template.Spec)))
	assert.Empty(t, initContainerNames(live))
	assert.Empty(t, templateVolumeNames(live))
	assert.NotContains(t, live.Spec.Template.Annotations, PluginInitContainersAnnotation)
	assert.NotContains(t, live.Annotations, pluginEnvVarsAnnotation("custom"))
}

// Against an API server, with the cluster controller's own apply of its server
// StatefulSet between the plugin's passes: a key removed from a plugin's
// spec.config goes from the servers in one roll and the cluster's next apply
// does not bring it back; deleting the plugins takes their settings,
// NEO4J_PLUGINS names, init container and volumes away; and the cluster's own
// env is untouched throughout.
func TestPluginEnvFollowsItsConfig_APIServer(t *testing.T) {
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
	cluster.Spec.Env = []corev1.EnvVar{{Name: "NEO4J_server_memory_pagecache_size", Value: "256m"}}
	require.NoError(t, c.Create(ctx, cluster))
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		latest.Status.Phase, latest.Status.ObservedGeneration = neo4jv1beta1.PhaseReady, latest.Generation
		return c.Status().Update(ctx, latest)
	}))
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
	rolledOut := func() {
		t.Helper()
		sts := live()
		sts.Status = rolledOutStatus(*sts.Spec.Replicas, sts.Generation)
		require.NoError(t, c.Status().Update(ctx, sts))
	}
	clusterWrites()
	rolledOut()
	for i := 0; i < 2; i++ {
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("c-server-%d", i), Namespace: ns,
				Labels: map[string]string{"app.kubernetes.io/name": "neo4j", "app.kubernetes.io/instance": "c"}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: "neo4j:5.26.0-enterprise"}}},
		}
		require.NoError(t, c.Create(ctx, p))
		p.Status = gatePod("", nil).Status
		require.NoError(t, c.Status().Update(ctx, p))
	}
	clusterEnv := func() map[string]corev1.EnvVar {
		t.Helper()
		sts := live()
		out := map[string]corev1.EnvVar{}
		env := envByName(neo4jContainerEnv(sts.Spec.Template.Spec))
		for name := range readOwnedEnvVarNames(sts) {
			out[name] = env[name]
		}
		return out
	}
	ownEnv := clusterEnv()
	require.Contains(t, ownEnv, "NEO4J_server_memory_pagecache_size")

	rec := record.NewFakeRecorder(64)
	plugins := &Neo4jPluginReconciler{Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Second,
		bolt: &pluginBolt{
			functional: func(context.Context, *DeploymentInfo) bool { return true },
		}}
	genai := genaiPlugin("c", "")
	genai.Namespace, genai.Generation, genai.Finalizers, genai.Status = ns, 0, nil, neo4jv1beta1.Neo4jPluginStatus{}
	custom := verifiedDownloadPlugin()
	custom.Namespace, custom.Generation = ns, 0
	custom.Spec.Config = map[string]string{"custom.setting": "on"}
	pass := func(p *neo4jv1beta1.Neo4jPlugin) {
		t.Helper()
		_, err := plugins.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(p)})
		require.NoError(t, err)
	}
	phase := func(p *neo4jv1beta1.Neo4jPlugin) string {
		t.Helper()
		got := &neo4jv1beta1.Neo4jPlugin{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(p), got))
		return got.Status.Phase
	}
	install := func(p *neo4jv1beta1.Neo4jPlugin) {
		t.Helper()
		require.NoError(t, c.Create(ctx, p))
		pass(p) // the finalizer
		pass(p) // the write
		rolledOut()
		pass(p)
		require.Equal(t, neo4jv1beta1.PhaseReady, phase(p))
	}
	install(genai)
	install(custom)
	require.Subset(t, envNames(neo4jContainerEnv(live().Spec.Template.Spec)), []string{baseURLVar, vectorVar, "NEO4J_custom_setting"})
	require.Contains(t, initContainerNames(live()), "plugin-init-custom")

	t.Run("a removed key goes in one roll, and the cluster does not bring it back", func(t *testing.T) {
		before := live().Generation
		require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
			p := &neo4jv1beta1.Neo4jPlugin{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(genai), p); err != nil {
				return err
			}
			return patchObject(ctx, c, p, func() { delete(p.Spec.Config, "genai.vector.enabled") })
		}))
		pass(genai)
		assert.Equal(t, before+1, live().Generation, "one write")
		assert.NotContains(t, envNames(neo4jContainerEnv(live().Spec.Template.Spec)), vectorVar)
		rolledOut()
		pass(genai)
		assert.Equal(t, neo4jv1beta1.PhaseReady, phase(genai))

		removed := live().Generation
		clusterWrites()
		pass(genai)
		pass(custom)
		assert.Equal(t, removed, live().Generation, "no pass of either controller changes the template again")
		assert.NotContains(t, envNames(neo4jContainerEnv(live().Spec.Template.Spec)), vectorVar)
		assert.Equal(t, ownEnv, clusterEnv())
	})

	t.Run("a change of the cluster's own template keeps the plugins' variables", func(t *testing.T) {
		// Found walking this fix on Kind: the cluster's apply of a template
		// change recorded the plugins' variables as its own, and its next pass
		// removed them.
		require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
				return err
			}
			return patchObject(ctx, c, latest, func() {
				latest.Spec.Env = append(latest.Spec.Env, corev1.EnvVar{Name: "WALK_OWNED_BY_CLUSTER", Value: "kept"})
			})
		}))
		before := live().Generation
		for i := 0; i < 3; i++ {
			clusterWrites()
		}
		sts := live()
		assert.Equal(t, before+1, sts.Generation, "one write: the cluster's change")
		names := envNames(neo4jContainerEnv(sts.Spec.Template.Spec))
		assert.Subset(t, names, []string{"WALK_OWNED_BY_CLUSTER", "NEO4J_PLUGINS", baseURLVar, "NEO4J_custom_setting"})
		assert.NotContains(t, readOwnedEnvVarNames(sts), "NEO4J_PLUGINS")
		assert.NotContains(t, readOwnedEnvVarNames(sts), baseURLVar)
		ownEnv = clusterEnv()
	})

	t.Run("deleting the plugins takes away what they wrote, and nothing else", func(t *testing.T) {
		require.NoError(t, c.Delete(ctx, genai))
		pass(genai)
		require.NoError(t, c.Delete(ctx, custom))
		pass(custom)

		sts := live()
		names := envNames(neo4jContainerEnv(sts.Spec.Template.Spec))
		assert.NotContains(t, names, baseURLVar)
		assert.NotContains(t, names, "NEO4J_custom_setting")
		v, _ := findEnv(neo4jContainer(&sts.Spec.Template.Spec), "NEO4J_PLUGINS")
		assert.Equal(t, "[]", v, "genai's name is gone")
		assert.NotContains(t, initContainerNames(sts), "plugin-init-custom")
		assert.NotContains(t, templateVolumeNames(sts), "plugin-auth-custom")
		assert.NotContains(t, templateVolumeNames(sts), resources.PluginCAVolumeName)
		for key := range sts.Annotations {
			assert.False(t, strings.HasPrefix(key, pluginEnvVarsAnnotationPrefix), "no record is left: %s", key)
		}
		assert.Equal(t, ownEnv, clusterEnv(), "the cluster's own env is untouched")
		for _, p := range []*neo4jv1beta1.Neo4jPlugin{genai, custom} {
			err := c.Get(ctx, client.ObjectKeyFromObject(p), &neo4jv1beta1.Neo4jPlugin{})
			assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "%s is gone: %v", p.Name, err)
		}
	})
}
