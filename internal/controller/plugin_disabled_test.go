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

// A Neo4jPlugin with spec.enabled: false is kept off its target's servers, and
// the CR stays (operations.md id 160; #540).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// disabledFixturePlugin is a Managed APOC plugin of target ref with a setting
// and an allowlist.
func disabledFixturePlugin(ref, phase string) *neo4jv1beta1.Neo4jPlugin {
	plugin := selfHealPlugin("apoc", ref, phase, "")
	plugin.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"apoc.*"}}
	return plugin
}

// The defect (#540): the plugin controller never read spec.enabled, so a
// plugin set to enabled: false was installed — and stayed installed — all the
// same. Disabling an installed plugin now takes off its target everything it
// wrote, in one write, the CR stays, Disabled once the servers run without it;
// enabling it again installs it.
func TestReconcile_DisablingAPluginTakesItOffItsTarget(t *testing.T) {
	for _, tg := range envTargets() {
		t.Run(tg.name, func(t *testing.T) {
			plugin := disabledFixturePlugin(tg.ref(), "")
			r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin}, tg.objects()...)...)
			installAndRoll(t, r, "apoc", tg.sts)
			require.Equal(t, 1, *writes)
			drainEvents(rec)

			editPlugin(t, r.Client, "apoc", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Enabled = false })
			res := reconcilePlugin(t, r, "apoc")

			assert.Equal(t, 2, *writes, "one write")
			live := liveSTS(t, r.Client, tg.sts)
			env := neo4jContainerEnv(live.Spec.Template.Spec)
			v, _ := findEnv(&corev1.Container{Env: env}, "NEO4J_PLUGINS")
			assert.Equal(t, `[]`, v, "the entrypoint installs nothing")
			assert.NotContains(t, envNames(env), "NEO4J_apoc_export_file_enabled")
			assert.NotContains(t, envNames(env), allowlistVar)
			assert.NotContains(t, live.Annotations, pluginEnvVarsAnnotation("apoc"))
			assert.NotContains(t, live.Annotations, pluginEntriesAnnotation("apoc"))
			got := getPlugin(t, r, "apoc")
			assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase, "not Disabled while the servers still run it")
			assert.Positive(t, res.RequeueAfter)

			// The servers run without it: Disabled, said once.
			rollOut(t, r.Client, tg.sts)
			res = reconcilePlugin(t, r, "apoc")
			got = getPlugin(t, r, "apoc")
			assert.Equal(t, neo4jv1beta1.PhaseDisabled, got.Status.Phase, got.Status.Message)
			assert.Zero(t, res.RequeueAfter)
			ready := findCondition(got.Status.Conditions, ConditionTypeReady)
			require.NotNil(t, ready)
			assert.Equal(t, metav1.ConditionFalse, ready.Status)
			assert.Equal(t, ConditionReasonDisabled, ready.Reason)
			assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginDisabled), 1)

			rv := got.ResourceVersion
			reconcilePlugin(t, r, "apoc")
			assert.Equal(t, rv, getPlugin(t, r, "apoc").ResourceVersion, "a pass with nothing to do writes nothing")
			assert.Equal(t, 2, *writes)
			assert.Empty(t, drainEvents(rec))

			// Enabled again: installed again.
			editPlugin(t, r.Client, "apoc", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Enabled = true })
			installAndRoll(t, r, "apoc", tg.sts)
			assert.Equal(t, 3, *writes)
			v, _ = envValueOf(t, r, tg.sts, "NEO4J_PLUGINS")
			assert.Equal(t, `["apoc"]`, v)
			assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstalled), 1)
		})
	}
}

// A plugin created disabled writes nothing and is Disabled at once.
func TestReconcile_APluginCreatedDisabledIsNeverInstalled(t *testing.T) {
	plugin := disabledFixturePlugin("c", "")
	plugin.Spec.Enabled = false
	r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), append([]client.Object{plugin, readyCluster(), selfHealSTS()}, readyClusterPods()...)...)

	res := reconcilePlugin(t, r, "apoc")

	assert.Zero(t, *writes)
	assert.Zero(t, res.RequeueAfter)
	assert.Equal(t, neo4jv1beta1.PhaseDisabled, getPlugin(t, r, "apoc").Status.Phase)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginDisabled), 1)
	assert.Empty(t, neo4jContainerEnv(liveSTS(t, r.Client, "c-server").Spec.Template.Spec))
}

// Removing a disabled plugin restarts every server, like any write of the
// plugin, so it waits for a fully available target (operations.md id 141)...
func TestReconcile_ARemovalOfADisabledPluginWaitsForTheTarget(t *testing.T) {
	plugin := disabledFixturePlugin("c", neo4jv1beta1.PhaseReady)
	plugin.Spec.Enabled, plugin.Generation = false, 2
	installed := installedSelfHealSTS()
	r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), append([]client.Object{plugin, degradedCluster(neo4jv1beta1.PhaseReady), installed}, readyClusterPods()...)...)

	res := reconcilePlugin(t, r, "apoc")

	assert.Zero(t, *writes)
	assert.Positive(t, res.RequeueAfter)
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseWaiting, got.Status.Phase)
	assert.Equal(t, "Waiting until Neo4jEnterpriseCluster c is Ready with every server available: "+
		"removing the disabled plugin restarts every server", got.Status.Message)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallDeferred), 1)
}

// ...except when what it removes is what keeps the servers down: the GenAI
// example's genai.vector.enabled crash-loops server 2, and disabling the
// plugin — the key still in its spec — takes the setting away at once (ids
// 152, 155): the repair exception judges the removal, not the install.
func TestReconcile_DisablingThePluginThatKeepsServersDownDoesNotWait(t *testing.T) {
	plugin, cluster, sts, pods := settingsDeadlock()
	plugin.Spec.Config["genai.vector.enabled"] = "true"
	plugin.Spec.Enabled, plugin.Generation = false, plugin.Generation+1
	objs := []client.Object{plugin, cluster, sts}
	for _, p := range pods {
		objs = append(objs, p)
	}
	r, rec, writes, messages := gateReconciler(t, boltMustNotRun(t), objs...)

	reconcilePlugin(t, r, "genai")

	assert.Equal(t, 1, *writes, "written although the cluster is not Ready")
	assert.NotContains(t, envNames(liveSTSEnv(t, r.Client, "c-server")), vectorVar)
	require.NotEmpty(t, *messages)
	assert.Contains(t, (*messages)[0], "Removing the disabled plugin from Neo4jEnterpriseCluster c although it is not Ready")
	got := getPlugin(t, r, "genai")
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase)
	assert.Equal(t, "Removing the plugin (spec.enabled: false): waiting for the servers to restart without it; "+
		"cannot start: c-server-2 (container neo4j exited with code 1)", got.Status.Message)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginSettingsFailing), 1)
}

// A disabled plugin needs nothing on its target: another plugin's write does
// not keep its procedure tokens or its NEO4J_PLUGINS names for it.
func TestOtherPlugins_LeavesOutDisabledPlugins(t *testing.T) {
	apoc := disabledFixturePlugin("c", neo4jv1beta1.PhaseReady)
	apoc.Spec.Dependencies = []neo4jv1beta1.PluginDependency{{Name: "n10s"}}
	genai := genaiPlugin("c", neo4jv1beta1.PhaseDisabled)
	genai.Spec.Enabled, genai.Spec.Config = false, nil
	genai.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"genai.*"}}
	gds := selfHealPlugin("gds", "c", neo4jv1beta1.PhaseReady, "")
	gds.Spec.Name, gds.Spec.Config = "graph-data-science", nil
	sts := selfHealSTS(
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc","n10s","genai","graph-data-science"]`},
		corev1.EnvVar{Name: allowlistVar, Value: "apoc.*,genai.*"},
	)
	r, _, _, _ := gateReconciler(t, nil, apoc, genai, gds, readyCluster(), sts)
	target := &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: readyCluster()}

	others, err := r.otherPlugins(context.Background(), apoc, target)
	require.NoError(t, err)
	require.Len(t, others, 1)
	assert.Equal(t, "gds", others[0].Name)

	// The uninstall of APOC leaves no token of the disabled GenAI plugin in the
	// allowlist, and GDS's name in NEO4J_PLUGINS.
	edited := liveSTS(t, r.Client, "c-server")
	r.withoutPlugin(context.Background(), edited, apoc, target, others)
	env := &corev1.Container{Env: neo4jContainerEnv(edited.Spec.Template.Spec)}
	_, ok := findEnv(env, allowlistVar)
	assert.False(t, ok, "no enabled plugin of the target puts anything there")
	v, _ := findEnv(env, "NEO4J_PLUGINS")
	assert.Equal(t, `["genai","graph-data-science"]`, v, "the disabled plugin's own pass takes genai away")
}
