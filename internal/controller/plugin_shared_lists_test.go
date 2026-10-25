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

// What several Neo4jPlugins share on one target: a cluster's procedure lists
// are what the target's plugins put there now, and NEO4J_PLUGINS keeps only
// the names a Managed plugin still adds (#548; operations.md id 155).

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const (
	allowlistVar    = "NEO4J_dbms_security_procedures_allowlist"
	unrestrictedVar = "NEO4J_dbms_security_procedures_unrestricted"
)

// installAndRoll runs the plugin's install pass and the roll it causes, so the
// plugin is Ready; it fails the test otherwise.
func installAndRoll(t *testing.T, r *Neo4jPluginReconciler, plugin, sts string) {
	t.Helper()
	reconcilePlugin(t, r, plugin)
	rollOut(t, r.Client, sts)
	reconcilePlugin(t, r, plugin)
	require.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, plugin).Status.Phase, getPlugin(t, r, plugin).Status.Message)
}

func envValueOf(t *testing.T, r *Neo4jPluginReconciler, sts, name string) (string, bool) {
	t.Helper()
	return findEnv(&corev1.Container{Env: liveSTSEnv(t, r.Client, sts)}, name)
}

// The defects (#548, from the code): a plugin added its tokens to a cluster's
// shared procedure lists and took them out only on its uninstall, so tokens
// stayed when its spec.security changed — here an APOC allowlist narrowed to
// apoc.coll.* went on loading all of APOC. And a list key in a plugin's
// spec.config replaced the list outright, dropping what the other plugins of
// the cluster had put there (GDS's gds.*). Each list is now what the target's
// plugins put there, written with the plugin's write.
func TestReconcile_ClusterProcedureListsFollowThePlugins(t *testing.T) {
	t.Run("a narrowed allowlist", func(t *testing.T) {
		apoc := selfHealPlugin("apoc", "c", "", "")
		apoc.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"apoc.*"}}
		r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{apoc, readyCluster(), selfHealSTS()}, readyClusterPods()...)...)
		installAndRoll(t, r, "apoc", "c-server")
		v, _ := envValueOf(t, r, "c-server", allowlistVar)
		require.Equal(t, "apoc.*", v)

		editPlugin(t, r.Client, "apoc", func(p *neo4jv1beta1.Neo4jPlugin) {
			p.Spec.Security.AllowedProcedures = []string{"apoc.coll.*"}
		})
		reconcilePlugin(t, r, "apoc")

		assert.Equal(t, 2, *writes, "one write")
		v, _ = envValueOf(t, r, "c-server", allowlistVar)
		assert.Equal(t, "apoc.coll.*", v, "the old token goes")
		v, _ = envValueOf(t, r, "c-server", unrestrictedVar)
		assert.Equal(t, "apoc.coll.*", v)
	})

	t.Run("a list in spec.config", func(t *testing.T) {
		gds := selfHealPlugin("gds", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
		gds.Spec.Name, gds.Spec.Config = "graph-data-science", nil
		apoc := selfHealPlugin("apoc", "c", "", "")
		apoc.Spec.Config = map[string]string{"dbms.security.procedures.unrestricted": "apoc.*"}
		sts := selfHealSTS(
			corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["graph-data-science"]`},
			corev1.EnvVar{Name: unrestrictedVar, Value: "gds.*,apoc.load.*"},
		)
		r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{gds, apoc, readyCluster(), sts}, readyClusterPods()...)...)

		reconcilePlugin(t, r, "apoc")

		require.Equal(t, 1, *writes)
		v, _ := envValueOf(t, r, "c-server", unrestrictedVar)
		assert.ElementsMatch(t, []string{"gds.*", "apoc.load.*", "apoc.*"}, csvTokens(v), "added to, not replaced")
		v, _ = envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
		assert.Equal(t, `["graph-data-science","apoc"]`, v)
	})

	t.Run("the same tokens in another order are left as they are", func(t *testing.T) {
		gds := selfHealPlugin("gds", "c", "", "")
		gds.Spec.Name, gds.Spec.Config = "graph-data-science", nil
		sts := selfHealSTS(
			corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["graph-data-science"]`},
			corev1.EnvVar{Name: unrestrictedVar, Value: "apoc.load.*,gds.*"},
		)
		r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{gds, readyCluster(), sts}, readyClusterPods()...)...)

		reconcilePlugin(t, r, "gds")

		assert.Zero(t, *writes, "a write would restart the servers for nothing")
	})
}

// A settled plugin does not restart the servers to take away a list token
// nobody puts there any more (an earlier operator's leftover): its next write
// does. A settled pass after an operator upgrade must restart nothing.
func TestReconcile_SettledPluginLeavesAStaleListToItsNextWrite(t *testing.T) {
	apoc := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	sts := installedSelfHealSTS()
	sts.Spec.Template.Spec.Containers[0].Env = append(sts.Spec.Template.Spec.Containers[0].Env,
		corev1.EnvVar{Name: allowlistVar, Value: "apoc.load.*"})
	r, rec, writes, _ := gateReconciler(t, noBolt(t), apoc, readyCluster(), sts)

	reconcilePlugin(t, r, "apoc")

	assert.Zero(t, *writes)
	assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "apoc").Status.Phase)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing))
}

// The defects (#548, from the code): NEO4J_PLUGINS names were not recorded, so
// a dependency dropped from spec.dependencies stayed — the entrypoint went on
// installing it at every start — and so did a plugin's own name once it left
// Managed mode: after a switch to VerifiedDownload the entrypoint installed
// the plugin from NEO4J_PLUGINS beside the verified JAR. The plugin records
// the names it adds and its write takes away those it no longer adds.
func TestReconcile_NEO4JPluginsFollowsThePlugin(t *testing.T) {
	t.Run("a dependency dropped", func(t *testing.T) {
		plugin := selfHealPlugin("apoc", "c", "", "")
		plugin.Spec.Dependencies = []neo4jv1beta1.PluginDependency{{Name: "apoc-extended"}}
		r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin, readyCluster(), selfHealSTS()}, readyClusterPods()...)...)
		installAndRoll(t, r, "apoc", "c-server")
		v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
		require.Equal(t, `["apoc","apoc-extended"]`, v)
		assert.Equal(t, "apoc,apoc-extended", liveSTS(t, r.Client, "c-server").Annotations[pluginEntriesAnnotation("apoc")])
		drainEvents(rec)

		editPlugin(t, r.Client, "apoc", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Dependencies = nil })
		reconcilePlugin(t, r, "apoc")

		assert.Equal(t, 2, *writes, "one write")
		v, _ = envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
		assert.Equal(t, `["apoc"]`, v)
		assert.Equal(t, "apoc", liveSTS(t, r.Client, "c-server").Annotations[pluginEntriesAnnotation("apoc")])
		rollOut(t, r.Client, "c-server")
		reconcilePlugin(t, r, "apoc")
		assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "apoc").Status.Phase)
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing), "a removal is not a loss")
	})

	t.Run("a switch to VerifiedDownload", func(t *testing.T) {
		plugin := verifiedDownloadSelfHeal("", "")
		plugin.Spec.InstallMode = PluginInstallModeManaged
		r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin, readyCluster(), selfHealSTS()}, readyClusterPods()...)...)
		installAndRoll(t, r, "custom", "c-server")
		v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
		require.Equal(t, `["custom"]`, v)

		editPlugin(t, r.Client, "custom", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.InstallMode = PluginInstallModeVerifiedDownload })
		reconcilePlugin(t, r, "custom")

		assert.Equal(t, 2, *writes, "the init container and the name go in one write")
		live := liveSTS(t, r.Client, "c-server")
		v, _ = findEnv(neo4jContainer(&live.Spec.Template.Spec), "NEO4J_PLUGINS")
		assert.Equal(t, `[]`, v, "the entrypoint does not install it beside the verified JAR")
		assert.Contains(t, initContainerNames(live), "plugin-init-custom")
		assert.NotContains(t, live.Annotations, pluginEntriesAnnotation("custom"))
	})

	t.Run("a name another plugin adds stays", func(t *testing.T) {
		plugin := selfHealPlugin("gds", "c", "", "")
		plugin.Spec.Name, plugin.Spec.Config = "graph-data-science", nil
		plugin.Spec.Dependencies = []neo4jv1beta1.PluginDependency{{Name: "apoc"}}
		apoc := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
		r, _, _, _ := gateReconciler(t, nil, append([]client.Object{plugin, apoc, readyCluster(), selfHealSTS()}, readyClusterPods()...)...)
		installAndRoll(t, r, "gds", "c-server")

		editPlugin(t, r.Client, "gds", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Dependencies = nil })
		reconcilePlugin(t, r, "gds")

		v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
		assert.Equal(t, `["graph-data-science","apoc"]`, v)
	})

	t.Run("the first pass after an upgrade records and removes nothing", func(t *testing.T) {
		plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
		sts := installedSelfHealSTS()
		sts.Spec.Template.Spec.Containers[0].Env[0].Value = `["apoc","apoc-extended"]` // a dependency dropped under that operator
		r, _, writes, _ := gateReconciler(t, noBolt(t), plugin, readyCluster(), sts)

		reconcilePlugin(t, r, "apoc")

		assert.Zero(t, *writes, "a metadata patch, no template write")
		live := liveSTS(t, r.Client, "c-server")
		v, _ := findEnv(neo4jContainer(&live.Spec.Template.Spec), "NEO4J_PLUGINS")
		assert.Equal(t, `["apoc","apoc-extended"]`, v)
		assert.Equal(t, "apoc", live.Annotations[pluginEntriesAnnotation("apoc")])
	})
}

// The uninstall takes away the names the plugin's record lists, also those it
// would no longer add itself — its own name after a switch away from Managed
// mode it has not written yet.
func TestUninstall_TakesAwayTheRecordedNames(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	plugin.Spec.InstallMode = PluginInstallModePreBaked
	sts := installedSelfHealSTS()
	sts.Annotations = map[string]string{pluginEntriesAnnotation("apoc"): "apoc"}
	r, _, _, _ := gateReconciler(t, nil, plugin, readyCluster(), sts)
	require.NoError(t, r.Delete(context.Background(), plugin))

	reconcilePlugin(t, r, "apoc")

	v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
	assert.Equal(t, `[]`, v)
	assert.NotContains(t, liveSTS(t, r.Client, "c-server").Annotations, pluginEntriesAnnotation("apoc"))
}

func csvTokens(value string) []string {
	set := splitCSVSet(value)
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

// The names record's key is a valid annotation key for any CR name, and two
// names never share one, as for the variables' record.
func TestPluginEntriesAnnotation(t *testing.T) {
	long := strings.Repeat("a", 60) + ".example-plugin"
	assert.Equal(t, "neo4j.com/plugin-entries.apoc", pluginEntriesAnnotation("apoc"))
	for _, name := range []string{"apoc", strings.Repeat("b", 48), strings.Repeat("b", 49), long, strings.Repeat("c", 253)} {
		assert.Empty(t, validation.IsQualifiedName(pluginEntriesAnnotation(name)), "%q", name)
	}
	assert.NotEqual(t, pluginEntriesAnnotation(long), pluginEntriesAnnotation(long+"2"))
	assert.NotEqual(t, pluginEntriesAnnotation("apoc"), pluginEnvVarsAnnotation("apoc"))
}
