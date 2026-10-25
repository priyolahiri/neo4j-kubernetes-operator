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

// A plugin's procedure lists add to the cluster's own; they never take their
// place (operations.md id 179; #564). The Neo4j entrypoint writes each
// NEO4J_<setting> variable into neo4j.conf in place of the line there, so the
// moment one plugin set an allowlist, the cluster's own spec.config list and
// the fleetManagement.* entry of spec.auraFleetManagement were gone from the
// servers. These tests start the servers as the pod does — the cluster's
// rendered neo4j.conf, its startup script's list block, then the entrypoint's
// replacement — and read the lists the server gets.

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// listSettings are the shared lists, as neo4j.conf names them.
var listSettings = []string{
	"dbms.security.http_auth_allowlist",
	"dbms.security.procedures.allowlist",
	"dbms.security.procedures.denylist",
	"dbms.security.procedures.unrestricted",
	"server.unmanaged_extension_classes",
}

// pluginListMergeSection returns the startup script's restart-neutral section
// that holds the list block; "" when the script has none.
func pluginListMergeSection(script string) string {
	var section []string
	in := false
	for _, line := range strings.Split(script, "\n") {
		switch strings.TrimSpace(line) {
		case resources.RestartNeutralBegin:
			in, section = true, nil
			continue
		case resources.RestartNeutralEnd:
			if in && strings.Contains(strings.Join(section, "\n"), resources.PluginListMergeMarker) {
				return strings.Join(section, "\n")
			}
			in = false
			continue
		}
		if in {
			section = append(section, line)
		}
	}
	return ""
}

// server52632 is a 5.26.32 server's image: its entrypoint installs
// fleet-management from products/ and applies the plugin's defaults.
var server52632 = entrypointsim.Image{FleetJAR: true}

// serverLists returns the procedure and extension lists a server of the
// cluster starts with, by setting, given the neo4j container's env: the
// ConfigMap's neo4j.conf, the startup script's list block run over it, then
// the image's entrypoint — the plugins' defaults of install_neo4j_plugins
// (apply_plugin_default_configuration), then its environment pass
// (add_env_setting_to_conf), which writes each NEO4J_<setting> variable in
// place of the conf's line (entrypointsim, the entrypoint's own functions).
func serverLists(t *testing.T, cm *corev1.ConfigMap, env []corev1.EnvVar) map[string]string {
	t.Helper()
	vars := map[string]string{}
	for _, e := range env {
		if e.ValueFrom == nil && (resources.IsAdditiveConfEnvName(e.Name) || e.Name == "NEO4J_PLUGINS") {
			vars[e.Name] = e.Value
		}
	}
	started := entrypointsim.Start(t, server52632, cm.Data["neo4j.conf"], pluginListMergeSection(cm.Data["startup.sh"]), vars)
	lists := map[string]string{}
	for _, key := range listSettings {
		if value, lines := entrypointsim.Setting(started, key); lines > 0 {
			require.Equal(t, 1, lines, "%s is set once\n%s", key, started)
			lists[key] = value
		}
	}
	return lists
}

// tokensOf is a list's tokens, sorted.
func tokensOf(list string) []string {
	out := csvTokens(list)
	sort.Strings(out)
	return out
}

// fleetSTS is cluster "c"'s server StatefulSet with fleet-management in
// NEO4J_PLUGINS, as the cluster controller writes it for Aura Fleet
// Management.
func fleetSTS() *appsv1.StatefulSet {
	return selfHealSTS(corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["fleet-management"]`})
}

// listCluster is cluster "c", Ready, with config in its spec.config, and its
// rendered ConfigMap.
func listCluster(config map[string]string, fleet bool) (*neo4jv1beta1.Neo4jEnterpriseCluster, *corev1.ConfigMap) {
	cluster := readyCluster()
	cluster.Spec.Image.Tag = "5.26.32-enterprise" // serverLists starts a 5.26.32 server
	cluster.Spec.Config = config
	if fleet {
		cluster.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
	}
	return cluster, resources.BuildConfigMapForEnterprise(cluster)
}

// allowingPlugin is a Neo4jPlugin of cluster "c", not installed yet, whose
// spec.security allows procs.
func allowingPlugin(name, plugin string, procs ...string) *neo4jv1beta1.Neo4jPlugin {
	p := selfHealPlugin(name, "c", "", "")
	p.Spec.Name, p.Spec.Config = plugin, nil
	p.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: procs}
	return p
}

// The defect (#564, from the code; reported with the #553 fix): a cluster's
// own allowlist in spec.config was lost from its servers as soon as a plugin
// set one — here GenAI's genai.*, ai.* — and with it APOC, which the
// cluster's list let in: the APOC plugin said its procedures were hidden.
func TestServers_AddAPluginsAllowlistToTheClustersOwn(t *testing.T) {
	cluster, cm := listCluster(map[string]string{"dbms.security.procedures.allowlist": "apoc.coll.*"}, false)
	apoc := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	sts := installedSelfHealSTS()
	sts.Annotations = map[string]string{
		pluginEnvVarsAnnotation("apoc"): "NEO4J_apoc_export_file_enabled", pluginEntriesAnnotation("apoc"): "apoc",
	}
	genai := allowingPlugin("genai-plugin", "genai", "genai.*", "ai.*")
	r, rec, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, genai, cluster, cm, sts}, readyClusterPods()...)...)

	installAndRoll(t, r, "genai-plugin", "c-server")

	lists := serverLists(t, cm, liveSTSEnv(t, r.Client, "c-server"))
	assert.Equal(t, []string{"ai.*", "apoc.coll.*", "genai.*"}, tokensOf(lists["dbms.security.procedures.allowlist"]),
		"the cluster's own entries and the plugin's, together")
	assert.Equal(t, []string{"ai.*", "apoc.*", "genai.*"}, tokensOf(lists["dbms.security.procedures.unrestricted"]),
		"the plugin's list, and the image's apoc.* for the APOC that NEO4J_PLUGINS installs (id 193)")

	drainEvents(rec)
	reconcilePlugin(t, r, "apoc")
	got := getPlugin(t, r, "apoc")
	assert.Nil(t, findCondition(got.Status.Conditions, ConditionTypeProceduresHidden),
		"apoc.coll.* in the cluster's list loads part of APOC")
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden))
}

// Aura Fleet Management puts fleetManagement.* in the cluster's allowlist and
// unrestricted list. A plugin that restricts procedures set its own lists in
// their place, and fleet management stopped working without a word.
func TestServers_KeepFleetManagementBesideARestrictingPlugin(t *testing.T) {
	cluster, cm := listCluster(nil, true)
	apoc := allowingPlugin("apoc", "apoc", "apoc.*")
	// The cluster controller lists fleet-management in NEO4J_PLUGINS.
	r, _, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, cluster, cm, fleetSTS()}, readyClusterPods()...)...)

	installAndRoll(t, r, "apoc", "c-server")

	lists := serverLists(t, cm, liveSTSEnv(t, r.Client, "c-server"))
	for _, key := range []string{"dbms.security.procedures.allowlist", "dbms.security.procedures.unrestricted"} {
		assert.Equal(t, []string{"apoc.*", "fleetManagement.*"}, tokensOf(lists[key]), key)
	}
	v, _ := envValueOf(t, r, "c-server", allowlistVar)
	assert.Equal(t, "apoc.*", v, "the plugin writes its own tokens; the servers add the cluster's")
}

// A later change to the cluster's own list reaches the servers: the
// ConfigMap's change restarts them, once, and they start with the new list and
// the plugin's — the plugin writes nothing for it.
func TestServers_GetALaterChangeToTheClustersOwnList(t *testing.T) {
	cluster, cm := listCluster(map[string]string{"dbms.security.procedures.allowlist": "apoc.coll.*"}, false)
	genai := allowingPlugin("genai-plugin", "genai", "genai.*", "ai.*")
	r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{genai, cluster, cm, selfHealSTS()}, readyClusterPods()...)...)
	installAndRoll(t, r, "genai-plugin", "c-server")
	before := *writes

	cluster.Spec.Config["dbms.security.procedures.allowlist"] = "apoc.path.*"
	manager := NewConfigMapManager(r.Client)
	manager.LiveConfig = nil
	require.NoError(t, manager.ReconcileConfigMap(context.Background(), cluster))

	sts := liveSTS(t, r.Client, "c-server")
	assert.NotEmpty(t, sts.Spec.Template.Annotations["neo4j.neo4j.com/config-restart"], "the servers restart for the new list")
	assert.Equal(t, before+1, *writes, "one write: the restart")
	rollOut(t, r.Client, "c-server")
	reconcilePlugin(t, r, "genai-plugin")
	assert.Equal(t, before+1, *writes, "the plugin writes nothing for the cluster's own list")

	updated := &corev1.ConfigMap{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: "c-config"}, updated))
	lists := serverLists(t, updated, liveSTSEnv(t, r.Client, "c-server"))
	assert.Equal(t, []string{"ai.*", "apoc.path.*", "genai.*"}, tokensOf(lists["dbms.security.procedures.allowlist"]))
}

// Deleting a plugin takes its tokens away and leaves the cluster's own list,
// beside what the other plugins of the cluster set.
func TestUninstall_LeavesTheClustersOwnListOnTheServers(t *testing.T) {
	cluster, cm := listCluster(map[string]string{"dbms.security.procedures.allowlist": "custom.procs.*"}, true)
	genai := allowingPlugin("genai-plugin", "genai", "genai.*", "ai.*")
	apoc := allowingPlugin("apoc", "apoc", "apoc.*")
	r, _, _, _ := gateReconciler(t, nil, append([]client.Object{genai, apoc, cluster, cm, fleetSTS()}, readyClusterPods()...)...)
	installAndRoll(t, r, "genai-plugin", "c-server")
	installAndRoll(t, r, "apoc", "c-server")

	require.NoError(t, r.Delete(context.Background(), getPlugin(t, r, "genai-plugin")))
	reconcilePlugin(t, r, "genai-plugin")

	lists := serverLists(t, cm, liveSTSEnv(t, r.Client, "c-server"))
	assert.Equal(t, []string{"apoc.*", "custom.procs.*", "fleetManagement.*"}, tokensOf(lists["dbms.security.procedures.allowlist"]))
	assert.Equal(t, []string{"apoc.*", "fleetManagement.*"}, tokensOf(lists["dbms.security.procedures.unrestricted"]))
}

// withoutPluginListMerge renders a startup script as the operator before the
// list block did: without the block, from its marker to its last
// operator_merge_list call, and the restart-neutral markers around it.
func withoutPluginListMerge(script string) string {
	lines := strings.Split(script, "\n")
	start := -1
	for i, line := range lines {
		if line == resources.PluginListMergeMarker {
			start = i
			break
		}
	}
	if start < 0 {
		return script
	}
	end := start
	for i := start; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "operator_merge_list ") {
			end = i
		}
	}
	if start > 0 && lines[start-1] == resources.RestartNeutralBegin {
		start--
	}
	if end+1 < len(lines) && lines[end+1] == resources.RestartNeutralEnd {
		end++
	}
	return strings.Join(append(lines[:start:start], lines[end+1:]...), "\n")
}

// An operator upgrade that adds the list block restarts no server: the block
// changes nothing for a running server. It is written to the ConfigMap, for
// each server's next start, and a settled plugin writes nothing either.
func TestOperatorUpgrade_TheListBlockRestartsNoServer(t *testing.T) {
	for name, cluster := range map[string]func() (*neo4jv1beta1.Neo4jEnterpriseCluster, *corev1.ConfigMap){
		"no list of its own": func() (*neo4jv1beta1.Neo4jEnterpriseCluster, *corev1.ConfigMap) { return listCluster(nil, false) },
		"a list in spec.config": func() (*neo4jv1beta1.Neo4jEnterpriseCluster, *corev1.ConfigMap) {
			return listCluster(map[string]string{"dbms.security.procedures.allowlist": "apoc.*"}, false)
		},
		"Aura Fleet Management": func() (*neo4jv1beta1.Neo4jEnterpriseCluster, *corev1.ConfigMap) { return listCluster(nil, true) },
		"a CalVer cluster's own": func() (*neo4jv1beta1.Neo4jEnterpriseCluster, *corev1.ConfigMap) {
			c, _ := listCluster(map[string]string{"dbms.security.procedures.unrestricted": "x.*"}, true)
			c.Spec.Image.Tag = "2026.08.1-enterprise"
			return c, resources.BuildConfigMapForEnterprise(c)
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, desired := cluster()
			require.Contains(t, desired.Data["startup.sh"], resources.PluginListMergeMarker, "the rendered script has the block")
			existing := desired.DeepCopy()
			existing.Data["startup.sh"] = withoutPluginListMerge(desired.Data["startup.sh"])
			require.NotEqual(t, desired.Data["startup.sh"], existing.Data["startup.sh"])
			apoc := allowingPlugin("apoc", "apoc", "apoc.*")
			apoc.Status.Phase, apoc.Status.Message = neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully"
			sts := selfHealSTS(
				corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
				corev1.EnvVar{Name: allowlistVar, Value: "apoc.*"},
				corev1.EnvVar{Name: unrestrictedVar, Value: "apoc.*"},
			)
			sts.Annotations = map[string]string{pluginEntriesAnnotation("apoc"): "apoc"}
			r, _, writes, _ := gateReconciler(t, noBolt(t), apoc, c, existing, sts)

			manager := NewConfigMapManager(r.Client)
			manager.LiveConfig = nil
			require.NoError(t, manager.ReconcileConfigMap(context.Background(), c))
			reconcilePlugin(t, r, "apoc")

			assert.Zero(t, *writes, "no StatefulSet write, so no restart")
			assert.Empty(t, liveSTS(t, r.Client, "c-server").Spec.Template.Annotations["neo4j.neo4j.com/config-restart"])
			written := &corev1.ConfigMap{}
			require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: "c-config"}, written))
			assert.Equal(t, desired.Data["startup.sh"], written.Data["startup.sh"], "written for the next start")
		})
	}
}

// A standalone is not affected: its plugins' lists join its own in its
// neo4j.conf (TestReconcileConfigMap_UserAndPluginAdditiveUnioned), and a
// plugin's write puts no list variable on its StatefulSet.
func TestStandalone_APluginPutsNoListVariableOnTheServer(t *testing.T) {
	apoc := allowingPlugin("apoc", "apoc", "apoc.*")
	apoc.Spec.ClusterRef = "sa"
	r, _, _, _ := gateReconciler(t, nil, apoc, readyStandalone(), standaloneSTS(), gatePod("sa-0", map[string]string{"app": "sa"}))

	installAndRoll(t, r, "apoc", "sa")

	for _, e := range liveSTSEnv(t, r.Client, "sa") {
		assert.False(t, resources.IsAdditiveConfEnvName(e.Name), "%s on a standalone", e.Name)
	}
}
