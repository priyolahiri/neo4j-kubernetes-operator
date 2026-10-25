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

// The image gives each plugin NEO4J_PLUGINS installs an entry in the
// unrestricted list (apoc.*, gds.*, genai.*, …), but only to a neo4j.conf
// without the list, and a NEO4J_dbms_security_procedures_unrestricted variable
// replaces it (operations.md id 193). So APOC lost apoc.* beside Aura Fleet
// Management's line or a GDS plugin, and GenAI genai.* under spec.security.
// The operator adds them back unless the deployment's own spec.config sets the
// list. A standalone's spec.env list variable joins its conf (id 193), and a
// plugin's ProceduresHidden is judged against the conf the servers will start
// with (id 194).

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

const unrestrictedSetting = "dbms.security.procedures.unrestricted"

// gdsPlugin is a Managed GDS Neo4jPlugin of cluster "c": its automatic
// unrestricted list is gds.*,apoc.load.*.
func gdsPlugin() *neo4jv1beta1.Neo4jPlugin {
	p := selfHealPlugin("gds", "c", "", "")
	p.Spec.Name, p.Spec.Config = "graph-data-science", nil
	return p
}

// GDS's list, a variable, used to replace the image's apoc.* for APOC; and a
// cluster's own spec.config list is left as it is.
func TestServers_KeepThePluginsUnrestrictedDefaults(t *testing.T) {
	t.Run("GDS and APOC", func(t *testing.T) {
		cluster, cm := listCluster(nil, false)
		apoc := selfHealPlugin("apoc", "c", "", "")
		r, _, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, gdsPlugin(), cluster, cm, selfHealSTS()}, readyClusterPods()...)...)
		installAndRoll(t, r, "apoc", "c-server")
		installAndRoll(t, r, "gds", "c-server")

		lists := serverLists(t, cm, liveSTSEnv(t, r.Client, "c-server"))
		assert.Equal(t, []string{"apoc.*", "apoc.load.*", "gds.*"}, tokensOf(lists[unrestrictedSetting]))
		v, _ := envValueOf(t, r, "c-server", unrestrictedVar)
		assert.Equal(t, "gds.*,apoc.load.*", v, "the plugin writes its own tokens; the servers add the image's")
	})
	t.Run("the cluster's own spec.config list is left as it is", func(t *testing.T) {
		cluster, cm := listCluster(map[string]string{unrestrictedSetting: "custom.*"}, true)
		apoc := selfHealPlugin("apoc", "c", "", "")
		r, _, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, gdsPlugin(), cluster, cm, fleetSTS()}, readyClusterPods()...)...)
		installAndRoll(t, r, "apoc", "c-server")
		installAndRoll(t, r, "gds", "c-server")

		lists := serverLists(t, cm, liveSTSEnv(t, r.Client, "c-server"))
		assert.Equal(t, []string{"apoc.load.*", "custom.*", "fleetManagement.*", "gds.*"}, tokensOf(lists[unrestrictedSetting]),
			"its list, Aura Fleet Management's and the plugin's: no image entries")
	})
}

// standaloneServer starts the standalone's server from its rendered conf:
// the entrypoint, with NEO4J_PLUGINS as plugins and env.
func standaloneServer(t *testing.T, image entrypointsim.Image, conf string, plugins []string, env map[string]string) string {
	t.Helper()
	vars := map[string]string{}
	for k, v := range env {
		vars[k] = v
	}
	if len(plugins) > 0 {
		raw, err := json.Marshal(plugins)
		require.NoError(t, err)
		vars["NEO4J_PLUGINS"] = string(raw)
	}
	return entrypointsim.Start(t, image, conf, "", vars)
}

// On a standalone the operator writes the list into its neo4j.conf, which
// the entrypoint then leaves alone: the image's entries are in it. Only a
// standalone whose conf sets the list (and whose spec.config does not) gets
// new ones, and restarts once for them; one whose conf does not set it is
// left to the entrypoint.
func TestStandaloneConf_KeepsThePluginsUnrestrictedDefaults(t *testing.T) {
	genai := func() neo4jv1beta1.Neo4jPlugin {
		p := pluginCR("genai-plugin", "genai", "sa", true, nil)
		p.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"genai.vector.*"}}
		return *p
	}
	apoc := func() neo4jv1beta1.Neo4jPlugin { return *pluginCR("apoc", "apoc", "sa", true, nil) }
	gds := func() neo4jv1beta1.Neo4jPlugin { return *pluginCR("gds", "graph-data-science", "sa", true, nil) }
	r5 := entrypointsim.Image{Release: entrypointsim.Release52632, FleetJAR: true}
	for _, tc := range []struct {
		name     string
		image    entrypointsim.Image
		fleet    bool
		config   map[string]string
		plugins  []neo4jv1beta1.Neo4jPlugin
		installs []string // NEO4J_PLUGINS
		want     string   // the server's unrestricted list
		restarts bool     // the conf changes from what the operator rendered before
	}{
		{"Aura Fleet Management and APOC", r5, true, nil, []neo4jv1beta1.Neo4jPlugin{apoc()},
			[]string{"apoc", "fleet-management"}, "fleetManagement.*,apoc.*", true},
		{"GDS and APOC", r5, false, nil, []neo4jv1beta1.Neo4jPlugin{apoc(), gds()},
			[]string{"apoc", "graph-data-science"}, "gds.*,apoc.load.*,apoc.*", true},
		{"GenAI with spec.security, 2026.10", entrypointsim.Image{Release: entrypointsim.Release2026100}, false, nil,
			[]neo4jv1beta1.Neo4jPlugin{genai()}, []string{"genai"}, "genai.vector.*,genai.*,ai.*", true},
		{"GenAI with spec.security, 2026.09", entrypointsim.Image{Release: entrypointsim.Release2026090}, false, nil,
			[]neo4jv1beta1.Neo4jPlugin{genai()}, []string{"genai"}, "genai.vector.*,genai.*", true},
		{"its own spec.config list: left as it is", r5, false, map[string]string{unrestrictedSetting: "custom.*"},
			[]neo4jv1beta1.Neo4jPlugin{apoc(), gds()}, []string{"apoc", "graph-data-science"}, "custom.*,gds.*,apoc.load.*", false},
		{"APOC alone: no list, the entrypoint writes the image's", r5, false, nil, []neo4jv1beta1.Neo4jPlugin{apoc()},
			[]string{"apoc"}, "apoc.*", false},
		{"Aura Fleet Management alone: its entry is the image's", r5, true, nil, nil,
			[]string{"fleet-management"}, "fleetManagement.*", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sa := standaloneForConf(tc.config)
			sa.Spec.Image.Tag = tc.image.Release + "-enterprise"
			if tc.fleet {
				sa.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
			}
			r, c := standaloneCMTestReconciler(t)
			for i := range tc.plugins {
				require.NoError(t, c.Create(context.Background(), &tc.plugins[i]))
			}
			// What the operator rendered before: the conf without the image's entries.
			before := legacyStandaloneConf(r, sa, tc.plugins)
			if _, fleetAllowlist := resources.Neo4jConfSettings(before)[resources.AllowlistSetting]; fleetAllowlist {
				before = r.renderConfigMap(sa, false).Data["neo4j.conf"]
				if union := unionPluginConf(tc.plugins, sa.Name, sa.Spec.Image.Tag, false); len(union) > 0 {
					before = resources.DedupeNeo4jConf(resources.UpsertNeo4jConfSettings(before, union))
				}
			}

			require.NoError(t, r.reconcileConfigMap(context.Background(), sa))
			conf := renderedConf(t, c)

			assert.Equal(t, tc.installs, standaloneNeo4jPlugins(sa, tc.plugins), "the names NEO4J_PLUGINS installs")
			got, lines := entrypointsim.Setting(standaloneServer(t, tc.image, conf, tc.installs, nil), unrestrictedSetting)
			assert.Equal(t, 1, lines)
			assert.Equal(t, tc.want, got)
			beforeUnres := resources.Neo4jConfSettings(before)[unrestrictedSetting]
			assert.Equal(t, tc.restarts, beforeUnres != resources.Neo4jConfSettings(conf)[unrestrictedSetting],
				"the conf's list changes (one restart) only where it set the list without the image's entries\nbefore %q", beforeUnres)
		})
	}
}

// A procedure or extension list set by hand in a standalone's spec.env, as a
// NEO4J_<setting> variable, used to replace the conf's line — its own
// spec.config's and its plugins' entries — as the entrypoint writes such a
// variable; on a cluster the startup script joins them. The standalone's
// neo4j.conf now holds it, joined with the others, and the variable is left
// off the StatefulSet (operations.md id 193).
func TestStandalone_ASpecEnvListJoinsTheConf(t *testing.T) {
	sa := standaloneForConf(map[string]string{resources.AllowlistSetting: "apoc.coll.*"})
	sa.Spec.Image.Tag = "5.26.32-enterprise"
	sa.Spec.Env = []corev1.EnvVar{
		{Name: allowlistVar, Value: "custom.*"},
		{Name: unrestrictedVar, Value: "custom.*"},
		{Name: "NEO4J_dbms_security_procedures_denylist", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"}}},
		{Name: "NEO4J_server_memory_heap_max__size", Value: "1G"},
	}
	genai := pluginCR("genai-plugin", "genai", "sa", true, nil)
	genai.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"genai.*", "ai.*"}}
	r, c := standaloneCMTestReconciler(t)
	require.NoError(t, c.Create(context.Background(), genai))

	require.NoError(t, r.reconcileConfigMap(context.Background(), sa))
	conf := renderedConf(t, c)
	settings := resources.Neo4jConfSettings(conf)
	assert.Equal(t, "apoc.coll.*,genai.*,ai.*,custom.*", settings[resources.AllowlistSetting])
	assert.Equal(t, "genai.*,ai.*,custom.*", settings[unrestrictedSetting], "its own list and the plugin's; no image entries, since its spec.env sets the list")

	env := map[string]string{}
	names := map[string]bool{}
	for _, e := range r.buildEnvVars(sa) {
		names[e.Name] = true
		if e.ValueFrom == nil {
			env[e.Name] = e.Value
		}
	}
	assert.False(t, names[allowlistVar], "the conf holds it")
	assert.False(t, names[unrestrictedVar], "the conf holds it")
	assert.True(t, names["NEO4J_dbms_security_procedures_denylist"], "a variable from a Secret stays a variable")
	assert.True(t, names["NEO4J_server_memory_heap_max__size"], "any other variable stays")

	started := standaloneServer(t, entrypointsim.Image{Release: entrypointsim.Release52632, FleetJAR: true}, conf, []string{"genai"}, env)
	allow, _ := entrypointsim.Setting(started, resources.AllowlistSetting)
	assert.Equal(t, []string{"ai.*", "apoc.coll.*", "custom.*", "genai.*"}, tokensOf(allow), "every entry on the server")

	// Before: the conf without the variable's entries, and the variable,
	// which the entrypoint writes in place of the line.
	legacy := legacyStandaloneConf(r, &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: sa.ObjectMeta, Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{Image: sa.Spec.Image, Config: sa.Spec.Config},
	}, []neo4jv1beta1.Neo4jPlugin{*genai})
	old := standaloneServer(t, entrypointsim.Image{Release: entrypointsim.Release52632, FleetJAR: true}, legacy, []string{"genai"},
		map[string]string{allowlistVar: "custom.*"})
	oldAllow, _ := entrypointsim.Setting(old, resources.AllowlistSetting)
	assert.Equal(t, "custom.*", oldAllow, "the premise: the variable alone")
}

// A standalone that sets the unrestricted list itself in its spec.env gets no
// image entries, as one whose spec.config sets it: the entrypoint writes such
// a variable in place of the whole line, and an explicit setting wins
// (operations.md id 193). Its plugins' lists still join it.
func TestStandaloneConf_ItsOwnSpecEnvListGetsNoImageEntries(t *testing.T) {
	r5 := entrypointsim.Image{Release: entrypointsim.Release52632, FleetJAR: true}
	installs := []string{"apoc", "graph-data-science"}
	for _, tc := range []struct {
		name     string
		env      corev1.EnvVar
		wantConf string // the conf's list
		resolved string // the variable's value in the pod; "" for none there
		want     string // the server's list
	}{
		{"a value of its own: in the conf, beside the plugins'",
			corev1.EnvVar{Name: unrestrictedVar, Value: "custom.*"},
			"gds.*,apoc.load.*,custom.*", "", "gds.*,apoc.load.*,custom.*"},
		{"from a Secret: a variable that replaces the line, so the conf is not changed for it",
			corev1.EnvVar{Name: unrestrictedVar, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"}}},
			"gds.*,apoc.load.*", "custom.*", "custom.*"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sa := standaloneForConf(nil)
			sa.Spec.Image.Tag = "5.26.32-enterprise"
			sa.Spec.Env = []corev1.EnvVar{tc.env}
			r, c := standaloneCMTestReconciler(t)
			for _, p := range []*neo4jv1beta1.Neo4jPlugin{pluginCR("apoc", "apoc", "sa", true, nil), pluginCR("gds", "graph-data-science", "sa", true, nil)} {
				require.NoError(t, c.Create(context.Background(), p))
			}

			require.NoError(t, r.reconcileConfigMap(context.Background(), sa))
			conf := renderedConf(t, c)
			assert.Equal(t, tc.wantConf, resources.Neo4jConfSettings(conf)[unrestrictedSetting], "no apoc.*: the standalone sets the list")

			var env map[string]string
			if tc.resolved != "" {
				env = map[string]string{unrestrictedVar: tc.resolved}
			}
			got, _ := entrypointsim.Setting(standaloneServer(t, r5, conf, installs, env), unrestrictedSetting)
			assert.Equal(t, tc.want, got)
		})
	}
}

// With Aura Fleet Management, an allowlist a standalone's spec.env sets gets
// fleet's entry, as one its spec.config sets does (operations.md id 192):
// before, the variable replaced the conf's line and fleet's procedures were
// not loaded.
func TestStandalone_ASpecEnvAllowlistKeepsFleetManagement(t *testing.T) {
	sa := standaloneForConf(nil)
	sa.Spec.Image.Tag = "5.26.21-enterprise"
	sa.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
	sa.Spec.Env = []corev1.EnvVar{{Name: allowlistVar, Value: "apoc.*"}}
	r, c := standaloneCMTestReconciler(t)

	require.NoError(t, r.reconcileConfigMap(context.Background(), sa))

	started := standaloneServer(t, entrypointsim.Image{Release: entrypointsim.Release52632, FleetJAR: true}, renderedConf(t, c),
		[]string{"fleet-management"}, nil)
	allow, lines := entrypointsim.Setting(started, resources.AllowlistSetting)
	assert.Equal(t, 1, lines)
	assert.Equal(t, []string{"apoc.*", "fleetManagement.*"}, tokensOf(allow))
}

// The operator upgrade's transient (v1.22.0 → this release, re-walk): the
// ConfigMap an earlier operator wrote still has Aura Fleet Management's
// allowlist, fleetManagement.* alone, until the target's controller writes the
// new one. The plugin is judged against the conf the servers will start with,
// so APOC is not announced hidden in between (operations.md id 194).
func TestProceduresHidden_IsJudgedAgainstWhatTheServersWillRun(t *testing.T) {
	t.Run("a cluster's ConfigMap from an earlier operator", func(t *testing.T) {
		cluster, desired := listCluster(nil, true)
		live := desired.DeepCopy()
		live.Data["neo4j.conf"] = legacyFleetConf(cluster)
		apoc := selfHealPlugin("apoc", "c", "", "")
		r, rec, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, cluster, live, fleetSTS()}, readyClusterPods()...)...)

		installAndRoll(t, r, "apoc", "c-server")
		reconcilePlugin(t, r, "apoc")

		assert.Nil(t, findCondition(getPlugin(t, r, "apoc").Status.Conditions, ConditionTypeProceduresHidden))
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden))
	})
	t.Run("a cluster's new list its ConfigMap does not have yet", func(t *testing.T) {
		cluster, desired := listCluster(nil, false)
		cluster.Spec.Config = map[string]string{resources.AllowlistSetting: "gds.*"}
		apoc := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
		r, rec, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, cluster, desired, installedSelfHealSTS()}, readyClusterPods()...)...)

		reconcilePlugin(t, r, "apoc")

		hidden := findCondition(getPlugin(t, r, "apoc").Status.Conditions, ConditionTypeProceduresHidden)
		require.NotNil(t, hidden, "the list the servers will start with hides it")
		assert.Contains(t, hidden.Message, `"gds.*" (neo4j.conf of ConfigMap c-config, set by Neo4jEnterpriseCluster c's own configuration)`)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden), 1)
	})
	t.Run("a standalone's ConfigMap from an earlier operator", func(t *testing.T) {
		sa := readyStandalone()
		sa.Spec.Image.Tag = "5.26.21-enterprise"
		sa.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
		live := renderStandaloneConfigMap(sa, true) // up to v1.22.x: fleet's allowlist line always
		live.Annotations = map[string]string{ownedStandaloneConfKeysAnnotation: joinSortedKeys(resources.Neo4jConfSettings(live.Data["neo4j.conf"]))}
		require.Equal(t, "fleetManagement.*", resources.Neo4jConfSettings(live.Data["neo4j.conf"])[resources.AllowlistSetting])
		apoc := selfHealPlugin("apoc", "sa", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
		sts := standaloneSTS(
			corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["fleet-management","apoc"]`},
			corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
		)
		r, rec, _, _ := gateReconciler(t, noBolt(t), apoc, sa, sts, live)

		reconcilePlugin(t, r, "apoc")

		assert.Nil(t, findCondition(getPlugin(t, r, "apoc").Status.Conditions, ConditionTypeProceduresHidden))
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden))
	})
	t.Run("a standalone's spec.env list its StatefulSet still has the old value of", func(t *testing.T) {
		sa := readyStandalone()
		sa.Spec.Env = []corev1.EnvVar{{Name: allowlistVar, Value: "gds.*"}}
		apoc := selfHealPlugin("apoc", "sa", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
		sts := standaloneSTS(
			corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
			corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
			corev1.EnvVar{Name: allowlistVar, Value: "apoc.*"},
		)
		live := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sa-config", Namespace: selfHealNS},
			Data: map[string]string{"neo4j.conf": "server.memory.heap.max_size=1G\n"}}
		r, rec, _, _ := gateReconciler(t, noBolt(t), apoc, sa, sts, live)

		reconcilePlugin(t, r, "apoc")

		hidden := findCondition(getPlugin(t, r, "apoc").Status.Conditions, ConditionTypeProceduresHidden)
		require.NotNil(t, hidden, "the conf the server will start with holds the spec.env list, gds.*")
		assert.Contains(t, hidden.Message, `"gds.*" (neo4j.conf of ConfigMap sa-config`)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden), 1)
	})
}
