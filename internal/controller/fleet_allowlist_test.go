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

// Aura Fleet Management's procedures join the allowlist only when something
// else sets it (operations.md id 192). Neo4j loads every procedure while
// dbms.security.procedures.allowlist is not set; the operator used to set it to
// fleetManagement.* on its own, so enabling fleet management hid APOC and every
// plugin without allowedProcedures. The Operations Manual: add a plugin to the
// allowlist only "if dbms.security.procedures.allowlist is set in your
// configuration … otherwise no change is needed" (configuration/plugins.adoc).

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/entrypointsim"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// legacyFleetConf is the neo4j.conf an operator up to v1.22.x rendered for a
// cluster with Aura Fleet Management (and no property sharding): the fleet
// block, with its allowlist line, appended last before the conf is
// de-duplicated.
func legacyFleetConf(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) string {
	noFleet := cluster.DeepCopy()
	noFleet.Spec.AuraFleetManagement = nil
	return resources.DedupeNeo4jConf(resources.BuildConfigMapForEnterprise(noFleet).Data["neo4j.conf"] +
		"\n# Aura Fleet Management\ndbms.security.procedures.unrestricted=fleetManagement.*\ndbms.security.procedures.allowlist=fleetManagement.*\n")
}

// A cluster whose own spec.config sets an allowlist renders the same
// neo4j.conf as before, byte for byte, and restarts nothing; one without
// renders no allowlist, and its one restart is the fix. Every cluster without
// Aura Fleet Management renders as before
// (TestOperatorUpgrade_TheListBlockRestartsNoServer).
func TestClusterConf_FleetJoinsTheAllowlistOnlyBesideAnother(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  map[string]string
		changes bool
	}{
		{"its own allowlist", map[string]string{"dbms.security.procedures.allowlist": "apoc.*"}, false},
		{"its own allowlist and unrestricted list", map[string]string{
			"dbms.security.procedures.allowlist": "apoc.*", "dbms.security.procedures.unrestricted": "apoc.*"}, false},
		{"fleet alone", nil, true},
		{"its own unrestricted list only", map[string]string{"dbms.security.procedures.unrestricted": "gds.*"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster, desired := listCluster(tc.config, true)
			conf := desired.Data["neo4j.conf"]
			legacy := legacyFleetConf(cluster)
			settings := resources.Neo4jConfSettings(conf)
			assert.Contains(t, settings["dbms.security.procedures.unrestricted"], "fleetManagement.*", "always unrestricted")
			if !tc.changes {
				assert.Equal(t, legacy, conf, "byte for byte as before")
				assert.Contains(t, settings["dbms.security.procedures.allowlist"], "fleetManagement.*")
			} else {
				assert.NotEqual(t, legacy, conf)
				_, set := settings["dbms.security.procedures.allowlist"]
				assert.False(t, set, "no allowlist: every procedure loads")
			}

			// The operator upgrade: the ConfigMap an earlier operator wrote.
			existing := desired.DeepCopy()
			existing.Data["neo4j.conf"] = legacy
			existing.Data["startup.sh"] = withoutPluginListMerge(desired.Data["startup.sh"])
			fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
				WithObjects(cluster, serverSTS("c", selfHealNS), existing).Build()
			manager := NewConfigMapManager(fc)
			manager.LiveConfig = nil
			require.NoError(t, manager.ReconcileConfigMap(context.Background(), cluster))
			sts := &appsv1.StatefulSet{}
			require.NoError(t, fc.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: "c-server"}, sts))
			_, restarted := sts.Spec.Template.Annotations["neo4j.neo4j.com/config-restart"]
			assert.Equal(t, tc.changes, restarted, "a restart only where the servers' allowlist changes")
		})
	}
}

// With Aura Fleet Management alone, nothing restricts the allowlist: the
// servers start with Neo4j's default, *, and an APOC plugin with no security
// block is not hidden. It used to be fleetManagement.*: the operator wrote it,
// and the image's entrypoint adds it too, for fleet-management in
// NEO4J_PLUGINS, to a neo4j.conf without the setting; the startup block puts
// * in its place.
func TestServers_FleetAloneLeavesTheAllowlistUnset(t *testing.T) {
	cluster, cm := listCluster(nil, true)
	apoc := selfHealPlugin("apoc", "c", "", "")
	r, rec, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, cluster, cm, fleetSTS()}, readyClusterPods()...)...)

	installAndRoll(t, r, "apoc", "c-server")

	lists := serverLists(t, cm, liveSTSEnv(t, r.Client, "c-server"))
	assert.Equal(t, "*", lists["dbms.security.procedures.allowlist"], "Neo4j's default: every procedure loads")
	assert.Equal(t, []string{"fleetManagement.*"}, tokensOf(lists["dbms.security.procedures.unrestricted"]))
	assert.Nil(t, findCondition(getPlugin(t, r, "apoc").Status.Conditions, ConditionTypeProceduresHidden))
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden))
}

// fleetPluginSTS is cluster "c"'s StatefulSet with a Managed fleet-management
// Neo4jPlugin written by an earlier operator: its automatic allowlist
// fleetManagement.* in the env, beside the given env.
func fleetPluginSTS(allowlist string, env ...corev1.EnvVar) *appsv1.StatefulSet {
	sts := selfHealSTS(append([]corev1.EnvVar{
		{Name: "NEO4J_PLUGINS", Value: `["fleet-management"]`},
		{Name: allowlistVar, Value: allowlist},
		{Name: unrestrictedVar, Value: "fleetManagement.*"},
	}, env...)...)
	sts.Annotations = map[string]string{pluginEntriesAnnotation("fleet"): "fleet-management"}
	return sts
}

func fleetPlugin() *neo4jv1beta1.Neo4jPlugin {
	p := selfHealPlugin("fleet", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	p.Spec.Name, p.Spec.Config = "fleet-management", nil
	return p
}

// fleetPluginCluster is readyCluster on a release whose entrypoint installs
// fleet-management (from 5.26.11).
func fleetPluginCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := readyCluster()
	c.Spec.Image.Tag = "5.26.21-enterprise"
	return c
}

// A fleet-management Neo4jPlugin wrote its automatic allowlist into the env,
// where it was the only entry: the same narrowing. The settled plugin takes it
// off with one write (no PluginMissing), and every procedure loads; beside
// another plugin's entries it is left to the plugin's next write, since the
// servers' allowlist does not change (the startup script adds the entry while
// fleet-management is installed).
func TestReconcile_AFleetManagementPluginTakesItsAllowlistOff(t *testing.T) {
	t.Run("its entry alone: one write", func(t *testing.T) {
		r, rec, writes, messages := gateReconciler(t, nil, append([]client.Object{fleetPlugin(), fleetPluginCluster(), fleetPluginSTS("fleetManagement.*")}, readyClusterPods()...)...)

		reconcilePlugin(t, r, "fleet")

		require.Equal(t, 1, *writes)
		_, ok := envValueOf(t, r, "c-server", allowlistVar)
		assert.False(t, ok, "no allowlist: every procedure loads")
		v, _ := envValueOf(t, r, "c-server", unrestrictedVar)
		assert.Equal(t, "fleetManagement.*", v, "the unrestricted entry stays")
		assert.Equal(t, neo4jv1beta1.PhaseInstalling, getPlugin(t, r, "fleet").Status.Phase)
		require.NotEmpty(t, *messages)
		assert.Contains(t, (*messages)[0], "Removing what the plugin no longer sets: "+allowlistVar)
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing))

		rollOut(t, r.Client, "c-server")
		reconcilePlugin(t, r, "fleet")
		reconcilePlugin(t, r, "fleet")
		assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "fleet").Status.Phase)
		assert.Equal(t, 1, *writes, "once")
	})

	t.Run("beside another plugin's entries: nothing written", func(t *testing.T) {
		genai := allowingPlugin("genai-plugin", "genai", "genai.*", "ai.*")
		genai.Status.Phase, genai.Status.Message = neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully"
		sts := fleetPluginSTS("fleetManagement.*,genai.*,ai.*")
		sts.Annotations[pluginEntriesAnnotation("genai-plugin")] = "genai"
		r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{fleetPlugin(), genai, fleetPluginCluster(), sts}, readyClusterPods()...)...)

		reconcilePlugin(t, r, "fleet")

		assert.Zero(t, *writes)
		assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "fleet").Status.Phase)
	})
}

// legacyStandaloneConf is the neo4j.conf an operator up to v1.22.x rendered
// for the standalone and its plugins: the fleet allowlist line always, and the
// fleet-management plugin's automatic allowlist in the plugins' union.
func legacyStandaloneConf(r *Neo4jEnterpriseStandaloneReconciler, sa *neo4jv1beta1.Neo4jEnterpriseStandalone, plugins []neo4jv1beta1.Neo4jPlugin) string {
	conf := r.renderConfigMap(sa, true).Data["neo4j.conf"]
	if union := unionPluginConfSettings(plugins, sa.Name, sa.Spec.Image.Tag); len(union) > 0 {
		conf = resources.DedupeNeo4jConf(resources.UpsertNeo4jConfSettings(conf, union))
	}
	return conf
}

// On a standalone the decision is made in its neo4j.conf union: the fleet
// entry joins an allowlist its own spec.config or a plugin sets, and
// otherwise the allowlist is Neo4j's default, *, written so that the image's
// entrypoint, which adds fleetManagement.* alone to a conf without the
// setting, leaves it alone. Where an allowlist is set, the conf is byte for
// byte what it was, so nothing restarts.
func TestStandaloneConf_FleetJoinsTheAllowlistOnlyBesideAnother(t *testing.T) {
	genai := func() neo4jv1beta1.Neo4jPlugin {
		p := pluginCR("genai-plugin", "genai", "sa", true, nil)
		p.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"genai.*", "ai.*"}}
		return *p
	}
	fleetCR := func() neo4jv1beta1.Neo4jPlugin { return *pluginCR("fleet", "fleet-management", "sa", true, nil) }
	for _, tc := range []struct {
		name    string
		fleet   bool // spec.auraFleetManagement
		config  map[string]string
		plugins []neo4jv1beta1.Neo4jPlugin
		changes bool
	}{
		{"Aura Fleet Management alone", true, nil, nil, true},
		{"and its own allowlist", true, map[string]string{"dbms.security.procedures.allowlist": "apoc.*"}, nil, false},
		{"and a plugin's allowlist", true, nil, []neo4jv1beta1.Neo4jPlugin{genai()}, false},
		{"a fleet-management plugin alone", false, nil, []neo4jv1beta1.Neo4jPlugin{fleetCR()}, true},
		{"a fleet-management plugin and another plugin's allowlist", false, nil, []neo4jv1beta1.Neo4jPlugin{fleetCR(), genai()}, false},
		{"a fleet-management plugin and its own allowlist", false, map[string]string{"dbms.security.procedures.allowlist": "apoc.*"},
			[]neo4jv1beta1.Neo4jPlugin{fleetCR()}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sa := standaloneForConf(tc.config)
			sa.Spec.Image.Tag = "5.26.21-enterprise" // fleet-management installs from 5.26.11
			if tc.fleet {
				sa.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
			}
			r, c := standaloneCMTestReconciler(t)
			for i := range tc.plugins {
				require.NoError(t, c.Create(context.Background(), &tc.plugins[i]))
			}
			legacy := legacyStandaloneConf(r, sa, tc.plugins)

			require.NoError(t, r.reconcileConfigMap(context.Background(), sa))

			conf := renderedConf(t, c)
			settings := resources.Neo4jConfSettings(conf)
			assert.Contains(t, settings["dbms.security.procedures.unrestricted"], "fleetManagement.*", "always unrestricted")
			if !tc.changes {
				assert.Equal(t, legacy, conf, "byte for byte as before")
				assert.Contains(t, settings["dbms.security.procedures.allowlist"], "fleetManagement.*")
				return
			}
			assert.Equal(t, "*", settings["dbms.security.procedures.allowlist"],
				"Neo4j's default, which keeps the entrypoint from narrowing it\n%s", conf)
			assert.True(t, strings.Contains(legacy, "dbms.security.procedures.allowlist=fleetManagement.*"))
			// The server: the entrypoint, with fleet-management in NEO4J_PLUGINS,
			// leaves a conf that sets the allowlist alone.
			started := entrypointsim.Start(t, entrypointsim.Image{FleetJAR: true}, conf, "",
				map[string]string{"NEO4J_PLUGINS": `["fleet-management"]`})
			allow, lines := entrypointsim.Setting(started, "dbms.security.procedures.allowlist")
			assert.Equal(t, "*", allow)
			assert.Equal(t, 1, lines)
		})
	}

	// From 2026.03 Fleet Manager is built in: nothing goes in NEO4J_PLUGINS,
	// the entrypoint adds no default, and no allowlist is written.
	t.Run("Aura Fleet Management on 2026.08.1", func(t *testing.T) {
		sa := standaloneForConf(nil)
		sa.Spec.Image.Tag = "2026.08.1-enterprise"
		sa.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
		r, c := standaloneCMTestReconciler(t)
		require.NoError(t, r.reconcileConfigMap(context.Background(), sa))
		_, set := resources.Neo4jConfSettings(renderedConf(t, c))["dbms.security.procedures.allowlist"]
		assert.False(t, set)
	})
}
