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

// A Managed Neo4jPlugin whose NEO4J_PLUGINS names the image of its target
// cannot install is refused, and nothing of it stays on the target
// (operations.md id 170; #563).

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// clusterOn is cluster "c", Ready with every server available, running tag.
func clusterOn(tag string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := readyCluster()
	c.Spec.Image.Tag = tag
	return c
}

// managedPlugin is a Managed Neo4jPlugin of cluster "c" named name, with no
// settings, in phase.
func managedPlugin(name, phase string) *neo4jv1beta1.Neo4jPlugin {
	plugin := selfHealPlugin(name, "c", phase, "")
	if phase == neo4jv1beta1.PhaseReady {
		plugin.Status.Message = "Plugin installed and configured successfully"
	}
	plugin.Spec.Name, plugin.Spec.Config = name, nil
	return plugin
}

// The defect (#563, confirmed on the images on 2026-10-24): a Managed plugin's
// name goes into NEO4J_PLUGINS, and the entrypoint exits on a name its
// /startup/neo4j-plugins.json does not list, so the server crash-loops:
// graphql on every supported release, n10s on CalVer. The operator wrote it
// anyway. Now such a plugin is Failed with one Warning, judged by the target's
// image tag, and nothing is written to the target.
func TestReconcile_APluginTheTargetsImageCannotInstallIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name, tag, plugin string
		edit              func(*neo4jv1beta1.Neo4jPlugin)
		field             string
	}{
		{name: "graphql on 5.26", tag: "5.26.32-enterprise", plugin: "graphql", field: "spec.name"},
		{name: "graphql on CalVer", tag: "2026.08.1-enterprise", plugin: "graphql", field: "spec.name"},
		{name: "n10s on CalVer", tag: "2026.08.1-enterprise", plugin: "n10s", field: "spec.name"},
		{name: "n10s on a floating CalVer tag", tag: "2026.08-enterprise", plugin: "n10s", field: "spec.name"},
		{name: "n10s on the first CalVer without it", tag: "2025.05.0-enterprise", plugin: "n10s", field: "spec.name"},
		{name: "the neosemantics alias on CalVer", tag: "2026.08.1-enterprise", plugin: "neosemantics", field: "spec.name"},
		{name: "a custom JAR's name", tag: "5.26-enterprise", plugin: "acme-procs", field: "spec.name"},
		{name: "fleet-management before 5.26.11", tag: "5.26.10-enterprise", plugin: "fleet-management", field: "spec.name"},
		{name: "a dependency on CalVer", tag: "2026.08.1-enterprise", plugin: "apoc", field: "spec.dependencies[0].name",
			edit: func(p *neo4jv1beta1.Neo4jPlugin) {
				p.Spec.Dependencies = []neo4jv1beta1.PluginDependency{{Name: "n10s"}}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := managedPlugin(tc.plugin, "")
			if tc.edit != nil {
				tc.edit(plugin)
			}
			r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), append([]client.Object{plugin, clusterOn(tc.tag), selfHealSTS()}, readyClusterPods()...)...)

			res := reconcilePlugin(t, r, tc.plugin)

			assert.Zero(t, *writes, "nothing is written to the target")
			assert.Empty(t, neo4jContainerEnv(liveSTS(t, r.Client, "c-server").Spec.Template.Spec))
			got := getPlugin(t, r, tc.plugin)
			assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
			assert.True(t, strings.HasPrefix(got.Status.Message, tc.field+": Invalid value: "), got.Status.Message)
			assert.Contains(t, got.Status.Message, "not a plugin the neo4j:"+tc.tag+" image can install")
			assert.Contains(t, got.Status.Message, "installMode: VerifiedDownload (source.url + source.checksum)")
			assert.Positive(t, res.RequeueAfter, "looked at again: the target's image can change")
			refusals := eventsWithReason(drainEvents(rec), EventReasonPluginUnknownToImage)
			require.Len(t, refusals, 1)
			assert.Contains(t, refusals[0], "Warning")

			// The next passes say nothing more and write nothing.
			rv := got.ResourceVersion
			reconcilePlugin(t, r, tc.plugin)
			reconcilePlugin(t, r, tc.plugin)
			assert.Equal(t, rv, getPlugin(t, r, tc.plugin).ResourceVersion)
			assert.Zero(t, *writes)
			assert.Empty(t, drainEvents(rec))
		})
	}
}

// The same names install where the image lists them: n10s on 5.26 (also from a
// floating tag) and on CalVer before 2025.05, fleet-management from 5.26.11.
// The refusal is per line, not per name.
func TestReconcile_APluginTheTargetsImageListsIsInstalled(t *testing.T) {
	for _, tc := range []struct{ tag, plugin string }{
		// n10s runs on 5.26.0 to 5.26.21 only (operations.md id 170).
		{"5.26.21-enterprise", "n10s"},
		{"5.26.21-enterprise", "neosemantics"},
		{"5.26.11-enterprise", "fleet-management"},
		{"2026.08.1-enterprise", "graph-data-science"},
	} {
		t.Run(tc.plugin+" on "+tc.tag, func(t *testing.T) {
			plugin := managedPlugin(tc.plugin, "")
			r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin, clusterOn(tc.tag), selfHealSTS()}, readyClusterPods()...)...)

			installAndRoll(t, r, tc.plugin, "c-server")

			assert.Equal(t, 1, *writes)
			v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
			assert.Equal(t, fmt.Sprintf("[%q]", r.mapPluginName(tc.plugin)), v)
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginUnknownToImage))
		})
	}
}

// The defect (v1.22.0 re-walk of the combined build): n10s, a name every 5.26
// entrypoint installs, was written to a 5.26.32 server, which crash-looped on
// the jackson-annotations its neosemantics 5.26.0 JAR bundles
// (NoSuchMethodError JsonProperty.isRequired()). From 5.26.22 it is refused like
// a name the image does not list: nothing written, Failed with the reason, one
// PluginUnknownToImage (operations.md id 170).
func TestReconcile_N10sIsRefusedWhereItsJARDoesNotRun(t *testing.T) {
	for _, tag := range []string{"5.26.32-enterprise", "5.26-enterprise"} {
		t.Run(tag, func(t *testing.T) {
			plugin := managedPlugin("n10s", "")
			r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), append([]client.Object{plugin, clusterOn(tag), selfHealSTS()}, readyClusterPods()...)...)

			reconcilePlugin(t, r, "n10s")

			assert.Zero(t, *writes)
			got := getPlugin(t, r, "n10s")
			assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
			assert.Contains(t, got.Status.Message, "NoSuchMethodError JsonProperty.isRequired()")
			assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginUnknownToImage), 1)
		})
	}
}

// A refused name stays off NEO4J_PLUGINS only in Managed mode: a VerifiedDownload
// (or PreBaked) plugin never touches NEO4J_PLUGINS, so any JAR installs.
func TestReconcile_AVerifiedDownloadOfAnyJARIsNotRefused(t *testing.T) {
	plugin := verifiedDownloadSelfHeal("", "")
	plugin.Name, plugin.Spec.Name = "graphql", "graphql"
	r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin, clusterOn("2026.08.1-enterprise"), selfHealSTS()}, readyClusterPods()...)...)

	installAndRoll(t, r, "graphql", "c-server")

	assert.Equal(t, 1, *writes)
	assert.Contains(t, initContainerNames(liveSTS(t, r.Client, "c-server")), "plugin-init-graphql")
	_, listed := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
	assert.False(t, listed)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginUnknownToImage))
}

// refusedDeadlock is a three-server CalVer cluster an earlier operator wrote a
// Managed graphql plugin to: NEO4J_PLUGINS holds it beside an APOC plugin's
// name, server 2 took the template and its entrypoint keeps exiting on it
// ("graphql" is not a known Neo4j plugin), servers 0 and 1 still run the
// template from before. The cluster is Forming; the plugin, Ready under that
// operator, has no record of its NEO4J_PLUGINS names (recorded is "" ) unless
// recorded says what.
func refusedDeadlock(recorded string) (*neo4jv1beta1.Neo4jPlugin, *neo4jv1beta1.Neo4jPlugin, *neo4jv1beta1.Neo4jEnterpriseCluster, *appsv1.StatefulSet, []*corev1.Pod) {
	graphql := managedPlugin("graphql", neo4jv1beta1.PhaseReady)
	apoc := managedPlugin("apoc", neo4jv1beta1.PhaseReady)
	written := []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["apoc","graphql"]`}}
	sts := selfHealSTS(written...)
	if recorded != "" {
		sts.Annotations = map[string]string{pluginEntriesAnnotation("graphql"): recorded}
	}
	sts.Spec.Replicas = ptr.To[int32](3)
	sts.Status = rolledOutStatus(3, 0)
	sts.Status.UpdateRevision, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = "rev-next", 1, 2
	cluster := clusterOn("2026.08.1-enterprise")
	cluster.Status.Phase = neo4jv1beta1.PhaseForming
	cluster.Spec.Topology.Servers = 3
	pods := []*corev1.Pod{
		running(gatePod("c-server-0", nil)), running(gatePod("c-server-1", nil)),
		crashLooping(gatePod("c-server-2", nil), 1, written...),
	}
	return graphql, apoc, cluster, sts, pods
}

// The repair (#563): the servers an earlier operator's write of a refused name
// keeps from starting recover. The plugin takes its name off NEO4J_PLUGINS as a
// disabled plugin does — the name its record lists, and with no record (every
// plugin an operator up to v1.22.x installed) its own as the entrypoint knows
// it — at once, although the cluster is not Ready (ids 152, 155), and the other
// plugin's name stays.
func TestReconcile_ARefusedPluginComesOffTheServersItKeepsDown(t *testing.T) {
	for _, tc := range []struct{ name, recorded, want string }{
		{"no record: its mapped name", "", `["apoc"]`},
		{"a record: what it lists too", "graphql,n10s", `["apoc"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			graphql, apoc, cluster, sts, pods := refusedDeadlock(tc.recorded)
			if tc.recorded != "" {
				// A dependency it once had, recorded and since dropped.
				sts.Spec.Template.Spec.Containers[0].Env[0].Value = `["apoc","graphql","n10s"]`
				pods[2] = crashLooping(gatePod("c-server-2", nil), 1, sts.Spec.Template.Spec.Containers[0].Env...)
			}
			objs := append(objects(graphql, cluster, sts, pods), apoc)
			r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), objs...)

			reconcilePlugin(t, r, "graphql")

			require.Equal(t, 1, *writes, "written although the cluster is Forming")
			v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
			assert.Equal(t, tc.want, v, "the refused names go, APOC's stays")
			assert.NotContains(t, liveSTS(t, r.Client, "c-server").Annotations, pluginEntriesAnnotation("graphql"))
			got := getPlugin(t, r, "graphql")
			assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
			refusal := strings.SplitN(got.Status.Message, "; taking it off", 2)[0]
			assert.Contains(t, refusal, `spec.name: Invalid value: "graphql": not a plugin the neo4j:2026.08.1-enterprise image can install`)
			assert.Equal(t, refusal+"; taking it off Neo4jEnterpriseCluster c: waiting for the servers to restart without it; "+
				"cannot start: c-server-2 (container neo4j exited with code 1)", got.Status.Message)
			events := drainEvents(rec)
			assert.Len(t, eventsWithReason(events, EventReasonPluginUnknownToImage), 1)
			repairs := eventsWithReason(events, EventReasonPluginSettingsFailing)
			require.Len(t, repairs, 1)
			assert.Contains(t, repairs[0], "Warning")
			assert.Contains(t, repairs[0], "c-server-2 (container neo4j exited with code 1)")

			// Written once; the passes that wait for the roll say nothing more.
			reconcilePlugin(t, r, "graphql")
			assert.Equal(t, 1, *writes)
			assert.Empty(t, drainEvents(rec))

			// The servers run without it: the refusal alone, and still no event.
			rollOut(t, r.Client, "c-server")
			for _, p := range pods {
				live := &corev1.Pod{}
				require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(p), live))
				live.Status = gatePod(p.Name, nil).Status
				require.NoError(t, r.Status().Update(context.Background(), live))
			}
			reconcilePlugin(t, r, "graphql")
			assert.Equal(t, refusal, getPlugin(t, r, "graphql").Status.Message)
			assert.Equal(t, 1, *writes)
			assert.Empty(t, drainEvents(rec))
		})
	}
}

// The repair is as narrow as ids 152 and 155 make it: a server down for any
// other reason makes the removal wait, and the plugin says so.
func TestReconcile_ARefusedPluginsRemovalWaitsWhenSomethingElseIsDown(t *testing.T) {
	graphql, apoc, cluster, sts, pods := refusedDeadlock("")
	pods[0].Status.Conditions[0].Status = corev1.ConditionFalse
	r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), append(objects(graphql, cluster, sts, pods), apoc)...)

	reconcilePlugin(t, r, "graphql")

	assert.Zero(t, *writes)
	got := getPlugin(t, r, "graphql")
	assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
	assert.True(t, strings.HasSuffix(got.Status.Message, "; it is taken off Neo4jEnterpriseCluster c once it is Ready with every server available, "+
		"since that restarts every server"), got.Status.Message)
	events := drainEvents(rec)
	assert.Len(t, eventsWithReason(events, EventReasonPluginUnknownToImage), 1)
	assert.Empty(t, eventsWithReason(events, EventReasonPluginSettingsFailing))
}

// The target's image can refuse a plugin it used to run: an installed n10s on a
// 5.26 cluster that moves to CalVer, whose entrypoint would exit on the name. The
// plugin is refused at the image change and its name comes off; while the
// cluster upgrades, and no server is down on it yet, that waits.
func TestReconcile_AnImageChangeRefusesAnInstalledPlugin(t *testing.T) {
	plugin := managedPlugin("n10s", "")
	cluster := clusterOn("5.26.21-enterprise")
	r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin, cluster, selfHealSTS()}, readyClusterPods()...)...)
	installAndRoll(t, r, "n10s", "c-server")
	require.Equal(t, 1, *writes)
	drainEvents(rec)

	live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(cluster), live))
	live.Spec.Image.Tag, live.Generation = "2026.08.1-enterprise", live.Generation+1
	require.NoError(t, r.Update(context.Background(), live))
	reconcilePlugin(t, r, "n10s")

	assert.Equal(t, 1, *writes, "the cluster has not acted on its new image: the removal waits")
	got := getPlugin(t, r, "n10s")
	assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
	assert.Contains(t, got.Status.Message, `spec.name: Invalid value: "n10s": not a plugin the neo4j:2026.08.1-enterprise image can install`)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginUnknownToImage), 1)

	// Once the target is Ready with every server available, the name comes off
	// in one write.
	live.Status.ObservedGeneration = live.Generation
	require.NoError(t, r.Update(context.Background(), live)) // the fake keeps no status subresource for clusters
	reconcilePlugin(t, r, "n10s")
	assert.Equal(t, 2, *writes)
	v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
	assert.Equal(t, `[]`, v)
	assert.Empty(t, drainEvents(rec), "the refusal was announced already")
}

// A refused plugin is not on its target, so another plugin's write keeps
// nothing for it (as for a disabled one, id 160), and it carries no
// ProceduresHidden condition.
func TestOtherPlugins_LeavesOutRefusedPlugins(t *testing.T) {
	apoc := managedPlugin("apoc", neo4jv1beta1.PhaseReady)
	n10s := managedPlugin("n10s", neo4jv1beta1.PhaseFailed)
	n10s.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"n10s.*"}}
	r, _, _, _ := gateReconciler(t, nil, apoc, n10s, clusterOn("2026.08.1-enterprise"), selfHealSTS())
	calver := &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: clusterOn("2026.08.1-enterprise")}
	lts := &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: clusterOn("5.26.21-enterprise")}

	others, err := r.otherPlugins(context.Background(), apoc, calver)
	require.NoError(t, err)
	assert.Empty(t, others, "n10s is refused on CalVer")
	others, err = r.otherPlugins(context.Background(), apoc, lts)
	require.NoError(t, err)
	require.Len(t, others, 1, "and installed on 5.26")
	assert.Equal(t, "n10s", others[0].Name)

	// Its allowlist would hide APOC; refused, it puts nothing on the servers.
	sts := liveSTS(t, r.Client, "c-server")
	require.NoError(t, r.desiredTarget(context.Background(), sts, apoc, calver))
	_, ok := findEnv(neo4jContainer(&sts.Spec.Template.Spec), allowlistVar)
	assert.False(t, ok)
}

// A refused plugin is not on its target's servers, so the target's allowlist
// hides nothing of it: no ProceduresHidden condition, no
// PluginProceduresHidden Warning beside the refusal (id 161).
func TestReconcile_ARefusedPluginCarriesNoHiddenProcedures(t *testing.T) {
	plugin := managedPlugin("n10s", "")
	apoc := managedPlugin("apoc", neo4jv1beta1.PhaseReady)
	apoc.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"apoc.*"}, Sandbox: true}
	sts := selfHealSTS(corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`}, corev1.EnvVar{Name: allowlistVar, Value: "apoc.*"})
	r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), append([]client.Object{plugin, apoc, clusterOn("2026.08.1-enterprise"), sts}, readyClusterPods()...)...)

	reconcilePlugin(t, r, "n10s")

	assert.Zero(t, *writes)
	got := getPlugin(t, r, "n10s")
	assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
	assert.Nil(t, findCondition(got.Status.Conditions, ConditionTypeProceduresHidden))
	events := drainEvents(rec)
	assert.Len(t, eventsWithReason(events, EventReasonPluginUnknownToImage), 1)
	assert.Empty(t, eventsWithReason(events, EventReasonPluginProceduresHidden))
}

// On a standalone, whose OrderedReady StatefulSet does not replace a pod that
// is not Ready (id 152), the refused name comes off and the plugin then deletes
// the pod that keeps exiting on it, so it starts again without it.
func TestReconcile_ARefusedPluginsStandalonePodIsReplaced(t *testing.T) {
	plugin := managedPlugin("graphql", neo4jv1beta1.PhaseReady)
	plugin.Spec.ClusterRef = "sa"
	standalone := readyStandalone()
	standalone.Spec.Image.Tag = "5.26.32-enterprise"
	standalone.Status.Phase = neo4jv1beta1.PhasePending
	written := corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["graphql"]`}
	sts := standaloneSTS(written)
	sts.Status.ReadyReplicas = 0
	pod := crashLooping(gatePod("sa-0", map[string]string{"app": "sa"}), 1, written)
	pod.Labels[appsv1.ControllerRevisionHashLabelKey] = sts.Status.UpdateRevision
	r, rec, writes, _ := gateReconciler(t, boltMustNotRun(t), plugin, standalone, sts, pod)

	reconcilePlugin(t, r, "graphql")
	require.Equal(t, 1, *writes, "the removal is written to the standalone that is down")
	v, _ := envValueOf(t, r, "sa", "NEO4J_PLUGINS")
	assert.Equal(t, `[]`, v)
	events := drainEvents(rec)
	require.Len(t, eventsWithReason(events, EventReasonPluginUnknownToImage), 1)
	require.Len(t, eventsWithReason(events, EventReasonPluginSettingsFailing), 1)

	// Not before the StatefulSet has seen the new template; then the pod goes.
	live := liveSTS(t, r.Client, "sa")
	live.Status.ObservedGeneration = live.Generation
	live.Status.UpdateRevision = fmt.Sprintf("rev-%d", live.Generation)
	require.NoError(t, r.Status().Update(context.Background(), live))
	reconcilePlugin(t, r, "graphql")

	err := r.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
	assert.True(t, client.IgnoreNotFound(err) == nil && err != nil, "the stuck pod is deleted: %v", err)
	replaced := eventsWithReason(drainEvents(rec), EventReasonPluginSettingsFailing)
	require.Len(t, replaced, 1)
	assert.Contains(t, replaced[0], "Deleted pod sa-0")
	assert.Equal(t, 1, *writes)
}
