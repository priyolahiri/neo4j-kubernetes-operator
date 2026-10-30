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

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// specEnvGDSCluster is cluster "c" on 5.26.32 whose own spec.env lists APOC
// and GDS in NEO4J_PLUGINS, as the journey's, with Aura Fleet Management on
// or off.
func specEnvGDSCluster(fleet bool) *neo4jv1beta1.Neo4jEnterpriseCluster {
	cluster := readyCluster()
	cluster.Spec.Image.Tag = "5.26.32-enterprise"
	cluster.Spec.Env = []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["apoc","graph-data-science"]`}}
	if fleet {
		cluster.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
	}
	return cluster
}

// clusterTemplateApply applies the cluster's server StatefulSet as its pass
// does (buildServerStatefulSet: its own NEO4J_PLUGINS as it renders it),
// standing in for the API server's UID on the create.
func clusterTemplateApply(t *testing.T, c client.Client, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	t.Helper()
	clusters := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: newTestScheme()}
	require.NoError(t, clusters.createOrUpdateResource(context.Background(), buildServerStatefulSet(cluster), cluster))
	if sts := liveSTS(t, c, "c-server"); sts.UID == "" {
		sts.UID = "c-server-uid"
		require.NoError(t, c.Update(context.Background(), sts))
	}
}

// #619 S7, the v1.24.0 journey's GDS cluster: its own spec.env NEO4J_PLUGINS
// listed graph-data-science, and a Managed GDS plugin was Ready. Taking the
// variable out of spec.env dropped the whole variable — fleet-management
// alone came back in that write — and GDS raised PluginMissing and wrote its
// name back: two rolls for one edit. The write that stops owning the variable
// now keeps the plugin's name and records it as the plugin's.
func TestClusterApply_RemovingItsOwnNeo4jPluginsKeepsThePluginsNames(t *testing.T) {
	for name, tc := range map[string]struct {
		fleet bool
		want  []string
	}{
		"Aura Fleet Management off": {want: []string{"graph-data-science"}},
		"Aura Fleet Management on":  {fleet: true, want: []string{"graph-data-science", "fleet-management"}},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := specEnvGDSCluster(tc.fleet)
			r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{gdsPlugin(), cluster}, readyClusterPods()...)...)
			clusterTemplateApply(t, r.Client, cluster)
			rollOut(t, r.Client, "c-server")
			installAndRoll(t, r, "gds", "c-server")
			clusterTemplateApply(t, r.Client, cluster)
			rollOut(t, r.Client, "c-server")
			reconcilePlugin(t, r, "gds")
			drainEvents(rec)
			before := *writes

			// The user takes NEO4J_PLUGINS out of spec.env.
			cluster.Spec.Env = nil
			clusterTemplateApply(t, r.Client, cluster)
			require.Equal(t, before+1, *writes, "the cluster's write")

			sts := liveSTS(t, r.Client, "c-server")
			assert.JSONEq(t, mustJSON(t, tc.want), neo4jPlugins(t, sts),
				"the plugin's name stays; apoc, which no plugin installs, goes")
			assert.Equal(t, "graph-data-science", sts.Annotations[pluginEntriesAnnotation("gds")], "recorded as the plugin's")
			_, owned := readOwnedEnvVarNames(sts)["NEO4J_PLUGINS"]
			assert.False(t, owned, "no longer the cluster's")
			assert.Equal(t, tc.fleet, fleetEntryRecorded(sts), "fleet-management is Fleet Management's, recorded in the same write")

			rollOut(t, r.Client, "c-server")
			reconcilePlugin(t, r, "gds")
			clusterTemplateApply(t, r.Client, cluster)
			reconcilePlugin(t, r, "gds")
			assert.Equal(t, before+1, *writes, "one roll: the plugin finds its name and its record in place, and writes nothing")
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing))
			assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "gds").Status.Phase)
		})
	}
}

// The standalone has the same shape: its template write dropped a variable it
// stopped owning, its plugins' names with it.
func TestStandaloneApply_RemovingItsOwnNeo4jPluginsKeepsThePluginsNames(t *testing.T) {
	for name, tc := range map[string]struct {
		fleet bool
		want  []string
	}{
		"Aura Fleet Management off": {want: []string{"graph-data-science"}},
		"Aura Fleet Management on":  {fleet: true, want: []string{"graph-data-science", "fleet-management"}},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
				ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: selfHealNS, Generation: 1},
				Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
					AcceptLicenseAgreement: "eval",
					Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.32-enterprise"},
					Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
					Env:                    []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["apoc","graph-data-science"]`}},
				},
				Status: neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: neo4jv1beta1.PhaseReady, ObservedGeneration: 1},
			}
			if tc.fleet {
				standalone.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
			}
			gds := gdsPlugin()
			gds.Spec.ClusterRef = "sa"
			r, rec, _, _ := gateReconciler(t, nil, gds, standalone, gatePod("sa-0", map[string]string{"app": "sa"}))
			sr := &Neo4jEnterpriseStandaloneReconciler{Client: r.Client, Scheme: newTestScheme(), Recorder: record.NewFakeRecorder(100)}
			apply := func() {
				t.Helper()
				live := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
				require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(standalone), live))
				require.NoError(t, sr.reconcileStatefulSet(ctx, live))
			}
			apply()
			rollOut(t, r.Client, "sa")
			installAndRoll(t, r, "gds", "sa")
			apply()
			rollOut(t, r.Client, "sa")
			reconcilePlugin(t, r, "gds")
			drainEvents(rec)
			// Rolls are counted as template changes: on this fake API server
			// every standalone apply sends an Update (it sets the update
			// strategy the fake defaults), which the real one takes as a no-op.
			before := liveSTS(t, r.Client, "sa").Spec.Template

			live := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
			require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(standalone), live))
			require.NoError(t, patchObject(ctx, r.Client, live, func() { live.Spec.Env = nil }))
			apply()

			sts := liveSTS(t, r.Client, "sa")
			assert.False(t, equality.Semantic.DeepEqual(before, sts.Spec.Template), "the standalone's write rolls the pod")
			assert.JSONEq(t, mustJSON(t, tc.want), neo4jPlugins(t, sts))
			assert.Equal(t, "graph-data-science", sts.Annotations[pluginEntriesAnnotation("gds")])

			rollOut(t, r.Client, "sa")
			reconcilePlugin(t, r, "gds")
			apply()
			reconcilePlugin(t, r, "gds")
			assert.True(t, equality.Semantic.DeepEqual(sts.Spec.Template, liveSTS(t, r.Client, "sa").Spec.Template),
				"one roll: the plugin finds its name and its record in place, and writes nothing")
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing))
		})
	}
}

// The plugins a hand-over keeps are the ones installed: Ready, or with a record
// of their own on the StatefulSet. One that never installed (refused while
// the cluster's own list lacked its name), a disabled one, another cluster's,
// and one not in Managed mode add nothing.
func TestInstalledPluginEntries(t *testing.T) {
	sts := selfHealSTS()
	sts.Annotations = map[string]string{pluginEntriesAnnotation("recorded"): "apoc"}

	ready := gdsPlugin()
	ready.Name, ready.Status.Phase = "ready", neo4jv1beta1.PhaseReady
	ready.Spec.Dependencies = []neo4jv1beta1.PluginDependency{{Name: "apoc"}}
	recorded := selfHealPlugin("recorded", "c", neo4jv1beta1.PhaseInstalling, "")
	refused := selfHealPlugin("refused", "c", "Failed", "Plugin installation refused: …")
	refused.Spec.Name = "bloom"
	disabled := selfHealPlugin("disabled", "c", neo4jv1beta1.PhaseReady, "")
	disabled.Spec.Enabled = false
	elsewhere := selfHealPlugin("elsewhere", "other", neo4jv1beta1.PhaseReady, "")
	prebaked := selfHealPlugin("prebaked", "c", neo4jv1beta1.PhaseReady, "")
	prebaked.Spec.InstallMode = PluginInstallModePreBaked

	got := installedPluginEntries(sts, []neo4jv1beta1.Neo4jPlugin{*ready, *recorded, *refused, *disabled, *elsewhere, *prebaked},
		"c", "5.26.32-enterprise")
	assert.Equal(t, map[string][]string{
		"ready":    {"graph-data-science", "apoc"},
		"recorded": {"apoc"},
	}, got)
}

func mustJSON(t *testing.T, names []string) string {
	t.Helper()
	v, err := MergeNeo4jPluginList("", names[0])
	require.NoError(t, err)
	for _, n := range names[1:] {
		v, err = MergeNeo4jPluginList(v, n)
		require.NoError(t, err)
	}
	return v
}
