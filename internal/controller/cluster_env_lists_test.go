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

// A cluster's own spec.env list joins its plugins' lists; it never replaces
// them (operations.md id 209). The v1.23 journey (fm526, 5.26.32, Aura Fleet
// Management, Managed APOC and GDS): with dbms.security.procedures.unrestricted
// in spec.config every server listed custom.*, fleetManagement.*, gds.* and
// apoc.load.*; moved to spec.env, the cluster's variable replaced GDS's on the
// StatefulSet and the servers listed fleetManagement.* and custom.* alone,
// with 75 procedures sandboxed and GDS Ready.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// fm526 is cluster "c" as the journey ran it: 5.26.32, Aura Fleet
// Management, and the unrestricted list custom.* in spec.config or spec.env.
func fm526(inEnv bool) *neo4jv1beta1.Neo4jEnterpriseCluster {
	cluster := readyCluster()
	cluster.Spec.Image.Tag = "5.26.32-enterprise"
	cluster.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
	if inEnv {
		cluster.Spec.Env = []corev1.EnvVar{{Name: unrestrictedVar, Value: "custom.*"}}
	} else {
		cluster.Spec.Config = map[string]string{unrestrictedSetting: "custom.*"}
	}
	return cluster
}

// clusterServerWrites applies the cluster's server StatefulSet as its
// controller's pass does. The fake client sets no UID, which the controller
// reads as "not created yet"; the API server's is stood in for.
func clusterServerWrites(t *testing.T, c client.Client, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	t.Helper()
	clusters := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: newTestScheme()}
	require.NoError(t, clusters.createOrUpdateResource(context.Background(), resources.BuildServerStatefulSetForEnterprise(cluster), cluster))
	if sts := liveSTS(t, c, "c-server"); sts.UID == "" {
		sts.UID = "c-server-uid"
		require.NoError(t, c.Update(context.Background(), sts))
	}
}

// The list in spec.env reaches the servers as the same list in spec.config
// does: the cluster's entries, Aura Fleet Management's and the plugins'.
func TestServers_AClustersSpecEnvListJoinsThePlugins(t *testing.T) {
	for name, inEnv := range map[string]bool{"spec.config": false, "spec.env": true} {
		t.Run(name, func(t *testing.T) {
			cluster := fm526(inEnv)
			cm := resources.BuildConfigMapForEnterprise(cluster)
			apoc := selfHealPlugin("apoc", "c", "", "")
			r, rec, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, gdsPlugin(), cluster, cm}, readyClusterPods()...)...)
			clusterServerWrites(t, r.Client, cluster)
			rollOut(t, r.Client, "c-server")

			installAndRoll(t, r, "apoc", "c-server")
			installAndRoll(t, r, "gds", "c-server")
			// The cluster's next pass, then the plugins' settled ones.
			clusterServerWrites(t, r.Client, cluster)
			drainEvents(rec)
			reconcilePlugin(t, r, "apoc")
			reconcilePlugin(t, r, "gds")
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing), "nothing to put back")

			lists := serverLists(t, cm, liveSTSEnv(t, r.Client, "c-server"))
			assert.Equal(t, []string{"apoc.load.*", "custom.*", "fleetManagement.*", "gds.*"}, tokensOf(lists[unrestrictedSetting]),
				"the cluster's list, Aura Fleet Management's and GDS's; no image entries, since the cluster sets the list")
			_, owned := readOwnedEnvVarNames(liveSTS(t, r.Client, "c-server"))[unrestrictedVar]
			assert.False(t, owned, "the variable is the plugins'")
		})
	}
}

// The operator upgrade: the StatefulSet an earlier operator wrote has the
// cluster's own value in the variable, recorded as the cluster's, and GDS's
// tokens gone. The cluster's first pass moves its list into neo4j.conf and, in
// the same write, gives the variable to the plugins — one roll; the plugins'
// settled passes then write nothing.
func TestClusterApply_HandsAListItMovesToTheConfToThePlugins(t *testing.T) {
	cluster := fm526(true)
	apoc := selfHealPlugin("apoc", "c", "", "")
	r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{apoc, gdsPlugin(), cluster}, readyClusterPods()...)...)
	clusterServerWrites(t, r.Client, cluster)
	rollOut(t, r.Client, "c-server")
	installAndRoll(t, r, "apoc", "c-server")
	installAndRoll(t, r, "gds", "c-server")

	// As an earlier operator left it: the cluster's own value in the
	// variable, the variable in the cluster's record.
	sts := liveSTS(t, r.Client, "c-server")
	container := neo4jContainer(&sts.Spec.Template.Spec)
	kept := container.Env[:0:0]
	for _, e := range container.Env {
		if e.Name != unrestrictedVar {
			kept = append(kept, e)
		}
	}
	kept = append(kept, corev1.EnvVar{Name: unrestrictedVar, Value: "custom.*"})
	container.Env = kept
	owned := map[string]string{unrestrictedVar: ""}
	for name := range readOwnedEnvVarNames(sts) {
		owned[name] = ""
	}
	sts.Annotations[ownedEnvVarsAnnotation] = joinSortedKeys(owned)
	require.NoError(t, r.Update(context.Background(), sts))
	rollOut(t, r.Client, "c-server")
	before := *writes
	drainEvents(rec)

	clusterServerWrites(t, r.Client, cluster)

	assert.Equal(t, before+1, *writes, "one write, so one roll")
	v, _ := envValueOf(t, r, "c-server", unrestrictedVar)
	assert.Equal(t, "gds.*,apoc.load.*", v, "the variable is what GDS asks for")
	_, stillOwned := readOwnedEnvVarNames(liveSTS(t, r.Client, "c-server"))[unrestrictedVar]
	assert.False(t, stillOwned)
	rollOut(t, r.Client, "c-server")
	reconcilePlugin(t, r, "apoc")
	reconcilePlugin(t, r, "gds")
	assert.Equal(t, before+1, *writes, "the plugins find their entries there")
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing))

	lists := serverLists(t, resources.BuildConfigMapForEnterprise(cluster), liveSTSEnv(t, r.Client, "c-server"))
	assert.Equal(t, []string{"apoc.load.*", "custom.*", "fleetManagement.*", "gds.*"}, tokensOf(lists[unrestrictedSetting]))
}

// A variable the cluster's spec.env takes from a Secret stays a variable the
// cluster sets; nothing is handed over, and the validator says what it costs.
func TestClusterApply_AListFromASecretStaysTheClusters(t *testing.T) {
	cluster := fm526(true)
	cluster.Spec.Env[0] = corev1.EnvVar{Name: unrestrictedVar, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"}}}
	r, _, _, _ := gateReconciler(t, nil, append([]client.Object{gdsPlugin(), cluster}, readyClusterPods()...)...)
	clusterServerWrites(t, r.Client, cluster)
	sts := liveSTS(t, r.Client, "c-server")
	e, ok := findEnvVar(neo4jContainer(&sts.Spec.Template.Spec), unrestrictedVar)
	require.True(t, ok)
	assert.NotNil(t, e.ValueFrom)
	_, owned := readOwnedEnvVarNames(sts)[unrestrictedVar]
	assert.True(t, owned)
	_, inConf := resources.Neo4jConfSettings(resources.BuildConfigMapForEnterprise(cluster).Data["neo4j.conf"])[unrestrictedSetting]
	assert.True(t, inConf, "Aura Fleet Management's line")
	assert.NotContains(t, resources.Neo4jConfSettings(resources.BuildConfigMapForEnterprise(cluster).Data["neo4j.conf"])[unrestrictedSetting], "custom")
}

// writtenPluginLists counts only the plugins that have written to the
// StatefulSet, on the cluster's image, not being deleted.
func TestWrittenPluginLists(t *testing.T) {
	cluster := fm526(true)
	written := gdsPlugin()
	notYet := allowingPlugin("genai-plugin", "genai", "genai.*")
	sts := &appsv1.StatefulSet{}
	sts.Annotations = map[string]string{pluginEntriesAnnotation("gds"): "graph-data-science"}
	got := writtenPluginLists(cluster, sts, []neo4jv1beta1.Neo4jPlugin{*written, *notYet})
	assert.Equal(t, map[string]string{unrestrictedVar: "gds.*,apoc.load.*"}, got)
}
