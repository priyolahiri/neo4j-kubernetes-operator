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

// A procedure or extension list a cluster's spec.env sets as a
// NEO4J_<setting> variable is in its neo4j.conf, not on its StatefulSet
// (resources.SpecEnvLists; operations.md id 209). On the StatefulSet the
// cluster controller owned the variable: the plugins left it alone, it put its
// own value back over theirs, and the servers lost the plugins' entries — GDS
// Ready with 75 of its and APOC's procedures sandboxed (v1.23 journey, fm526).
//
// The write that moves such a variable into the conf stops owning it on the
// StatefulSet. Dropping it there would leave the plugins' entries off the
// servers until each plugin noticed and wrote them back — a second roll. So
// that write hands the variable over: it becomes what the plugins that have
// written to the StatefulSet ask for in it, as their own writes would have
// made it (setClusterPluginLists), and the plugins find nothing to do.

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// handOverSharedLists returns merged — the neo4j container's env the cluster
// controller is about to write — with each shared list variable it stops
// owning in this write given to the cluster's plugins: a variable it owned
// (previousOwned) that the live env (current) has and its desired env no
// longer does gets the value the plugins written to sts ask for in it
// (pluginListSettings), or goes when they ask for nothing. A list the plugins
// cannot be read for is dropped, as before; each plugin then writes its own
// back.
func (r *Neo4jEnterpriseClusterReconciler) handOverSharedLists(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	sts *appsv1.StatefulSet, merged, current, desired []corev1.EnvVar, previousOwned map[string]struct{}) []corev1.EnvVar {
	inDesired := envByName(desired)
	var released []string
	for _, e := range current {
		_, owned := previousOwned[e.Name]
		_, still := inDesired[e.Name]
		if owned && !still && resources.IsAdditiveConfEnvName(e.Name) {
			released = append(released, e.Name)
		}
	}
	if len(released) == 0 {
		return merged
	}
	plugins := &neo4jv1beta1.Neo4jPluginList{}
	if err := r.List(ctx, plugins, client.InNamespace(cluster.Namespace)); err != nil {
		log.FromContext(ctx).V(1).Info("Cannot list the cluster's plugins; its plugins write their list entries back themselves",
			"error", err.Error())
		return merged
	}
	values := writtenPluginLists(cluster, sts, plugins.Items)
	for _, name := range released {
		if value := values[name]; value != "" {
			merged = append(merged, corev1.EnvVar{Name: name, Value: value})
		}
	}
	return merged
}

// writtenPluginLists returns, by variable name, the union of what the
// cluster's plugins that have written to sts ask for in each shared list:
// enabled, not refused by the cluster's image (pluginOnImage), not being
// deleted, and with a record of their own on sts. A plugin that has not
// written yet puts its tokens there with its own write (operations.md id 173).
func writtenPluginLists(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet, plugins []neo4jv1beta1.Neo4jPlugin) map[string]string {
	values := map[string]string{}
	for i := range plugins {
		p := &plugins[i]
		if p.Spec.ClusterRef != cluster.Name || p.DeletionTimestamp != nil || !pluginOnImage(p, cluster.Spec.Image.Tag) {
			continue
		}
		_, env := sts.Annotations[pluginEnvVarsAnnotation(p.Name)]
		_, entries := sts.Annotations[pluginEntriesAnnotation(p.Name)]
		if !env && !entries {
			continue
		}
		for key, tokens := range pluginListSettings(p) {
			name := resources.Neo4jSettingEnvVarName(key)
			values[name] = resources.MergeConfListValues(values[name], tokens)
		}
	}
	return values
}
