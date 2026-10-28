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

// A deployment whose own spec.env sets NEO4J_PLUGINS owns that variable: the
// cluster's or the standalone's record of its env lists it, and the plugin
// controller refuses to write it (pluginEnvOwnedError, operations.md id 135).
// Aura Fleet Management's merge had no such guard. On a cluster it added
// fleet-management to the variable, and the cluster's template apply put its
// own value back in the same pass, so the servers restarted without end
// (#608: revision 9 to 32 in six minutes, phase Forming, no ERROR).
//
// spec.env and spec.auraFleetManagement are both the deployment's own spec,
// so the deployment's own write puts the name there (operations.md id 223):
//   - a cluster renders fleet-management into its own NEO4J_PLUGINS while its
//     spec installs the plugin (clusterNeo4jPlugins), in the template it
//     applies and records, so the template apply and the fleet merge agree;
//     the conf roll's restart stamp carries a change of it (carryOwnNeo4jPlugins),
//     so turning Fleet Management on or off rolls the servers once;
//   - a standalone adds it in its template write (reconcileStatefulSet), which
//     its conf hash moves on the same toggle; its controller never puts a live
//     value back, so it never looped.
//
// The fleet merge never writes a variable the deployment owns. One it cannot
// add to — read from a Secret or ConfigMap, or a value that is not a JSON list
// — is left as it is, and the validator says so (FleetPluginsEnvWarnings).

import (
	"context"
	"errors"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// ownNeo4jPluginsWithFleet returns e, the NEO4J_PLUGINS variable of a
// deployment's own spec.env, with fleet-management added while the
// deployment's spec installs Aura Fleet Management (installs) on an image whose
// entrypoint installs the plugin: not one with Fleet Manager built in
// (2026.03 and later, operations.md id 192), where the name is left to the
// user. MergeNeo4jPluginList keeps the user's names and order and appends the
// name once; a list that names it already is returned byte for byte. A
// variable read from a Secret or ConfigMap, or a value that is not a JSON list,
// is returned as it is: nothing can be added to it.
func ownNeo4jPluginsWithFleet(e corev1.EnvVar, installs bool, imageTag string) corev1.EnvVar {
	if !installs || e.ValueFrom != nil || validation.FleetManagementBuiltIn(imageTag) {
		return e
	}
	merged, err := MergeNeo4jPluginList(e.Value, fleetManagementPluginEntry)
	if err != nil {
		return e
	}
	e.Value = merged
	return e
}

// clusterNeo4jPlugins returns the NEO4J_PLUGINS variable the cluster renders
// from its own spec.env — the builder copies it as it is
// (resources.BuildPodSpecForEnterprise) — with fleet-management added
// (ownNeo4jPluginsWithFleet), judged on spec.image.tag, the image the builder
// renders. False when spec.env does not set the variable: it is then shared by
// the plugin, fleet and Aura writers, never the cluster's.
func clusterNeo4jPlugins(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (corev1.EnvVar, bool) {
	for _, e := range cluster.Spec.Env {
		if e.Name == "NEO4J_PLUGINS" {
			return ownNeo4jPluginsWithFleet(e, clusterInstallsFleetManagement(cluster), cluster.Spec.Image.Tag), true
		}
	}
	return corev1.EnvVar{}, false
}

// buildServerStatefulSet is the server StatefulSet the cluster applies:
// resources.BuildServerStatefulSetForEnterprise, with the cluster's own
// NEO4J_PLUGINS as it renders it (clusterNeo4jPlugins). Both the normal pass
// and the upgrade's Staging build it here, so a template the upgrade writes
// keeps the name, and drops it on an image with Fleet Manager built in.
// Nothing changes for a cluster whose spec.env does not set NEO4J_PLUGINS:
// the static template never carries the variable.
func buildServerStatefulSet(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *appsv1.StatefulSet {
	sts := resources.BuildServerStatefulSetForEnterprise(cluster)
	rendered, own := clusterNeo4jPlugins(cluster)
	if !own {
		return sts
	}
	if container := neo4jContainer(&sts.Spec.Template.Spec); container != nil {
		for i := range container.Env {
			if container.Env[i].Name == "NEO4J_PLUGINS" {
				container.Env[i] = rendered
				break
			}
		}
	}
	return sts
}

// carryOwnNeo4jPlugins writes the cluster's own NEO4J_PLUGINS, as it renders
// it now, into sts — the server StatefulSet the conf roll's restart stamp is
// about to write (triggerRollingRestartForConfigChange) — and reports whether
// that changed it. Turning Aura Fleet Management on or off changes neo4j.conf
// (its procedure lines) and, for a cluster whose spec.env sets NEO4J_PLUGINS,
// the variable: carried here, both reach the servers in one write, one roll,
// and the template apply later in the pass finds the variable as it wants it.
// Only a plain value the cluster owns on sts (its record lists the variable)
// and still renders; anything else is the template apply's.
func carryOwnNeo4jPlugins(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, sts *appsv1.StatefulSet) bool {
	rendered, own := clusterNeo4jPlugins(cluster)
	if !own || rendered.ValueFrom != nil || !neo4jPluginsOwnedByTarget(sts) {
		return false
	}
	container := neo4jContainer(&sts.Spec.Template.Spec)
	if container == nil {
		return false
	}
	for i, e := range container.Env {
		if e.Name != "NEO4J_PLUGINS" {
			continue
		}
		if e.ValueFrom != nil || e.Value == rendered.Value {
			return false
		}
		env := append([]corev1.EnvVar(nil), container.Env...)
		env[i].Value = rendered.Value
		container.Env = env
		return true
	}
	return false
}

// errNeo4jPluginsNotAList marks a NEO4J_PLUGINS value that is not a JSON list,
// which nothing can merge a name into (addFleetManagementPlugin).
var errNeo4jPluginsNotAList = errors.New("NEO4J_PLUGINS is not a JSON list")

// addFleetEntryInWrite is addFleetManagementPlugin for a write the deployment
// makes of its own template — the cluster's create and template apply, the
// standalone's template write — after the record of the env the deployment
// owns is written on sts. It records the name as Fleet Management's when it
// adds it to a variable the deployment does not own (operations.md id 222),
// and a value that is not a JSON list does not fail the write: the merge
// reports one that is not the deployment's, the validator one that is
// (FleetPluginsEnvWarnings). On a cluster a NEO4J_PLUGINS its spec.env sets
// already carries the name as it renders it (clusterNeo4jPlugins), or cannot
// take it, so nothing is added there; a standalone's gets it here.
func addFleetEntryInWrite(ctx context.Context, sts *appsv1.StatefulSet) error {
	added, err := addFleetManagementPlugin(&sts.Spec.Template)
	if errors.Is(err, errNeo4jPluginsNotAList) {
		log.FromContext(ctx).V(1).Info("NEO4J_PLUGINS is not a JSON list; fleet-management is not added to it",
			"statefulSet", sts.Name, "reason", err.Error())
		return nil
	}
	if err != nil {
		return err
	}
	if added && !neo4jPluginsOwnedByTarget(sts) {
		setFleetEntryRecord(sts, true)
	}
	return nil
}
