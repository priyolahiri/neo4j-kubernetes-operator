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
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// fleetManagementPluginEntry is the NEO4J_PLUGINS name of the Aura Fleet
// Management plugin.
const fleetManagementPluginEntry = "fleet-management"

// errNoNeo4jContainer is returned for a pod template without a neo4j container.
var errNoNeo4jContainer = errors.New("neo4j container not found")

// clusterInstallsFleetManagement and standaloneInstallsFleetManagement report
// whether the deployment's own spec installs the fleet-management plugin
// (spec.auraFleetManagement.enabled).
func clusterInstallsFleetManagement(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	return cluster.Spec.AuraFleetManagement != nil && cluster.Spec.AuraFleetManagement.Enabled
}

func standaloneInstallsFleetManagement(standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) bool {
	return standalone.Spec.AuraFleetManagement != nil && standalone.Spec.AuraFleetManagement.Enabled
}

// addFleetManagementPlugin merges fleet-management into the NEO4J_PLUGINS
// variable of the template's neo4j container, with MergeNeo4jPluginList, and
// reports whether that changed the template. It writes a new env list, never
// into the one the template had, so a caller whose template shares its arrays
// with another (a struct copy does) leaves that other alone; the containers
// array itself is the caller's to copy.
//
// The static server template never carries NEO4J_PLUGINS: the plugin, fleet
// and Aura writers share the variable. This is applied to the StatefulSet being
// written — by the fleet merge of every pass, and by the write that creates
// the StatefulSet (operations.md id 175) — never to what the cluster or the
// standalone records as its own env.
func addFleetManagementPlugin(template *corev1.PodTemplateSpec) (bool, error) {
	idx := -1
	for i := range template.Spec.Containers {
		if template.Spec.Containers[i].Name == "neo4j" {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false, errNoNeo4jContainer
	}
	env := template.Spec.Containers[idx].Env
	at := -1
	current := ""
	for i := range env {
		if env[i].Name == "NEO4J_PLUGINS" {
			at, current = i, env[i].Value
			break
		}
	}
	updated, err := MergeNeo4jPluginList(current, fleetManagementPluginEntry)
	if err != nil {
		return false, fmt.Errorf("merge plugin list: %w", err)
	}
	if at >= 0 && updated == current {
		return false, nil // already listed
	}
	merged := append([]corev1.EnvVar(nil), env...)
	if at < 0 {
		merged = append(merged, corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: updated})
	} else {
		merged[at].Value = updated
	}
	template.Spec.Containers[idx].Env = merged
	return true, nil
}
