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
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// fleetManagementPluginEntry is the NEO4J_PLUGINS name of the Aura Fleet
// Management plugin.
const fleetManagementPluginEntry = "fleet-management"

// errNoNeo4jContainer is returned for a pod template without a neo4j container.
var errNoNeo4jContainer = errors.New("neo4j container not found")

// fleetPluginWriteError marks the fleet merge's write of the StatefulSet
// failing, as opposed to the read or the inspection of its template before it.
// On an image with Fleet Manager built in (2026.03+) the merge never writes
// (addFleetManagementPlugin adds nothing, id 192), so a failure there is never
// a failure to add the plugin.
type fleetPluginWriteError struct{ err error }

func (e *fleetPluginWriteError) Error() string { return e.err.Error() }
func (e *fleetPluginWriteError) Unwrap() error { return e.err }

// fleetPluginPatchFailure words AuraFleetManagementPluginPatchFailed, and the
// line logged with it, for what failed: the write that adds fleet-management
// to NEO4J_PLUGINS, or the check before it — the read of the server
// StatefulSet, its neo4j container, the list already there — which is all a
// pass does on an image with Fleet Manager built in. It said "Failed to add
// fleet-management to NEO4J_PLUGINS" for both, on images where nothing is ever
// added (knowledge rule 192).
func fleetPluginPatchFailure(err error) (event, logMsg string) {
	var write *fleetPluginWriteError
	if errors.As(err, &write) {
		return fmt.Sprintf("Failed to add fleet-management to NEO4J_PLUGINS: %v", err),
			"Could not add fleet-management to NEO4J_PLUGINS; trying again on the next pass"
	}
	return fmt.Sprintf("Could not check the server StatefulSet's NEO4J_PLUGINS for fleet-management: %v", err),
		"Could not check the server StatefulSet's NEO4J_PLUGINS for fleet-management; trying again on the next pass"
}

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
//
// Not on an image with Fleet Manager built in (2026.03 and later,
// validation.FleetManagementBuiltIn on the template's own image, so a held
// upgrade's template is judged by the image it rolls out): its entrypoint
// cannot install the plugin — it wgets "null" and starts without it — and the
// procedures are the server's own (operations.md id 192). A name an earlier
// write put there stays: removing it would restart every server for an entry
// the entrypoint skips.
//
// Never into a NEO4J_PLUGINS read from a Secret or ConfigMap: a value beside
// valueFrom is an env var the API server refuses, so the write failed, and
// what the source holds cannot be read (operations.md id 223). A value that
// is not a JSON list is errNeo4jPluginsNotAList.
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
	if validation.FleetManagementBuiltIn(imageTag(template.Spec.Containers[idx].Image)) {
		return false, nil
	}
	env := template.Spec.Containers[idx].Env
	at := -1
	current := ""
	for i := range env {
		if env[i].Name == "NEO4J_PLUGINS" {
			if env[i].ValueFrom != nil {
				return false, nil
			}
			at, current = i, env[i].Value
			break
		}
	}
	updated, err := MergeNeo4jPluginList(current, fleetManagementPluginEntry)
	if err != nil {
		return false, fmt.Errorf("merge plugin list: %w: %w", errNeo4jPluginsNotAList, err)
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
