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

package validation

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// SpecEnvListWarnings says, for each procedure or extension list a
// deployment's spec.env takes from a Secret or ConfigMap, what that costs
// (operations.md id 209). A list given as a value is joined into the
// deployment's neo4j.conf with the rest (resources.SpecEnvListKey); one read
// from elsewhere the operator cannot see, so it stays a variable:
//   - on a cluster the servers' startup script joins it with the cluster's own
//     entries, but the plugins cannot write theirs into a variable the cluster
//     sets, so the servers start without them;
//   - on a standalone Neo4j's entrypoint writes it in place of the whole list,
//     so its spec.config's, Aura Fleet Management's and its plugins' entries
//     are not on the server.
//
// Advisory: a list a user keeps in a Secret is theirs to keep.
func SpecEnvListWarnings(env []corev1.EnvVar, standalone bool) []string {
	var warnings []string
	for i, e := range env {
		key, ok := resources.AdditiveConfKeyForEnvName(e.Name)
		if !ok || e.ValueFrom == nil {
			continue
		}
		path := field.NewPath("spec", "env").Index(i)
		if standalone {
			warnings = append(warnings, fmt.Sprintf(
				"%s (%s) comes from a Secret or ConfigMap, which the operator cannot read, so it stays a variable, and Neo4j's "+
					"entrypoint writes it in place of the whole %s: the entries of the standalone's spec.config, of Aura Fleet "+
					"Management and of its Neo4jPlugins are not on the server. Set the list as a value in spec.env, or in "+
					"spec.config, and they are joined with it",
				path, e.Name, key))
			continue
		}
		warnings = append(warnings, fmt.Sprintf(
			"%s (%s) comes from a Secret or ConfigMap, which the operator cannot read, so it stays a variable that the "+
				"cluster sets: its Neo4jPlugins cannot add their entries for %s to it, and the servers start without them. "+
				"Set the list as a value in spec.env, or in spec.config, and the plugins' entries are joined with it",
			path, e.Name, key))
	}
	return warnings
}
