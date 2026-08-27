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
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// validateNetworkPolicy checks spec.networkPolicy shape. Schema markers
// (+kubebuilder:validation:Required on NetworkPolicyPeerCluster.Name) catch
// a missing field but not an explicitly-empty string, same reasoning as the
// analogous UpstreamClusterRef/UpstreamBackupRef checks on
// Neo4jReplicaDatabase.
func validateNetworkPolicy(spec *neo4jv1beta1.NetworkPolicySpec, path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec == nil {
		return allErrs
	}
	for i, peer := range spec.AllowReplicasFrom {
		if peer.Name == "" {
			allErrs = append(allErrs, field.Required(
				path.Child("allowReplicasFrom").Index(i).Child("name"),
				"required for each entry"))
		}
	}
	return allErrs
}
