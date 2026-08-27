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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestValidateNetworkPolicy(t *testing.T) {
	tests := []struct {
		name           string
		spec           *neo4jv1beta1.NetworkPolicySpec
		expectedErrors int
	}{
		{name: "nil spec — no errors"},
		{name: "disabled, no peers — no errors", spec: &neo4jv1beta1.NetworkPolicySpec{}},
		{
			name: "one valid peer — no errors",
			spec: &neo4jv1beta1.NetworkPolicySpec{
				Enabled:           true,
				AllowReplicasFrom: []neo4jv1beta1.NetworkPolicyPeerCluster{{Name: "dr-cluster", Namespace: "dr"}},
			},
		},
		{
			name: "peer with empty name — rejected",
			spec: &neo4jv1beta1.NetworkPolicySpec{
				Enabled:           true,
				AllowReplicasFrom: []neo4jv1beta1.NetworkPolicyPeerCluster{{Namespace: "dr"}},
			},
			expectedErrors: 1,
		},
		{
			name: "two peers, second empty name — rejected",
			spec: &neo4jv1beta1.NetworkPolicySpec{
				Enabled: true,
				AllowReplicasFrom: []neo4jv1beta1.NetworkPolicyPeerCluster{
					{Name: "dr-cluster", Namespace: "dr"},
					{Namespace: "qa"},
				},
			},
			expectedErrors: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validateNetworkPolicy(tt.spec, field.NewPath("spec", "networkPolicy"))
			assert.Len(t, errs, tt.expectedErrors)
		})
	}
}
