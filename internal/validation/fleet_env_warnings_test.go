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
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A NEO4J_PLUGINS a deployment's own spec.env sets is the deployment's: the
// operator adds Aura Fleet Management's fleet-management to a JSON list given
// as a value, and cannot to one read from a Secret or ConfigMap, or to a value
// that is not a JSON list. Each validator says so as a ValidationWarning
// (operations.md id 223); nothing else does.
func TestFleetPluginsEnvWarnings(t *testing.T) {
	fromSecret := corev1.EnvVar{Name: "NEO4J_PLUGINS", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"}}}
	on := &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
	const lts = "5.26.32-enterprise"

	cases := []struct {
		name  string
		env   []corev1.EnvVar
		fleet *neo4jv1beta1.AuraFleetManagementSpec
		tag   string
		want  string
	}{
		{"from a Secret", []corev1.EnvVar{{Name: "A", Value: "1"}, fromSecret}, on, lts,
			"spec.env[1] (NEO4J_PLUGINS) comes from a Secret or ConfigMap, which the operator cannot read: Aura Fleet Management needs fleet-management in that list, and the operator cannot add it"},
		{"not a JSON list", []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: "apoc"}}, on, lts,
			`spec.env[0] (NEO4J_PLUGINS) is not a JSON list such as ["apoc"]: Aura Fleet Management needs fleet-management in that list, and the operator cannot add it`},
		{"a list of numbers", []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: "[1]"}}, on, lts, "is not a JSON list"},
		{"a JSON list: the operator adds the name", []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["apoc"]`}}, on, lts, ""},
		{"empty: the operator adds the name", []corev1.EnvVar{{Name: "NEO4J_PLUGINS"}}, on, lts, ""},
		{"Fleet Manager built in", []corev1.EnvVar{fromSecret}, on, "2026.08.1-enterprise", ""},
		{"Fleet Management off", []corev1.EnvVar{fromSecret}, &neo4jv1beta1.AuraFleetManagementSpec{}, lts, ""},
		{"no block", []corev1.EnvVar{fromSecret}, nil, lts, ""},
		{"no NEO4J_PLUGINS", []corev1.EnvVar{{Name: "A", Value: "apoc"}}, on, lts, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := FleetPluginsEnvWarnings(tc.env, tc.fleet, tc.tag)
			if tc.want == "" {
				assert.Empty(t, got)
				return
			}
			require.Len(t, got, 1)
			assert.Contains(t, got[0], tc.want)
		})
	}

	// Both validators raise it.
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "c"}}
	cluster.Spec.Image.Tag, cluster.Spec.AuraFleetManagement, cluster.Spec.Env = lts, on, []corev1.EnvVar{fromSecret}
	assert.Len(t, (&ClusterValidator{}).NoEffectWarnings(cluster), 1)
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{Name: "sa"}}
	standalone.Spec.Image.Tag, standalone.Spec.AuraFleetManagement, standalone.Spec.Env = lts, on, []corev1.EnvVar{fromSecret}
	assert.Len(t, (&StandaloneValidator{}).NoEffectWarnings(standalone), 1)
}
