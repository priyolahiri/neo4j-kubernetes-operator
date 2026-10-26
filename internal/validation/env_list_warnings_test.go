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

// A procedure list a deployment's spec.env takes from a Secret or ConfigMap
// cannot be joined with the others (operations.md id 209): each validator
// says what it costs there, as a ValidationWarning. A list given as a value,
// and any other variable from a Secret, say nothing.
func TestSpecEnvListWarnings(t *testing.T) {
	fromSecret := &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "s"}, Key: "k"}}
	env := []corev1.EnvVar{
		{Name: "NEO4J_dbms_security_procedures_unrestricted", Value: "custom.*"},
		{Name: "NEO4J_server_memory_pagecache_size", ValueFrom: fromSecret},
		{Name: "NEO4J_dbms_security_procedures_allowlist", ValueFrom: fromSecret},
	}

	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "c"}}
	cluster.Spec.Env = env
	got := (&ClusterValidator{}).NoEffectWarnings(cluster)
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "spec.env[2] (NEO4J_dbms_security_procedures_allowlist) comes from a Secret or ConfigMap")
	assert.Contains(t, got[0], "its Neo4jPlugins cannot add their entries for dbms.security.procedures.allowlist")

	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{Name: "sa"}}
	standalone.Spec.Env = env
	got = (&StandaloneValidator{}).NoEffectWarnings(standalone)
	require.Len(t, got, 1)
	assert.Contains(t, got[0], "writes it in place of the whole dbms.security.procedures.allowlist")

	assert.Empty(t, SpecEnvListWarnings(env[:2], false))
}
