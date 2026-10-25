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

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every kind that names a deployment: connected, a deployment that does not
// exist yet is pending, as the operator waits for it (operations.md id 182).
// The database, user, role, binding and auth rule reported it as an error,
// and the alias, composite, replica, backup and plugin said nothing.
func TestValidate_Connected_AMissingDeploymentIsPendingOnEveryKind(t *testing.T) {
	const head = "apiVersion: neo4j.neo4j.com/v1beta1\n"
	manifests := map[string]string{
		"Neo4jDatabase": head + "kind: Neo4jDatabase\nmetadata: {name: orders, namespace: neo4j}\n" +
			"spec: {clusterRef: nowhere, name: orders}\n",
		"Neo4jUser": head + "kind: Neo4jUser\nmetadata: {name: alice, namespace: neo4j}\n" +
			"spec: {clusterRef: nowhere, username: alice, externalAuth: [{provider: oidc-sso, id: alice}]}\n",
		"Neo4jRole": head + "kind: Neo4jRole\nmetadata: {name: analyst, namespace: neo4j}\n" +
			"spec: {clusterRef: nowhere, name: analyst}\n",
		"Neo4jRoleBinding": head + "kind: Neo4jRoleBinding\nmetadata: {name: bob, namespace: neo4j}\n" +
			"spec: {clusterRef: nowhere, username: bob, roles: [reader]}\n",
		"Neo4jAuthRule": head + "kind: Neo4jAuthRule\nmetadata: {name: rule, namespace: neo4j}\n" +
			"spec: {clusterRef: nowhere, name: rule, condition: \"abac.oidc.user_attribute('dept') = 'sales'\", grantedRoles: [reader]}\n",
		"Neo4jCompositeDatabase": head + "kind: Neo4jCompositeDatabase\nmetadata: {name: sales, namespace: neo4j}\n" +
			"spec: {clusterRef: nowhere, name: sales}\n",
		"Neo4jDatabaseAlias": head + "kind: Neo4jDatabaseAlias\nmetadata: {name: latest, namespace: neo4j}\n" +
			"spec: {clusterRef: nowhere, name: latest, targetDatabase: orders}\n",
		"Neo4jReplicaDatabase": head + "kind: Neo4jReplicaDatabase\nmetadata: {name: r, namespace: neo4j}\n" +
			"spec: {clusterRef: nowhere, upstreamDatabase: foo, source: {mode: network, upstreamClusterRef: {name: prod}}}\n",
		"Neo4jBackup": head + "kind: Neo4jBackup\nmetadata: {name: nightly, namespace: neo4j}\n" +
			"spec: {instanceRef: nowhere, database: orders, storage: {type: pvc, pvc: {name: backups}}}\n",
		"Neo4jPlugin": head + "kind: Neo4jPlugin\nmetadata: {name: apoc, namespace: neo4j}\n" +
			"spec: {clusterRef: nowhere, name: apoc, version: \"5.26.0\"}\n",
	}
	for kind, manifest := range manifests {
		t.Run(kind, func(t *testing.T) {
			results, err := validateSource(writeManifest(t, manifest), testClient(t), "")
			require.NoError(t, err)
			require.Len(t, results, 1)
			text := findingsText(results[0])
			assert.Equal(t, 0, results[0].errorCount(), text)
			require.Equal(t, 1, results[0].pendingCount(), text)
			assert.True(t, strings.Contains(text, `"nowhere"`), text)
		})
	}
}
