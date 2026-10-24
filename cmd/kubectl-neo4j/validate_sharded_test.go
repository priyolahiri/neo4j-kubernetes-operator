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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const brokenShardedManifest = `
apiVersion: neo4j.neo4j.com/v1beta1
kind: Neo4jShardedDatabase
metadata: {name: products, namespace: neo4j}
spec:
  clusterRef: sc
  name: system
  defaultCypherLanguage: "5"
  propertySharding:
    propertyShards: 2
    graphShard: {primaries: 1}
    propertyShardTopology: {replicas: 1}
`

// A sharded database whose cluster is not there yet: every spec error is
// reported at its own field, and the missing cluster is a wait, as it is for
// the operator. The validator used to stop at the cluster read, and this
// command printed the read error as one error at "spec" (#532).
func TestValidate_Connected_ShardedDatabaseReportsEveryErrorWithoutItsCluster(t *testing.T) {
	results, err := validateSource(writeManifest(t, brokenShardedManifest), testClient(t), "")
	require.NoError(t, err)
	require.Len(t, results, 1)

	text := findingsText(results[0])
	assert.Equal(t, 2, results[0].errorCount(), text)
	assert.Contains(t, text, "spec.name: ")
	assert.Contains(t, text, "spec.defaultCypherLanguage: ")
	assert.Equal(t, 1, results[0].pendingCount(), text)
	assert.Contains(t, text, `waiting for Neo4jEnterpriseCluster "sc"`)
}
