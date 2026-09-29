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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The sharded family's language is set on the parent even when the spec
// leaves it out: shard sub-databases inherit the parent's language, and the
// server no longer forces CYPHER_25 for sharding (design §5.9), so without the
// clause a family would take the server default.
func TestShardedCreateAlwaysSetsTheFamilyLanguage(t *testing.T) {
	db := &neo4jv1beta1.Neo4jShardedDatabase{}
	db.Spec.Name = "products"
	db.Spec.PropertySharding.PropertyShards = 2
	db.Spec.PropertySharding.GraphShard.Primaries = 1
	db.Spec.PropertySharding.PropertyShardTopology.Replicas = 1

	q, _, err := buildCreateShardedDatabaseCypher(db)
	require.NoError(t, err)
	assert.Contains(t, q, " SET DEFAULT LANGUAGE CYPHER 25 ")

	db.Spec.DefaultCypherLanguage = "25"
	q, _, err = buildCreateShardedDatabaseCypher(db)
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(q, "SET DEFAULT LANGUAGE"))
}
