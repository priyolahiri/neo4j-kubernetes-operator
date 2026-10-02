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
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// TestResolveDatabaseCreateInputs pins what the Neo4jDatabase controller hands
// to CREATE DATABASE once it knows what hosts the database.
//
//   - defaultCypherLanguage reaches the server only on CalVer. The 5.26 LTS has
//     no DEFAULT LANGUAGE clause, so it is dropped there.
//   - topology is ignored for a standalone host, as the validator warns and
//     docs/api_reference/neo4jdatabase.md says. Before this the controller
//     forwarded it, so a standalone with `topology.secondaries: 2` sent
//     `TOPOLOGY 1 PRIMARY 2 SECONDARIES` to a one-server DBMS.
func TestResolveDatabaseCreateInputs(t *testing.T) {
	withSpec := func(lang string, topo *neo4jv1beta1.DatabaseTopology) *neo4jv1beta1.Neo4jDatabase {
		return &neo4jv1beta1.Neo4jDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jDatabaseSpec{
				ClusterRef:            "prod",
				Name:                  "movies",
				DefaultCypherLanguage: lang,
				Topology:              topo,
			},
		}
	}
	topo := &neo4jv1beta1.DatabaseTopology{Primaries: 2, Secondaries: 1}

	t.Run("CalVer cluster keeps both", func(t *testing.T) {
		gotTopo, gotLang := resolveDatabaseCreateInputs(withSpec("25", topo), "2026.08.1-enterprise", false)
		assert.Equal(t, topo, gotTopo)
		assert.Equal(t, "25", gotLang)
	})

	t.Run("CalVer keeps an explicit 5 too", func(t *testing.T) {
		_, gotLang := resolveDatabaseCreateInputs(withSpec("5", nil), "2026.08.1-enterprise", false)
		assert.Equal(t, "5", gotLang)
	})

	t.Run("LTS cluster drops the language and keeps the topology", func(t *testing.T) {
		gotTopo, gotLang := resolveDatabaseCreateInputs(withSpec("5", topo), "5.26-enterprise", false)
		assert.Equal(t, topo, gotTopo)
		assert.Empty(t, gotLang)
	})

	t.Run("standalone ignores topology and still honours the language on CalVer", func(t *testing.T) {
		gotTopo, gotLang := resolveDatabaseCreateInputs(withSpec("25", topo), "2026.08.1-enterprise", true)
		assert.Nil(t, gotTopo)
		assert.Equal(t, "25", gotLang)
	})

	t.Run("standalone on the LTS ignores topology and drops the language", func(t *testing.T) {
		gotTopo, gotLang := resolveDatabaseCreateInputs(withSpec("5", topo), "5.26-enterprise", true)
		assert.Nil(t, gotTopo)
		assert.Empty(t, gotLang)
	})

	t.Run("nothing set stays nothing", func(t *testing.T) {
		gotTopo, gotLang := resolveDatabaseCreateInputs(withSpec("", nil), "2026.08.1-enterprise", false)
		assert.Nil(t, gotTopo)
		assert.Empty(t, gotLang)
	})
}

// TestDatabaseHostImageTag pins how the controller reads the version of the
// deployment a database lands on: from the cluster, or from the standalone
// when the reference resolved to one.
func TestDatabaseHostImageTag(t *testing.T) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{Image: neo4jv1beta1.ImageSpec{Tag: "5.26-enterprise"}},
	}
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{Image: neo4jv1beta1.ImageSpec{Tag: "2026.08.1-enterprise"}},
	}
	assert.Equal(t, "5.26-enterprise", databaseHostImageTag(cluster, nil, false))
	assert.Equal(t, "2026.08.1-enterprise", databaseHostImageTag(nil, standalone, true))
}
