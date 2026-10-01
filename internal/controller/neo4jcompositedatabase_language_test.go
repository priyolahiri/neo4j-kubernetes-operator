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

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The composite's DEFAULT LANGUAGE CYPHER clause does not parse on the 5.26 LTS
// (CREATE COMPOSITE DATABASE and ALTER DATABASE both reject it), so a "5" that
// the validator now accepts there must be dropped before it reaches either
// statement, exactly as Neo4jDatabase drops it. On CalVer it is sent as asked.
func TestCompositeCypherLanguage(t *testing.T) {
	cluster := func(tag string) ResolvedTarget {
		c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		c.Spec.Image.Tag = tag
		return ResolvedTarget{Found: true, Cluster: c}
	}
	standalone := func(tag string) ResolvedTarget {
		s := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		s.Spec.Image.Tag = tag
		return ResolvedTarget{Found: true, Standalone: s}
	}
	composite := func(lang string) *neo4jv1beta1.Neo4jCompositeDatabase {
		return &neo4jv1beta1.Neo4jCompositeDatabase{
			Spec: neo4jv1beta1.Neo4jCompositeDatabaseSpec{DefaultCypherLanguage: lang},
		}
	}

	tests := []struct {
		name   string
		cd     *neo4jv1beta1.Neo4jCompositeDatabase
		target ResolvedTarget
		want   string
	}{
		{"5 is dropped on the 5.26 LTS cluster", composite("5"), cluster("5.26-enterprise"), ""},
		{"5 is dropped on the 5.26 LTS standalone", composite("5"), standalone("5.26.31-enterprise"), ""},
		{"5 is sent on CalVer", composite("5"), cluster("2026.08.1-enterprise"), "5"},
		{"25 is sent on CalVer", composite("25"), cluster("2026.08.1-enterprise"), "25"},
		{"25 is sent on a CalVer standalone", composite("25"), standalone("2025.12.0-enterprise"), "25"},
		{"nothing requested, nothing sent (LTS)", composite(""), cluster("5.26-enterprise"), ""},
		{"nothing requested, nothing sent (CalVer)", composite(""), cluster("2026.08.1-enterprise"), ""},
		// A tag that cannot be parsed cannot be gated: the request passes through
		// unchanged, the same answer the validator and Neo4jDatabase give.
		{"an unreadable tag passes the request through", composite("5"), cluster("latest"), "5"},
		{"a target with no deployment passes the request through", composite("25"), ResolvedTarget{}, "25"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, compositeCypherLanguage(tt.cd, tt.target))
		})
	}
}
