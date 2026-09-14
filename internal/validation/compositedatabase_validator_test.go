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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func compositeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, neo4jv1beta1.AddToScheme(s))
	return s
}

func composite(name string, constituents ...neo4jv1beta1.CompositeConstituent) *neo4jv1beta1.Neo4jCompositeDatabase {
	return &neo4jv1beta1.Neo4jCompositeDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "neo4j"},
		Spec: neo4jv1beta1.Neo4jCompositeDatabaseSpec{
			ClusterRef:   "prod",
			Constituents: constituents,
		},
	}
}

func constituent(name, target string) neo4jv1beta1.CompositeConstituent {
	return neo4jv1beta1.CompositeConstituent{Name: name, TargetDatabase: target}
}

// Every rule here was learned from a live server, not from the docs.
func TestCompositeDatabaseValidator_Names(t *testing.T) {
	v := NewCompositeDatabaseValidator(fake.NewClientBuilder().WithScheme(compositeScheme(t)).Build())

	t.Run("a well-formed composite is accepted", func(t *testing.T) {
		res := v.Validate(t.Context(), composite("cineasts", constituent("latest", "movies-latest")))
		assert.Empty(t, res.Errors)
	})

	// The server's own words: "Database name '...' contains illegal characters.
	// Use simple ascii characters, numbers, dots and dashes."
	t.Run("underscores are refused, as the server refuses them", func(t *testing.T) {
		res := v.Validate(t.Context(), composite("bad_name", constituent("latest", "movies")))
		require.NotEmpty(t, res.Errors)
		assert.Contains(t, res.Errors.ToAggregate().Error(), "underscores")
	})

	// `a.b` is how a constituent of composite `a` is addressed, so a dotted
	// composite name can never have constituents at all.
	t.Run("a dotted composite name is refused", func(t *testing.T) {
		res := v.Validate(t.Context(), composite("a.b", constituent("latest", "movies")))
		require.NotEmpty(t, res.Errors)
		assert.Contains(t, res.Errors.ToAggregate().Error(), "may not contain a dot")
	})

	t.Run("backticks are refused — names go into DDL unparameterised", func(t *testing.T) {
		res := v.Validate(t.Context(), composite("ev`il", constituent("latest", "movies")))
		require.NotEmpty(t, res.Errors)
		assert.Contains(t, res.Errors.ToAggregate().Error(), "backtick")
	})

	t.Run("system is reserved", func(t *testing.T) {
		assert.NotEmpty(t, v.Validate(t.Context(), composite("system", constituent("a", "b"))).Errors)
	})
}

func TestCompositeDatabaseValidator_Constituents(t *testing.T) {
	v := NewCompositeDatabaseValidator(fake.NewClientBuilder().WithScheme(compositeScheme(t)).Build())

	// The alias is `<composite>.<name>`, so a dot in the name would produce
	// `composite.a.b` — not a namespace the server recognises.
	t.Run("a dotted constituent name is refused", func(t *testing.T) {
		res := v.Validate(t.Context(), composite("cineasts", constituent("a.b", "movies")))
		require.NotEmpty(t, res.Errors)
		assert.Contains(t, res.Errors.ToAggregate().Error(), "already namespaced")
	})

	// Two constituents with the same name collide on one alias, last write
	// winning — silently, since neither CREATE fails.
	t.Run("duplicate constituent names are refused", func(t *testing.T) {
		res := v.Validate(t.Context(), composite("cineasts",
			constituent("latest", "movies-a"), constituent("latest", "movies-b")))
		require.NotEmpty(t, res.Errors)
		assert.Contains(t, res.Errors.ToAggregate().Error(), "Duplicate")
	})

	t.Run("a constituent cannot target its own composite", func(t *testing.T) {
		res := v.Validate(t.Context(), composite("cineasts", constituent("self", "cineasts")))
		require.NotEmpty(t, res.Errors)
		assert.Contains(t, res.Errors.ToAggregate().Error(), "own composite")
	})

	t.Run("system cannot be a constituent", func(t *testing.T) {
		res := v.Validate(t.Context(), composite("cineasts", constituent("sys", "system")))
		assert.NotEmpty(t, res.Errors)
	})

	t.Run("underscores in a target are refused", func(t *testing.T) {
		res := v.Validate(t.Context(), composite("cineasts", constituent("latest", "movies_latest")))
		assert.NotEmpty(t, res.Errors)
	})
}

// DEFAULT LANGUAGE CYPHER does not parse on the 5.26 LTS — `CREATE COMPOSITE
// DATABASE` there accepts only IF NOT EXISTS / WAIT / NOWAIT / OPTIONS, and
// `ALTER DATABASE ... SET` accepts only OPTION, ACCESS READ and TOPOLOGY.
// Without this gate the user gets a raw Cypher syntax error in status.message
// with no hint that the field is version-dependent.
func TestCompositeDatabaseValidator_CypherLanguageIsCalVerOnly(t *testing.T) {
	deployment := func(tag string) *neo4jv1beta1.Neo4jEnterpriseCluster {
		return &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: tag},
			},
		}
	}
	withLang := func(v string) *neo4jv1beta1.Neo4jCompositeDatabase {
		cd := composite("cineasts", constituent("latest", "movies-latest"))
		cd.Spec.DefaultCypherLanguage = v
		return cd
	}

	t.Run("refused on the 5.26 LTS", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(compositeScheme(t)).
			WithObjects(deployment("5.26-enterprise")).Build()
		res := NewCompositeDatabaseValidator(c).Validate(t.Context(), withLang("25"))
		require.NotEmpty(t, res.Errors)
		assert.Contains(t, res.Errors.ToAggregate().Error(), "does not parse on the 5.26 LTS")
	})

	t.Run("accepted on CalVer", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(compositeScheme(t)).
			WithObjects(deployment("2026.08.1-enterprise")).Build()
		assert.Empty(t, NewCompositeDatabaseValidator(c).Validate(t.Context(), withLang("25")).Errors)
	})

	// Applying a composite and its cluster together is ordinary GitOps. A
	// missing deployment must not be an error — the controller reports Pending
	// and retries, and this check runs again once it exists.
	t.Run("a deployment that does not exist yet is not an error", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(compositeScheme(t)).Build()
		assert.Empty(t, NewCompositeDatabaseValidator(c).Validate(t.Context(), withLang("25")).Errors)
	})

	t.Run("no language set, nothing to gate", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(compositeScheme(t)).
			WithObjects(deployment("5.26-enterprise")).Build()
		res := NewCompositeDatabaseValidator(c).Validate(t.Context(),
			composite("cineasts", constituent("latest", "movies-latest")))
		assert.Empty(t, res.Errors)
	})

	t.Run("a standalone is resolved the same way", func(t *testing.T) {
		sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "neo4j"},
			Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
				Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(compositeScheme(t)).WithObjects(sa).Build()
		res := NewCompositeDatabaseValidator(c).Validate(t.Context(), withLang("5"))
		require.NotEmpty(t, res.Errors)
		assert.True(t, strings.Contains(res.Errors.ToAggregate().Error(), "5.26"))
	})
}
