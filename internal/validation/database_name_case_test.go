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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// Neo4j normalises database and alias names to lower case, so names that
// differ only in case are one name (knowledge operations.md id 146). The
// rules that compare two names from the same spec compare them that way.

// Two constituents spelled `latest` and `Latest` are one alias: the controller
// would re-point it between their targets on every pass.
func TestCompositeDatabaseValidator_NamesDifferingInCaseAreOneName(t *testing.T) {
	v := NewCompositeDatabaseValidator(fake.NewClientBuilder().WithScheme(compositeScheme(t)).Build())

	res := v.Validate(t.Context(), composite("cineasts",
		constituent("latest", "movies-a"), constituent("Latest", "movies-b")))
	require.NotEmpty(t, res.Errors)
	assert.Contains(t, res.Errors.ToAggregate().Error(), "already used by constituents[0]")

	res = v.Validate(t.Context(), composite("cineasts", constituent("self", "Cineasts")))
	require.NotEmpty(t, res.Errors)
	assert.Contains(t, res.Errors.ToAggregate().Error(), "cannot target its own composite")
}

func TestAliasValidator_SelfTargetInAnotherCaseRejected(t *testing.T) {
	a := aliasCR(func(a *neo4jv1beta1.Neo4jDatabaseAlias) {
		a.Spec.TargetDatabase = "Foo"
	})
	res := NewAliasValidator(nil).Validate(context.Background(), a)
	require.NotEmpty(t, res.Errors, "`Foo` is the alias's own name, `foo`")
	assert.Contains(t, res.Errors.ToAggregate().Error(), "same name")
}

func TestReplicaValidator_UpstreamNameInAnotherCaseWarns(t *testing.T) {
	r := replicaCR(func(r *neo4jv1beta1.Neo4jReplicaDatabase) {
		r.Spec.Name = "Foo"
	})
	res := NewReplicaValidator(nil).Validate(context.Background(), r)
	assert.Empty(t, res.Errors)
	found := false
	for _, w := range res.Warnings {
		found = found || strings.Contains(w, "same name as its upstream")
	}
	assert.True(t, found, "`Foo` is the upstream `foo`: %v", res.Warnings)
}
