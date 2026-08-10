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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func aliasCR(mutate func(*neo4jv1beta1.Neo4jDatabaseAlias)) *neo4jv1beta1.Neo4jDatabaseAlias {
	a := &neo4jv1beta1.Neo4jDatabaseAlias{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "dr"},
		Spec: neo4jv1beta1.Neo4jDatabaseAliasSpec{
			ClusterRef:     "dr-cluster",
			TargetDatabase: "foo-replica",
		},
	}
	if mutate != nil {
		mutate(a)
	}
	return a
}

// The CCDR failover shape: alias `foo` fronting replica `foo-replica`.
func TestAliasValidator_FailoverShapeValid(t *testing.T) {
	res := NewAliasValidator(nil).Validate(context.Background(), aliasCR(nil))
	if len(res.Errors) != 0 {
		t.Fatalf("expected no errors, got %v", res.Errors)
	}
}

func TestAliasValidator_SelfTargetRejected(t *testing.T) {
	a := aliasCR(func(a *neo4jv1beta1.Neo4jDatabaseAlias) {
		a.Spec.TargetDatabase = "foo"
	})
	if res := NewAliasValidator(nil).Validate(context.Background(), a); len(res.Errors) == 0 {
		t.Fatal("expected an alias targeting its own name to be rejected")
	}
}

func TestAliasValidator_SystemRejected(t *testing.T) {
	target := aliasCR(func(a *neo4jv1beta1.Neo4jDatabaseAlias) {
		a.Spec.TargetDatabase = "system"
	})
	if res := NewAliasValidator(nil).Validate(context.Background(), target); len(res.Errors) == 0 {
		t.Error("expected aliasing the system database to be rejected")
	}

	named := aliasCR(func(a *neo4jv1beta1.Neo4jDatabaseAlias) {
		a.Spec.Name = "system"
	})
	if res := NewAliasValidator(nil).Validate(context.Background(), named); len(res.Errors) == 0 {
		t.Error("expected an alias named `system` to be rejected")
	}
}

func TestAliasValidator_BacktickRejected(t *testing.T) {
	a := aliasCR(func(a *neo4jv1beta1.Neo4jDatabaseAlias) {
		a.Spec.Name = "x`; DROP DATABASE neo4j; //"
	})
	if res := NewAliasValidator(nil).Validate(context.Background(), a); len(res.Errors) == 0 {
		t.Fatal("expected a backtick in the alias name to be rejected (Cypher injection guard)")
	}
}

// spec.name defaults to metadata.name, so the self-target check must use the
// effective name rather than only the explicit field.
func TestAliasValidator_SelfTargetViaDefaultedName(t *testing.T) {
	a := aliasCR(func(a *neo4jv1beta1.Neo4jDatabaseAlias) {
		a.Name = "samename"
		a.Spec.Name = ""
		a.Spec.TargetDatabase = "samename"
	})
	if res := NewAliasValidator(nil).Validate(context.Background(), a); len(res.Errors) == 0 {
		t.Fatal("expected self-target to be caught when the name comes from metadata.name")
	}
}
