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

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// AliasValidator validates Neo4jDatabaseAlias specs inline (invariant 1 —
// there is no admission webhook).
type AliasValidator struct {
	Client client.Client
}

// NewAliasValidator constructs an AliasValidator.
func NewAliasValidator(c client.Client) *AliasValidator {
	return &AliasValidator{Client: c}
}

// Validate checks a Neo4jDatabaseAlias spec.
func (v *AliasValidator) Validate(_ context.Context, alias *neo4jv1beta1.Neo4jDatabaseAlias) ValidationResult {
	var res ValidationResult
	specPath := field.NewPath("spec")

	name := alias.Spec.Name
	if name == "" {
		name = alias.Name
	}

	// Names are interpolated into Cypher admin DDL, which accepts no
	// parameters for identifiers.
	if strings.Contains(name, "`") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("name"), name,
			"alias name may not contain a backtick"))
	}
	if strings.Contains(alias.Spec.TargetDatabase, "`") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("targetDatabase"),
			alias.Spec.TargetDatabase, "database name may not contain a backtick"))
	}

	// An alias that points at itself is accepted by neither Neo4j nor sense.
	if name != "" && name == alias.Spec.TargetDatabase {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("targetDatabase"),
			alias.Spec.TargetDatabase, "an alias cannot target a database of the same name"))
	}

	// `system` cannot be aliased.
	if strings.EqualFold(alias.Spec.TargetDatabase, "system") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("targetDatabase"),
			alias.Spec.TargetDatabase, "the system database cannot be aliased"))
	}
	if strings.EqualFold(name, "system") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("name"), name,
			"an alias may not be named `system`"))
	}

	return res
}
