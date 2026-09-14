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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// CompositeDatabaseValidator checks a Neo4jCompositeDatabase before the
// reconciler acts on it (there is no admission webhook).
type CompositeDatabaseValidator struct {
	Client client.Client
}

// NewCompositeDatabaseValidator constructs a CompositeDatabaseValidator.
func NewCompositeDatabaseValidator(c client.Client) *CompositeDatabaseValidator {
	return &CompositeDatabaseValidator{Client: c}
}

// Validate checks a Neo4jCompositeDatabase spec.
//
// Everything here is offline except the version gate, which needs the
// referenced deployment's image tag. A missing deployment is NOT an error —
// applying a composite and its cluster together is ordinary GitOps, and the
// controller reports Pending and retries.
func (v *CompositeDatabaseValidator) Validate(ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase) ValidationResult {
	var res ValidationResult
	specPath := field.NewPath("spec")

	name := effectiveCompositeName(cd)

	// Names are interpolated into Cypher admin DDL, which takes no parameters
	// for identifiers.
	if strings.Contains(name, "`") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("name"), name,
			"composite database name may not contain a backtick"))
	}
	// The server refuses underscores outright: "Database name '...' contains
	// illegal characters. Use simple ascii characters, numbers, dots and
	// dashes." Catching it here turns a server-side reconcile failure into an
	// apply-time message.
	if strings.Contains(name, "_") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("name"), name,
			"Neo4j database names may not contain underscores — use dashes"))
	}
	if strings.EqualFold(name, "system") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("name"), name,
			"the system database name is reserved"))
	}
	// A dot is legal in a database name but reserved in practice: `a.b` is how
	// a constituent of composite `a` is addressed, so a composite whose own
	// name contains one can never have constituents.
	if strings.Contains(name, ".") {
		res.Errors = append(res.Errors, field.Invalid(specPath.Child("name"), name,
			"a composite database name may not contain a dot: `<composite>.<constituent>` is how "+
				"constituents are addressed, so a dotted composite name cannot have any"))
	}

	res.Errors = append(res.Errors, v.validateConstituents(cd, name, specPath)...)
	res.Errors = append(res.Errors, v.validateCypherLanguage(ctx, cd, specPath)...)

	return res
}

func (v *CompositeDatabaseValidator) validateConstituents(
	cd *neo4jv1beta1.Neo4jCompositeDatabase, composite string, specPath *field.Path,
) field.ErrorList {
	var errs field.ErrorList
	path := specPath.Child("constituents")

	seenNames := map[string]int{}
	for i, c := range cd.Spec.Constituents {
		cPath := path.Index(i)

		if strings.Contains(c.Name, "`") || strings.Contains(c.TargetDatabase, "`") {
			errs = append(errs, field.Invalid(cPath, c.Name,
				"constituent names and targets may not contain a backtick"))
		}
		// A dot in a constituent name would produce `composite.a.b`, which is
		// not a namespace the server recognises.
		if strings.Contains(c.Name, ".") {
			errs = append(errs, field.Invalid(cPath.Child("name"), c.Name,
				"a constituent name may not contain a dot — it is already namespaced "+
					"under the composite"))
		}
		if strings.Contains(c.Name, "_") || strings.Contains(c.TargetDatabase, "_") {
			errs = append(errs, field.Invalid(cPath, c.Name,
				"Neo4j database and alias names may not contain underscores — use dashes"))
		}
		if strings.EqualFold(c.TargetDatabase, "system") {
			errs = append(errs, field.Invalid(cPath.Child("targetDatabase"), c.TargetDatabase,
				"the system database cannot be a constituent"))
		}
		// A constituent targeting the composite itself is a cycle the server
		// will not resolve, and it is an easy copy-paste mistake.
		if c.TargetDatabase == composite {
			errs = append(errs, field.Invalid(cPath.Child("targetDatabase"), c.TargetDatabase,
				"a constituent cannot target its own composite"))
		}
		// The fully-qualified alias is what actually has to be unique; two
		// constituents with the same name would collide silently, with the
		// last write winning.
		if prev, dup := seenNames[c.Name]; dup {
			errs = append(errs, field.Duplicate(cPath.Child("name"),
				fmt.Sprintf("%q is already used by constituents[%d]", c.Name, prev)))
		}
		seenNames[c.Name] = i
	}
	return errs
}

// validateCypherLanguage refuses defaultCypherLanguage on the 5.26 LTS.
//
// The clause genuinely does not parse there — `CREATE COMPOSITE DATABASE`
// accepts only IF NOT EXISTS / WAIT / NOWAIT / OPTIONS, and `ALTER DATABASE
// ... SET` accepts only OPTION, ACCESS READ and TOPOLOGY. Without this the
// user would get a raw Cypher syntax error in status.message with no
// indication that the field is version-gated.
func (v *CompositeDatabaseValidator) validateCypherLanguage(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase, specPath *field.Path,
) field.ErrorList {
	if cd.Spec.DefaultCypherLanguage == "" {
		return nil
	}
	tag, found := v.imageTagFor(ctx, cd)
	if !found {
		// The deployment is not there yet. Applying both together is normal;
		// the controller retries, and this check runs again once it exists.
		return nil
	}
	parsed, err := neo4j.ParseVersion(tag)
	if err != nil || parsed.IsCalver {
		return nil
	}
	return field.ErrorList{field.Invalid(
		specPath.Child("defaultCypherLanguage"), cd.Spec.DefaultCypherLanguage,
		fmt.Sprintf("not supported on Neo4j %s: the DEFAULT LANGUAGE CYPHER clause does not "+
			"parse on the 5.26 LTS, for composite databases or any other kind. Remove the "+
			"field, or run a CalVer image", tag))}
}

// imageTagFor resolves the referenced deployment's image tag, from either Kind.
func (v *CompositeDatabaseValidator) imageTagFor(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase,
) (string, bool) {
	// `kubectl neo4j validate` runs offline validators with a nil client. The
	// version gate simply cannot be evaluated then, which reads as "not found"
	// — the same answer as a deployment that does not exist yet, and equally
	// non-fatal.
	if v.Client == nil {
		return "", false
	}
	key := types.NamespacedName{Name: cd.Spec.ClusterRef, Namespace: cd.Namespace}

	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := v.Client.Get(ctx, key, cluster); err == nil {
		return cluster.Spec.Image.Tag, cluster.Spec.Image.Tag != ""
	} else if !errors.IsNotFound(err) {
		return "", false
	}

	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	if err := v.Client.Get(ctx, key, standalone); err == nil {
		return standalone.Spec.Image.Tag, standalone.Spec.Image.Tag != ""
	}
	return "", false
}

// effectiveCompositeName returns spec.name if set, else metadata.name.
func effectiveCompositeName(cd *neo4jv1beta1.Neo4jCompositeDatabase) string {
	if cd.Spec.Name != "" {
		return cd.Spec.Name
	}
	return cd.Name
}
