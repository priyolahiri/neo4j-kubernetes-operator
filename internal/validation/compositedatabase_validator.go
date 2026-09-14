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
	"regexp"
	"sort"
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
	remoteErrs, remoteWarnings := v.validateRemoteConstituents(ctx, cd, specPath)
	res.Errors = append(res.Errors, remoteErrs...)
	res.Warnings = append(res.Warnings, remoteWarnings...)

	return res
}

// driverSettingKey constrains DRIVER map keys. They are Cypher map keys, which
// cannot be parameterised, so they are the one part of a remote alias
// statement built from spec text — and therefore the one part that has to be
// constrained rather than escaped.
var driverSettingKey = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

// validateRemoteConstituents checks the remote block on each constituent.
//
// The most valuable rule here is the keystore one. Without it, a stored-
// credential alias fails inside the server with:
//
//	50N09 … 50N00: Internal exception raised TransactionStateTransitionException:
//	Failed to create alias for remote database: the required setting(s)
//	[dbms.security.keystore.path, dbms.security.keystore.password] are missing
//
// which names neither the CR, the constituent, nor the field to set. Catching
// it at apply time turns that into one sentence.
func (v *CompositeDatabaseValidator) validateRemoteConstituents(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase, specPath *field.Path,
) (field.ErrorList, []string) {
	var errs field.ErrorList
	var warnings []string

	var needsKeystore, needsCypher25 bool
	for i, c := range cd.Spec.Constituents {
		if c.Remote == nil {
			continue
		}
		rPath := specPath.Child("constituents").Index(i).Child("remote")

		switch {
		case c.Remote.OIDCCredentialForwarding && c.Remote.CredentialsSecretRef != "":
			errs = append(errs, field.Invalid(rPath, c.Remote.URL,
				"set exactly one of oidcCredentialForwarding or credentialsSecretRef: "+
					"an alias stores one form of credential, not both"))
		case !c.Remote.OIDCCredentialForwarding && c.Remote.CredentialsSecretRef == "":
			errs = append(errs, field.Required(rPath,
				"a remote constituent needs an authentication mode: set "+
					"oidcCredentialForwarding: true to forward the querying user's token, "+
					"or credentialsSecretRef to store native credentials"))
		case c.Remote.OIDCCredentialForwarding:
			needsCypher25 = true
		default:
			needsKeystore = true
		}

		for _, k := range sortedDriverKeys(c.Remote.DriverSettings) {
			if !driverSettingKey.MatchString(k) {
				errs = append(errs, field.Invalid(rPath.Child("driverSettings").Key(k), k,
					"driver setting names must match [a-zA-Z][a-zA-Z0-9_]* — they are Cypher "+
						"map keys, which cannot be parameterised"))
			}
		}

		// A plaintext scheme carries the credential in the clear when this
		// alias stores one. Warn rather than refuse: a private network is a
		// legitimate if unusual choice, and the CRD pattern already rejects
		// anything that is not a Bolt URL.
		if c.Remote.CredentialsSecretRef != "" && !strings.Contains(c.Remote.URL, "+s") {
			warnings = append(warnings, fmt.Sprintf(
				"constituent %q sends stored credentials to %s over an unencrypted scheme; "+
					"prefer neo4j+s:// so the credential is not exposed in transit",
				c.Name, c.Remote.URL))
		}
	}

	if !needsKeystore && !needsCypher25 {
		return errs, warnings
	}

	tag, found := v.imageTagFor(ctx, cd)

	if needsKeystore && !v.deploymentHasKeystore(ctx, cd) {
		errs = append(errs, field.Required(
			specPath.Child("constituents"),
			fmt.Sprintf("a remote constituent with credentialsSecretRef needs "+
				"spec.remoteAliasKeystore on %s: Neo4j encrypts stored alias credentials "+
				"and refuses to create the alias without a keystore (%s, %s). "+
				"Either configure the keystore, or use oidcCredentialForwarding, "+
				"which stores no credential and needs none",
				cd.Spec.ClusterRef, settingKeystorePath, settingKeystorePassword)))
	}

	if needsCypher25 && found {
		if parsed, err := neo4j.ParseVersion(tag); err == nil && !parsed.IsCalver {
			errs = append(errs, field.Invalid(
				specPath.Child("constituents"), tag,
				"oidcCredentialForwarding requires Cypher 25, which the 5.26 LTS does not "+
					"have — the OIDC CREDENTIAL FORWARDING clause does not parse there. "+
					"Use credentialsSecretRef with a keystore, or run a CalVer image"))
		}
	}

	return errs, warnings
}

// deploymentHasKeystore reports whether the referenced deployment configures a
// remote-alias keystore. A deployment that does not exist yet reads as "yes",
// so applying a composite alongside its cluster is not an error — the
// controller reports Pending and this runs again once it is there.
func (v *CompositeDatabaseValidator) deploymentHasKeystore(
	ctx context.Context, cd *neo4jv1beta1.Neo4jCompositeDatabase,
) bool {
	if v.Client == nil {
		return true
	}
	key := types.NamespacedName{Name: cd.Spec.ClusterRef, Namespace: cd.Namespace}

	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := v.Client.Get(ctx, key, cluster); err == nil {
		return cluster.Spec.RemoteAliasKeystore != nil && cluster.Spec.RemoteAliasKeystore.SecretRef != ""
	} else if !errors.IsNotFound(err) {
		return true
	}

	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	if err := v.Client.Get(ctx, key, standalone); err == nil {
		return standalone.Spec.RemoteAliasKeystore != nil && standalone.Spec.RemoteAliasKeystore.SecretRef != ""
	}
	return true
}

// sortedDriverKeys mirrors the builder's ordering so validation and rendering
// walk the same list.
func sortedDriverKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// The settings named in the keystore error, kept in sync with the resource
// builder by being the same strings the server itself reports.
const (
	settingKeystorePath     = "dbms.security.keystore.path"
	settingKeystorePassword = "dbms.security.keystore.password"
)

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
