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
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// documentedAuthProviders are the values Neo4j documents for
// dbms.security.authentication_providers and dbms.security.authorization_providers,
// identically on the 5.26 LTS and on CalVer: the built-in `native` and `ldap`
// providers, `plugin-<name>` for an auth plugin or add-on such as Kerberos
// (Operations Manual, configuration settings), and `oidc-<name>` for an SSO
// provider configured under dbms.security.oidc.<name>.* (Operations Manual,
// "Single sign-on integration", /5/ and /current/).
var documentedAuthProviders = []string{"native", "ldap", "oidc-<name>", "plugin-<name>"}

// legacyAuthProviders were accepted by this validator before its list was checked
// against the manual, and are not values Neo4j documents. They warn instead of
// failing, so a CR that was accepted before is not newly refused (knowledge rule
// 103); the operator still writes them into the setting as given. Each entry is
// what to use instead.
var legacyAuthProviders = map[string]string{
	"oidc":     "an OIDC provider is referenced as oidc-<name>, where <name> is a key in spec.auth.oidc",
	"kerberos": "Kerberos is the Neo4j Kerberos Add-On, which is listed as plugin-<name>",
	"jwt":      "Neo4j documents no jwt provider; sign-in with a JWT goes through an OIDC provider (oidc-<name>)",
	"saml":     "Neo4j documents no SAML provider; its single sign-on is OIDC (oidc-<name>)",
	"custom":   "an auth plugin is listed as plugin-<name>",
}

// oidcProviderNameRegex validates OIDC provider names (alphanumeric + hyphens, used as Neo4j config key segments)
var oidcProviderNameRegex = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-]*$`)

// AuthValidator validates Neo4j authentication configuration
type AuthValidator struct{}

// NewAuthValidator creates a new auth validator
func NewAuthValidator() *AuthValidator {
	return &AuthValidator{}
}

// Validate validates the authentication configuration
func (v *AuthValidator) Validate(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) field.ErrorList {
	return v.ValidateAuthSpec(cluster.Spec.Auth, field.NewPath("spec", "auth"))
}

// ValidateAuthSpec validates an AuthSpec (shared between cluster and standalone).
func (v *AuthValidator) ValidateAuthSpec(auth *neo4jv1beta1.AuthSpec, authPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if auth == nil {
		return allErrs
	}

	// Validate provider lists
	allErrs = append(allErrs, v.validateProviderList(auth.AuthenticationProviders, authPath.Child("authenticationProviders"))...)
	allErrs = append(allErrs, v.validateProviderList(auth.AuthorizationProviders, authPath.Child("authorizationProviders"))...)

	// Validate LDAP typed fields
	if auth.LDAP != nil {
		allErrs = append(allErrs, v.validateLDAP(auth.LDAP, authPath.Child("ldap"))...)
	}

	// Validate OIDC providers
	if len(auth.OIDC) > 0 {
		allErrs = append(allErrs, v.validateOIDCProviders(auth.OIDC, authPath.Child("oidc"))...)
	}

	// Validate TrustStore
	if auth.TrustStore != nil {
		if auth.TrustStore.Name == "" {
			allErrs = append(allErrs, field.Required(
				authPath.Child("trustStore", "name"),
				"name must specify the Secret containing the CA certificate",
			))
		}
	}

	return allErrs
}

// validateProviderList validates a list of provider names. A documented value
// passes, a legacy one passes here and warns from AuthProviderWarnings, and
// anything else is refused.
func (v *AuthValidator) validateProviderList(providers []string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	for i, provider := range providers {
		if isDocumentedAuthProvider(provider) {
			continue
		}
		if _, legacy := legacyAuthProviders[provider]; legacy {
			continue
		}
		allErrs = append(allErrs, field.NotSupported(fldPath.Index(i), provider, documentedAuthProviders))
	}
	return allErrs
}

// isDocumentedAuthProvider reports whether provider is one of the values Neo4j
// documents. "oidc-<name>" is not checked against spec.auth.oidc here.
// "plugin-<name>" was refused until this list was checked against the manual,
// which blocked the Kerberos Add-On setup that docs/user_guide/security.md
// describes ([plugin-Neo4j-Kerberos, native]).
func isDocumentedAuthProvider(provider string) bool {
	switch {
	case provider == "native", provider == "ldap":
		return true
	case strings.HasPrefix(provider, "oidc-"):
		return true
	case strings.HasPrefix(provider, "plugin-") && len(provider) > len("plugin-"):
		return true
	}
	return false
}

// AuthProviderWarnings reports provider names that ValidateAuthSpec still
// accepts but Neo4j does not document (legacyAuthProviders). Advisory only: it
// never adds an error, and the cluster and standalone reconcilers emit each
// line as a ValidationWarning event.
func AuthProviderWarnings(auth *neo4jv1beta1.AuthSpec, authPath *field.Path) []string {
	if auth == nil {
		return nil
	}
	var warnings []string
	lists := []struct {
		field, setting string
		providers      []string
	}{
		{"authenticationProviders", "dbms.security.authentication_providers", auth.AuthenticationProviders},
		{"authorizationProviders", "dbms.security.authorization_providers", auth.AuthorizationProviders},
	}
	for _, list := range lists {
		for i, provider := range list.providers {
			instead, legacy := legacyAuthProviders[provider]
			if !legacy {
				continue
			}
			warnings = append(warnings, fmt.Sprintf(
				"%s %q is not a provider Neo4j documents for %s (%s): %s. It is written into the setting as given",
				authPath.Child(list.field).Index(i), provider, list.setting,
				strings.Join(documentedAuthProviders, ", "), instead))
		}
	}
	return warnings
}

// validateLDAP validates Neo4jLDAPSpec typed fields.
func (v *AuthValidator) validateLDAP(ldap *neo4jv1beta1.Neo4jLDAPSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if ldap.Host == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("host"), "LDAP host is required"))
	}

	if ldap.Authorization != nil {
		authzPath := fldPath.Child("authorization")
		// If useSystemAccount is true, systemAccountSecretRef must be set
		if ldap.Authorization.UseSystemAccount != nil && *ldap.Authorization.UseSystemAccount {
			if ldap.Authorization.SystemAccountSecretRef == "" {
				allErrs = append(allErrs, field.Required(
					authzPath.Child("systemAccountSecretRef"),
					"systemAccountSecretRef is required when useSystemAccount is true",
				))
			}
		}
	}

	return allErrs
}

// validateOIDCProviders validates all OIDC provider specs.
func (v *AuthValidator) validateOIDCProviders(providers map[string]neo4jv1beta1.Neo4jOIDCProviderSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	for name, provider := range providers {
		providerPath := fldPath.Key(name)

		// Validate provider name format (used as Neo4j config key segment)
		if !oidcProviderNameRegex.MatchString(name) {
			allErrs = append(allErrs, field.Invalid(
				providerPath,
				name,
				"OIDC provider name must start with a letter and contain only alphanumeric characters and hyphens",
			))
		}

		// Audience is required
		if provider.Audience == "" {
			allErrs = append(allErrs, field.Required(
				providerPath.Child("audience"),
				"audience is required for OIDC providers",
			))
		}

		// Either discovery URI or manual endpoints must be provided
		hasDiscovery := provider.WellKnownDiscoveryURI != ""
		hasManualEndpoints := provider.AuthEndpoint != "" || provider.TokenEndpoint != "" || provider.JWKSURI != "" || provider.Issuer != ""
		if !hasDiscovery && !hasManualEndpoints {
			allErrs = append(allErrs, field.Required(
				providerPath.Child("wellKnownDiscoveryURI"),
				"either wellKnownDiscoveryURI or manual endpoints (authEndpoint, tokenEndpoint, jwksURI, issuer) must be provided",
			))
		}
	}

	return allErrs
}
