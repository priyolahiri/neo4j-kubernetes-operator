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
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func clusterWithAuth(provider string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	var auth *neo4jv1beta1.AuthSpec
	if provider != "" {
		auth = &neo4jv1beta1.AuthSpec{
			AuthenticationProviders: []string{provider},
		}
	}
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth:                   auth,
		},
	}
}

// ---- Backward compatibility tests (old Provider field) ----

func TestAuthValidator_Validate_NilAuth(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
	}
	errs := v.Validate(cluster)
	if len(errs) != 0 {
		t.Errorf("expected no errors for nil auth, got: %v", errs)
	}
}

func TestAuthValidator_BackwardCompat_OldProviderField(t *testing.T) {
	v := NewAuthValidator()

	cases := []struct {
		name     string
		provider string
		wantErrs int
	}{
		{"native provider - no errors", "native", 0},
		{"ldap provider - no errors", "ldap", 0},
		// Legacy names Neo4j does not document: still accepted (they warn, see
		// TestAuthProviders_MatchTheManual), so no CR accepted before is refused.
		{"kerberos provider - no errors", "kerberos", 0},
		{"jwt provider - no errors", "jwt", 0},
		{"plugin provider - no errors", "plugin-Neo4j-Kerberos", 0},
		{"plugin prefix with no name - NotSupported", "plugin-", 1},
		{"invalid provider - NotSupported", "invalid", 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := clusterWithAuth(tc.provider)
			errs := v.Validate(cluster)
			if len(errs) != tc.wantErrs {
				t.Errorf("expected %d errors, got %d: %v", tc.wantErrs, len(errs), errs)
			}
		})
	}
}

func TestAuthValidator_BackwardCompat_LDAPWithTypedConfig(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				AuthenticationProviders: []string{"ldap"},
				AuthorizationProviders:  []string{"ldap"},
				LDAP: &neo4jv1beta1.Neo4jLDAPSpec{
					Host: "ldap://ldap.example.com",
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 0 {
		t.Errorf("expected no errors when typed LDAP config replaces secretRef, got: %v", errs)
	}
}

// ---- Multi-provider list tests ----

func TestAuthValidator_ProviderList_Valid(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				AuthenticationProviders: []string{"ldap", "native"},
				AuthorizationProviders:  []string{"ldap", "native"},
				LDAP: &neo4jv1beta1.Neo4jLDAPSpec{
					Host: "ldap://ldap.example.com",
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 0 {
		t.Errorf("expected no errors, got: %v", errs)
	}
}

func TestAuthValidator_ProviderList_OIDCFormat(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				AuthenticationProviders: []string{"oidc-okta", "native"},
				AuthorizationProviders:  []string{"oidc-okta", "native"},
				OIDC: map[string]neo4jv1beta1.Neo4jOIDCProviderSpec{
					"okta": {
						WellKnownDiscoveryURI: "https://dev-123.okta.com/.well-known/openid-configuration",
						Audience:              "client-id",
					},
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 0 {
		t.Errorf("expected no errors for oidc-<name> format, got: %v", errs)
	}
}

func TestAuthValidator_ProviderList_InvalidName(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				AuthenticationProviders: []string{"invalid-provider"},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 1 {
		t.Errorf("expected 1 error for invalid provider name, got %d: %v", len(errs), errs)
	}
}

// ---- LDAP typed field validation ----

func TestAuthValidator_LDAP_HostRequired(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				AuthenticationProviders: []string{"ldap", "native"},
				LDAP: &neo4jv1beta1.Neo4jLDAPSpec{
					Host: "", // empty
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 1 {
		t.Errorf("expected 1 error for empty LDAP host, got %d: %v", len(errs), errs)
	}
}

func TestAuthValidator_LDAP_SystemAccountRequiresSecret(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				LDAP: &neo4jv1beta1.Neo4jLDAPSpec{
					Host: "ldap://ldap.example.com",
					Authorization: &neo4jv1beta1.LDAPAuthorizationSpec{
						UseSystemAccount:       ptr.To(true),
						SystemAccountSecretRef: "", // missing
					},
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 1 {
		t.Errorf("expected 1 error for missing systemAccountSecretRef, got %d: %v", len(errs), errs)
	}
}

func TestAuthValidator_LDAP_SystemAccountWithSecret_OK(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				LDAP: &neo4jv1beta1.Neo4jLDAPSpec{
					Host: "ldap://ldap.example.com",
					Authorization: &neo4jv1beta1.LDAPAuthorizationSpec{
						UseSystemAccount:       ptr.To(true),
						SystemAccountSecretRef: "ldap-bind-creds",
					},
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 0 {
		t.Errorf("expected no errors, got: %v", errs)
	}
}

// ---- OIDC validation ----

func TestAuthValidator_OIDC_AudienceRequired(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				OIDC: map[string]neo4jv1beta1.Neo4jOIDCProviderSpec{
					"okta": {
						WellKnownDiscoveryURI: "https://dev-123.okta.com/.well-known/openid-configuration",
						Audience:              "", // missing
					},
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 1 {
		t.Errorf("expected 1 error for missing audience, got %d: %v", len(errs), errs)
	}
}

func TestAuthValidator_OIDC_EndpointsRequired(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				OIDC: map[string]neo4jv1beta1.Neo4jOIDCProviderSpec{
					"custom": {
						Audience: "my-app",
						// No discovery URI and no manual endpoints
					},
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 1 {
		t.Errorf("expected 1 error for missing endpoints, got %d: %v", len(errs), errs)
	}
}

func TestAuthValidator_OIDC_ManualEndpoints_OK(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				OIDC: map[string]neo4jv1beta1.Neo4jOIDCProviderSpec{
					"custom": {
						Audience:      "my-app",
						AuthEndpoint:  "https://idp.example.com/authorize",
						TokenEndpoint: "https://idp.example.com/token",
						JWKSURI:       "https://idp.example.com/jwks",
						Issuer:        "https://idp.example.com/",
					},
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 0 {
		t.Errorf("expected no errors, got: %v", errs)
	}
}

func TestAuthValidator_OIDC_InvalidProviderName(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				OIDC: map[string]neo4jv1beta1.Neo4jOIDCProviderSpec{
					"123-bad": { // starts with number
						WellKnownDiscoveryURI: "https://example.com/.well-known/openid-configuration",
						Audience:              "my-app",
					},
				},
			},
		},
	}
	errs := v.Validate(cluster)
	hasNameError := false
	for _, err := range errs {
		if err.Field == `spec.auth.oidc[123-bad]` {
			hasNameError = true
		}
	}
	if !hasNameError {
		t.Errorf("expected error for invalid OIDC provider name, got: %v", errs)
	}
}

// ---- TrustStore validation ----

func TestAuthValidator_TrustStore_SecretRefRequired(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				TrustStore: &neo4jv1beta1.SecretKeyRef{
					Name: "", // empty
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 1 {
		t.Errorf("expected 1 error for missing trustStore.secretRef, got %d: %v", len(errs), errs)
	}
}

func TestAuthValidator_TrustStore_Valid(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Auth: &neo4jv1beta1.AuthSpec{
				TrustStore: &neo4jv1beta1.SecretKeyRef{
					Name: "my-ca-cert",
					Key:  "ca.crt",
				},
			},
		},
	}
	errs := v.Validate(cluster)
	if len(errs) != 0 {
		t.Errorf("expected no errors, got: %v", errs)
	}
}

// The provider lists are written verbatim into dbms.security.authentication_providers
// and dbms.security.authorization_providers. The manual documents the same value
// space for both settings on the 5.26 LTS and on CalVer: native, ldap,
// oidc-<name> (Single sign-on integration, /5/ and /current/) and plugin-<name>
// (configuration settings). The validator used to accept oidc, jwt, kerberos,
// saml and custom, none of which Neo4j documents, and to refuse plugin-<name>,
// which it does. Old names keep validating and warn instead (knowledge rule 103).
func TestAuthProviders_MatchTheManual(t *testing.T) {
	v := NewAuthValidator()
	authPath := field.NewPath("spec", "auth")
	spec := func(p string) *neo4jv1beta1.AuthSpec {
		return &neo4jv1beta1.AuthSpec{
			AuthenticationProviders: []string{p},
			AuthorizationProviders:  []string{p},
		}
	}

	for _, p := range []string{"native", "ldap", "oidc-okta", "plugin-Neo4j-Kerberos"} {
		t.Run("documented "+p, func(t *testing.T) {
			assert.Empty(t, v.ValidateAuthSpec(spec(p), authPath), "a documented provider must validate")
			assert.Empty(t, AuthProviderWarnings(spec(p), authPath), "a documented provider must not warn")
		})
	}

	for _, p := range []string{"oidc", "kerberos", "jwt", "saml", "custom"} {
		t.Run("legacy "+p, func(t *testing.T) {
			assert.Empty(t, v.ValidateAuthSpec(spec(p), authPath),
				"a name accepted before must not become an error")
			warnings := AuthProviderWarnings(spec(p), authPath)
			require.Len(t, warnings, 2, "one warning per list that names it")
			assert.Contains(t, warnings[0], `spec.auth.authenticationProviders[0] "`+p+`"`)
			assert.Contains(t, warnings[0], "dbms.security.authentication_providers")
			assert.Contains(t, warnings[1], `spec.auth.authorizationProviders[0] "`+p+`"`)
			assert.Contains(t, warnings[1], "dbms.security.authorization_providers")
		})
	}

	t.Run("an unknown name is still refused, and does not also warn", func(t *testing.T) {
		errs := v.ValidateAuthSpec(spec("kerberos5"), authPath)
		require.Len(t, errs, 2)
		assert.Contains(t, errs[0].Error(), "plugin-<name>", "the error lists the documented values")
		assert.Empty(t, AuthProviderWarnings(spec("kerberos5"), authPath))
	})
}

// docs/user_guide/security.md tells Kerberos Add-On users to list
// [plugin-Neo4j-Kerberos, native]. The validator refused plugin-<name> until it
// was checked against the manual, so the documented setup could not be applied.
func TestAuthProviders_SecurityGuideKerberosSetupValidates(t *testing.T) {
	cluster := clusterWithAuth("plugin-Neo4j-Kerberos")
	cluster.Spec.Auth.AuthenticationProviders = append(cluster.Spec.Auth.AuthenticationProviders, "native")
	assert.Empty(t, NewAuthValidator().Validate(cluster))
	assert.Empty(t, AuthProviderWarnings(cluster.Spec.Auth, field.NewPath("spec", "auth")))
}

// The warnings reach the user through each Kind's existing ValidationWarning
// channel: ClusterValidator.NoEffectWarnings (also what ValidateCreateWithWarnings
// returns) and StandaloneValidator.NoEffectWarnings, both turned into events by
// their reconcilers.
func TestAuthProviderWarnings_ReachBothKinds(t *testing.T) {
	cluster := clusterWithAuth("saml")
	assert.True(t, containsSubstring(NewClusterValidator(nil).NoEffectWarnings(cluster),
		`spec.auth.authenticationProviders[0] "saml"`), "cluster must surface the provider warning")

	standalone := validStandalone()
	standalone.Spec.Auth = &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"kerberos"}}
	assert.True(t, containsSubstring(NewStandaloneValidator().NoEffectWarnings(standalone),
		`spec.auth.authenticationProviders[0] "kerberos"`), "standalone must surface the provider warning")
}

func containsSubstring(lines []string, sub string) bool {
	for _, l := range lines {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}
