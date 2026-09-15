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

// Neither NEO4J_CONF nor NEO4J_UDC_PACKAGING may be set on a standalone. They
// are one bug seen from two ends, and reintroducing either resurrects it.
//
// The Neo4j entrypoint turns every NEO4J_<name> variable that is not on its own
// control-variable allowlist into a neo4j.conf setting. NEO4J_UDC_PACKAGING is
// not on that list, so it became `UDC.PACKAGING` — a setting no Neo4j version
// declares — and with the operator's strict config validation the server
// refused to start:
//
//	Failed to read config /var/lib/neo4j/conf/neo4j.conf
//	Unrecognized setting. No declared setting with name: UDC.PACKAGING
//
// NEO4J_CONF=/conf was the workaround: it pointed the server at the read-only
// ConfigMap mount, which is the entrypoint's INPUT, rather than the merged
// configuration the entrypoint assembles in ${NEO4J_HOME}/conf. That avoided
// the bad setting and silently discarded every OTHER env-var-delivered setting
// too — including the LDAP system-account credentials, which are passed as env
// vars specifically so they never land in the ConfigMap. No error, no warning;
// SHOW SETTINGS simply reported defaults.
//
// Verified on a live 2026.08.1 standalone: with both removed, a
// NEO4J_dbms_security_key_name env var reaches the server (SHOW SETTINGS
// returns its value); with NEO4J_CONF set it returned the built-in default.
func TestStandaloneEnvVars_NoConfOverrideAndNoUDCPackaging(t *testing.T) {
	r := &Neo4jEnterpriseStandaloneReconciler{}
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-neo4j", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
		},
	}

	names := map[string]bool{}
	for _, e := range r.buildEnvVars(standalone) {
		names[e.Name] = true
	}

	assert.False(t, names["NEO4J_CONF"],
		"NEO4J_CONF must not be set: it points the server at the read-only ConfigMap "+
			"mount instead of the config the entrypoint assembles, silently discarding "+
			"every NEO4J_<setting> env var — LDAP credentials among them")
	assert.False(t, names["NEO4J_UDC_PACKAGING"],
		"NEO4J_UDC_PACKAGING must not be set: the entrypoint turns it into the "+
			"undeclared setting UDC.PACKAGING, which strict config validation rejects "+
			"at startup. Packaging is reported by /var/lib/neo4j/packaging_info now")
}

// The mechanism the fix depends on: an operator-supplied NEO4J_<setting> env
// var has to survive onto the container, because that is how the entrypoint
// learns about it.
func TestStandaloneEnvVars_SettingEnvVarsReachTheContainer(t *testing.T) {
	r := &Neo4jEnterpriseStandaloneReconciler{}
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-neo4j", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
			Auth: &neo4jv1beta1.AuthSpec{
				LDAP: &neo4jv1beta1.Neo4jLDAPSpec{
					Authorization: &neo4jv1beta1.LDAPAuthorizationSpec{
						UseSystemAccount:       func() *bool { b := true; return &b }(),
						SystemAccountSecretRef: "ldap-creds",
					},
				},
			},
		},
	}

	var found bool
	for _, e := range r.buildEnvVars(standalone) {
		if e.Name == "NEO4J_dbms_security_ldap_authorization_system__password" {
			found = true
			assert.NotNil(t, e.ValueFrom, "the LDAP password must come from a Secret, never a literal")
		}
	}
	assert.True(t, found,
		"the LDAP system-account password is delivered as an env var so it stays out of "+
			"the ConfigMap; it only works because NEO4J_CONF no longer redirects the server")
}
