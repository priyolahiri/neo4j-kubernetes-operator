/*
Copyright 2025.

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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1alpha1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1alpha1"
)

func clusterWithAuth(provider, secretRef string) *neo4jv1alpha1.Neo4jEnterpriseCluster {
	var auth *neo4jv1alpha1.AuthSpec
	if provider != "" || secretRef != "" {
		auth = &neo4jv1alpha1.AuthSpec{
			Provider:  provider,
			SecretRef: secretRef,
		}
	}
	return &neo4jv1alpha1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
		Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
			Auth: auth,
		},
	}
}

func TestAuthValidator_Validate(t *testing.T) {
	v := NewAuthValidator()

	cases := []struct {
		name      string
		provider  string
		secretRef string
		wantErrs  int
		errField  string
	}{
		{
			name:     "nil auth - no errors",
			provider: "", secretRef: "", wantErrs: 0,
		},
		{
			name:     "native provider without secretRef - no errors",
			provider: "native", secretRef: "", wantErrs: 0,
		},
		{
			name:     "ldap provider with secretRef - no errors",
			provider: "ldap", secretRef: "my-ldap-secret", wantErrs: 0,
		},
		{
			name:     "kerberos provider with secretRef - no errors",
			provider: "kerberos", secretRef: "my-krb-secret", wantErrs: 0,
		},
		{
			name:     "jwt provider with secretRef - no errors",
			provider: "jwt", secretRef: "my-jwt-secret", wantErrs: 0,
		},
		{
			name:     "ldap without secretRef - requires secretRef",
			provider: "ldap", secretRef: "", wantErrs: 1, errField: "spec.auth.secretRef",
		},
		{
			name:     "kerberos without secretRef - requires secretRef",
			provider: "kerberos", secretRef: "", wantErrs: 1, errField: "spec.auth.secretRef",
		},
		{
			name:     "invalid provider - NotSupported error",
			provider: "invalid", secretRef: "", wantErrs: 2, errField: "spec.auth.provider",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cluster := clusterWithAuth(tc.provider, tc.secretRef)
			errs := v.Validate(cluster)

			if len(errs) != tc.wantErrs {
				t.Errorf("expected %d errors, got %d: %v", tc.wantErrs, len(errs), errs)
				return
			}

			if tc.errField != "" && len(errs) > 0 {
				found := false
				for _, err := range errs {
					if err.Field == tc.errField {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected error on field %q, got: %v", tc.errField, errs)
				}
			}
		})
	}
}

func TestAuthValidator_Validate_NilAuth(t *testing.T) {
	v := NewAuthValidator()
	cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"},
	}
	errs := v.Validate(cluster)
	if len(errs) != 0 {
		t.Errorf("expected no errors for nil auth, got: %v", errs)
	}
}
