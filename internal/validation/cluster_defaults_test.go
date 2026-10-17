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

// ApplyDefaults fills in what the user left unset and nothing else
// (operations.md id 150): a provider list the user wrote, in spec.auth or in
// spec.config, reaches neo4j.conf as written; a default the CRD schema has is
// the schema's; and a cluster whose stored spec carries the defaults an
// earlier operator wrote into it renders as one whose spec does not.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// withCRDDefaults applies to obj, decoded from a manifest, the defaults the
// API server applies before it stores one: those of the CRD schema in
// config/crd/bases (the +kubebuilder:default markers). It walks the schema as
// the API server's structural defaulting does: a property with a default that
// the object lacks gets it, and every property the object has, every
// additionalProperties value and every array item is defaulted in turn.
func withCRDDefaults(t *testing.T, crdFile string, obj map[string]any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "bases", crdFile))
	require.NoError(t, err)
	var crd apiextensionsv1.CustomResourceDefinition
	require.NoError(t, yaml.Unmarshal(raw, &crd))
	require.NotEmpty(t, crd.Spec.Versions)
	var walk func(x any, s *apiextensionsv1.JSONSchemaProps)
	walk = func(x any, s *apiextensionsv1.JSONSchemaProps) {
		if s == nil {
			return
		}
		switch x := x.(type) {
		case map[string]any:
			for name, prop := range s.Properties {
				if _, found := x[name]; found || prop.Default == nil {
					continue
				}
				var value any
				require.NoError(t, json.Unmarshal(prop.Default.Raw, &value))
				x[name] = value
			}
			for name, value := range x {
				if prop, found := s.Properties[name]; found {
					walk(value, &prop)
				} else if s.AdditionalProperties != nil {
					walk(value, s.AdditionalProperties.Schema)
				}
			}
		case []any:
			if s.Items != nil {
				for _, item := range x {
					walk(item, s.Items.Schema)
				}
			}
		}
	}
	walk(obj, crd.Spec.Versions[0].Schema.OpenAPIV3Schema)
}

func defaultsCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
			Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
			Storage:                neo4jv1beta1.StorageSpec{Size: "10Gi"},
		},
	}
}

// providerLines are the auth provider settings of a rendered neo4j.conf.
func providerLines(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) []string {
	var lines []string
	for _, line := range strings.Split(resources.BuildConfigMapForEnterprise(cluster).Data["neo4j.conf"], "\n") {
		if strings.HasPrefix(line, resources.AuthenticationProvidersSetting+"=") ||
			strings.HasPrefix(line, resources.AuthorizationProvidersSetting+"=") {
			lines = append(lines, line)
		}
	}
	return lines
}

// A provider list the user set, in spec.auth or in spec.config, is the one
// Neo4j gets. ApplyDefaults used to replace authorizationProviders whenever
// authenticationProviders was empty, and its native lists took the settings
// from spec.config.
func TestClusterValidator_ApplyDefaults_KeepsTheUsersAuthProviders(t *testing.T) {
	const (
		authn = "dbms.security.authentication_providers="
		authz = "dbms.security.authorization_providers="
	)
	cases := []struct {
		name      string
		auth      *neo4jv1beta1.AuthSpec
		config    map[string]string
		wantAuth  *neo4jv1beta1.AuthSpec
		wantLines []string
	}{
		{
			name:      "nothing set: Neo4j's defaults, written out",
			wantAuth:  &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"native"}, AuthorizationProviders: []string{"native"}},
			wantLines: []string{authn + "native", authz + "native"},
		},
		{
			name:      "an admin secret alone: the same",
			auth:      &neo4jv1beta1.AuthSpec{AdminSecret: "admin"},
			wantAuth:  &neo4jv1beta1.AuthSpec{AdminSecret: "admin", AuthenticationProviders: []string{"native"}, AuthorizationProviders: []string{"native"}},
			wantLines: []string{authn + "native", authz + "native"},
		},
		{
			name:      "authorization providers alone are kept",
			auth:      &neo4jv1beta1.AuthSpec{AuthorizationProviders: []string{"ldap", "native"}},
			wantAuth:  &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"native"}, AuthorizationProviders: []string{"ldap", "native"}},
			wantLines: []string{authn + "native", authz + "ldap,native"},
		},
		{
			name:      "authentication providers alone: authorization is Neo4j's default, unwritten as before",
			auth:      &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"ldap", "native"}},
			wantAuth:  &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"ldap", "native"}},
			wantLines: []string{authn + "ldap,native"},
		},
		{
			name:      "both lists are kept",
			auth:      &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"oidc-okta", "native"}, AuthorizationProviders: []string{"oidc-okta"}},
			wantAuth:  &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"oidc-okta", "native"}, AuthorizationProviders: []string{"oidc-okta"}},
			wantLines: []string{authn + "oidc-okta,native", authz + "oidc-okta"},
		},
		{
			name: "both settings in spec.config reach neo4j.conf",
			config: map[string]string{
				resources.AuthenticationProvidersSetting: "ldap,native",
				resources.AuthorizationProvidersSetting:  "ldap",
			},
			wantLines: []string{authn + "ldap,native", authz + "ldap"},
		},
		{
			name:      "the authorization setting in spec.config reaches neo4j.conf",
			config:    map[string]string{resources.AuthorizationProvidersSetting: "ldap,native"},
			wantAuth:  &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"native"}},
			wantLines: []string{authn + "native", authz + "ldap,native"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := defaultsCluster()
			cluster.Spec.Auth = tc.auth.DeepCopy()
			cluster.Spec.Config = tc.config
			NewClusterValidator(nil).ApplyDefaults(context.Background(), cluster)
			assert.Equal(t, tc.wantAuth, cluster.Spec.Auth)
			assert.ElementsMatch(t, tc.wantLines, providerLines(cluster))
		})
	}
}

// A value the user set is never replaced.
func TestClusterValidator_ApplyDefaults_KeepsEverySetValue(t *testing.T) {
	cluster := defaultsCluster()
	cluster.Spec.Image.PullPolicy = "Always"
	cluster.Spec.Storage.RetentionPolicy = "Retain"
	cluster.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "cert-manager", IssuerRef: &neo4jv1beta1.IssuerRef{Name: "ca", Kind: "ClusterIssuer"}}
	cluster.Spec.Service = &neo4jv1beta1.ServiceSpec{Type: "LoadBalancer"}
	cluster.Spec.Auth = &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"ldap"}, AuthorizationProviders: []string{"ldap"}}
	want := cluster.DeepCopy()
	NewClusterValidator(nil).ApplyDefaults(context.Background(), cluster)
	assert.Equal(t, want.Spec, cluster.Spec)
}

// The defaults ApplyDefaults takes from the CRD schema are the schema's. The
// API server fills them in before the operator reads a cluster; `kubectl neo4j
// validate` decodes a manifest the API server has not seen, and judged a tls
// block without a mode as disabled while the API server stored it as
// cert-manager.
func TestClusterValidator_ApplyDefaults_AgreesWithTheCRDDefaults(t *testing.T) {
	manifest := map[string]any{
		"apiVersion": neo4jv1beta1.GroupVersion.String(),
		"kind":       "Neo4jEnterpriseCluster",
		"metadata":   map[string]any{"name": "c", "namespace": "default"},
		"spec": map[string]any{
			"acceptLicenseAgreement": "eval",
			"image":                  map[string]any{"repo": "neo4j", "tag": "2026.08.1-enterprise"},
			"topology":               map[string]any{"servers": int64(3)},
			"storage":                map[string]any{"size": "10Gi"},
			"tls":                    map[string]any{"issuerRef": map[string]any{"name": "ca"}},
			"service":                map[string]any{},
		},
	}
	decode := func(obj map[string]any) *neo4jv1beta1.Neo4jEnterpriseCluster {
		raw, err := json.Marshal(obj)
		require.NoError(t, err)
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, json.Unmarshal(raw, cluster))
		return cluster
	}
	decoded := decode(manifest)
	withCRDDefaults(t, "neo4j.neo4j.com_neo4jenterpriseclusters.yaml", manifest)
	stored := decode(manifest)

	NewClusterValidator(nil).ApplyDefaults(context.Background(), decoded)
	assert.Equal(t, stored.Spec.Image.PullPolicy, decoded.Spec.Image.PullPolicy, "spec.image.pullPolicy")
	assert.Equal(t, stored.Spec.Storage.RetentionPolicy, decoded.Spec.Storage.RetentionPolicy, "spec.storage.retentionPolicy")
	assert.Equal(t, stored.Spec.TLS.Mode, decoded.Spec.TLS.Mode, "spec.tls.mode")
	assert.Equal(t, stored.Spec.Service.Type, decoded.Spec.Service.Type, "spec.service.type")
}

// storedByAnEarlierOperator is what an earlier operator's first pass wrote
// into the stored spec: its ApplyDefaults, sent back with the finalizer, and
// the CRD defaults the API server then gave the blocks those defaults created
// (tls.strictPeerValidation).
func storedByAnEarlierOperator(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	if cluster.Spec.Image.PullPolicy == "" {
		cluster.Spec.Image.PullPolicy = "IfNotPresent"
	}
	if cluster.Spec.TLS == nil {
		cluster.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "disabled", StrictPeerValidation: ptr.To(true)}
	}
	if cluster.Spec.Auth == nil {
		cluster.Spec.Auth = &neo4jv1beta1.AuthSpec{}
	}
	if len(cluster.Spec.Auth.AuthenticationProviders) == 0 {
		cluster.Spec.Auth.AuthenticationProviders = []string{"native"}
		cluster.Spec.Auth.AuthorizationProviders = []string{"native"}
	}
	if cluster.Spec.Service == nil {
		cluster.Spec.Service = &neo4jv1beta1.ServiceSpec{Type: "ClusterIP"}
	}
	if cluster.Spec.Storage.RetentionPolicy == "" {
		cluster.Spec.Storage.RetentionPolicy = "Delete"
	}
}

// A cluster whose stored spec carries the defaults an earlier operator wrote
// into it gets the same neo4j.conf, server StatefulSet and Services as one
// applied today, whose stored spec does not: an operator upgrade restarts
// nothing, and a user who removes those defaults from the stored spec
// restarts nothing either. The exceptions are the specs the earlier defaults
// got wrong (authorizationProviders without authenticationProviders, provider
// settings in spec.config), which TestClusterValidator_ApplyDefaults_KeepsTheUsersAuthProviders
// covers.
func TestClusterValidator_ApplyDefaults_RendersAsTheDefaultsAnEarlierOperatorStored(t *testing.T) {
	cases := map[string]func(*neo4jv1beta1.Neo4jEnterpriseCluster){
		"no optional block": func(*neo4jv1beta1.Neo4jEnterpriseCluster) {},
		"an admin secret": func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Spec.Auth = &neo4jv1beta1.AuthSpec{AdminSecret: "admin"}
		},
		"authentication providers alone": func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Spec.Auth = &neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"ldap", "native"}, LDAP: &neo4jv1beta1.Neo4jLDAPSpec{Host: "ldap://ldap:389"}}
		},
		"cert-manager TLS and a LoadBalancer": func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
			c.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "cert-manager", IssuerRef: &neo4jv1beta1.IssuerRef{Name: "ca", Kind: "ClusterIssuer"}}
			c.Spec.Service = &neo4jv1beta1.ServiceSpec{Type: "LoadBalancer"}
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			today := defaultsCluster()
			edit(today)
			earlier := today.DeepCopy()
			storedByAnEarlierOperator(earlier)

			v := NewClusterValidator(nil)
			v.ApplyDefaults(context.Background(), today)
			v.ApplyDefaults(context.Background(), earlier)

			assert.Equal(t, resources.BuildConfigMapForEnterprise(earlier).Data,
				resources.BuildConfigMapForEnterprise(today).Data, "the ConfigMap")
			assert.Equal(t, resources.BuildServerStatefulSetForEnterprise(earlier).Spec,
				resources.BuildServerStatefulSetForEnterprise(today).Spec, "the server StatefulSet")
			assert.Equal(t, resources.BuildClientServiceForEnterprise(earlier).Spec,
				resources.BuildClientServiceForEnterprise(today).Spec, "the client Service")
		})
	}
}
