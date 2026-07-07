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
	"context"
	"os"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// TestCELValidations exercises the apiserver-enforced CEL x-kubernetes-validations
// on the non-Aura CRDs against a real (envtest) apiserver — proving both that the
// rules register (valid syntax + within the cost budget) AND that they accept
// valid specs while rejecting invalid ones (guarding against a logic inversion
// that a registration-only check would miss). Skipped when KUBEBUILDER_ASSETS is
// unset; `make test-unit` sets it.
func TestCELValidations(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test-unit`")
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	defer func() { _ = env.Stop() }()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("client-go scheme: %v", err)
	}
	if err := neo4jv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("neo4j scheme: %v", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx := context.Background()
	const ns = "default"

	accept := func(name string, obj client.Object) {
		t.Helper()
		if err := c.Create(ctx, obj); err != nil {
			t.Errorf("%s: expected apiserver to ACCEPT, got error: %v", name, err)
			return
		}
		_ = c.Delete(ctx, obj)
	}
	reject := func(name string, obj client.Object) {
		t.Helper()
		if err := c.Create(ctx, obj); err == nil {
			t.Errorf("%s: expected apiserver to REJECT (CEL), but it was accepted", name)
			_ = c.Delete(ctx, obj)
		}
	}
	meta := func(n string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: n, Namespace: ns} }

	// --- Neo4jUser: at least one auth provider ---
	accept("user with password", &neo4jv1beta1.Neo4jUser{
		ObjectMeta: meta("cel-user-ok"),
		Spec: neo4jv1beta1.Neo4jUserSpec{
			ClusterRef:        "c1",
			PasswordSecretRef: &neo4jv1beta1.SecretKeyRef{Name: "pw"},
		},
	})
	reject("user with no auth provider", &neo4jv1beta1.Neo4jUser{
		ObjectMeta: meta("cel-user-bad"),
		Spec:       neo4jv1beta1.Neo4jUserSpec{ClusterRef: "c1"},
	})

	// --- Neo4jPlugin: VerifiedDownload supply-chain gate + https url ---
	accept("plugin Managed (gate inert)", &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: meta("cel-plugin-managed"),
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			ClusterRef: "c1", Name: "apoc",
		},
	})
	// Regression: a Managed plugin with a source that OMITS url (the common
	// official/community case, e.g. apoc/gds/bloom) must be accepted. The https
	// rule must not error on the absent optional `url` field.
	accept("plugin Managed with source, no url", &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: meta("cel-plugin-managed-src"),
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			ClusterRef: "c1", Name: "apoc",
			Source: &neo4jv1beta1.PluginSource{Type: "official"},
		},
	})
	reject("plugin VerifiedDownload without source", &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: meta("cel-plugin-vd-nosource"),
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			ClusterRef: "c1", Name: "custom", InstallMode: "VerifiedDownload",
		},
	})
	accept("plugin VerifiedDownload with verified https source", &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: meta("cel-plugin-vd-ok"),
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			ClusterRef: "c1", Name: "custom", InstallMode: "VerifiedDownload",
			Source: &neo4jv1beta1.PluginSource{
				Type:     "url",
				URL:      "https://mirror.example.com/custom.jar",
				Checksum: "sha256:" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			},
		},
	})
	reject("plugin source url over http", &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: meta("cel-plugin-http"),
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			ClusterRef: "c1", Name: "custom", InstallMode: "VerifiedDownload",
			Source: &neo4jv1beta1.PluginSource{
				Type:     "url",
				URL:      "http://mirror.example.com/custom.jar",
				Checksum: "sha256:" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			},
		},
	})

	// --- Neo4jEnterpriseCluster: topology cross-field + TLS conditionals ---
	minimalCluster := func(n string, mutate func(*neo4jv1beta1.Neo4jEnterpriseCluster)) *neo4jv1beta1.Neo4jEnterpriseCluster {
		cl := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: meta(n),
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				Image:    neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
				Storage:  neo4jv1beta1.StorageSpec{Size: "10Gi"},
				Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
			},
		}
		mutate(cl)
		return cl
	}

	accept("cluster minimal", minimalCluster("cel-cluster-ok", func(*neo4jv1beta1.Neo4jEnterpriseCluster) {}))
	reject("cluster minSystemPrimaries > servers", minimalCluster("cel-cluster-mp", func(cl *neo4jv1beta1.Neo4jEnterpriseCluster) {
		five := int32(5)
		cl.Spec.Topology.MinSystemPrimaries = &five // servers is 3
	}))
	reject("cluster serverRoles all SECONDARY", minimalCluster("cel-cluster-sec", func(cl *neo4jv1beta1.Neo4jEnterpriseCluster) {
		cl.Spec.Topology.Servers = 2
		cl.Spec.Topology.ServerRoles = []neo4jv1beta1.ServerRoleHint{
			{ServerIndex: 0, ModeConstraint: "SECONDARY"},
			{ServerIndex: 1, ModeConstraint: "SECONDARY"},
		}
	}))
	reject("cluster tls cert-manager without issuerRef", minimalCluster("cel-cluster-tls-bad", func(cl *neo4jv1beta1.Neo4jEnterpriseCluster) {
		cl.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "cert-manager"}
	}))
	accept("cluster tls cert-manager with issuerRef", minimalCluster("cel-cluster-tls-ok", func(cl *neo4jv1beta1.Neo4jEnterpriseCluster) {
		cl.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "cert-manager", IssuerRef: &neo4jv1beta1.IssuerRef{Name: "ca-cluster-issuer"}}
	}))
}
