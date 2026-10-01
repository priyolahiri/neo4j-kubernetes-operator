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

// TestCompositeDatabaseNameSchema proves, against a real (envtest) apiserver,
// that Neo4jCompositeDatabase.spec.name is refused at apply time for the names
// the inline validator refuses. The schema used to admit dots, so a composite
// named `a.b` was accepted by the apiserver and only failed at reconcile.
// Skipped when KUBEBUILDER_ASSETS is unset; `make test-unit` sets it.
func TestCompositeDatabaseNameSchema(t *testing.T) {
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

	composite := func(k8sName, specName string) *neo4jv1beta1.Neo4jCompositeDatabase {
		return &neo4jv1beta1.Neo4jCompositeDatabase{
			ObjectMeta: metav1.ObjectMeta{Name: k8sName, Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jCompositeDatabaseSpec{
				ClusterRef: "c1",
				Name:       specName,
				Constituents: []neo4jv1beta1.CompositeConstituent{
					{Name: "latest", TargetDatabase: "movies"},
				},
			},
		}
	}

	for i, name := range []string{"cineasts", "Cineasts", "movies-2026"} {
		obj := composite("cdb-ok-"+string(rune('a'+i)), name)
		if err := c.Create(ctx, obj); err != nil {
			t.Errorf("spec.name %q: expected apiserver to ACCEPT, got %v", name, err)
			continue
		}
		_ = c.Delete(ctx, obj)
	}

	for i, name := range []string{"a.b", "my.composite", "movies.", "bad_name", "1abc", "ab"} {
		obj := composite("cdb-bad-"+string(rune('a'+i)), name)
		if err := c.Create(ctx, obj); err == nil {
			t.Errorf("spec.name %q: expected apiserver to REJECT (schema pattern/length), but it was accepted", name)
			_ = c.Delete(ctx, obj)
		}
	}
}
