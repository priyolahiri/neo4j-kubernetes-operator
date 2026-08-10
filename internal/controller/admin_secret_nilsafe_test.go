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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// `spec.auth` is optional on both Cluster and Standalone, so a deployment that
// omits it is a legal spec — and for a quick standalone, a natural one. Every
// path that reads the admin secret name must therefore tolerate a nil Auth.
//
// Found by the pre-release verification journey: a Neo4jDatabase pointed at a
// standalone without `spec.auth` panicked the reconciler with a nil-pointer
// dereference, and controller-runtime turned that into a crash-loop where the
// CR simply never got a status. The same unguarded dereference was present in
// ResolvedTarget.NewClient, which the user/role/rolebinding/authrule and
// replication controllers all route through.
func TestAdminSecretHelpers_NilAuthDoesNotPanic(t *testing.T) {
	t.Run("cluster with nil Auth falls back to the default", func(t *testing.T) {
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		}
		if cluster.Spec.Auth != nil {
			t.Fatal("precondition: Auth should be nil")
		}
		if got := getClusterAdminSecretName(cluster); got != DefaultAdminSecretNamePlugin {
			t.Errorf("got %q, want the default %q", got, DefaultAdminSecretNamePlugin)
		}
	})

	t.Run("standalone with nil Auth falls back to the default", func(t *testing.T) {
		sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		}
		if got := getStandaloneAdminSecretName(sa); got != DefaultAdminSecretNamePlugin {
			t.Errorf("got %q, want the default %q", got, DefaultAdminSecretNamePlugin)
		}
	})

	t.Run("explicit adminSecret still wins", func(t *testing.T) {
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				Auth: &neo4jv1beta1.AuthSpec{AdminSecret: "custom-secret"},
			},
		}
		if got := getClusterAdminSecretName(cluster); got != "custom-secret" {
			t.Errorf("got %q, want %q", got, "custom-secret")
		}
		sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
				Auth: &neo4jv1beta1.AuthSpec{AdminSecret: "sa-secret"},
			},
		}
		if got := getStandaloneAdminSecretName(sa); got != "sa-secret" {
			t.Errorf("got %q, want %q", got, "sa-secret")
		}
	})

	// Auth present but AdminSecret empty is the other way to reach the same
	// crash — the guard must check both.
	t.Run("Auth set but AdminSecret empty falls back", func(t *testing.T) {
		cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{Auth: &neo4jv1beta1.AuthSpec{}},
		}
		if got := getClusterAdminSecretName(cluster); got != DefaultAdminSecretNamePlugin {
			t.Errorf("got %q, want the default %q", got, DefaultAdminSecretNamePlugin)
		}
	})
}

// ResolvedTarget.NewClient is the shared entry point for every CR that talks to
// a Neo4j deployment — users, roles, role bindings, auth rules, and the
// replication CRDs all route through it. It must not panic on a nil Auth. It
// will still fail to connect here (the referenced Secret does not exist in the
// fake client, and there is no Neo4j to dial), but that must surface as an
// error return, not a process-killing panic.
func TestResolvedTargetNewClient_NilAuthDoesNotPanic(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := neo4jv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding neo4j scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding core scheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ResolvedTarget.NewClient panicked on nil spec.auth: %v", r)
		}
	}()

	standalone := ResolvedTarget{
		Found:      true,
		Standalone: &neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"}},
	}
	if _, err := standalone.NewClient(c); err == nil {
		t.Log("standalone NewClient returned no error; the important assertion is that it did not panic")
	}

	cluster := ResolvedTarget{
		Found:   true,
		Cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"}},
	}
	if _, err := cluster.NewClient(c); err == nil {
		t.Log("cluster NewClient returned no error; the important assertion is that it did not panic")
	}
}
