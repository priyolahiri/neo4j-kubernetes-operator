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
	"testing"

	certv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func tlsSecret(name, certificate string) *corev1.Secret {
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-tls-secret", Namespace: "default"}}
	if certificate != "" {
		s.Annotations = map[string]string{certv1.CertificateNameKey: certificate}
	}
	return s
}

func secretExists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, &corev1.Secret{})
	if err != nil && !errors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

// TestDeleteIssuedTLSSecret pins #479: only the Secret cert-manager issued for
// this deployment's {name}-tls Certificate is deleted.
func TestDeleteIssuedTLSSecret(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		secret      *corev1.Secret
		wantDeleted bool
	}{
		{"issued for this deployment", tlsSecret("c", "c-tls"), true},
		{"created by the user", tlsSecret("c", ""), false},
		{"issued for another Certificate", tlsSecret("c", "other-tls"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(tc.secret).Build()
			if err := deleteIssuedTLSSecret(ctx, fc, "default", "c", "c-tls"); err != nil {
				t.Fatal(err)
			}
			if gone := !secretExists(t, fc, "c-tls-secret"); gone != tc.wantDeleted {
				t.Errorf("deleted=%v, want %v", gone, tc.wantDeleted)
			}
		})
	}
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).Build()
	if err := deleteIssuedTLSSecret(ctx, fc, "default", "c", "c-tls"); err != nil {
		t.Errorf("no Secret is not an error: %v", err)
	}
}

// TestHandleDeletion_DeletesTheIssuedTLSSecret: both Kinds' deletion paths
// remove the issued Secret and leave unrelated ones.
func TestHandleDeletion_DeletesTheIssuedTLSSecret(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()

	cluster := minimalCluster("cd", "default")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.DeletionTimestamp = &now
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(cluster, tlsSecret("cd", "cd-tls"), tlsSecret("other", "other-tls")).Build()
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Scheme: newTestScheme()}
	if _, err := r.handleDeletion(ctx, cluster); err != nil {
		t.Fatalf("cluster handleDeletion: %v", err)
	}
	if secretExists(t, fc, "cd-tls-secret") {
		t.Error("cluster: the issued TLS Secret must be deleted")
	}
	if !secretExists(t, fc, "other-tls-secret") {
		t.Error("cluster: another deployment's Secret must be kept")
	}

	sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{
		Name: "sd", Namespace: "default", Finalizers: []string{StandaloneFinalizer}, DeletionTimestamp: &now}}
	// The standalone's Certificate is {name}-tls-cert, not the cluster's
	// {name}-tls — the first cut assumed one name and the live walk caught it.
	fs := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(sa, tlsSecret("sd", "sd-tls-cert")).Build()
	rs := &Neo4jEnterpriseStandaloneReconciler{Client: fs, Scheme: newTestScheme()}
	if _, err := rs.handleDeletion(ctx, sa); err != nil {
		t.Fatalf("standalone handleDeletion: %v", err)
	}
	if secretExists(t, fs, "sd-tls-secret") {
		t.Error("standalone: the issued TLS Secret must be deleted")
	}
}
