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
	"errors"
	"testing"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// An unchanged Certificate must not be written. Every Update passes through
// cert-manager's admission webhook, so writing one each reconcile made a
// webhook outage fail the whole standalone reconcile — diagnostics included —
// although nothing had changed. Found on the v1.17.0 verification journey.
func TestStandaloneTLSCertificate_UnchangedIsNotWritten(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	require.NoError(t, certmanagerv1.AddToScheme(scheme))

	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "neo4j", UID: "u1"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			TLS: &neo4jv1beta1.TLSSpec{
				Mode:      "cert-manager",
				IssuerRef: &neo4jv1beta1.IssuerRef{Name: "ca-cluster-issuer", Kind: "ClusterIssuer"},
			},
		},
	}

	updates := 0
	webhookDown := interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*certmanagerv1.Certificate); ok {
				updates++
				return errors.New(`failed calling webhook "webhook.cert-manager.io": connection refused`)
			}
			return c.Update(ctx, obj, opts...)
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone).WithInterceptorFuncs(webhookDown).Build()
	r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme}

	// First reconcile creates it; the second finds it identical.
	require.NoError(t, r.reconcileTLSCertificate(context.Background(), standalone))
	require.NoError(t, r.reconcileTLSCertificate(context.Background(), standalone))
	require.Zero(t, updates, "an unchanged Certificate was written")

	// A real change is still applied — here, and so still subject to the webhook.
	standalone.Spec.TLS.IssuerRef.Name = "other-issuer"
	require.Error(t, r.reconcileTLSCertificate(context.Background(), standalone))
	require.Equal(t, 1, updates)
}
