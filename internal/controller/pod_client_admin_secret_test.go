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

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// The per-server clients — live config, TLS reload and restart-pending
// (dialClusterPod), the split-brain check and the upgrade's per-server
// database gate (createPodSpecificNeo4jClient) — use the admin Secret every
// other client of a cluster uses: spec.auth.adminSecret, or neo4j-admin-secret
// when it is unset. They refused a cluster without spec.auth.adminSecret,
// which the CRD allows and the StatefulSet serves with the default Secret.
// Each case's API holds only the Secret the servers were given, so a client
// that reads any other fails.
func TestPodClients_UseTheAdminSecretTheServersWereGiven(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))

	dialers := map[string]func(client.Client, *neo4jv1beta1.Neo4jEnterpriseCluster) (*neo4jclient.Client, error){
		"dialClusterPod": func(c client.Client, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (*neo4jclient.Client, error) {
			return dialClusterPod(c, cluster, cluster.Name+"-server-0")
		},
		"createPodSpecificNeo4jClient": func(c client.Client, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (*neo4jclient.Client, error) {
			return (&SplitBrainDetector{Client: c}).createPodSpecificNeo4jClient(context.Background(), cluster, cluster.Name+"-server-0")
		},
	}
	cases := map[string]struct {
		auth   *neo4jv1beta1.AuthSpec
		secret string
	}{
		"no auth block":            {nil, DefaultAdminSecretName},
		"an auth block without it": {&neo4jv1beta1.AuthSpec{AuthenticationProviders: []string{"native"}}, DefaultAdminSecretName},
		"an explicit admin secret": {&neo4jv1beta1.AuthSpec{AdminSecret: "custom-admin"}, "custom-admin"},
	}
	for dialer, dial := range dialers {
		for name, tc := range cases {
			t.Run(dialer+"/"+name, func(t *testing.T) {
				c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: tc.secret, Namespace: "default"},
					Data:       map[string][]byte{"username": []byte("neo4j"), "password": []byte("testpassword")},
				}).Build()
				cluster := minimalCluster("c", "default")
				cluster.Spec.Auth = tc.auth
				nc, err := dial(c, cluster)
				require.NoError(t, err)
				_ = nc.Close()
			})
		}
	}
}
