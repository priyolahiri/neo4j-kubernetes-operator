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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// A cluster refused on its first apply said "Update validation failed" (seen
// on a CCDR proxy cluster without TLS, v1.21.1 journey): every object read
// from the API server carries a CreationTimestamp, so the reconciler always
// takes its update path. The wording now follows whether any pass has ever
// accepted the cluster; which validations run is unchanged.
func TestReconcile_ValidationFailureWording(t *testing.T) {
	cases := []struct {
		name        string
		finalizers  []string
		wantStatus  string
		wantEvent   string
		notInStatus string
	}{
		{
			name:        "refused at creation",
			wantStatus:  "Validation failed: validation failed: ",
			wantEvent:   "Warning " + EventReasonValidationFailed + " Cluster validation failed: validation failed: ",
			notInStatus: "Update",
		},
		{
			name:       "refused after an earlier pass accepted it",
			finalizers: []string{ClusterFinalizer},
			wantStatus: "Update validation failed: validation failed: ",
			wantEvent:  "Warning " + EventReasonValidationFailed + " Cluster update validation failed: validation failed: ",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := noEffectTestScheme(t)
			cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ccdr", Namespace: "default", Generation: 1,
					// What the API server sets on create, and what sent every
					// pass down the update path.
					CreationTimestamp: metav1.Now(),
					Finalizers:        tc.finalizers,
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement:  "eval",
					Image:                   neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
					Topology:                neo4jv1beta1.TopologyConfiguration{Servers: 3},
					Storage:                 neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
					CrossClusterReplication: &neo4jv1beta1.CrossClusterReplicationSpec{Enabled: true},
				},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).
				WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()
			rec := record.NewFakeRecorder(20)
			r := &Neo4jEnterpriseClusterReconciler{
				Client: c, Scheme: scheme, Recorder: rec, Validator: validation.NewClusterValidator(c),
			}

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			require.NoError(t, err, "a spec the user must fix is not a reconcile error")

			got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cluster), got))
			assert.Equal(t, "Failed", got.Status.Phase)
			assert.True(t, strings.HasPrefix(got.Status.Message, tc.wantStatus),
				"status.message = %q, want prefix %q", got.Status.Message, tc.wantStatus)
			assert.Contains(t, got.Status.Message, "spec.crossClusterReplication.enabled",
				"the same refusal either way: only its name changed")
			if tc.notInStatus != "" {
				assert.NotContains(t, got.Status.Message, tc.notInStatus)
			}

			var found bool
			for _, e := range drainEvents(rec) {
				if strings.HasPrefix(e, tc.wantEvent) {
					found = true
				}
			}
			assert.True(t, found, "no event starting %q", tc.wantEvent)
		})
	}
}
