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

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// deploymentRefClient holds objs and no deployment; readErr, when set, fails
// every read of a cluster or standalone with it.
func deploymentRefClient(t *testing.T, readErr error, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	b := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...)
	if readErr != nil {
		b = b.WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				switch obj.(type) {
				case *neo4jv1beta1.Neo4jEnterpriseCluster, *neo4jv1beta1.Neo4jEnterpriseStandalone:
					return readErr
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
	}
	return b.Build()
}

// deploymentRefResult is what each validator reports, put side by side.
type deploymentRefResult struct {
	errs          field.ErrorList
	warnings      []string
	targetPending string
}

// The validators that resolve a clusterRef report a deployment that does not
// exist yet as pending, beside every error of the spec, never as an error of
// its own: the reconcilers wait for it (operations.md id 182). As an error it
// made their wait unreachable.
func TestValidators_AMissingDeploymentIsPendingBesideTheSpecErrors(t *testing.T) {
	ctx := context.Background()
	meta := metav1.ObjectMeta{Name: "x", Namespace: "ns"}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "pw", Namespace: "ns"},
		Data: map[string][]byte{"password": []byte("a-long-password")}}

	cases := []struct {
		name         string
		invalidField string
		run          func(c client.Client) deploymentRefResult
	}{
		{"Neo4jDatabase", "spec.name", func(c client.Client) deploymentRefResult {
			r := NewDatabaseValidator(c).Validate(ctx, &neo4jv1beta1.Neo4jDatabase{ObjectMeta: meta,
				Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "missing", Name: "system",
					Topology: &neo4jv1beta1.DatabaseTopology{Primaries: -1}}})
			return deploymentRefResult{r.Errors, r.Warnings, r.TargetPending}
		}},
		{"Neo4jUser", "spec.username", func(c client.Client) deploymentRefResult {
			r := NewUserValidator(c).Validate(ctx, &neo4jv1beta1.Neo4jUser{ObjectMeta: meta,
				Spec: neo4jv1beta1.Neo4jUserSpec{ClusterRef: "missing", Username: "1alice",
					PasswordSecretRef: &neo4jv1beta1.SecretKeyRef{Name: "pw"}}})
			return deploymentRefResult{r.Errors, r.Warnings, r.TargetPending}
		}},
		{"Neo4jRole", "spec.name", func(c client.Client) deploymentRefResult {
			r := NewRoleValidator(c).Validate(ctx, &neo4jv1beta1.Neo4jRole{ObjectMeta: meta,
				Spec: neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "missing", Name: "1analyst"}})
			return deploymentRefResult{r.Errors, r.Warnings, r.TargetPending}
		}},
		{"Neo4jRoleBinding", "spec.username", func(c client.Client) deploymentRefResult {
			r := NewRoleBindingValidator(c).Validate(ctx, &neo4jv1beta1.Neo4jRoleBinding{ObjectMeta: meta,
				Spec: neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "missing", Username: "1bob", Roles: []string{"reader"}}})
			return deploymentRefResult{r.Errors, r.Warnings, r.TargetPending}
		}},
		{"Neo4jAuthRule", "spec.name", func(c client.Client) deploymentRefResult {
			r := NewAuthRuleValidator(c).Validate(ctx, &neo4jv1beta1.Neo4jAuthRule{ObjectMeta: meta,
				Spec: neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "missing", Name: "1rule",
					Condition: "abac.oidc.user_attribute('dept') = 'sales'", GrantedRoles: []string{"reader"}}})
			return deploymentRefResult{r.Errors, r.Warnings, r.TargetPending}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.run(deploymentRefClient(t, nil, secret))
			require.NotEmpty(t, got.errs)
			for _, e := range got.errs {
				assert.NotEqual(t, "spec.clusterRef", e.Field, "a missing deployment is not an error: %v", e)
			}
			assert.True(t, strings.Contains(got.errs.ToAggregate().Error(), tc.invalidField), "%v", got.errs)
			assert.Contains(t, got.targetPending, `"missing"`)
			assert.Contains(t, got.targetPending, "spec.clusterRef")

			// A deployment that cannot be read is neither: a warning that the
			// rules needing it were not checked.
			unreadable := tc.run(deploymentRefClient(t, apierrors.NewServiceUnavailable("etcd is unhappy"), secret))
			assert.Empty(t, unreadable.targetPending)
			assert.True(t, strings.Contains(strings.Join(unreadable.warnings, "\n"), "etcd is unhappy"), "%q", unreadable.warnings)
		})
	}
}

// A database whose host is missing still has every rule of its own checked:
// the validator used to return as soon as the host lookup failed, so the seed,
// options and conflict rules waited for the cluster.
func TestDatabaseValidator_ChecksTheSpecWithoutItsHost(t *testing.T) {
	db := &neo4jv1beta1.Neo4jDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns"},
		Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "missing", Name: "orders",
			SeedURI: "ftp2://bucket/orders.backup"},
	}
	res := NewDatabaseValidator(deploymentRefClient(t, nil)).Validate(context.Background(), db)
	require.Len(t, res.Errors, 1, "%v", res.Errors)
	assert.Equal(t, "spec.seedURI", res.Errors[0].Field)
	assert.NotEmpty(t, res.TargetPending)
}
