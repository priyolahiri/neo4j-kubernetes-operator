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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The Neo4jPlugin controller adds NEO4J_PLUGINS to the server StatefulSet
// while the cluster controller may be in the middle of its own update. When the
// cluster controller's update then hit a conflict, the retry took its "desired"
// StatefulSet from the object the previous attempt had read the live state
// into. It applied that stale template and recorded every env name in it —
// NEO4J_PLUGINS included — as cluster-owned, so the next pass removed the
// plugin's variable as "owned but no longer desired". The plugin stayed Ready
// with APOC gone (seen in CI: "Should install APOC plugin on
// Neo4jEnterpriseCluster" waited 40 minutes for the variable).
func TestCreateOrUpdateResource_ConflictRetryKeepsTheDesiredSpec(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))

	const ns = "plugins"
	owner := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: ns, UID: "cluster-uid"},
	}
	desired := func() *appsv1.StatefulSet {
		labels := map[string]string{"app": "c"}
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "c-server", Namespace: ns},
			Spec: appsv1.StatefulSetSpec{
				Replicas:       ptr.To[int32](2),
				UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
				Selector:       &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name: "neo4j", Image: "neo4j:2026.08.1-enterprise",
						Env: []corev1.EnvVar{{Name: "NEO4J_AUTH", Value: "neo4j/x"}},
					}}},
				},
			},
		}
	}
	// pluginPatch is what the plugin controller does: add an env var to the
	// live StatefulSet, outside the cluster controller's desired template.
	pluginPatch := func(c client.Client, name, value string) {
		t.Helper()
		live := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "c-server", Namespace: ns}, live))
		live.Spec.Template.Spec.Containers[0].Env = append(live.Spec.Template.Spec.Containers[0].Env,
			corev1.EnvVar{Name: name, Value: value})
		require.NoError(t, c.Update(ctx, live))
	}

	// The API server defaults fields the desired StatefulSet leaves empty, so
	// the cluster controller's pass writes the StatefulSet every time — which
	// is what gives the plugin controller's patch a window to conflict in.
	defaulted := func(obj client.Object) {
		if sts, ok := obj.(*appsv1.StatefulSet); ok && sts.Spec.UpdateStrategy.RollingUpdate == nil {
			sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To[int32](0)}
		}
	}
	base := fake.NewClientBuilder().WithScheme(scheme).Build()
	racePending := false
	c := interceptor.NewClient(base, interceptor.Funcs{
		// The API server gives every object a UID; the fake client does not,
		// and the cluster controller treats a StatefulSet without one as new.
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				obj.SetUID(types.UID(obj.GetName() + "-uid"))
			}
			defaulted(obj)
			return cl.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok && racePending {
				// The plugin's second patch lands between the cluster
				// controller's read and its write: the write conflicts.
				racePending = false
				pluginPatch(cl, "NEO4J_apoc_export_file_enabled", "true")
			}
			defaulted(obj)
			return cl.Update(ctx, obj, opts...)
		},
	})
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}

	// The cluster creates its StatefulSet, then the plugin installs APOC.
	require.NoError(t, r.createOrUpdateResource(ctx, desired(), owner))
	pluginPatch(c, "NEO4J_PLUGINS", `["apoc"]`)

	// A cluster pass whose update conflicts with the plugin's next patch.
	racePending = true
	require.NoError(t, r.createOrUpdateResource(ctx, desired(), owner))
	require.False(t, racePending, "the race was not exercised")

	live := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "c-server", Namespace: ns}, live))
	owned := readOwnedEnvVarNames(live)
	assert.NotContains(t, owned, "NEO4J_PLUGINS", "a plugin's variable must never be recorded as cluster-owned")
	assert.Contains(t, owned, "NEO4J_AUTH")

	// The next ordinary pass must leave the plugin's variables in place.
	require.NoError(t, r.createOrUpdateResource(ctx, desired(), owner))
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "c-server", Namespace: ns}, live))
	env := map[string]string{}
	for _, e := range live.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	assert.Equal(t, `["apoc"]`, env["NEO4J_PLUGINS"])
	assert.Equal(t, "true", env["NEO4J_apoc_export_file_enabled"])
	assert.Equal(t, "neo4j/x", env["NEO4J_AUTH"])
}
