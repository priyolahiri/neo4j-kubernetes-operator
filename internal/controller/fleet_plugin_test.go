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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// A new deployment with spec.auraFleetManagement.enabled had its StatefulSet
// created without fleet-management in NEO4J_PLUGINS and patched a second
// later, so its servers restarted during their first start (walk of the
// v1.22.x fixes). The plugin is in the write that creates the StatefulSet, and
// stays out of the record of the env the deployment owns.

// statefulSetWrites counts the creates, updates and patches of StatefulSets.
type statefulSetWrites struct {
	mu sync.Mutex
	n  int
}

func (w *statefulSetWrites) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

func (w *statefulSetWrites) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.n = 0
}

func (w *statefulSetWrites) note(obj client.Object) {
	if _, ok := obj.(*appsv1.StatefulSet); ok {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.n++
	}
}

func (w *statefulSetWrites) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			w.note(obj)
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			w.note(obj)
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			w.note(obj)
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
}

func neo4jPlugins(t *testing.T, sts *appsv1.StatefulSet) string {
	t.Helper()
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name != "neo4j" {
			continue
		}
		for _, e := range c.Env {
			if e.Name == "NEO4J_PLUGINS" {
				return e.Value
			}
		}
	}
	return ""
}

func TestClusterReconcile_CreatesTheServersWithTheFleetPlugin(t *testing.T) {
	ctx := context.Background()
	scheme := noEffectTestScheme(t)
	cluster := minimalCluster("fleet", "prod")
	cluster.Finalizers = []string{ClusterFinalizer}
	// The first 5.26 whose entrypoint installs fleet-management; the validator
	// refuses it on an earlier one (operations.md id 191).
	cluster.Spec.Image.Tag = "5.26.11-enterprise"
	cluster.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
	writes := &statefulSetWrites{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, standardStorageClass()).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).WithInterceptorFuncs(writes.funcs()).Build()
	rec := record.NewFakeRecorder(200)
	r := &Neo4jEnterpriseClusterReconciler{
		Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Minute,
		Validator: validation.NewClusterValidator(c), ConfigMapManager: NewConfigMapManager(c),
	}
	key := client.ObjectKeyFromObject(cluster)
	server := func() *appsv1.StatefulSet {
		t.Helper()
		sts := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "prod", Name: "fleet-server"}, sts))
		return sts
	}

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, 1, writes.count(), "the first pass creates the server StatefulSet, once")
	created := server()
	assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, created), "the servers start with the plugin")
	assert.NotContains(t, readOwnedEnvVarNames(created), "NEO4J_PLUGINS",
		"NEO4J_PLUGINS is not the cluster's own: plugins and fleet share it")

	writes.reset()
	for _, err := range reconcileTimes(t, r, key, repeatPasses) {
		require.NoError(t, err)
	}
	assert.Zero(t, writes.count(), "no later pass writes the StatefulSet: the servers do not restart for the plugin")
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonAuraFleetPluginPatchFailed))

	// A change the cluster renders keeps the plugin.
	live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(ctx, key, live))
	require.NoError(t, patchObject(ctx, c, live, func() {
		live.Spec.Env = []corev1.EnvVar{{Name: "NEO4J_server_memory_pagecache_size", Value: "256m"}}
	}))
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, server()))
	assert.Contains(t, readOwnedEnvVarNames(server()), "NEO4J_server_memory_pagecache_size")
	assert.NotContains(t, readOwnedEnvVarNames(server()), "NEO4J_PLUGINS")
}

func TestStandaloneReconcile_CreatesThePodWithTheFleetPlugin(t *testing.T) {
	ctx := context.Background()
	scheme := noEffectTestScheme(t)
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "dev", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			// The first 5.26 whose entrypoint installs fleet-management (id 191).
			Image:               neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.11-enterprise"},
			Storage:             neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
			AuraFleetManagement: &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true},
		},
	}
	writes := &statefulSetWrites{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone, standardStorageClass()).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).WithInterceptorFuncs(writes.funcs()).Build()
	rec := record.NewFakeRecorder(200)
	r := &Neo4jEnterpriseStandaloneReconciler{
		Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Minute, Validator: validation.NewStandaloneValidator(),
	}
	key := client.ObjectKeyFromObject(standalone)

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, 1, writes.count(), "the first pass creates the StatefulSet, once")
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, key, sts))
	assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts), "the pod starts with the plugin")
	assert.NotContains(t, strings.Split(sts.Annotations[standaloneOwnedEnvVarsAnnotation], ","), "NEO4J_PLUGINS",
		"NEO4J_PLUGINS is not the standalone's own")

	writes.reset()
	for _, err := range reconcileTimes(t, r, key, repeatPasses) {
		require.NoError(t, err)
	}
	assert.Zero(t, writes.count(), "no later pass writes the StatefulSet: the pod does not restart for the plugin")
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonAuraFleetPluginPatchFailed))
}

func TestAddFleetManagementPlugin(t *testing.T) {
	template := func(env ...corev1.EnvVar) *corev1.PodTemplateSpec {
		return &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "init-like"}, {Name: "neo4j", Env: env},
		}}}
	}

	empty := template(corev1.EnvVar{Name: "A", Value: "1"})
	changed, err := addFleetManagementPlugin(empty)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, []corev1.EnvVar{{Name: "A", Value: "1"}, {Name: "NEO4J_PLUGINS", Value: `["fleet-management"]`}},
		empty.Spec.Containers[1].Env)

	shared := []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["apoc"]`}}
	withAPOC := template(shared...)
	withAPOC.Spec.Containers[1].Env = shared
	changed, err = addFleetManagementPlugin(withAPOC)
	require.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, `["apoc","fleet-management"]`, withAPOC.Spec.Containers[1].Env[0].Value, "merged, not replaced")
	assert.Equal(t, `["apoc"]`, shared[0].Value, "a new env list: the one the template shared is left alone")

	changed, err = addFleetManagementPlugin(withAPOC)
	require.NoError(t, err)
	assert.False(t, changed, "already listed")

	_, err = addFleetManagementPlugin(&corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "other"}}}})
	require.ErrorIs(t, err, errNoNeo4jContainer)
}
