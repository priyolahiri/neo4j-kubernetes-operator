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

// Fleet Manager is built into Neo4j from 2026.03 (operations.md id 192): the
// image's neo4j-plugins.json still names fleet-management, with no versions
// URL and no JAR in products/, so the entrypoint wgets "null", logs an ERROR
// and starts without it. The operator adds the name to NEO4J_PLUGINS nowhere
// there, and leaves one an earlier write put there.

import (
	"context"
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

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

func TestAddFleetManagementPlugin_NotOnAnImageWithFleetBuiltIn(t *testing.T) {
	template := func(image string, env ...corev1.EnvVar) *corev1.PodTemplateSpec {
		return &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: image, Env: env}}}}
	}
	for _, image := range []string{"neo4j:2026.03.0-enterprise", "neo4j:2026.08.1-enterprise", "neo4j:2026.09-enterprise",
		"registry.example:5000/neo4j:2026.08.1-enterprise@sha256:0000000000000000000000000000000000000000000000000000000000000000"} {
		fresh := template(image, corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`})
		changed, err := addFleetManagementPlugin(fresh)
		require.NoError(t, err)
		assert.False(t, changed, image)
		assert.Equal(t, `["apoc"]`, fresh.Spec.Containers[0].Env[0].Value, "%s: not added", image)

		// What an earlier operator wrote stays: no write, no restart.
		legacy := template(image, corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["fleet-management"]`})
		changed, err = addFleetManagementPlugin(legacy)
		require.NoError(t, err)
		assert.False(t, changed, image)
		assert.Equal(t, `["fleet-management"]`, legacy.Spec.Containers[0].Env[0].Value, "%s: not removed", image)
	}
	for _, image := range []string{"neo4j:2026.02.2-enterprise", "neo4j:5.26.32-enterprise", "neo4j:5.26-enterprise"} {
		older := template(image)
		changed, err := addFleetManagementPlugin(older)
		require.NoError(t, err)
		assert.True(t, changed, "%s: the entrypoint installs it from products/", image)
	}
}

// A cluster and a standalone on 2026.08.1 with Aura Fleet Management start
// without fleet-management in NEO4J_PLUGINS, and later passes write nothing.
func TestReconcile_AuraFleetManagementOnABuiltInImageAddsNoPlugin(t *testing.T) {
	ctx := context.Background()
	scheme := noEffectTestScheme(t)

	t.Run("cluster", func(t *testing.T) {
		cluster := minimalCluster("fleet", "prod")
		cluster.Spec.Image.Tag = "2026.08.1-enterprise"
		cluster.Finalizers = []string{ClusterFinalizer}
		cluster.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
		writes := &statefulSetWrites{}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, standardStorageClass()).
			WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).WithInterceptorFuncs(writes.funcs()).Build()
		r := &Neo4jEnterpriseClusterReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(200), RequeueAfter: time.Minute,
			Validator: validation.NewClusterValidator(c), ConfigMapManager: NewConfigMapManager(c),
		}
		key := client.ObjectKeyFromObject(cluster)
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		sts := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "prod", Name: "fleet-server"}, sts))
		assert.NotContains(t, neo4jPlugins(t, sts), "fleet-management")
		writes.reset()
		for _, err := range reconcileTimes(t, r, key, repeatPasses) {
			require.NoError(t, err)
		}
		assert.Zero(t, writes.count())
	})

	t.Run("standalone", func(t *testing.T) {
		standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "dev", Finalizers: []string{StandaloneFinalizer}},
			Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
				AuraFleetManagement:    &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true},
			},
		}
		writes := &statefulSetWrites{}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone, standardStorageClass()).
			WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).WithInterceptorFuncs(writes.funcs()).Build()
		r := &Neo4jEnterpriseStandaloneReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(200), RequeueAfter: time.Minute,
			Validator: validation.NewStandaloneValidator(),
		}
		key := client.ObjectKeyFromObject(standalone)
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		sts := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, key, sts))
		assert.NotContains(t, neo4jPlugins(t, sts), "fleet-management")
		writes.reset()
		for _, err := range reconcileTimes(t, r, key, repeatPasses) {
			require.NoError(t, err)
		}
		assert.Zero(t, writes.count())
	})
}

// builtInFleetCluster is cluster "c", Ready, on 2026.08.1.
func builtInFleetCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := readyCluster()
	c.Spec.Image.Tag = "2026.08.1-enterprise"
	return c
}

// A Managed fleet-management Neo4jPlugin on an image with Fleet Manager built
// in installs nothing and is Ready, with no ProceduresHidden: the allowlist
// names extensions in the plugins directory, not the server's own procedures.
// One an earlier operator installed keeps what it wrote: nothing restarts.
func TestReconcile_AFleetManagementPluginOnABuiltInImageInstallsNothing(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		plugin := selfHealPlugin("fleet", "c", "", "")
		plugin.Spec.Name, plugin.Spec.Config = "fleet-management", nil
		sts := selfHealSTS(corev1.EnvVar{Name: allowlistVar, Value: "apoc.*"})
		r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin, builtInFleetCluster(), sts}, readyClusterPods()...)...)

		reconcilePlugin(t, r, "fleet")

		assert.Zero(t, *writes, "nothing to install")
		got := getPlugin(t, r, "fleet")
		assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
		assert.Contains(t, got.Status.Message, "Fleet Manager is built into neo4j:2026.08.1-enterprise")
		assert.Nil(t, findCondition(got.Status.Conditions, ConditionTypeProceduresHidden))
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginProceduresHidden))
		_, listed := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
		assert.False(t, listed)
	})

	t.Run("installed by an earlier operator", func(t *testing.T) {
		plugin := fleetPlugin()
		sts := selfHealSTS(
			corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["fleet-management"]`},
			corev1.EnvVar{Name: unrestrictedVar, Value: "fleetManagement.*"},
		)
		sts.Annotations = map[string]string{pluginEntriesAnnotation("fleet"): "fleet-management"}
		r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{plugin, builtInFleetCluster(), sts}, readyClusterPods()...)...)

		reconcilePlugin(t, r, "fleet")

		assert.Zero(t, *writes, "no restart for a name the entrypoint skips")
		assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "fleet").Status.Phase)
		v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
		assert.Equal(t, `["fleet-management"]`, v)
	})
}
