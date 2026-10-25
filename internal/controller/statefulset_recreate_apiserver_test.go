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

// Against an API server, a StatefulSet a storage expansion deleted and the
// operator created again from a ControllerRevision after a restart carries the
// cluster-template hash (operations.md id 171). The re-walk of 2a50fa56: what a
// revision holds is the template the API server stored — defaulted and
// converted — never what the operator renders, the two never compared equal,
// and the hash stayed absent until Staging wrote the template. The fake
// client defaults nothing, so the tests beside this one could not see it.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func TestStatefulSetCreatedAgainFromARevisionIsStamped_APIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test-unit`")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	defer func() { _ = env.Stop() }()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := context.Background()

	live := func(ns string) *appsv1.StatefulSet {
		sts := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "c-server"}, sts))
		return sts
	}
	// revision writes the server StatefulSet of a cluster on tag, as the
	// cluster and the APOC plugin left it, records the template the API server
	// stored in the ControllerRevision name as the StatefulSet controller does,
	// and deletes the StatefulSet. No garbage collector runs here: the pods
	// stay, as an orphan delete leaves them.
	revision := func(ns, tag, name string) corev1.PodTemplateSpec {
		cluster := expansionCluster("1Gi")
		cluster.Namespace, cluster.Spec.Image.Tag = ns, tag
		sts := resources.BuildServerStatefulSetForEnterprise(cluster)
		withPlugins(sts)
		written := *sts.Spec.Template.DeepCopy()
		require.NoError(t, c.Create(ctx, sts))
		stored := live(ns)
		require.False(t, equality.Semantic.DeepEqual(written, stored.Spec.Template),
			"the API server stores more than the operator and the plugin wrote")
		require.NoError(t, c.Delete(ctx, stored))
		template := map[string]any{}
		raw, err := json.Marshal(stored.Spec.Template)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &template))
		template["$patch"] = "replace"
		raw, err = json.Marshal(map[string]any{"spec": map[string]any{"template": template}})
		require.NoError(t, err)
		require.NoError(t, c.Create(ctx, &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Data: runtime.RawExtension{Raw: raw}, Revision: 1}))
		return stored.Spec.Template
	}
	pods := func(ns string, revisions ...string) {
		for i, rev := range revisions {
			pod := heldServerPod(i, "neo4j:"+holdTarget, true)
			pod.Namespace = ns
			pod.Labels[appsv1.ControllerRevisionHashLabelKey] = rev
			require.NoError(t, c.Create(ctx, pod))
		}
	}
	namespace := func(name string) {
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}))
	}

	t.Run("in one step", func(t *testing.T) {
		const ns = "steady"
		namespace(ns)
		stored := revision(ns, holdTarget, "c-server-77f8")
		pods(ns, "c-server-77f8", "c-server-77f8", "c-server-77f8")

		steady := expansionCluster("2Gi")
		steady.Namespace = ns
		steady.Status.UpgradeStatus = nil
		desired := resources.BuildServerStatefulSetForEnterprise(steady).Spec.Template
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme} // a new process: nothing remembered

		// Both templates are compared as the API server stores them now,
		// whatever form the one in hand is in: a revision an older API server
		// wrote lacks a default a newer one adds. Here, not stored at all: the
		// create, as a dry run.
		unstored := resources.BuildServerStatefulSetForEnterprise(steady)
		withPlugins(unstored)
		writeOwnedTemplateItems(unstored, desired)
		assert.True(t, r.templateApplied(ctx, unstored, desired))

		require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(steady), steady))
		after := live(ns)
		assert.True(t, equality.Semantic.DeepEqual(stored, after.Spec.Template), "the template the orphaned pods run")
		assert.Equal(t, podTemplateSpecHash(desired), after.Annotations[clusterTemplateHashAnnotation],
			"the stamp, as on any write of the template the operator renders")

		// The update of the live StatefulSet, as the next pass would make it.
		assert.True(t, r.templateApplied(ctx, after, desired))
		changed := *desired.DeepCopy()
		changed.Spec.NodeSelector = map[string]string{"disk": "ssd"} // a field only the stamp catches
		assert.False(t, r.templateApplied(ctx, after, changed), "a spec changed with the expansion is not hidden behind the stamp")

		for pass := 0; pass < 2; pass++ {
			require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(steady), steady))
		}
		assert.Equal(t, after.Generation, live(ns).Generation, "the next passes leave the template as it is")
	})

	t.Run("in two steps under a held partition", func(t *testing.T) {
		const ns = "held"
		namespace(ns)
		before := revision(ns, holdRunning, "c-server-before")
		target := revision(ns, holdTarget, "c-server-target")
		pods(ns, "c-server-before", "c-server-before", "c-server-target")

		grown := expansionCluster("2Gi")
		grown.Namespace = ns
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
		require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))
		first := live(ns)
		require.True(t, equality.Semantic.DeepEqual(before, first.Spec.Template), "first on the template the servers below the partition run")
		require.Equal(t, "2", first.Annotations[recreateRestorePartitionAnnotation])

		// The StatefulSet controller records it as the current revision.
		first.Status = appsv1.StatefulSetStatus{ObservedGeneration: first.Generation, Replicas: 3,
			CurrentRevision: "c-server-before", UpdateRevision: "c-server-before"}
		require.NoError(t, c.Status().Update(ctx, first))
		require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))

		after := live(ns)
		assert.True(t, equality.Semantic.DeepEqual(target, after.Spec.Template), "the held template, written back")
		assert.Equal(t, int32(2), *statefulSetPartition(after))
		assert.NotContains(t, after.Annotations, recreateRestorePartitionAnnotation)
		assert.Equal(t, podTemplateSpecHash(resources.BuildServerStatefulSetForEnterprise(grown).Spec.Template),
			after.Annotations[clusterTemplateHashAnnotation],
			"the stamp, so a change of the spec reaches the StatefulSet while a server is down")
	})
}
