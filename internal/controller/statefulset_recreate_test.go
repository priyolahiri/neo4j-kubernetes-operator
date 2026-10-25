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

// A StatefulSet a storage expansion deleted is created again as it was
// (statefulset_recreate.go, operations.md id 171). The #562 walk: with APOC and
// GDS installed and an upgrade held, an expansion created the server
// StatefulSet again from the cluster's own template — NEO4J_PLUGINS, the
// plugins' variables, procedure lists, plugin-init and plugin-ca gone — and
// the StatefulSet controller replaced running server-2 with server-1 held
// down: quorum lost, then an ungated roll and three plugin restore rolls.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

const pluginRecord = "neo4j.com/plugin-env-vars.apoc"

// recreateClient is a fake API server that gives what it stores a UID, as the
// create branch tells a new StatefulSet by it.
func recreateClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	for _, o := range objs {
		o.SetUID(types.UID(o.GetName() + "-uid"))
	}
	return interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			obj.SetUID(types.UID(obj.GetName() + "-uid"))
			return cl.Create(ctx, obj, opts...)
		},
	})
}

// withPlugins is the server StatefulSet as the cluster and a VerifiedDownload
// APOC plugin left it, held at partition 2 by a Paused upgrade.
func withPlugins(sts *appsv1.StatefulSet) {
	c := &sts.Spec.Template.Spec
	c.Containers[0].Env = append(c.Containers[0].Env,
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
		corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"})
	c.InitContainers = append(c.InitContainers, corev1.Container{Name: "plugin-init-apoc", Image: "curlimages/curl"})
	c.Volumes = append(c.Volumes, corev1.Volume{Name: "plugin-ca"})
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[pluginRecord] = "NEO4J_apoc_export_file_enabled"
	sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To(int32(2))}
}

func expansionCluster(size string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := heldCluster(upgradePhasePaused, true)
	c.UID = "cluster-uid"
	c.Spec.Storage.Size = size
	return c
}

func storedServerSTS(t *testing.T, c client.Client) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "c-server", Namespace: "default"}, sts))
	return sts
}

func dataSize(sts *appsv1.StatefulSet) string {
	for _, vct := range sts.Spec.VolumeClaimTemplates {
		q := vct.Spec.Resources.Requests[corev1.ResourceStorage]
		return q.String()
	}
	return ""
}

func TestCreateOrUpdateResource_AStatefulSetAnExpansionDeletedIsCreatedAgainAsItWas(t *testing.T) {
	ctx := context.Background()
	c := recreateClient(t)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme()}

	// The StatefulSet the operator created, then the plugin's writes.
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(expansionCluster("1Gi")), expansionCluster("1Gi")))
	live := storedServerSTS(t, c)
	withPlugins(live)
	require.NoError(t, c.Update(ctx, live))
	before := storedServerSTS(t, c)

	// The expansion deletes it, orphaning the pods; the next pass creates it.
	require.NoError(t, r.orphanDeleteStatefulSet(ctx, "default", "c-server"))
	grown := expansionCluster("2Gi")
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))

	after := storedServerSTS(t, c)
	assert.Equal(t, before.Spec.Template, after.Spec.Template, "the same template: the same revision, no pod restarts")
	assert.Equal(t, "2Gi", dataSize(after), "with the new claim templates")
	assert.Equal(t, int32(2), *after.Spec.UpdateStrategy.RollingUpdate.Partition, "the held upgrade's partition")
	assert.Equal(t, "NEO4J_apoc_export_file_enabled", after.Annotations[pluginRecord], "the plugin's record")
	for _, k := range []string{clusterTemplateHashAnnotation, ownedEnvVarsAnnotation} {
		assert.Equal(t, before.Annotations[k], after.Annotations[k], k)
	}
	_, kept := r.deletedServerStatefulSets.m.Load("default/c-server")
	assert.False(t, kept, "forgotten once created")
}

// After an operator restart the memory is gone: the template is read from the
// ControllerRevision the orphaned pods run.
func TestCreateOrUpdateResource_AfterARestartTheTemplateComesFromTheRevision(t *testing.T) {
	ctx := context.Background()
	old := resources.BuildServerStatefulSetForEnterprise(expansionCluster("1Gi"))
	withPlugins(old)
	patch := map[string]any{"spec": map[string]any{"template": old.Spec.Template}}
	raw, err := json.Marshal(patch)
	require.NoError(t, err)
	rev := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "c-server-6d4b5f", Namespace: "default"},
		Data:       runtime.RawExtension{Raw: raw}, Revision: 7,
	}
	pods := []client.Object{rev}
	for i := 0; i < 3; i++ {
		pod := heldServerPod(i, "neo4j:"+holdTarget, true)
		pod.Labels[appsv1.ControllerRevisionHashLabelKey] = "c-server-6d4b5f"
		pods = append(pods, pod)
	}
	c := recreateClient(t, pods...)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme()} // a new process: nothing remembered

	grown := expansionCluster("2Gi")
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))

	after := storedServerSTS(t, c)
	// Semantically: a quantity read back from JSON is spelled canonically.
	assert.True(t, equality.Semantic.DeepEqual(old.Spec.Template, after.Spec.Template), "the template the orphaned pods run")
	assert.Equal(t, int32(3), *after.Spec.Replicas)
	assert.Equal(t, "2Gi", dataSize(after))
	assert.Equal(t, int32(2), *after.Spec.UpdateStrategy.RollingUpdate.Partition, "the held upgrade's partition, from its record")
	// The stamp, as on any write of the template the operator renders: a
	// cluster short of a server applies only what the stamp shows changed
	// (#562 re-walk: it stayed missing while the upgrade was held).
	assert.Equal(t, podTemplateSpecHash(resources.BuildServerStatefulSetForEnterprise(grown).Spec.Template),
		after.Annotations[clusterTemplateHashAnnotation])

	// The next pass's apply keeps the template: no pod restarts.
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(grown), grown))
	next := storedServerSTS(t, c)
	assert.True(t, equality.Semantic.DeepEqual(old.Spec.Template, next.Spec.Template), "the next pass changed the template")

	// The same for a cluster no upgrade holds.
	steady := expansionCluster("1Gi")
	steady.Status.UpgradeStatus = nil
	rendered := resources.BuildServerStatefulSetForEnterprise(steady)
	withPlugins(rendered)
	raw, err = json.Marshal(map[string]any{"spec": map[string]any{"template": rendered.Spec.Template}})
	require.NoError(t, err)
	objs := []client.Object{&appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "c-server-77f8", Namespace: "default"}, Data: runtime.RawExtension{Raw: raw}}}
	for i := 0; i < 3; i++ {
		pod := heldServerPod(i, "neo4j:"+holdTarget, true)
		pod.Labels[appsv1.ControllerRevisionHashLabelKey] = "c-server-77f8"
		objs = append(objs, pod)
	}
	sc := recreateClient(t, objs...)
	r = &Neo4jEnterpriseClusterReconciler{Client: sc, Scheme: sc.Scheme()}
	steady.Spec.Storage.Size = "2Gi"
	for pass := 0; pass < 2; pass++ {
		require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(steady), steady))
		assert.True(t, equality.Semantic.DeepEqual(rendered.Spec.Template, storedServerSTS(t, sc).Spec.Template),
			"pass %d changed the template of a cluster no upgrade holds", pass)
	}

	// A cluster created for the first time has no pods to read.
	fresh := recreateClient(t)
	r = &Neo4jEnterpriseClusterReconciler{Client: fresh, Scheme: fresh.Scheme()}
	first := expansionCluster("1Gi")
	first.Status.UpgradeStatus = nil
	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(first), first))
	assert.Equal(t, resources.BuildServerStatefulSetForEnterprise(first).Spec.Template.Spec.InitContainers,
		storedServerSTS(t, fresh).Spec.Template.Spec.InitContainers)
}

// The stamp says the operator applied what it renders: a template read from a
// revision is stamped only when the apply would leave it as it is — a spec
// changed with the expansion is not hidden behind the stamp. (The fake client
// defaults nothing, so its dry run returns what it is sent; the API server's
// defaults are TestStatefulSetCreatedAgainFromARevisionIsStamped_APIServer's.)
func TestTemplateApplied(t *testing.T) {
	ctx := context.Background()
	c := recreateClient(t)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme()}
	desired := resources.BuildServerStatefulSetForEnterprise(expansionCluster("2Gi")).Spec.Template
	recreated := func() *appsv1.StatefulSet {
		sts := resources.BuildServerStatefulSetForEnterprise(expansionCluster("1Gi"))
		withPlugins(sts) // the plugins' items, appended after the operator's
		writeOwnedTemplateItems(sts, desired)
		return sts
	}
	assert.True(t, r.templateApplied(ctx, recreated(), desired), "what the operator renders, and the plugins' items")

	changed := *desired.DeepCopy()
	changed.Spec.NodeSelector = map[string]string{"disk": "ssd"} // a field only the stamp catches
	assert.False(t, r.templateApplied(ctx, recreated(), changed))

	changed = *desired.DeepCopy()
	changed.Spec.Containers[0].Env = append(changed.Spec.Containers[0].Env, corev1.EnvVar{Name: "NEO4J_server_x", Value: "1"})
	assert.False(t, r.templateApplied(ctx, recreated(), changed))

	// A dry run the API server refuses stamps nothing; the next write of the
	// template stamps it.
	refusing := interceptor.NewClient(fake.NewClientBuilder().WithScheme(c.Scheme()).Build(), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			return apierrors.NewForbidden(appsv1.Resource("statefulsets"), obj.GetName(), errors.New("quota exceeded"))
		},
	})
	r = &Neo4jEnterpriseClusterReconciler{Client: refusing, Scheme: c.Scheme()}
	assert.False(t, r.templateApplied(ctx, recreated(), desired))
}

func TestTemplateFromControllerRevision(t *testing.T) {
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "neo4j", Image: "neo4j:x"}}}}
	raw, err := json.Marshal(map[string]any{"spec": map[string]any{"template": map[string]any{
		"spec": template.Spec, "$patch": "replace"}}})
	require.NoError(t, err)
	got, err := templateFromControllerRevision(&appsv1.ControllerRevision{Data: runtime.RawExtension{Raw: raw}})
	require.NoError(t, err)
	assert.Equal(t, "neo4j:x", got.Spec.Containers[0].Image)

	_, err = templateFromControllerRevision(&appsv1.ControllerRevision{})
	assert.Error(t, err)
	_, err = templateFromControllerRevision(&appsv1.ControllerRevision{Data: runtime.RawExtension{Raw: []byte(`{"spec":{}}`)}})
	assert.Error(t, err)
}

// The standalone's expansion does the same: its one pod is not replaced, and
// a VerifiedDownload plugin's init container stays.
func TestReconcileStatefulSet_AStandaloneStatefulSetAnExpansionDeletedIsCreatedAgainAsItWas(t *testing.T) {
	ctx := context.Background()
	r, _ := standaloneCMTestReconciler(t)
	sa := standaloneForSTS("2026.08.1-enterprise")
	require.NoError(t, r.reconcileStatefulSet(ctx, sa))
	live := getSTS(t, r)
	live.Spec.Template.Spec.InitContainers = append(live.Spec.Template.Spec.InitContainers,
		corev1.Container{Name: "plugin-init-apoc", Image: "curlimages/curl"})
	live.Spec.Template.Spec.Containers[0].Env = append(live.Spec.Template.Spec.Containers[0].Env,
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`})
	require.NoError(t, r.Update(ctx, live))
	before := getSTS(t, r)

	require.NoError(t, r.orphanDeleteStandaloneStatefulSet(ctx, "default", "sa"))
	sa.Spec.Storage.Size = "2Gi"
	require.NoError(t, r.reconcileStatefulSet(ctx, sa))

	after := getSTS(t, r)
	assert.Equal(t, before.Spec.Template, after.Spec.Template, "the same template: the pod is not replaced")
	q := after.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	assert.Equal(t, 0, q.Cmp(resource.MustParse("2Gi")), "with the new claim templates")
}
