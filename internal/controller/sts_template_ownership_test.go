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

// The cluster controller keeps the init containers and volumes it did not
// render on the server StatefulSet — a VerifiedDownload plugin's — and still
// drops the ones it stops rendering (operations.md id 141).

import (
	"context"
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
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

const ownershipNS = "owned"

// ownershipCluster is the Neo4jEnterpriseCluster "c" that owns the server
// StatefulSet; its trusted CA Secret is what a VerifiedDownload plugin's CA
// volume projects.
func ownershipCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: ownershipNS, UID: "cluster-uid"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			TrustedCASecrets: []neo4jv1beta1.TrustedCASecret{{Name: "corp-ca"}},
		},
	}
}

// ownershipDesired is the server StatefulSet the cluster controller renders:
// the neo4j container and its plugins emptyDir, and — while the cluster trusts
// extra CAs — the truststore init container and its volume.
func ownershipDesired(image string, truststore bool) *appsv1.StatefulSet {
	labels := map[string]string{"app": "c"}
	spec := corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "neo4j", Image: image,
			Env:          []corev1.EnvVar{{Name: "NEO4J_AUTH", Value: "neo4j/x"}},
			VolumeMounts: []corev1.VolumeMount{{Name: "plugins", MountPath: "/plugins"}},
		}},
		Volumes: []corev1.Volume{{Name: "plugins", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
	}
	if truststore {
		spec.InitContainers = []corev1.Container{{
			Name: "truststore-init", Image: image,
			VolumeMounts: []corev1.VolumeMount{{Name: "truststore", MountPath: "/truststore"}},
		}}
		spec.Volumes = append(spec.Volumes, corev1.Volume{
			Name:         "truststore",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "corp-ca"}},
		})
	}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "c-server", Namespace: ownershipNS},
		Spec: appsv1.StatefulSetSpec{
			Replicas:       ptr.To[int32](2),
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
			Selector:       &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       spec,
			},
		},
	}
}

// verifiedDownloadPlugin is a custom plugin fetched from a private mirror: an
// init container, an auth volume and (from the cluster's trusted CAs) a CA
// volume.
func verifiedDownloadPlugin() *neo4jv1beta1.Neo4jPlugin {
	return &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{Name: "custom", Namespace: ownershipNS, Generation: 1},
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			ClusterRef: "c", Name: "custom", Version: "1.0.0", Enabled: true,
			InstallMode: PluginInstallModeVerifiedDownload,
			Source: &neo4jv1beta1.PluginSource{
				Type: "url", URL: "https://mirror.example.com/custom-1.0.0.jar",
				Checksum:   "sha256:0000000000000000000000000000000000000000000000000000000000000000",
				AuthSecret: "mirror-token",
			},
		},
	}
}

// apiServerDefaults stands in for the defaulting the API server does and the
// fake client does not: a live template never equals the one that was sent.
func apiServerDefaults(obj client.Object) {
	sts, ok := obj.(*appsv1.StatefulSet)
	if !ok {
		return
	}
	if sts.Spec.UpdateStrategy.RollingUpdate == nil {
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{Partition: ptr.To[int32](0)}
	}
	pod := &sts.Spec.Template.Spec
	for _, list := range [][]corev1.Container{pod.InitContainers, pod.Containers} {
		for i := range list {
			if list[i].TerminationMessagePath == "" {
				list[i].TerminationMessagePath = corev1.TerminationMessagePathDefault
			}
			if list[i].TerminationMessagePolicy == "" {
				list[i].TerminationMessagePolicy = corev1.TerminationMessageReadFile
			}
		}
	}
	for i := range pod.Volumes {
		if s := pod.Volumes[i].Secret; s != nil && s.DefaultMode == nil {
			s.DefaultMode = ptr.To[int32](corev1.SecretVolumeSourceDefaultMode)
		}
	}
}

// ownershipClient is a fake API server that gives objects UIDs and defaults
// StatefulSets as the real one does; writes counts the StatefulSet updates.
func ownershipClient(t *testing.T, objs ...client.Object) (client.Client, *runtime.Scheme, *int) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	writes := 0
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				obj.SetUID(types.UID(obj.GetName() + "-uid"))
			}
			apiServerDefaults(obj)
			return cl.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok {
				writes++
			}
			apiServerDefaults(obj)
			return cl.Update(ctx, obj, opts...)
		},
	}), scheme, &writes
}

func liveServerSTS(t *testing.T, c client.Client) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ownershipNS, Name: "c-server"}, sts))
	return sts
}

// markStable reports both servers ready, so the cluster controller compares
// every field of the template rather than the few it checks while forming.
func markStable(t *testing.T, c client.Client) {
	t.Helper()
	sts := liveServerSTS(t, c)
	sts.Status.Replicas = 2
	sts.Status.ReadyReplicas = 2
	require.NoError(t, c.Status().Update(context.Background(), sts))
}

func initContainerNames(sts *appsv1.StatefulSet) []string {
	var names []string
	for _, c := range sts.Spec.Template.Spec.InitContainers {
		names = append(names, c.Name)
	}
	return names
}

func templateVolumeNames(sts *appsv1.StatefulSet) []string {
	var names []string
	for _, v := range sts.Spec.Template.Spec.Volumes {
		names = append(names, v.Name)
	}
	return names
}

// The defect: the cluster controller replaced the pod template's init
// containers and volumes wholesale whenever it applied its template — and a
// foreign init container made a stable cluster's template look changed, so it
// applied it on the next pass. The servers restarted without the plugin's JAR
// while the plugin stayed Ready.
func TestCreateOrUpdateResource_KeepsAPluginsInitContainerAndVolumes(t *testing.T) {
	ctx := context.Background()
	cluster := ownershipCluster()
	c, scheme, writes := ownershipClient(t)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
	plugins := &Neo4jPluginReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(8)}
	plugin := verifiedDownloadPlugin()
	target := &DeploymentInfo{Object: cluster, Type: "cluster", Name: "c", Namespace: ownershipNS}

	require.NoError(t, r.createOrUpdateResource(ctx, ownershipDesired("neo4j:2026.08.1-enterprise", true), cluster))
	markStable(t, c)
	require.NoError(t, plugins.installPluginViaEnvironment(ctx, plugin, target))
	installed := liveServerSTS(t, c)
	require.Contains(t, initContainerNames(installed), "plugin-init-custom")
	require.Subset(t, templateVolumeNames(installed), []string{"plugin-auth-custom", "plugin-ca"})

	// A cluster pass with nothing to change leaves the template alone. (It
	// still writes the StatefulSet: the API server defaults fields of its spec
	// the desired object leaves empty — a write the API server finds empty.)
	require.NoError(t, r.createOrUpdateResource(ctx, ownershipDesired("neo4j:2026.08.1-enterprise", true), cluster))
	quiet := liveServerSTS(t, c)
	assert.True(t, equality.Semantic.DeepEqual(installed.Spec.Template, quiet.Spec.Template),
		"a plugin's init container is not a change to the cluster's template: %v", initContainerNames(quiet))

	// An upgrade applies the cluster's template and keeps the plugin's items.
	require.NoError(t, r.createOrUpdateResource(ctx, ownershipDesired("neo4j:2026.09.0-enterprise", true), cluster))
	upgraded := liveServerSTS(t, c)
	assert.Equal(t, "neo4j:2026.09.0-enterprise", upgraded.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, []string{"truststore-init", "plugin-init-custom"}, initContainerNames(upgraded))
	assert.ElementsMatch(t, []string{"plugins", "truststore", "plugin-auth-custom", "plugin-ca"}, templateVolumeNames(upgraded))
	assert.Equal(t, "plugins,truststore", upgraded.Annotations[ownedVolumesAnnotation], "only the cluster's are recorded as its own")
	assert.Equal(t, "truststore-init", upgraded.Annotations[ownedInitContainersAnnotation])

	// What the cluster stops rendering goes; the plugin's stays.
	require.NoError(t, r.createOrUpdateResource(ctx, ownershipDesired("neo4j:2026.09.0-enterprise", false), cluster))
	cleared := liveServerSTS(t, c)
	assert.Equal(t, []string{"plugin-init-custom"}, initContainerNames(cleared))
	assert.ElementsMatch(t, []string{"plugins", "plugin-auth-custom", "plugin-ca"}, templateVolumeNames(cleared))

	// Neither controller has anything left to change: no oscillation.
	require.NoError(t, r.createOrUpdateResource(ctx, ownershipDesired("neo4j:2026.09.0-enterprise", false), cluster))
	assert.True(t, equality.Semantic.DeepEqual(cleared.Spec.Template, liveServerSTS(t, c).Spec.Template),
		"the cluster's next pass changes nothing")
	missing, err := plugins.missingFromTarget(ctx, plugin, target)
	require.NoError(t, err)
	assert.Empty(t, missing, "the plugin finds nothing missing")
	before := *writes
	require.NoError(t, plugins.installPluginViaEnvironment(ctx, plugin, target))
	assert.Equal(t, before, *writes, "and its install pass writes nothing, defaulted init container and all")
}

// Against an API server, with its own defaulting and the cluster's real server
// StatefulSet: once a VerifiedDownload plugin is installed, neither the
// cluster controller's passes nor the plugin's change the pod template again —
// the StatefulSet's generation, which counts spec changes, stays put.
func TestClusterAndPluginLeaveEachOthersItemsAlone_APIServer(t *testing.T) {
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

	cluster := minimalCluster("c", "default")
	cluster.Spec.TrustedCASecrets = []neo4jv1beta1.TrustedCASecret{{Name: "corp-ca"}}
	require.NoError(t, c.Create(ctx, cluster))
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
	plugins := &Neo4jPluginReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(8)}
	plugin := verifiedDownloadPlugin()
	plugin.Namespace = "default"
	target := &DeploymentInfo{Object: cluster, Type: "cluster", Name: "c", Namespace: "default"}
	key := types.NamespacedName{Namespace: "default", Name: "c-server"}
	live := func() *appsv1.StatefulSet {
		sts := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, key, sts))
		return sts
	}

	require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(cluster), cluster))
	sts := live()
	sts.Status.Replicas = *sts.Spec.Replicas
	sts.Status.ReadyReplicas = *sts.Spec.Replicas
	require.NoError(t, c.Status().Update(ctx, sts))
	require.NoError(t, plugins.installPluginViaEnvironment(ctx, plugin, target))
	installed := live()
	require.Contains(t, initContainerNames(installed), "plugin-init-custom")

	for i := 0; i < 2; i++ {
		require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(cluster), cluster))
		require.NoError(t, plugins.installPluginViaEnvironment(ctx, plugin, target))
	}
	after := live()
	assert.Equal(t, installed.Generation, after.Generation, "no pass changed the pod template")
	assert.Contains(t, initContainerNames(after), "plugin-init-custom")
	assert.Subset(t, templateVolumeNames(after), []string{"plugin-auth-custom", "plugin-ca"})
	changes, err := plugins.installChangesTarget(ctx, plugin, target)
	require.NoError(t, err)
	assert.False(t, changes, "the API server's defaults are not a change to the plugin's init container")
	missing, err := plugins.missingFromTarget(ctx, plugin, target)
	require.NoError(t, err)
	assert.Empty(t, missing)
}

// A StatefulSet an earlier operator wrote has no record of the init
// containers and volumes the cluster rendered. That operator replaced them
// wholesale, so everything on the template is the cluster's except what the
// plugin controller lists as its own and the volumes only its init containers
// mount: a truststore init container the cluster stops rendering still goes.
func TestCreateOrUpdateResource_ReadsAnEarlierOperatorsTemplate(t *testing.T) {
	ctx := context.Background()
	cluster := ownershipCluster()
	earlier := ownershipDesired("neo4j:2026.08.1-enterprise", true)
	earlier.Annotations = map[string]string{
		ownedEnvVarsAnnotation:        "NEO4J_AUTH",
		clusterTemplateHashAnnotation: podTemplateSpecHash(earlier.Spec.Template),
	}
	earlier.UID = "c-server-uid"
	pod := &earlier.Spec.Template
	pod.Annotations = map[string]string{PluginInitContainersAnnotation: "plugin-init-custom"}
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{
		Name: "plugin-init-custom", Image: "curlimages/curl:8.5.0",
		VolumeMounts: []corev1.VolumeMount{
			{Name: "plugins", MountPath: "/plugins"},
			{Name: "plugin-auth-custom", MountPath: "/etc/plugin-auth"},
			{Name: "plugin-ca", MountPath: "/etc/plugin-ca"},
		},
	})
	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{Name: "plugin-auth-custom", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "mirror-token"}}},
		corev1.Volume{Name: "plugin-ca", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}}},
	)
	c, scheme, _ := ownershipClient(t, earlier)
	markStable(t, c)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}

	owned := readOwnedTemplateItems(earlier, earlier.Spec.Template)
	assert.Equal(t, map[string]struct{}{"truststore-init": {}}, owned.initContainers)
	assert.Equal(t, map[string]struct{}{"plugins": {}, "truststore": {}}, owned.volumes)

	// Nothing to change: a pass leaves the earlier operator's template alone.
	require.NoError(t, r.createOrUpdateResource(ctx, ownershipDesired("neo4j:2026.08.1-enterprise", true), cluster))
	assert.True(t, equality.Semantic.DeepEqual(earlier.Spec.Template, liveServerSTS(t, c).Spec.Template),
		"the plugin's init container and volumes are not a change")

	// The user clears spec.trustedCASecrets.
	require.NoError(t, r.createOrUpdateResource(ctx, ownershipDesired("neo4j:2026.08.1-enterprise", false), cluster))
	live := liveServerSTS(t, c)
	assert.Equal(t, []string{"plugin-init-custom"}, initContainerNames(live))
	assert.ElementsMatch(t, []string{"plugins", "plugin-auth-custom", "plugin-ca"}, templateVolumeNames(live))
	assert.Equal(t, "plugins", live.Annotations[ownedVolumesAnnotation], "and the record is written")
	assert.Empty(t, live.Annotations[ownedInitContainersAnnotation])
}
