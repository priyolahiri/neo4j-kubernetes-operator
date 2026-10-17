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

// A VerifiedDownload plugin's CA volume follows its target's
// spec.trustedCASecrets: the plugin controller, its only writer, brings it in
// line with the list when the user changes it, and writes nothing while the
// list stays the same (operations.md id 147).

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// trustedCAs is a spec.trustedCASecrets list of the named Secrets.
func trustedCAs(names ...string) []neo4jv1beta1.TrustedCASecret {
	var out []neo4jv1beta1.TrustedCASecret
	for _, n := range names {
		out = append(out, neo4jv1beta1.TrustedCASecret{Name: n})
	}
	return out
}

// caTarget is cluster "c" in phase, trusting the named CA Secrets, as the
// plugin sees it.
func caTarget(phase string, cas ...string) (*neo4jv1beta1.Neo4jEnterpriseCluster, *DeploymentInfo) {
	cluster := minimalCluster("c", selfHealNS)
	cluster.Status.Phase = phase
	cluster.Spec.TrustedCASecrets = trustedCAs(cas...)
	return cluster, &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: cluster, IsReady: phase == neo4jv1beta1.PhaseReady}
}

// installedWith is sts after the plugins' install while their target was
// target, as the API server keeps it (defaulted).
func installedWith(t *testing.T, sts *appsv1.StatefulSet, target *DeploymentInfo, plugins ...*neo4jv1beta1.Neo4jPlugin) *appsv1.StatefulSet {
	t.Helper()
	r := &Neo4jPluginReconciler{}
	for _, p := range plugins {
		require.NoError(t, r.applyPluginToTemplate(&sts.Spec.Template, p, target))
	}
	apiServerDefaults(sts)
	return sts
}

// caReconciler is a plugin reconciler on a fake API server that defaults
// StatefulSets as the real one does; writes counts the StatefulSet updates and
// messages records every status message written.
func caReconciler(t *testing.T, objs ...client.Object) (*Neo4jPluginReconciler, *record.FakeRecorder, *int, *[]string) {
	t.Helper()
	writes := 0
	var messages []string
	c := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jPlugin{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); ok {
					writes++
					apiServerDefaults(obj)
				}
				return cl.Update(ctx, obj, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if p, ok := obj.(*neo4jv1beta1.Neo4jPlugin); ok {
					messages = append(messages, p.Status.Message)
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).
		Build()
	rec := record.NewFakeRecorder(32)
	bolt := &pluginBolt{
		functional: func(context.Context, *DeploymentInfo) bool { return true },
		configure:  func(context.Context, *neo4jv1beta1.Neo4jPlugin, *DeploymentInfo) error { return nil },
	}
	return &Neo4jPluginReconciler{
		Client: c, Scheme: newTestScheme(), Recorder: rec,
		RequeueAfter: 30 * time.Second, bolt: bolt,
	}, rec, &writes, &messages
}

func liveTemplate(t *testing.T, c client.Client, name string) corev1.PodTemplateSpec {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: name}, sts))
	return sts.Spec.Template
}

// projectedCAs lists the Secrets the template's plugin-ca volume projects, in
// order, and whether each is optional; nil when there is no such volume.
func projectedCAs(template corev1.PodTemplateSpec) []string {
	i := volumeIndex(&template.Spec, resources.PluginCAVolumeName)
	if i < 0 {
		return nil
	}
	var out []string
	for _, s := range template.Spec.Volumes[i].Projected.Sources {
		name := s.Secret.Name
		if s.Secret.Optional != nil && *s.Secret.Optional {
			name += " (optional)"
		}
		out = append(out, name)
	}
	return out
}

func initContainer(t *testing.T, template corev1.PodTemplateSpec, name string) corev1.Container {
	t.Helper()
	for _, c := range template.Spec.InitContainers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no init container %s in %v", name, template.Spec.InitContainers)
	return corev1.Container{}
}

// The defect: the CA volume was added when absent and never touched again, so
// a CA added to spec.trustedCASecrets never reached the download (one from a
// newly trusted mirror failed) and a removed one stayed trusted — and stayed a
// Secret the servers could not start without once it was deleted. The
// target's own rollout re-checks the Ready plugin (its truststore changes the
// pod template), which now updates the volume, in one write, without a
// Warning: the user asked for this.
func TestReconcile_SettledPluginFollowsTheTargetsTrustedCAs(t *testing.T) {
	for _, tc := range []struct {
		name      string
		before    []string
		after     []string
		projected []string
	}{
		{"a CA added", []string{"corp-ca"}, []string{"corp-ca", "mirror-ca"}, []string{"corp-ca (optional)", "mirror-ca (optional)"}},
		{"a CA removed", []string{"corp-ca", "mirror-ca"}, []string{"mirror-ca"}, []string{"mirror-ca (optional)"}},
		{"a CA replaced", []string{"corp-ca"}, []string{"mirror-ca"}, []string{"mirror-ca (optional)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
			_, before := caTarget(neo4jv1beta1.PhaseReady, tc.before...)
			cluster, _ := caTarget(neo4jv1beta1.PhaseReady, tc.after...)
			sts := installedWith(t, selfHealSTS(), before, plugin)
			r, rec, writes, messages := caReconciler(t, plugin, cluster, sts)

			reconcilePlugin(t, r, "custom")

			assert.Equal(t, 1, *writes, "one write")
			live := liveTemplate(t, r.Client, "c-server")
			assert.Equal(t, tc.projected, projectedCAs(live))
			assert.Contains(t, *messages,
				"Updating the plugin's trusted CAs: spec.trustedCASecrets of Neo4jEnterpriseCluster c changed (volume plugin-ca of StatefulSet c-server)")
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing), "nothing was lost")

			// Nothing left to change: the next pass is a settled one again.
			p := getPlugin(t, r, "custom")
			p.Status.Phase = neo4jv1beta1.PhaseReady
			require.NoError(t, r.Status().Update(context.Background(), p))
			*writes = 0
			reconcilePlugin(t, r, "custom")
			assert.Zero(t, *writes)
		})
	}
}

// Clearing the list takes the CA volume and the init container's mount of it
// away in the same write: a mount without its volume is an invalid pod, and a
// volume left behind would keep projecting a Secret the user is free to delete.
func TestReconcile_ClearedTrustedCAsTakeTheCAVolumeAway(t *testing.T) {
	plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	_, before := caTarget(neo4jv1beta1.PhaseReady, "corp-ca")
	cluster, _ := caTarget(neo4jv1beta1.PhaseReady)
	r, _, writes, messages := caReconciler(t, plugin, cluster, installedWith(t, selfHealSTS(), before, plugin))

	reconcilePlugin(t, r, "custom")

	assert.Equal(t, 1, *writes)
	live := liveTemplate(t, r.Client, "c-server")
	assert.Nil(t, projectedCAs(live), "no CA volume")
	ic := initContainer(t, live, "plugin-init-custom")
	assert.False(t, containerMountsVolume(ic, resources.PluginCAVolumeName))
	assert.NotContains(t, ic.Args[1], "/etc/plugin-ca", "the script no longer reads it")
	assert.Contains(t, *messages, "Updating the plugin's trusted CAs: spec.trustedCASecrets of Neo4jEnterpriseCluster c changed "+
		"(init container plugin-init-custom, volume plugin-ca of StatefulSet c-server)")
}

// And a list that becomes non-empty gives the init container its mount and the
// pod the volume, together — a change of the list, not something lost.
func TestReconcile_FirstTrustedCAAddsTheCAVolume(t *testing.T) {
	plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	_, before := caTarget(neo4jv1beta1.PhaseReady)
	cluster, _ := caTarget(neo4jv1beta1.PhaseReady, "corp-ca")
	r, rec, writes, _ := caReconciler(t, plugin, cluster, installedWith(t, selfHealSTS(), before, plugin))

	reconcilePlugin(t, r, "custom")

	assert.Equal(t, 1, *writes)
	live := liveTemplate(t, r.Client, "c-server")
	assert.Equal(t, []string{"corp-ca (optional)"}, projectedCAs(live))
	assert.True(t, containerMountsVolume(initContainer(t, live, "plugin-init-custom"), resources.PluginCAVolumeName))
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing))
}

// The same list writes nothing — including the volume an earlier operator
// wrote, whose projections are not optional, and a list in another order.
// Rewriting either would restart the servers for nothing.
func TestReconcile_UnchangedTrustedCAsWriteNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		earlier bool
		after   []string
	}{
		{"this operator's volume", false, []string{"corp-ca", "mirror-ca"}},
		{"an earlier operator's volume", true, []string{"corp-ca", "mirror-ca"}},
		{"the list reordered", false, []string{"mirror-ca", "corp-ca"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
			_, before := caTarget(neo4jv1beta1.PhaseReady, "corp-ca", "mirror-ca")
			cluster, _ := caTarget(neo4jv1beta1.PhaseReady, tc.after...)
			sts := installedWith(t, selfHealSTS(), before, plugin)
			if tc.earlier {
				// Its projections are not optional, and its init script is
				// not this operator's.
				for _, s := range sts.Spec.Template.Spec.Volumes[volumeIndex(&sts.Spec.Template.Spec, resources.PluginCAVolumeName)].Projected.Sources {
					s.Secret.Optional = nil
				}
				sts.Spec.Template.Spec.InitContainers[0].Args[1] += "# an earlier operator's script\n"
			}
			r, rec, writes, _ := caReconciler(t, plugin, cluster, sts)
			rv := getPlugin(t, r, "custom").ResourceVersion

			reconcilePlugin(t, r, "custom")

			assert.Zero(t, *writes)
			assert.Equal(t, rv, getPlugin(t, r, "custom").ResourceVersion, "no status write")
			assert.Empty(t, drainEvents(rec))
		})
	}
}

// Every VerifiedDownload plugin of a target shares the volume and builds the
// same one, so the first to pass updates it and the others find nothing to
// change. When the list is cleared, each takes its own mount away, and the
// volume goes with the last one — never while another still mounts it.
func TestReconcile_PluginsShareTheCAVolume(t *testing.T) {
	first := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	second := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	second.Name, second.Spec.Name = "other", "other"
	_, before := caTarget(neo4jv1beta1.PhaseReady, "corp-ca")

	t.Run("a CA added", func(t *testing.T) {
		cluster, _ := caTarget(neo4jv1beta1.PhaseReady, "corp-ca", "mirror-ca")
		r, _, writes, _ := caReconciler(t, first.DeepCopy(), second.DeepCopy(), cluster, installedWith(t, selfHealSTS(), before, first, second))

		reconcilePlugin(t, r, "custom")
		assert.Equal(t, 1, *writes)
		reconcilePlugin(t, r, "other")
		assert.Equal(t, 1, *writes, "the second plugin finds the volume up to date")
		assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "other").Status.Phase)
		assert.Equal(t, []string{"corp-ca (optional)", "mirror-ca (optional)"}, projectedCAs(liveTemplate(t, r.Client, "c-server")))
	})

	t.Run("the list cleared", func(t *testing.T) {
		cluster, _ := caTarget(neo4jv1beta1.PhaseReady)
		r, _, writes, _ := caReconciler(t, first.DeepCopy(), second.DeepCopy(), cluster, installedWith(t, selfHealSTS(), before, first, second))

		reconcilePlugin(t, r, "custom")
		assert.Equal(t, 1, *writes)
		live := liveTemplate(t, r.Client, "c-server")
		assert.False(t, containerMountsVolume(initContainer(t, live, "plugin-init-custom"), resources.PluginCAVolumeName))
		assert.NotNil(t, projectedCAs(live), "the other plugin's init container still mounts it")

		reconcilePlugin(t, r, "other")
		assert.Equal(t, 2, *writes)
		live = liveTemplate(t, r.Client, "c-server")
		assert.False(t, containerMountsVolume(initContainer(t, live, "plugin-init-other"), resources.PluginCAVolumeName))
		assert.Nil(t, projectedCAs(live), "gone with the last mount")
	})
}

// The update restarts every server, so it waits like any write of the plugin:
// the target's own rollout of the change usually has it Forming when the
// plugin looks. It says so once, as an install that waits, not as a loss.
func TestReconcile_TrustedCAUpdateWaitsForTheTarget(t *testing.T) {
	plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	_, before := caTarget(neo4jv1beta1.PhaseReady, "corp-ca")
	cluster, _ := caTarget(neo4jv1beta1.PhaseForming, "corp-ca", "mirror-ca")
	r, rec, writes, _ := caReconciler(t, plugin, cluster, installedWith(t, selfHealSTS(), before, plugin))

	res := reconcilePlugin(t, r, "custom")

	assert.Positive(t, res.RequeueAfter)
	assert.Zero(t, *writes, "not while the servers roll")
	assert.Equal(t, neo4jv1beta1.PhaseWaiting, getPlugin(t, r, "custom").Status.Phase)
	events := drainEvents(rec)
	assert.Len(t, eventsWithReason(events, EventReasonPluginInstallDeferred), 1)
	assert.Empty(t, eventsWithReason(events, EventReasonPluginMissing))

	setClusterAvailability(t, r.Client, neo4jv1beta1.PhaseReady, false)
	reconcilePlugin(t, r, "custom")
	assert.Equal(t, 1, *writes, "written once the target is Ready")
	assert.Equal(t, []string{"corp-ca (optional)", "mirror-ca (optional)"}, projectedCAs(liveTemplate(t, r.Client, "c-server")))
}

// A standalone's plugin follows the standalone's own list the same way.
func TestReconcile_StandalonePluginFollowsItsTrustedCAs(t *testing.T) {
	plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	plugin.Spec.ClusterRef = "sa"
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: selfHealNS},
		Spec:       neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{TrustedCASecrets: trustedCAs("mirror-ca")},
		Status:     neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: neo4jv1beta1.PhaseReady},
	}
	earlier := standalone.DeepCopy()
	earlier.Spec.TrustedCASecrets = trustedCAs("corp-ca")
	sts := pluginTestSTS("sa", selfHealNS)
	sts.Name = "sa"
	sts = installedWith(t, sts, &DeploymentInfo{Type: "standalone", Name: "sa", Namespace: selfHealNS, Object: earlier}, plugin)
	r, _, writes, messages := caReconciler(t, plugin, standalone, sts)

	reconcilePlugin(t, r, "custom")

	assert.Equal(t, 1, *writes)
	assert.Equal(t, []string{"mirror-ca (optional)"}, projectedCAs(liveTemplate(t, r.Client, "sa")))
	assert.Contains(t, *messages,
		"Updating the plugin's trusted CAs: spec.trustedCASecrets of Neo4jEnterpriseStandalone sa changed (volume plugin-ca of StatefulSet sa)")
}

// pluginPodDrift tells what the list changed from what something removed.
func TestPluginPodDrift_TrustedCAs(t *testing.T) {
	plugin := verifiedDownloadSelfHeal("", "")
	r := &Neo4jPluginReconciler{}
	template := func(cas ...string) corev1.PodSpec {
		_, target := caTarget(neo4jv1beta1.PhaseReady, cas...)
		sts := selfHealSTS()
		require.NoError(t, r.applyPluginToTemplate(&sts.Spec.Template, plugin, target))
		return sts.Spec.Template.Spec
	}

	for _, tc := range []struct {
		name              string
		live, desired     corev1.PodSpec
		missing, outdated []string
	}{
		{"unchanged", template("a"), template("a"), nil, nil},
		{"another CA", template("a"), template("a", "b"), nil, []string{"volume plugin-ca"}},
		{"the first CA", template(), template("a"), nil, []string{"init container plugin-init-custom", "volume plugin-ca"}},
		{"no CA any more", template("a"), template(), nil, []string{"init container plugin-init-custom", "volume plugin-ca"}},
		{"removed with its init container", selfHealSTS().Spec.Template.Spec, template("a"),
			[]string{"init container plugin-init-custom", "volume plugin-auth-custom", "volume plugin-ca"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			missing, outdated := pluginPodDrift(tc.live, tc.desired)
			assert.Equal(t, tc.missing, missing)
			assert.Equal(t, tc.outdated, outdated)
		})
	}
}

// Against an API server, which refuses a pod template whose init container
// mounts a volume the pod lacks, and defaults what it keeps: the cluster
// controller renders its truststore from each list, the plugin follows it, and
// once both have passed neither changes the template again.
func TestPluginCAVolumeFollowsTrustedCASecrets_APIServer(t *testing.T) {
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
	cluster.Spec.TrustedCASecrets = trustedCAs("corp-ca")
	require.NoError(t, c.Create(ctx, cluster))
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
	plugins := &Neo4jPluginReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(8)}
	first, second := verifiedDownloadPlugin(), verifiedDownloadPlugin()
	first.Namespace, second.Namespace = "default", "default"
	second.Name, second.Spec.Name = "other", "other"
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
	require.NoError(t, plugins.installPluginViaEnvironment(ctx, first, target))
	require.NoError(t, plugins.installPluginViaEnvironment(ctx, second, target))

	for _, step := range []struct {
		name      string
		cas       []string
		projected []string
	}{
		{"a CA added", []string{"corp-ca", "mirror-ca"}, []string{"corp-ca (optional)", "mirror-ca (optional)"}},
		{"a CA removed", []string{"mirror-ca"}, []string{"mirror-ca (optional)"}},
		{"the list cleared", nil, nil},
		{"a first CA again", []string{"corp-ca"}, []string{"corp-ca (optional)"}},
	} {
		t.Run(step.name, func(t *testing.T) {
			cluster.Spec.TrustedCASecrets = trustedCAs(step.cas...)
			// The cluster renders its truststore from the new list, and the
			// plugins follow it.
			require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(cluster), cluster))
			for _, p := range []*neo4jv1beta1.Neo4jPlugin{first, second} {
				_, outdated, err := plugins.missingFromTarget(ctx, p, target)
				require.NoError(t, err)
				if outdated != "" {
					require.NoError(t, plugins.installPluginViaEnvironment(ctx, p, target))
				}
			}
			updated := live()
			assert.Equal(t, step.projected, projectedCAs(updated.Spec.Template))
			for _, name := range []string{"plugin-init-custom", "plugin-init-other"} {
				assert.Equal(t, len(step.cas) > 0, containerMountsVolume(initContainer(t, updated.Spec.Template, name), resources.PluginCAVolumeName), name)
			}

			// Settled: no pass of either controller changes the template.
			for i := 0; i < 2; i++ {
				require.NoError(t, r.createOrUpdateResource(ctx, resources.BuildServerStatefulSetForEnterprise(cluster), cluster))
				for _, p := range []*neo4jv1beta1.Neo4jPlugin{first, second} {
					require.NoError(t, plugins.installPluginViaEnvironment(ctx, p, target))
					missing, outdated, err := plugins.missingFromTarget(ctx, p, target)
					require.NoError(t, err)
					assert.Empty(t, missing)
					assert.Empty(t, outdated)
				}
			}
			assert.Equal(t, updated.Generation, live().Generation, "no pass changed the pod template")
		})
	}
}
