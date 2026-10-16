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

// A write of a Neo4jPlugin to its target's StatefulSet restarts every server,
// so it waits until the target is Ready with every server available; a
// VerifiedDownload plugin's init container and volumes are written once, and
// put back when something removes them (operations.md id 141).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// boltMustNotRun fails the test if a pass that should only wait connects to
// Neo4j or configures anything.
func boltMustNotRun(t *testing.T) *pluginBolt {
	return &pluginBolt{
		functional: func(context.Context, *DeploymentInfo) bool {
			t.Error("a pass that waits for its target must not connect to Neo4j")
			return true
		},
		configure: func(context.Context, *neo4jv1beta1.Neo4jPlugin, *DeploymentInfo) error {
			t.Error("a pass that waits for its target must not configure anything")
			return nil
		},
	}
}

// setClusterAvailability gives cluster "c" phase and, when down is set, a
// server unavailable.
func setClusterAvailability(t *testing.T, c client.Client, phase string, down bool) {
	t.Helper()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: "c"}, cluster))
	cluster.Status.Phase = phase
	cluster.Status.Conditions = nil
	if down {
		cluster.Status.Conditions = degradedCluster(phase).Status.Conditions
	}
	require.NoError(t, c.Update(context.Background(), cluster))
}

// The defect: getTargetDeployment computed whether the target could take a
// restart of every server, and nothing read it. An install on a cluster short
// of a server rolled the rest, which can cost the cluster its majority.
func TestReconcile_InstallWaitsUntilTheTargetIsFullyAvailable(t *testing.T) {
	standaloneSTS := pluginTestSTS("sa", selfHealNS)
	standaloneSTS.Name = "sa"
	for _, tc := range []struct {
		name    string
		target  client.Object
		sts     *appsv1.StatefulSet
		ref     string
		message string
	}{
		{"a cluster with a server down, inside the grace period", degradedCluster(neo4jv1beta1.PhaseReady), selfHealSTS(), "c",
			"Waiting until Neo4jEnterpriseCluster c is Ready with every server available: installing the plugin restarts every server"},
		{"a Degraded cluster", degradedCluster(neo4jv1beta1.PhaseDegraded), selfHealSTS(), "c",
			"Waiting until Neo4jEnterpriseCluster c is Ready with every server available"},
		{"a cluster rolling its servers", func() client.Object {
			c := minimalCluster("c", selfHealNS)
			c.Status.Phase = neo4jv1beta1.PhaseForming
			return c
		}(), selfHealSTS(), "c", "Waiting until Neo4jEnterpriseCluster c is Ready"},
		{"a standalone that is not Ready", &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: selfHealNS},
			Status:     neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: neo4jv1beta1.PhasePending},
		}, standaloneSTS, "sa", "Waiting until Neo4jEnterpriseStandalone sa is Ready: installing the plugin restarts it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := selfHealPlugin("apoc", tc.ref, "", "")
			r, rec, stsUpdates := selfHealReconciler(t, boltMustNotRun(t), nil, plugin, tc.target, tc.sts.DeepCopy())

			res := reconcilePlugin(t, r, "apoc")

			assert.Positive(t, res.RequeueAfter, "it looks again")
			assert.Zero(t, *stsUpdates, "the servers are not restarted")
			got := getPlugin(t, r, "apoc")
			assert.Equal(t, neo4jv1beta1.PhaseWaiting, got.Status.Phase)
			assert.Contains(t, got.Status.Message, tc.message)
			deferred := eventsWithReason(drainEvents(rec), EventReasonPluginInstallDeferred)
			require.Len(t, deferred, 1, "the wait is announced")
			assert.Contains(t, deferred[0], "Normal")

			// The passes that go on waiting change nothing and say nothing.
			rv := got.ResourceVersion
			res = reconcilePlugin(t, r, "apoc")
			assert.Positive(t, res.RequeueAfter)
			assert.Equal(t, rv, getPlugin(t, r, "apoc").ResourceVersion, "no status write")
			assert.Empty(t, drainEvents(rec), "no second announcement")
			assert.Zero(t, *stsUpdates)
		})
	}
}

// Once every server is back the install goes ahead.
func TestReconcile_DeferredInstallProceedsWhenTheClusterRecovers(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", "", "")
	bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
	r, rec, stsUpdates := selfHealReconciler(t, bolt, nil, plugin, degradedCluster(neo4jv1beta1.PhaseDegraded), selfHealSTS())

	reconcilePlugin(t, r, "apoc")
	require.Equal(t, neo4jv1beta1.PhaseWaiting, getPlugin(t, r, "apoc").Status.Phase)
	require.Zero(t, *stsUpdates)
	drainEvents(rec)

	setClusterAvailability(t, r.Client, neo4jv1beta1.PhaseReady, false)
	res := reconcilePlugin(t, r, "apoc")

	assert.Equal(t, 1, *stsUpdates, "the install writes the StatefulSet")
	v, ok := findEnv(&corev1.Container{Env: liveSTSEnv(t, r.Client, "c-server")}, "NEO4J_PLUGINS")
	require.True(t, ok)
	assert.Equal(t, `["apoc"]`, v)
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase, "and waits for the servers it restarted")
	assert.Positive(t, res.RequeueAfter)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallDeferred))
}

// A Ready plugin whose target lost what it set waits too: restoring it is an
// install. It says once what is missing and why it waits, and restores it when
// the cluster has every server again.
func TestReconcile_RestoringWaitsUntilTheClusterIsFullyAvailable(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	sts := selfHealSTS(corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"})
	r, rec, stsUpdates := selfHealReconciler(t, boltMustNotRun(t), nil, plugin, degradedCluster(neo4jv1beta1.PhaseReady), sts)

	res := reconcilePlugin(t, r, "apoc")

	assert.Positive(t, res.RequeueAfter)
	assert.Zero(t, *stsUpdates, "not restored while a server is down")
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseWaiting, got.Status.Phase, "not Ready: the servers do not have it")
	events := drainEvents(rec)
	missing := eventsWithReason(events, EventReasonPluginMissing)
	require.Len(t, missing, 1)
	assert.Contains(t, missing[0], "Warning")
	assert.Contains(t, missing[0], "NEO4J_PLUGINS")
	assert.Contains(t, missing[0], "waits until Neo4jEnterpriseCluster c is Ready with every server available")
	assert.Len(t, events, 1, "one event says both")

	rv := got.ResourceVersion
	reconcilePlugin(t, r, "apoc")
	assert.Equal(t, rv, getPlugin(t, r, "apoc").ResourceVersion)
	assert.Empty(t, drainEvents(rec))
	assert.Zero(t, *stsUpdates)

	r.bolt = &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
	setClusterAvailability(t, r.Client, neo4jv1beta1.PhaseReady, false)
	reconcilePlugin(t, r, "apoc")
	assert.Equal(t, 1, *stsUpdates, "restored once every server is back")
	_, ok := findEnv(&corev1.Container{Env: liveSTSEnv(t, r.Client, "c-server")}, "NEO4J_PLUGINS")
	assert.True(t, ok)
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, getPlugin(t, r, "apoc").Status.Phase)
}

// A Ready plugin with nothing to restore is untouched by its target's
// availability: no write, no status write, no event, still Ready.
func TestReconcile_SettledPluginIgnoresADegradedCluster(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	sts := selfHealSTS(
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
		corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
	)
	r, rec, stsUpdates := selfHealReconciler(t, noBolt(t), nil, plugin, degradedCluster(neo4jv1beta1.PhaseDegraded), sts)
	rv := getPlugin(t, r, "apoc").ResourceVersion

	res := reconcilePlugin(t, r, "apoc")

	assert.Equal(t, ctrl.Result{}, res)
	assert.Zero(t, *stsUpdates)
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, rv, got.ResourceVersion)
	assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
	assert.Empty(t, drainEvents(rec))
}

// The install's own write rolls the servers, and the cluster reports Forming
// until they are back. A pass with nothing more to write is part of that
// install, not a new one: it does not wait for the cluster, it waits for the
// pods, as before.
func TestReconcile_WaitingForItsOwnRestartIsNotDeferred(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation")
	sts := selfHealSTS(
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
		corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
	)
	forming := minimalCluster("c", selfHealNS)
	forming.Status.Phase = neo4jv1beta1.PhaseForming
	bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
	r, rec, stsUpdates := selfHealReconciler(t, bolt, nil, plugin, forming, sts)

	res := reconcilePlugin(t, r, "apoc")

	assert.Positive(t, res.RequeueAfter)
	assert.Zero(t, *stsUpdates)
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase)
	assert.Equal(t, "Waiting for pods to be ready after plugin installation", got.Status.Message)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallDeferred))
}

// verifiedDownloadSelfHeal is a VerifiedDownload plugin of cluster "c" with a
// setting, so its install edits the env and the pod template both.
func verifiedDownloadSelfHeal(phase, message string) *neo4jv1beta1.Neo4jPlugin {
	plugin := selfHealPlugin("custom", "c", phase, message)
	plugin.Spec.Name = "custom"
	plugin.Spec.Config = map[string]string{"custom.setting": "on"}
	plugin.Spec.InstallMode = PluginInstallModeVerifiedDownload
	plugin.Spec.Source = verifiedDownloadPlugin().Spec.Source
	return plugin
}

// The env edits and the init container used to be two StatefulSet writes,
// each a restart of the servers; and the init container was written on every
// pass, changed or not.
func TestInstallPluginViaEnvironment_VerifiedDownloadWritesOnce(t *testing.T) {
	plugin := verifiedDownloadSelfHeal("", "")
	r, _, stsUpdates := selfHealReconciler(t, nil, nil, plugin, selfHealSTS())
	target := &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: ownershipCluster()}

	require.NoError(t, r.installPluginViaEnvironment(context.Background(), plugin, target))

	assert.Equal(t, 1, *stsUpdates, "one write")
	sts := &appsv1.StatefulSet{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: "c-server"}, sts))
	v, _ := findEnv(&sts.Spec.Template.Spec.Containers[0], "NEO4J_custom_setting")
	assert.Equal(t, "on", v)
	assert.Equal(t, []string{"plugin-init-custom"}, initContainerNames(sts))
	assert.ElementsMatch(t, []string{"plugin-auth-custom", "plugin-ca"}, templateVolumeNames(sts))
	assert.Equal(t, "plugin-init-custom", sts.Spec.Template.Annotations[PluginInitContainersAnnotation])

	// The API server defaults the init container; the next pass still finds
	// nothing to change.
	apiServerDefaults(sts)
	require.NoError(t, r.Update(context.Background(), sts))
	*stsUpdates = 0
	require.NoError(t, r.installPluginViaEnvironment(context.Background(), plugin, target))
	assert.Zero(t, *stsUpdates)
	changes, err := r.installChangesTarget(context.Background(), plugin, target)
	require.NoError(t, err)
	assert.False(t, changes)
}

// Before the cluster controller kept a plugin's init container, it dropped it
// on every template apply, and a Ready plugin never noticed: it checked only
// env vars. A StatefulSet recreated from the cluster's own template (storage
// expansion does that) loses it as well. The plugin now puts it back.
func TestReconcile_SettledVerifiedDownloadPluginRestoresItsInitContainer(t *testing.T) {
	plugin := verifiedDownloadSelfHeal(neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	cluster := readyCluster()
	cluster.Spec.TrustedCASecrets = []neo4jv1beta1.TrustedCASecret{{Name: "corp-ca"}}
	// The env survived; the init container and its volumes did not.
	sts := selfHealSTS(corev1.EnvVar{Name: "NEO4J_custom_setting", Value: "on"})
	bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
	r, rec, stsUpdates := selfHealReconciler(t, bolt, nil, plugin, cluster, sts)

	reconcilePlugin(t, r, "custom")

	assert.Equal(t, 1, *stsUpdates)
	live := &appsv1.StatefulSet{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: "c-server"}, live))
	assert.Equal(t, []string{"plugin-init-custom"}, initContainerNames(live))
	assert.ElementsMatch(t, []string{"plugin-auth-custom", "plugin-ca"}, templateVolumeNames(live))
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, getPlugin(t, r, "custom").Status.Phase)
	missing := eventsWithReason(drainEvents(rec), EventReasonPluginMissing)
	require.Len(t, missing, 1)
	assert.Contains(t, missing[0], "init container plugin-init-custom")
	assert.Contains(t, missing[0], "volume plugin-auth-custom")
}

// Only what is gone is put right: a live init container differs from the one
// the plugin builds in what the API server defaulted, and that is not drift.
func TestPluginPodDrift(t *testing.T) {
	plugin := verifiedDownloadSelfHeal("", "")
	r := &Neo4jPluginReconciler{Recorder: record.NewFakeRecorder(1)}
	target := &DeploymentInfo{Type: "cluster", Name: "c", Object: ownershipCluster()}
	desired := selfHealSTS().Spec.Template
	r.applyVerifiedDownload(&desired, plugin, target)

	missing, outdated := pluginPodDrift(desired.Spec, desired.Spec)
	assert.Empty(t, missing, "intact")
	assert.Empty(t, outdated, "intact")

	defaulted := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: *desired.DeepCopy()}}
	apiServerDefaults(defaulted)
	defaulted.Spec.Template.Spec.InitContainers[0].Image = "mirror.example.com/curl:8.5.0"
	missing, outdated = pluginPodDrift(defaulted.Spec.Template.Spec, desired.Spec)
	assert.Empty(t, missing, "defaulted, or changed by someone else")
	assert.Empty(t, outdated, "defaulted, or changed by someone else")

	gone := desired.DeepCopy()
	gone.Spec.InitContainers = nil
	gone.Spec.Volumes = gone.Spec.Volumes[:1]
	missing, outdated = pluginPodDrift(gone.Spec, desired.Spec)
	assert.Equal(t, []string{"init container plugin-init-custom", "volume plugin-ca"}, missing)
	assert.Empty(t, outdated, "removed with its init container: missing, not a change of the CAs")
}

// The cluster controller keeps volumes it did not render, so the CA volume
// VerifiedDownload plugins share would stay after the last of them is
// uninstalled — still projecting CA Secrets a pod cannot start without once
// they are deleted. Uninstall removes it once nothing mounts it.
func TestUninstall_RemovesTheSharedCAVolumeOnceUnused(t *testing.T) {
	ctx := context.Background()
	first, second := verifiedDownloadSelfHeal("", ""), verifiedDownloadSelfHeal("", "")
	second.Name, second.Spec.Name = "other", "other"
	target := &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS, Object: ownershipCluster()}
	r, _, _ := selfHealReconciler(t, nil, nil, selfHealSTS())
	require.NoError(t, r.installPluginViaEnvironment(ctx, first, target))
	require.NoError(t, r.installPluginViaEnvironment(ctx, second, target))
	live := func() *appsv1.StatefulSet {
		sts := &appsv1.StatefulSet{}
		require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: selfHealNS, Name: "c-server"}, sts))
		return sts
	}
	require.ElementsMatch(t, []string{"plugin-auth-custom", "plugin-auth-other", resources.PluginCAVolumeName}, templateVolumeNames(live()))

	require.NoError(t, r.removeVerifiedDownloadInitContainer(ctx, first, target))
	assert.ElementsMatch(t, []string{"plugin-auth-other", resources.PluginCAVolumeName}, templateVolumeNames(live()),
		"the other plugin still mounts it")

	require.NoError(t, r.removeVerifiedDownloadInitContainer(ctx, second, target))
	assert.Empty(t, templateVolumeNames(live()))
	assert.Empty(t, initContainerNames(live()))
}
