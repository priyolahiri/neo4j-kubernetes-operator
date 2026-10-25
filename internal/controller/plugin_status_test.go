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

// Tests for status.installedVersion and status.installationTime on
// Neo4jPlugin: recorded when the plugin reaches Ready, set once, and never
// churned by the reconciles that follow.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func newPluginForStatus(name, version string) *neo4jv1beta1.Neo4jPlugin {
	return &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			ClusterRef: "c", Name: "apoc", Version: version, Enabled: true,
		},
	}
}

func newPluginReconciler(p *neo4jv1beta1.Neo4jPlugin) *Neo4jPluginReconciler {
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(p).WithStatusSubresource(p).Build()
	return &Neo4jPluginReconciler{Client: fc, Recorder: record.NewFakeRecorder(10)}
}

func getPlugin(t *testing.T, r *Neo4jPluginReconciler, name string) *neo4jv1beta1.Neo4jPlugin {
	t.Helper()
	latest := &neo4jv1beta1.Neo4jPlugin{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, latest))
	return latest
}

// TestUpdatePluginStatus_RecordsInstallOnReady: a plugin that has not reached
// Ready has nothing installed to report; reaching it records the requested
// version and the time, and later reconciles do not touch either.
func TestUpdatePluginStatus_RecordsInstallOnReady(t *testing.T) {
	plugin := newPluginForStatus("install", "5.26.0")
	r := newPluginReconciler(plugin)
	ctx := context.Background()

	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, "Installing plugin")
	got := getPlugin(t, r, "install")
	assert.Empty(t, got.Status.InstalledVersion, "still installing: nothing installed to report")
	assert.Nil(t, got.Status.InstallationTime)

	r.updatePluginStatus(ctx, plugin, "Failed", "Plugin installation failed: boom")
	got = getPlugin(t, r, "install")
	assert.Empty(t, got.Status.InstalledVersion, "a failed install records nothing")
	assert.Nil(t, got.Status.InstallationTime)

	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	got = getPlugin(t, r, "install")
	assert.Equal(t, "5.26.0", got.Status.InstalledVersion)
	require.NotNil(t, got.Status.InstallationTime)
	assert.WithinDuration(t, time.Now(), got.Status.InstallationTime.Time, 10*time.Second)

	// The reconcile that follows a settled plugin repeats the same Ready write.
	// Nothing to say: no write at all (a write is a watch event, and the
	// plugin reconciler was once observed rewriting its CR every second).
	rv := got.ResourceVersion
	installed := got.Status.InstallationTime.DeepCopy()
	for i := 0; i < 3; i++ {
		r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	}
	got = getPlugin(t, r, "install")
	assert.Equal(t, rv, got.ResourceVersion, "repeated Ready must not write status")
	assert.True(t, installed.Equal(got.Status.InstallationTime), "installationTime is set once")
}

// TestUpdatePluginStatus_InstallTimeSurvivesTransientPhases: Ready -> Installing
// -> Ready for the SAME version (a config edit, a pod restart) is not a new
// installation, and neither is a Failed blip in between.
func TestUpdatePluginStatus_InstallTimeSurvivesTransientPhases(t *testing.T) {
	plugin := newPluginForStatus("blip", "5.26.0")
	r := newPluginReconciler(plugin)
	ctx := context.Background()

	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseReady, "ok")
	first := getPlugin(t, r, "blip").Status.InstallationTime.DeepCopy()

	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation")
	r.updatePluginStatus(ctx, plugin, "Failed", "Plugin configuration failed: transient")
	mid := getPlugin(t, r, "blip")
	assert.Equal(t, "5.26.0", mid.Status.InstalledVersion, "the last good install is still what is installed")
	assert.True(t, first.Equal(mid.Status.InstallationTime))

	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseReady, "ok")
	got := getPlugin(t, r, "blip")
	assert.Equal(t, "5.26.0", got.Status.InstalledVersion)
	assert.True(t, first.Equal(got.Status.InstallationTime), "same version: not a new installation")
}

// TestUpdatePluginStatus_BackfillsAnAlreadyReadyPlugin: a plugin that was Ready
// under an operator that never wrote these fields has an unchanged phase,
// message and generation. The "nothing changed" shortcut must not leave it
// without them forever.
func TestUpdatePluginStatus_BackfillsAnAlreadyReadyPlugin(t *testing.T) {
	plugin := newPluginForStatus("backfill", "5.26.0")
	plugin.Status = neo4jv1beta1.Neo4jPluginStatus{
		Phase: neo4jv1beta1.PhaseReady, Message: "ok", ObservedGeneration: 1,
	}
	r := newPluginReconciler(plugin)

	r.updatePluginStatus(context.Background(), plugin, neo4jv1beta1.PhaseReady, "ok")

	got := getPlugin(t, r, "backfill")
	assert.Equal(t, "5.26.0", got.Status.InstalledVersion)
	assert.NotNil(t, got.Status.InstallationTime)
}

// TestUpdatePluginStatus_NewVersionIsANewInstallation: changing spec.version
// reinstalls the plugin, so the recorded version follows it and so does the
// time -- an installationTime older than installedVersion would read as "this
// version has been here since then".
func TestUpdatePluginStatus_NewVersionIsANewInstallation(t *testing.T) {
	plugin := newPluginForStatus("bump", "5.26.0")
	r := newPluginReconciler(plugin)
	ctx := context.Background()

	r.updatePluginStatus(ctx, plugin, neo4jv1beta1.PhaseReady, "ok")
	old := metav1.NewTime(time.Now().Add(-72 * time.Hour))
	seeded := getPlugin(t, r, "bump")
	seeded.Status.InstallationTime = &old
	require.NoError(t, r.Status().Update(ctx, seeded))

	bumped := getPlugin(t, r, "bump")
	bumped.Spec.Version = "5.26.1"
	r.updatePluginStatus(ctx, bumped, neo4jv1beta1.PhaseReady, "ok")

	got := getPlugin(t, r, "bump")
	assert.Equal(t, "5.26.1", got.Status.InstalledVersion)
	require.NotNil(t, got.Status.InstallationTime)
	assert.True(t, got.Status.InstallationTime.After(old.Time), "a new version is a new installation")
}

// A Disabled plugin kept the version and time of its last install, so
// status read as installed while nothing of it was on the servers. Both are
// cleared once the plugin is Disabled — not while its removal still rolls out
// (Installing) — and enabling it again records a new installation
// (operations.md id 196).
func TestReconcile_ADisabledPluginReportsNothingInstalled(t *testing.T) {
	for _, tg := range envTargets() {
		t.Run(tg.name, func(t *testing.T) {
			plugin := disabledFixturePlugin(tg.ref(), "")
			r, _, _, _ := gateReconciler(t, nil, append([]client.Object{plugin}, tg.objects()...)...)
			installAndRoll(t, r, "apoc", tg.sts)
			first := getPlugin(t, r, "apoc").Status
			require.Equal(t, "5.26.0", first.InstalledVersion)
			require.NotNil(t, first.InstallationTime)

			editPlugin(t, r.Client, "apoc", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Enabled = false })
			reconcilePlugin(t, r, "apoc")
			removing := getPlugin(t, r, "apoc")
			require.Equal(t, neo4jv1beta1.PhaseInstalling, removing.Status.Phase)
			assert.Equal(t, "5.26.0", removing.Status.InstalledVersion, "the servers still run it")

			rollOut(t, r.Client, tg.sts)
			reconcilePlugin(t, r, "apoc")
			disabled := getPlugin(t, r, "apoc")
			require.Equal(t, neo4jv1beta1.PhaseDisabled, disabled.Status.Phase)
			assert.Empty(t, disabled.Status.InstalledVersion)
			assert.Nil(t, disabled.Status.InstallationTime)

			editPlugin(t, r.Client, "apoc", func(p *neo4jv1beta1.Neo4jPlugin) { p.Spec.Enabled = true })
			installAndRoll(t, r, "apoc", tg.sts)
			again := getPlugin(t, r, "apoc").Status
			assert.Equal(t, "5.26.0", again.InstalledVersion)
			require.NotNil(t, again.InstallationTime)
		})
	}
}

// A plugin Disabled under an operator that kept both fields is cleared by its
// next pass, which otherwise has nothing to write.
func TestUpdatePluginStatus_ClearsAnAlreadyDisabledPlugin(t *testing.T) {
	installed := metav1.NewTime(time.Now().Add(-time.Hour))
	plugin := newPluginForStatus("kept", "5.26.0")
	plugin.Spec.Enabled = false
	plugin.Status = neo4jv1beta1.Neo4jPluginStatus{
		Phase: neo4jv1beta1.PhaseDisabled, Message: "off", ObservedGeneration: 1,
		InstalledVersion: "5.26.0", InstallationTime: &installed,
		Conditions: []metav1.Condition{{
			Type: ConditionTypeReady, Status: metav1.ConditionFalse, Reason: ConditionReasonDisabled,
			Message: "off", ObservedGeneration: 1, LastTransitionTime: installed,
		}},
	}
	r := newPluginReconciler(plugin)

	assert.False(t, r.updatePluginStatus(context.Background(), plugin, neo4jv1beta1.PhaseDisabled, "off"), "no transition to announce")

	got := getPlugin(t, r, "kept")
	assert.Empty(t, got.Status.InstalledVersion)
	assert.Nil(t, got.Status.InstallationTime)
	rv := got.ResourceVersion
	r.updatePluginStatus(context.Background(), plugin, neo4jv1beta1.PhaseDisabled, "off")
	assert.Equal(t, rv, getPlugin(t, r, "kept").ResourceVersion, "then nothing more to write")
}
