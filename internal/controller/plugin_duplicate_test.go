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

// Two Neo4jPlugins of one target with the same spec.name: an enabled CR wins
// over a disabled one, a disabled CR is never held back, the removal of the
// one that leaves the tuple keeps what the one that stays installs, and the
// CRs of a tuple are reconciled again when another of them changes
// (operations.md id 195).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// duplicatePlugin is selfHealPlugin named name, created on day of June 2026,
// enabled or not.
func duplicatePlugin(name, ref string, day int, enabled bool) *neo4jv1beta1.Neo4jPlugin {
	plugin := selfHealPlugin(name, ref, "", "")
	plugin.UID = types.UID("uid-" + name)
	plugin.CreationTimestamp = metav1.NewTime(time.Date(2026, 6, day, 0, 0, 0, 0, time.UTC))
	plugin.Spec.Enabled = enabled
	return plugin
}

// deletePlugin starts the deletion of the plugin name: its finalizer keeps it.
func deletePlugin(t *testing.T, r *Neo4jPluginReconciler, name string) {
	t.Helper()
	require.NoError(t, r.Delete(context.Background(), getPlugin(t, r, name)))
}

// The defect (#566 walk): the oldest CR of a tuple won it whether it was
// enabled or not, so a disabled CR kept a newer enabled one Failed until it
// was deleted. Every CR of the tuple now names the oldest enabled one, in
// every order of creation.
func TestCheckForDuplicatePlugin_AnEnabledCRWinsOverADisabledOne(t *testing.T) {
	cases := []struct {
		name               string
		offDay, e1Day, e2D int
	}{
		{"the disabled CR oldest", 1, 2, 3},
		{"the disabled CR between the enabled ones", 2, 1, 3},
		{"the disabled CR newest", 3, 1, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			off := duplicatePlugin("off", "c", tc.offDay, false)
			e1 := duplicatePlugin("e1", "c", tc.e1Day, true)
			e2 := duplicatePlugin("e2", "c", tc.e2D, true)
			r, _, _, _ := gateReconciler(t, nil, off, e1, e2)
			for _, p := range []*neo4jv1beta1.Neo4jPlugin{off, e1, e2} {
				winner, err := r.checkForDuplicatePlugin(context.Background(), p)
				require.NoError(t, err)
				assert.Equal(t, "e1", winner, "seen from %s", p.Name)
			}
		})
	}

	t.Run("only disabled CRs: the oldest", func(t *testing.T) {
		older, newer := duplicatePlugin("older", "c", 1, false), duplicatePlugin("newer", "c", 2, false)
		r, _, _, _ := gateReconciler(t, nil, older, newer)
		winner, err := r.checkForDuplicatePlugin(context.Background(), newer)
		require.NoError(t, err)
		assert.Equal(t, "older", winner)
	})
}

// The scenario of the report, on both kinds of target: an older disabled CR
// and a newer enabled one. The enabled one installs the plugin; the disabled
// one is Disabled, says which CR keeps the plugin on the servers, and neither
// its passes nor its deletion take the plugin off them.
func TestReconcile_ADisabledDuplicateDoesNotHoldBackAnEnabledOne(t *testing.T) {
	for _, tg := range envTargets() {
		t.Run(tg.name, func(t *testing.T) {
			off := duplicatePlugin("apoc-off", tg.ref(), 1, false)
			on := duplicatePlugin("apoc-on", tg.ref(), 2, true)
			r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{off, on}, tg.objects()...)...)
			target := "Neo4jEnterpriseCluster c"
			if tg.name == "standalone" {
				target = "Neo4jEnterpriseStandalone sa"
			}

			reconcilePlugin(t, r, "apoc-off")
			got := getPlugin(t, r, "apoc-off")
			assert.Equal(t, neo4jv1beta1.PhaseDisabled, got.Status.Phase, got.Status.Message)
			assert.Equal(t, `Plugin disabled (spec.enabled: false): Neo4jPlugin "apoc-on" installs "apoc" on `+target+" in its place",
				got.Status.Message)
			assert.Zero(t, *writes)

			installAndRoll(t, r, "apoc-on", tg.sts)
			assert.Equal(t, 1, *writes)
			events := drainEvents(rec)
			assert.Empty(t, eventsWithReason(events, EventReasonPluginDuplicate))
			assert.Len(t, eventsWithReason(events, EventReasonPluginDisabled), 1)

			// The disabled CR's next pass leaves the plugin where it is…
			reconcilePlugin(t, r, "apoc-off")
			assert.Equal(t, 1, *writes)
			v, _ := envValueOf(t, r, tg.sts, "NEO4J_PLUGINS")
			assert.Equal(t, `["apoc"]`, v)

			// …and so does its deletion.
			deletePlugin(t, r, "apoc-off")
			reconcilePlugin(t, r, "apoc-off")
			assert.Equal(t, 1, *writes, "no restart")
			v, _ = envValueOf(t, r, tg.sts, "NEO4J_PLUGINS")
			assert.Equal(t, `["apoc"]`, v)
			assert.Contains(t, envNames(liveSTSEnv(t, r.Client, tg.sts)), "NEO4J_apoc_export_file_enabled")
			assert.Contains(t, liveSTS(t, r.Client, tg.sts).Annotations, pluginEnvVarsAnnotation("apoc-on"))
			assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "apoc-on").Status.Phase)
		})
	}
}

// Two enabled CRs and a disabled one: the older enabled one installs, the
// newer is a Failed duplicate, the disabled one names the installer. Deleting
// the installer hands the plugin to the duplicate without a restart: its
// removal keeps what the duplicate installs too, which used to be taken off the
// servers and put back.
func TestReconcile_TheWinnersDeletionHandsThePluginToItsDuplicate(t *testing.T) {
	e1 := duplicatePlugin("e1", "c", 1, true)
	off := duplicatePlugin("off", "c", 2, false)
	e2 := duplicatePlugin("e2", "c", 3, true)
	r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{e1, off, e2, readyCluster(), selfHealSTS()}, readyClusterPods()...)...)

	installAndRoll(t, r, "e1", "c-server")
	reconcilePlugin(t, r, "e2")
	got := getPlugin(t, r, "e2")
	assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
	assert.Equal(t, `duplicate Neo4jPlugin: "e1" (older) already targets cluster "c" with plugin name "apoc"; `+
		"delete one CR, or set spec.enabled: false on one, before this one can reconcile", got.Status.Message)
	reconcilePlugin(t, r, "off")
	assert.Contains(t, getPlugin(t, r, "off").Status.Message, `Neo4jPlugin "e1" installs "apoc"`)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginDuplicate), 1)
	require.Equal(t, 1, *writes)

	// Its deletion wakes the other two.
	assert.ElementsMatch(t, []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: selfHealNS, Name: "off"}},
		{NamespacedName: types.NamespacedName{Namespace: selfHealNS, Name: "e2"}},
	}, r.pluginsSharingTuple(context.Background(), getPlugin(t, r, "e1")))

	deletePlugin(t, r, "e1")
	reconcilePlugin(t, r, "e1")
	assert.Equal(t, 1, *writes, "the servers keep the plugin e2 installs")
	v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
	assert.Equal(t, `["apoc"]`, v)
	assert.Contains(t, envNames(liveSTSEnv(t, r.Client, "c-server")), "NEO4J_apoc_export_file_enabled")
	assert.NotContains(t, liveSTS(t, r.Client, "c-server").Annotations, pluginEnvVarsAnnotation("e1"), "e1's records go")

	reconcilePlugin(t, r, "e2")
	if getPlugin(t, r, "e2").Status.Phase != neo4jv1beta1.PhaseReady {
		rollOut(t, r.Client, "c-server")
		reconcilePlugin(t, r, "e2")
	}
	assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "e2").Status.Phase, getPlugin(t, r, "e2").Status.Message)
	assert.Equal(t, 1, *writes, "e2 finds the plugin installed: no restart")
	assert.Contains(t, liveSTS(t, r.Client, "c-server").Annotations, pluginEnvVarsAnnotation("e2"))

	reconcilePlugin(t, r, "off")
	assert.Contains(t, getPlugin(t, r, "off").Status.Message, `Neo4jPlugin "e2" installs "apoc"`)
}

// The deletion of a duplicate that never installed anything left the servers
// without the plugin the winner installs: its uninstall took away the
// NEO4J_PLUGINS name and the settings they share, and the winner put them
// back, two restarts.
func TestReconcile_DeletingALosingDuplicateLeavesTheWinnersInstall(t *testing.T) {
	e1 := duplicatePlugin("e1", "c", 1, true)
	e2 := duplicatePlugin("e2", "c", 2, true)
	r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{e1, e2, readyCluster(), selfHealSTS()}, readyClusterPods()...)...)
	installAndRoll(t, r, "e1", "c-server")
	reconcilePlugin(t, r, "e2")
	require.Equal(t, neo4jv1beta1.PhaseFailed, getPlugin(t, r, "e2").Status.Phase)

	deletePlugin(t, r, "e2")
	reconcilePlugin(t, r, "e2")

	assert.Equal(t, 1, *writes, "no restart")
	v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
	assert.Equal(t, `["apoc"]`, v)
	assert.Contains(t, envNames(liveSTSEnv(t, r.Client, "c-server")), "NEO4J_apoc_export_file_enabled")
	assert.Contains(t, liveSTS(t, r.Client, "c-server").Annotations, pluginEnvVarsAnnotation("e1"))
}

// A VerifiedDownload plugin's init container and volumes are named after its
// spec.name, so the duplicate that takes the plugin over writes the same
// ones: the removal of the one that leaves keeps them.
func TestReconcile_TheWinnersDeletionKeepsTheDuplicatesInitContainer(t *testing.T) {
	first := verifiedDownloadSelfHeal("", "")
	first.UID, first.CreationTimestamp = "uid-first", metav1.NewTime(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	second := verifiedDownloadSelfHeal("", "")
	second.Name, second.UID = "custom-2", "uid-second"
	second.CreationTimestamp = metav1.NewTime(time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC))
	r, _, writes, _ := gateReconciler(t, nil, append([]client.Object{first, second, readyCluster(), selfHealSTS()}, readyClusterPods()...)...)
	installAndRoll(t, r, "custom", "c-server")
	installed := liveSTS(t, r.Client, "c-server")
	require.Equal(t, []string{"plugin-init-custom"}, initContainerNames(installed))

	deletePlugin(t, r, "custom")
	reconcilePlugin(t, r, "custom")

	assert.Equal(t, 1, *writes, "no restart")
	live := liveSTS(t, r.Client, "c-server")
	assert.Equal(t, []string{"plugin-init-custom"}, initContainerNames(live))
	assert.Equal(t, templateVolumeNames(installed), templateVolumeNames(live))
}

// pluginsSharingTuple wakes the other CRs of the same target and spec.name,
// not the CR itself, and none of another target's or another plugin's.
func TestPluginsSharingTuple(t *testing.T) {
	a := duplicatePlugin("a", "c", 1, true)
	b := duplicatePlugin("b", "c", 2, false)
	otherTarget := duplicatePlugin("other-target", "c2", 3, true)
	otherName := duplicatePlugin("other-name", "c", 4, true)
	otherName.Spec.Name = "graph-data-science"
	r, _, _, _ := gateReconciler(t, nil, a, b, otherTarget, otherName)

	assert.Equal(t, []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: selfHealNS, Name: "b"}}},
		r.pluginsSharingTuple(context.Background(), a))
	assert.Empty(t, r.pluginsSharingTuple(context.Background(), otherTarget))
}

// Only what can change a tuple's winner wakes the others: a status write,
// which every pass makes, does not.
func TestTupleOwnershipChanged(t *testing.T) {
	old := duplicatePlugin("a", "c", 1, true)
	statusOnly := old.DeepCopy()
	statusOnly.Status.Phase = neo4jv1beta1.PhaseReady
	edited := old.DeepCopy()
	edited.Generation++
	deleting := old.DeepCopy()
	deleting.DeletionTimestamp = ptr.To(metav1.Now())

	assert.False(t, tupleOwnershipChanged.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: statusOnly}))
	assert.True(t, tupleOwnershipChanged.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: edited}))
	assert.True(t, tupleOwnershipChanged.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: deleting}))
	assert.True(t, tupleOwnershipChanged.Create(event.CreateEvent{Object: old}))
	assert.True(t, tupleOwnershipChanged.Delete(event.DeleteEvent{Object: old}))
}

// End to end against an API server: a duplicate held back is reconciled again
// when the CR that holds it back is disabled, enabled again, and deleted —
// before the watch on the tuple it stayed Failed until the operator restarted.
// No target exists, so the CR that wins waits for it (Pending).
func TestPluginController_ADuplicateFollowsTheCRThatHoldsItBack(t *testing.T) {
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

	scheme := newTestScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
	require.NoError(t, err)
	r := &Neo4jPluginReconciler{
		Client: mgr.GetClient(), Scheme: scheme, Recorder: record.NewFakeRecorder(256), RequeueAfter: time.Second,
	}
	require.NoError(t, r.SetupWithManager(mgr))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mgr.Start(ctx) }()

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	phase := func(name string) string {
		p := &neo4jv1beta1.Neo4jPlugin{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: selfHealNS, Name: name}, p); err != nil {
			return ""
		}
		return p.Status.Phase
	}
	// A merge patch, as kubectl sends it: an Update of the Go type omits a
	// false spec.enabled, which the CRD then defaults back to true.
	setEnabled := func(name string, enabled bool) {
		p := &neo4jv1beta1.Neo4jPlugin{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: selfHealNS}}
		patch := fmt.Sprintf(`{"spec":{"enabled":%t}}`, enabled)
		require.NoError(t, c.Patch(ctx, p, client.RawPatch(types.MergePatchType, []byte(patch))))
	}
	newPlugin := func(name string) *neo4jv1beta1.Neo4jPlugin {
		return &neo4jv1beta1.Neo4jPlugin{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: selfHealNS},
			Spec:       neo4jv1beta1.Neo4jPluginSpec{ClusterRef: "missing", Name: "apoc", Version: "5.26.0", Enabled: true},
		}
	}

	require.NoError(t, c.Create(ctx, newPlugin("first")))
	require.Eventually(t, func() bool { return phase("first") == neo4jv1beta1.PhasePending },
		30*time.Second, 100*time.Millisecond, "the first CR waits for its target")
	time.Sleep(1100 * time.Millisecond) // a later creationTimestamp, which has second resolution
	require.NoError(t, c.Create(ctx, newPlugin("second")))
	require.Eventually(t, func() bool { return phase("second") == neo4jv1beta1.PhaseFailed },
		30*time.Second, 100*time.Millisecond, "the second CR is a duplicate")

	setEnabled("first", false)
	require.Eventually(t, func() bool { return phase("second") == neo4jv1beta1.PhasePending },
		30*time.Second, 100*time.Millisecond, "the duplicate takes over from the disabled CR")

	setEnabled("first", true)
	require.Eventually(t, func() bool { return phase("second") == neo4jv1beta1.PhaseFailed },
		30*time.Second, 100*time.Millisecond, "and is held back again when the older CR is enabled again")

	require.NoError(t, c.Delete(ctx, newPlugin("first")))
	require.Eventually(t, func() bool { return phase("second") == neo4jv1beta1.PhasePending },
		30*time.Second, 100*time.Millisecond, "the duplicate takes over from the deleted CR")
}
