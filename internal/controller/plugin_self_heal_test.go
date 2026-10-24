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

// A Ready Neo4jPlugin re-adds what something removed from its target's
// StatefulSet, and leaves the StatefulSet alone when nothing is missing
// (operations.md id 135).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	driver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const selfHealNS = "default"

// selfHealPlugin is an APOC Neo4jPlugin for target ref at generation 1, in
// phase, with its finalizer already on (so a pass goes straight to work).
func selfHealPlugin(name, ref, phase, message string) *neo4jv1beta1.Neo4jPlugin {
	return &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: selfHealNS, Generation: 1,
			Finalizers: []string{PluginFinalizer},
		},
		Spec: neo4jv1beta1.Neo4jPluginSpec{
			ClusterRef: ref, Name: "apoc", Version: "5.26.0", Enabled: true,
			Config: map[string]string{"apoc.export.file.enabled": "true"},
		},
		Status: neo4jv1beta1.Neo4jPluginStatus{Phase: phase, Message: message, ObservedGeneration: 1},
	}
}

// selfHealSTS is the cluster "c"'s server StatefulSet with env on its neo4j
// container.
func selfHealSTS(env ...corev1.EnvVar) *appsv1.StatefulSet {
	sts := pluginTestSTS("c", selfHealNS)
	sts.Spec.Template.Spec.Containers[0].Env = env
	return sts
}

// installedSelfHealSTS is selfHealSTS with the APOC plugin of selfHealPlugin
// already written and rolled out: a pass has nothing to write and goes on to
// configure the plugin.
func installedSelfHealSTS() *appsv1.StatefulSet {
	return selfHealSTS(
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
		corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
	)
}

// readyCluster is cluster "c" Ready with every server available: what a write
// of the plugin to the servers waits for (operations.md id 141).
func readyCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := minimalCluster("c", selfHealNS)
	c.Status.Phase = neo4jv1beta1.PhaseReady
	return c
}

// degradedCluster is cluster "c" formed and serving with a server down: phase
// Ready inside the grace period, Degraded past it.
func degradedCluster(phase string) *neo4jv1beta1.Neo4jEnterpriseCluster {
	c := minimalCluster("c", selfHealNS)
	c.Status.Phase = phase
	c.Status.Conditions = []metav1.Condition{serversUnavailableCondition("1 of 2 servers unavailable: c-server-1")}
	c.Status.Conditions[0].LastTransitionTime = metav1.Now()
	return c
}

// readyClusterPods are cluster "c"'s two server pods, Running and Ready.
func readyClusterPods() []client.Object {
	var pods []client.Object
	for i := 0; i < 2; i++ {
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("c-server-%d", i), Namespace: selfHealNS,
				Labels: map[string]string{"app.kubernetes.io/name": "neo4j", "app.kubernetes.io/instance": "c"},
			},
			Status: corev1.PodStatus{
				Phase:      corev1.PodRunning,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		})
	}
	return pods
}

// selfHealReconciler builds a plugin reconciler on a fake client holding
// objs; stsUpdates counts the StatefulSet writes that reach it, and update,
// when set, runs in place of a StatefulSet write.
func selfHealReconciler(t *testing.T, bolt *pluginBolt, update func() error, objs ...client.Object) (*Neo4jPluginReconciler, *record.FakeRecorder, *int) {
	t.Helper()
	stsUpdates := 0
	c := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jPlugin{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); ok {
					stsUpdates++
					if update != nil {
						return update()
					}
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).
		Build()
	rec := record.NewFakeRecorder(32)
	return &Neo4jPluginReconciler{
		Client: c, Scheme: newTestScheme(), Recorder: rec,
		RequeueAfter: 30 * time.Second, bolt: bolt,
	}, rec, &stsUpdates
}

func reconcilePlugin(t *testing.T, r *Neo4jPluginReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: selfHealNS, Name: name}})
	require.NoError(t, err)
	return res
}

func liveSTSEnv(t *testing.T, c client.Client, name string) []corev1.EnvVar {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: selfHealNS, Name: name}, sts))
	return sts.Spec.Template.Spec.Containers[0].Env
}

// eventsWithReason keeps the recorded events of one reason.
// noBolt fails the test if a pass reaches a step that needs a running Neo4j.
func noBolt(t *testing.T) *pluginBolt {
	return &pluginBolt{
		functional: func(context.Context, *DeploymentInfo) bool {
			t.Error("a settled plugin whose target is intact must not connect to Neo4j")
			return true
		},
	}
}

// A Ready plugin is reconciled again on every pod-template change of its
// target, most of them someone else's rollouts. When the template still
// carries the plugin, the pass changes nothing: no Bolt, no StatefulSet
// write, no status write — and so no Ready → Installing flap.
func TestReconcile_SettledPluginLeavesAnIntactTargetAlone(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	sts := selfHealSTS(
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc","fleet-management"]`},
		corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
	)
	r, rec, stsUpdates := selfHealReconciler(t, noBolt(t), nil, plugin, minimalCluster("c", selfHealNS), sts)
	before := getPlugin(t, r, "apoc").ResourceVersion

	res := reconcilePlugin(t, r, "apoc")

	assert.Equal(t, ctrl.Result{}, res, "nothing to wait for")
	assert.Zero(t, *stsUpdates, "an intact target is not written")
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, before, got.ResourceVersion, "status is not written")
	assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
	assert.Empty(t, drainEvents(rec))
}

// The defect: something removed NEO4J_PLUGINS from the server StatefulSet (a
// conflict retry of the cluster controller did, #508) and the plugin stayed
// Ready. Its pass now says so, leaves Ready while it repairs, and puts the
// variable back.
func TestReconcile_SettledPluginReAddsWhatWasRemoved(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	sts := selfHealSTS(corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"})
	bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
	// No server pods yet: the re-add restarts them, so the pass waits.
	r, rec, stsUpdates := selfHealReconciler(t, bolt, nil, plugin, readyCluster(), sts)

	res := reconcilePlugin(t, r, "apoc")

	assert.Equal(t, 1, *stsUpdates)
	v, ok := findEnv(&corev1.Container{Env: liveSTSEnv(t, r.Client, "c-server")}, "NEO4J_PLUGINS")
	require.True(t, ok, "NEO4J_PLUGINS is put back")
	assert.Equal(t, `["apoc"]`, v)
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase, "not Ready while the servers do not have it")
	ready := findCondition(got.Status.Conditions, ConditionTypeReady)
	require.NotNil(t, ready)
	assert.NotEqual(t, metav1.ConditionTrue, ready.Status)
	assert.Positive(t, res.RequeueAfter)
	missing := eventsWithReason(drainEvents(rec), EventReasonPluginMissing)
	require.Len(t, missing, 1)
	assert.Contains(t, missing[0], "Warning")
	assert.Contains(t, missing[0], "NEO4J_PLUGINS")

	// The servers come back with it: Ready again.
	for _, pod := range readyClusterPods() {
		require.NoError(t, r.Create(context.Background(), pod))
	}
	reconcilePlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseReady, getPlugin(t, r, "apoc").Status.Phase)
	assert.Equal(t, 1, *stsUpdates, "the pass that finds it back writes nothing")
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing))
}

// A settled pass no longer runs the install, so what the install did for a
// plugin that was already Ready must still happen when it is needed: an env
// copy of a standalone plugin's neo4j.conf setting, left by an operator from
// before id 114, is still taken out (it overrode the merged neo4j.conf).
func TestReconcile_SettledStandalonePluginDropsALeftoverConfCopy(t *testing.T) {
	plugin := selfHealPlugin("apoc", "sa", neo4jv1beta1.PhaseReady, "Plugin installed and configured successfully")
	plugin.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"apoc.coll.*"}}
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: selfHealNS},
		Status:     neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: neo4jv1beta1.PhaseReady},
	}
	// Only the plugin's tokens: the copy goes. With another plugin's tokens
	// in it, only this plugin's go.
	allowlistOnly := []corev1.EnvVar{
		{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
		{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
		{Name: "NEO4J_dbms_security_procedures_allowlist", Value: "apoc.coll.*"},
	}
	shared := []corev1.EnvVar{
		{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
		{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
		{Name: "NEO4J_dbms_security_procedures_unrestricted", Value: "gds.*,apoc.coll.*"},
	}
	for _, tc := range []struct {
		name  string
		env   []corev1.EnvVar
		check func(t *testing.T, env []corev1.EnvVar)
	}{
		{"its own copy", allowlistOnly, func(t *testing.T, env []corev1.EnvVar) {
			_, left := findEnv(&corev1.Container{Env: env}, "NEO4J_dbms_security_procedures_allowlist")
			assert.False(t, left, "the copy is gone; neo4j.conf carries the setting")
		}},
		{"a copy shared with another plugin", shared, func(t *testing.T, env []corev1.EnvVar) {
			v, _ := findEnv(&corev1.Container{Env: env}, "NEO4J_dbms_security_procedures_unrestricted")
			assert.Equal(t, "gds.*", v, "the plugin's tokens are gone, the other's stay")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := pluginTestSTS("sa", selfHealNS)
			sts.Name = "sa"
			sts.Spec.Template.Spec.Containers[0].Env = tc.env
			bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
			r, rec, stsUpdates := selfHealReconciler(t, bolt, nil, plugin.DeepCopy(), standalone.DeepCopy(), sts)

			reconcilePlugin(t, r, "apoc")

			assert.Equal(t, 1, *stsUpdates)
			tc.check(t, liveSTSEnv(t, r.Client, "sa"))
			require.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing), 1)
		})
	}
}

// What a settled plugin puts right is what is gone, never a value someone else
// set: two writers that disagree on a value would restart the servers in turn
// for ever, each woken by the other's write.
func TestPluginEnvDrift(t *testing.T) {
	const unrestricted = "NEO4J_dbms_security_procedures_unrestricted"
	desired := []corev1.EnvVar{
		{Name: "NEO4J_PLUGINS", Value: `["fleet-management","apoc"]`},
		{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
		{Name: unrestricted, Value: "gds.*,apoc.coll.*"},
	}
	without := func(name string) []corev1.EnvVar {
		var out []corev1.EnvVar
		for _, e := range desired {
			if e.Name != name {
				out = append(out, e)
			}
		}
		return out
	}
	with := func(name, value string) []corev1.EnvVar {
		return append(without(name), corev1.EnvVar{Name: name, Value: value})
	}
	cases := []struct {
		name       string
		live       []corev1.EnvVar
		standalone bool
		want       []string
	}{
		{"intact", desired, false, nil},
		{"NEO4J_PLUGINS removed", without("NEO4J_PLUGINS"), false, []string{"NEO4J_PLUGINS"}},
		{"NEO4J_PLUGINS without the plugin", with("NEO4J_PLUGINS", `["fleet-management"]`), false, []string{"NEO4J_PLUGINS"}},
		{"a setting removed", without("NEO4J_apoc_export_file_enabled"), false, []string{"NEO4J_apoc_export_file_enabled"}},
		{"a setting someone else changed", with("NEO4J_apoc_export_file_enabled", "false"), false, nil},
		{"a list another writer changed", with(unrestricted, "gds.*"), false, nil},
		// What the plugin's write would take away is a removal, not a loss
		// (id 155): a name its record says it no longer adds, a cluster list
		// no plugin of the target puts anything in.
		{"NEO4J_PLUGINS with a name the write takes away", with("NEO4J_PLUGINS", `["fleet-management","apoc","apoc-extended"]`), false, nil},
		{"a cluster list nobody asks for", append(desired, corev1.EnvVar{Name: "NEO4J_dbms_security_procedures_allowlist", Value: "apoc.load.*"}), false, nil},
		// On a standalone the plugin's edits only take its tokens out of a
		// conf list, or the copy out of the env (id 114): one left behind by
		// an earlier operator is put right.
		{"standalone: a conf copy the plugin removes", append(desired, corev1.EnvVar{Name: "NEO4J_dbms_security_procedures_allowlist", Value: "apoc.coll.*"}), true, []string{"NEO4J_dbms_security_procedures_allowlist"}},
		{"standalone: a list still holding the plugin's tokens", with(unrestricted, "gds.*,apoc.coll.*,apoc.load.*"), true, []string{unrestricted}},
		{"standalone: a setting someone else changed", with("NEO4J_apoc_export_file_enabled", "false"), true, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// desired stands for what the plugin's edits make of live.
			assert.Equal(t, tc.want, pluginEnvDrift(tc.live, desired, tc.standalone))
		})
	}
}

// The watch: a StatefulSet maps to the Ready plugins it is the target of.
func TestReadyPluginsForStatefulSet(t *testing.T) {
	inOtherNS := selfHealPlugin("apoc-elsewhere", "c", neo4jv1beta1.PhaseReady, "")
	inOtherNS.Namespace = "other"
	r, _, _ := selfHealReconciler(t, nil, nil,
		selfHealPlugin("apoc-c", "c", neo4jv1beta1.PhaseReady, ""),
		selfHealPlugin("gds-c", "c", neo4jv1beta1.PhaseInstalling, ""),
		selfHealPlugin("failed-c", "c", neo4jv1beta1.PhaseFailed, ""),
		selfHealPlugin("apoc-sa", "sa", neo4jv1beta1.PhaseReady, ""),
		inOtherNS,
	)
	mapped := func(stsName string) []string {
		var names []string
		for _, req := range r.readyPluginsForStatefulSet(context.Background(), &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: selfHealNS},
		}) {
			assert.Equal(t, selfHealNS, req.Namespace)
			names = append(names, req.Name)
		}
		return names
	}
	assert.Equal(t, []string{"apoc-c"}, mapped("c-server"), "a cluster's server StatefulSet; only Ready plugins")
	assert.Equal(t, []string{"apoc-sa"}, mapped("sa"), "a standalone's StatefulSet")
	assert.Empty(t, mapped("minio"))
}

// The watch passes a new StatefulSet and a changed pod template, not the
// status writes of every pod transition.
func TestPodTemplateChanged(t *testing.T) {
	old := selfHealSTS(corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`})
	status := old.DeepCopy()
	status.Status.ReadyReplicas = 1
	status.Status.UpdatedReplicas = 1
	meta := old.DeepCopy()
	meta.Annotations = map[string]string{"neo4j.com/cluster-template-hash": "x"}
	removed := old.DeepCopy()
	removed.Spec.Template.Spec.Containers[0].Env = nil

	assert.False(t, podTemplateChanged.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: status}), "status only")
	assert.False(t, podTemplateChanged.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: meta}), "StatefulSet metadata only")
	assert.True(t, podTemplateChanged.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: removed}), "the pod template changed")
	assert.True(t, podTemplateChanged.Create(event.CreateEvent{Object: old}), "a recreated StatefulSet")
	assert.False(t, podTemplateChanged.Delete(event.DeleteEvent{Object: old}))
}

// A pass with nothing to add does not write the StatefulSet. Every pass that
// waited for the servers used to write it, racing the writes of the
// StatefulSet's own controller during the restart.
func TestInstallPluginViaEnvironment_WritesNothingWhenNothingChanged(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseInstalling, "")
	r, _, stsUpdates := selfHealReconciler(t, nil, nil, plugin, selfHealSTS())
	deployment := &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS}

	require.NoError(t, r.installPluginViaEnvironment(context.Background(), plugin, deployment))
	require.Equal(t, 1, *stsUpdates, "the install writes it")
	require.NoError(t, r.installPluginViaEnvironment(context.Background(), plugin, deployment))
	assert.Equal(t, 1, *stsUpdates, "the next pass has nothing to write")
}

// A variable the cluster controller renders from the cluster's own spec (here
// NEO4J_PLUGINS in spec.env) is put back by that controller whenever the live
// value differs. The plugin does not write it — the two would restart the
// servers in turn — and says why.
func TestInstallPluginViaEnvironment_LeavesClusterOwnedVariablesAlone(t *testing.T) {
	ctx := context.Background()
	deployment := &DeploymentInfo{Type: "cluster", Name: "c", Namespace: selfHealNS}
	owned := func() *appsv1.StatefulSet {
		sts := selfHealSTS(corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`})
		sts.Annotations = map[string]string{ownedEnvVarsAnnotation: "NEO4J_AUTH,NEO4J_PLUGINS"}
		return sts
	}
	gds := selfHealPlugin("gds", "c", "", "")
	gds.Spec.Name = "graph-data-science"
	gds.Spec.Config = nil

	r, _, stsUpdates := selfHealReconciler(t, nil, nil, gds, owned())
	err := r.installPluginViaEnvironment(ctx, gds, deployment)
	var ownedErr *pluginEnvOwnedError
	require.ErrorAs(t, err, &ownedErr)
	assert.Contains(t, err.Error(), "NEO4J_PLUGINS")
	assert.Contains(t, err.Error(), "spec.env")
	assert.False(t, retryablePluginError(err), "only a change to the cluster's spec resolves it")
	assert.Zero(t, *stsUpdates)

	// A plugin the cluster's own list already names changes nothing: no conflict.
	apoc := selfHealPlugin("apoc", "c", "", "")
	apoc.Spec.Config = nil
	r, _, stsUpdates = selfHealReconciler(t, nil, nil, apoc, owned())
	require.NoError(t, r.installPluginViaEnvironment(ctx, apoc, deployment))
	assert.Zero(t, *stsUpdates)

	// The same variable, not the cluster's: written as before.
	free := owned()
	free.Annotations = map[string]string{ownedEnvVarsAnnotation: "NEO4J_AUTH"}
	r, _, stsUpdates = selfHealReconciler(t, nil, nil, gds, free)
	require.NoError(t, r.installPluginViaEnvironment(ctx, gds, deployment))
	assert.Equal(t, 1, *stsUpdates)
	v, _ := findEnv(&corev1.Container{Env: liveSTSEnv(t, r.Client, "c-server")}, "NEO4J_PLUGINS")
	assert.Equal(t, `["apoc","graph-data-science"]`, v)
}

// A transient PluginInstallFailed Warning (v1.21.0 journey): the StatefulSet
// kept changing under the install write (its controller rewrites status at
// every pod transition of a restart) until the conflict retries ran out. (The
// other path to it, a configuration step opening a Bolt client while the
// servers restarted, is gone with the step: nothing is set at runtime, #548.)
func TestReconcile_InstallConflictIsNotAFailure(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseInstalling, "Installing plugin")
	bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
	conflict := func() error {
		return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, "c-server",
			errors.New("the object has been modified; please apply your changes to the latest version and try again"))
	}
	r, rec, _ := selfHealReconciler(t, bolt, conflict, plugin, readyCluster(), selfHealSTS())

	res := reconcilePlugin(t, r, "apoc")

	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase)
	assert.Contains(t, got.Status.Message, "Retrying the plugin installation")
	assert.Positive(t, res.RequeueAfter)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallFailed))
}

// A real failure is still one: Failed and a Warning. A retry that fails the
// same way writes nothing and says nothing more. It used to write Installing
// and then Failed again, each write re-queuing the next pass, so a lasting
// failure flipped the phase and repeated the Warning as fast as passes ran.
func TestReconcile_ARealFailureIsAnnouncedOnceAndRetried(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseInstalling, "Installing plugin")
	bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
	refused := func() error {
		return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, "c-server",
			errors.New("exceeded quota: compute-resources"))
	}
	objs := append([]client.Object{plugin, readyCluster(), selfHealSTS()}, readyClusterPods()...)
	r, rec, _ := selfHealReconciler(t, bolt, refused, objs...)

	res := reconcilePlugin(t, r, "apoc")
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
	assert.Contains(t, got.Status.Message, "exceeded quota")
	assert.Positive(t, res.RequeueAfter, "the cause can lie outside the plugin, so it is retried")
	require.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallFailed), 1)

	rv := got.ResourceVersion
	res = reconcilePlugin(t, r, "apoc")
	assert.Equal(t, rv, getPlugin(t, r, "apoc").ResourceVersion, "the retry writes no status")
	assert.Positive(t, res.RequeueAfter)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstallFailed), "and repeats no Warning")
}

// A pass that continues an install keeps the status the last one wrote.
// Each pass used to announce "Installing plugin" and then "Waiting for pods":
// two writes, each re-queuing the next pass, for as long as the servers took
// to restart.
func TestReconcile_WaitingForTheServersWritesNothingMore(t *testing.T) {
	plugin := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation")
	bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}
	sts := selfHealSTS(
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
		corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
	)
	r, _, stsUpdates := selfHealReconciler(t, bolt, nil, plugin, minimalCluster("c", selfHealNS), sts)
	rv := getPlugin(t, r, "apoc").ResourceVersion

	res := reconcilePlugin(t, r, "apoc")

	assert.Positive(t, res.RequeueAfter)
	assert.Equal(t, rv, getPlugin(t, r, "apoc").ResourceVersion, "no status write")
	assert.Zero(t, *stsUpdates, "no StatefulSet write")
}

func TestRetryablePluginError(t *testing.T) {
	conflict := apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, "c-server", errors.New("modified"))
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"StatefulSet conflict, wrapped", fmt.Errorf("failed to update StatefulSet with plugin configuration: %w", conflict), true},
		{"driver connectivity error, wrapped", fmt.Errorf("configure: %w", &driver.ConnectivityError{Inner: errors.New("EOF")}), true},
		{"connection refused", errors.New("dial tcp 10.0.0.1:7687: connect: connection refused"), true},
		{"server shutting down", errors.New("Neo.TransientError.General.DatabaseUnavailable: server is shutting down"), true},
		{"deadline", fmt.Errorf("verify: %w", context.DeadlineExceeded), true},
		{"Neo4j rejects a setting", errors.New("Neo.ClientError.General.InvalidArguments: unknown setting"), false},
		{"missing admin Secret", errors.New(`failed to get credentials: secrets "neo4j-admin-secret" not found`), false},
		{"owned by the cluster", &pluginEnvOwnedError{statefulSet: "c-server", cluster: "c", names: []string{"NEO4J_PLUGINS"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, retryablePluginError(tc.err))
		})
	}
}

// End to end against an API server: with the plugin controller running, a
// Ready plugin's NEO4J_PLUGINS is removed from its target's StatefulSet, and
// the controller notices and puts it back. Before the StatefulSet watch,
// nothing reconciled the plugin and the variable stayed gone.
func TestPluginController_ReAddsARemovedNeo4jPlugins(t *testing.T) {
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
	rec := record.NewFakeRecorder(256)
	r := &Neo4jPluginReconciler{
		Client: mgr.GetClient(), Scheme: scheme, Recorder: rec, RequeueAfter: time.Second,
		bolt: &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }},
	}
	require.NoError(t, r.SetupWithManager(mgr))
	// envtest runs no StatefulSet controller; a plugin is Ready only once its
	// write has rolled out.
	simulateStatefulSetController(t, mgr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mgr.Start(ctx) }()

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	// The target: a Ready standalone, its StatefulSet and its one Ready pod.
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: selfHealNS},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
		},
	}
	require.NoError(t, c.Create(ctx, standalone))
	standalone.Status.Phase = neo4jv1beta1.PhaseReady
	standalone.Status.ObservedGeneration = standalone.Generation
	require.NoError(t, c.Status().Update(ctx, standalone))
	labels := map[string]string{"app": "sa"}
	container := corev1.Container{Name: "neo4j", Image: "neo4j:5.26-enterprise"}
	require.NoError(t, c.Create(ctx, &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: selfHealNS},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    ptr.To[int32](1),
			ServiceName: "sa",
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{container}},
			},
		},
	}))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sa-0", Namespace: selfHealNS, Labels: labels},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{container}},
	}
	require.NoError(t, c.Create(ctx, pod))
	pod.Status = corev1.PodStatus{
		Phase:      corev1.PodRunning,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
	}
	require.NoError(t, c.Status().Update(ctx, pod))

	require.NoError(t, c.Create(ctx, &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{Name: "apoc", Namespace: selfHealNS},
		Spec:       neo4jv1beta1.Neo4jPluginSpec{ClusterRef: "sa", Name: "apoc", Version: "5.26.0", Enabled: true},
	}))

	listsAPOC := func() bool {
		sts := &appsv1.StatefulSet{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: selfHealNS, Name: "sa"}, sts); err != nil {
			return false
		}
		v, ok := findEnv(&sts.Spec.Template.Spec.Containers[0], "NEO4J_PLUGINS")
		return ok && strings.Contains(v, `"apoc"`)
	}
	pluginPhase := func() string {
		p := &neo4jv1beta1.Neo4jPlugin{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: selfHealNS, Name: "apoc"}, p); err != nil {
			return ""
		}
		return p.Status.Phase
	}
	require.Eventually(t, func() bool { return listsAPOC() && pluginPhase() == neo4jv1beta1.PhaseReady },
		30*time.Second, 100*time.Millisecond, "the plugin installs")

	// Something removes the variable, as the cluster controller's conflict
	// retry once did.
	require.NoError(t, retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		sts := &appsv1.StatefulSet{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: selfHealNS, Name: "sa"}, sts); err != nil {
			return err
		}
		var kept []corev1.EnvVar
		for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
			if e.Name != "NEO4J_PLUGINS" {
				kept = append(kept, e)
			}
		}
		sts.Spec.Template.Spec.Containers[0].Env = kept
		return c.Update(ctx, sts)
	}))

	require.Eventually(t, listsAPOC, 30*time.Second, 100*time.Millisecond, "the plugin controller puts NEO4J_PLUGINS back")
	require.Eventually(t, func() bool { return pluginPhase() == neo4jv1beta1.PhaseReady },
		30*time.Second, 100*time.Millisecond, "and the plugin is Ready again")
	assert.NotEmpty(t, eventsWithReason(drainEvents(rec), EventReasonPluginMissing), "saying what it found")
}
