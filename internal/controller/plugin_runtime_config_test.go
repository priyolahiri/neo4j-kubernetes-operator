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

// What a Neo4jPlugin pass does once its write has reached the servers: it sets
// nothing at runtime, it waits for the restart its write caused without asking
// Neo4j anything, and it announces Ready once (#548, #530).

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The defects (#548, from the code; #530): once the roll was done,
// configurePlugin called dbms.setConfigValue for every spec.config key but
// APOC's. Neo4j takes only a dynamic setting there ("The setting is not
// dynamic and cannot be changed at runtime", Operations Manual "Update dynamic
// settings", 5.x and dev), and on a cluster only on the server the call runs
// against — while the servers already had every value from their env. A
// static key such as gds.model.store_location turned the plugin Failed,
// retried every RequeueAfter. An APOC plugin with spec.security opened a Bolt
// client only to log "Skipping". Neither may ask Neo4j for anything once its
// roll is done: these targets have no admin Secret, so any client a pass
// builds fails.
func TestReconcile_NothingIsSetAtRuntime(t *testing.T) {
	gds := selfHealPlugin("gds", "c", "", "")
	gds.Spec.Name, gds.Spec.Version = "graph-data-science", "2.13.0"
	gds.Spec.Config = map[string]string{"gds.model.store_location": "/data/gds-models"}
	apoc := selfHealPlugin("apoc", "sa", "", "")
	apoc.Spec.Security = &neo4jv1beta1.PluginSecurity{AllowedProcedures: []string{"apoc.*"}}

	for _, tc := range []struct {
		name   string
		plugin *neo4jv1beta1.Neo4jPlugin
		objs   []client.Object
		sts    string
	}{
		{"a static GDS setting on a cluster", gds,
			append([]client.Object{readyCluster(), selfHealSTS()}, readyClusterPods()...), "c-server"},
		{"an APOC allowlist on a standalone", apoc,
			[]client.Object{readyStandalone(), standaloneSTS(), gatePod("sa-0", map[string]string{"app": "sa"})}, "sa"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, rec, writes, _ := gateReconciler(t, nil, append([]client.Object{tc.plugin}, tc.objs...)...)

			reconcilePlugin(t, r, tc.plugin.Name)
			require.Equal(t, 1, *writes, "the install")
			rollOut(t, r.Client, tc.sts)
			res := reconcilePlugin(t, r, tc.plugin.Name)

			got := getPlugin(t, r, tc.plugin.Name)
			assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase, got.Status.Message)
			assert.Zero(t, res.RequeueAfter)
			events := drainEvents(rec)
			assert.Empty(t, eventsWithReason(events, EventReasonPluginInstallFailed))
			assert.Len(t, eventsWithReason(events, EventReasonPluginInstalled), 1)
		})
	}
}

// The defect (#548): while a standalone's one pod restarted to take the
// plugin, each pass of the plugin asked Neo4j whether the standalone answered
// before it looked at the roll, so the plugin went Waiting ("Waiting for
// standalone sa to be functional") whenever the pod was down and Installing
// again whenever it was up — a status write each time, each re-queuing the
// next pass. A pass with nothing to write waits for its roll and asks Neo4j
// nothing.
func TestReconcile_ARestartingStandaloneIsNotAWait(t *testing.T) {
	const waiting = "Waiting for pods to be ready after plugin installation"
	plugin := selfHealPlugin("apoc", "sa", neo4jv1beta1.PhaseInstalling, waiting)
	sts := standaloneSTS(
		corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc"]`},
		corev1.EnvVar{Name: "NEO4J_apoc_export_file_enabled", Value: "true"},
	)
	// Rolling: the pod is being replaced on the template the install wrote.
	sts.Status.UpdateRevision, sts.Status.UpdatedReplicas, sts.Status.ReadyReplicas = "rev-next", 0, 0
	standalone := readyStandalone()
	standalone.Status.Phase = neo4jv1beta1.PhasePending
	pod := gatePod("sa-0", map[string]string{"app": "sa"})
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	asked := 0
	bolt := &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool {
		asked++
		return false // the pod is down
	}}
	r, rec, writes, messages := gateReconciler(t, bolt, plugin, standalone, sts, pod)

	for i := 0; i < 3; i++ {
		res := reconcilePlugin(t, r, "apoc")
		assert.Positive(t, res.RequeueAfter)
		got := getPlugin(t, r, "apoc")
		assert.Equal(t, neo4jv1beta1.PhaseInstalling, got.Status.Phase)
		assert.Equal(t, waiting, got.Status.Message)
	}
	assert.Zero(t, asked, "a pass with nothing to write asks Neo4j nothing")
	assert.Zero(t, *writes)
	for _, m := range *messages {
		assert.NotContains(t, m, "to be functional")
	}

	// The pod is back on the new template: Ready, announced once.
	pod.Status.Conditions[0].Status = corev1.ConditionTrue
	require.NoError(t, r.Status().Update(context.Background(), pod))
	rollOut(t, r.Client, "sa")
	reconcilePlugin(t, r, "apoc")
	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase, got.Status.Message)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginInstalled), 1)
}

// The defect (#548): PluginInstalled was emitted after the Ready status write
// whatever that write did. A pass that read the plugin from before the last
// Ready write — the informer cache lags behind the write's own watch event —
// went through to Ready again, wrote nothing, and announced the install a
// second time. It is announced from the write that makes the plugin Ready.
func TestReconcile_ReadyIsAnnouncedOnce(t *testing.T) {
	const ready = "Plugin installed and configured successfully"
	stored := selfHealPlugin("apoc", "c", neo4jv1beta1.PhaseReady, ready)
	stored.Status.InstalledVersion = stored.Spec.Version
	stored.Status.InstallationTime = &metav1.Time{Time: time.Now().Add(-time.Minute)}
	SetReadyCondition(&stored.Status.Conditions, 1, metav1.ConditionTrue, ConditionReasonReady, ready)
	// The cache's copy, from before that write.
	stale := stored.DeepCopy()
	stale.Status.Phase, stale.Status.Message = neo4jv1beta1.PhaseInstalling, "Waiting for pods to be ready after plugin installation"
	stale.Status.Conditions = nil
	first := false

	sts := installedSelfHealSTS()
	sts.Annotations = map[string]string{pluginEnvVarsAnnotation("apoc"): "NEO4J_apoc_export_file_enabled"}
	c := fake.NewClientBuilder().
		WithScheme(newTestScheme()).
		WithObjects(append([]client.Object{stored, readyCluster(), sts}, readyClusterPods()...)...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jPlugin{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if p, ok := obj.(*neo4jv1beta1.Neo4jPlugin); ok && first {
					first = false
					stale.DeepCopyInto(p)
					return nil
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	rec := record.NewFakeRecorder(16)
	r := &Neo4jPluginReconciler{Client: c, Scheme: newTestScheme(), Recorder: rec, RequeueAfter: 30 * time.Second,
		bolt: &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}}
	rv := getPlugin(t, r, "apoc").ResourceVersion
	first = true

	reconcilePlugin(t, r, "apoc")

	got := getPlugin(t, r, "apoc")
	assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
	assert.Equal(t, rv, got.ResourceVersion, "nothing to write")
	for _, e := range drainEvents(rec) {
		assert.False(t, strings.Contains(e, EventReasonPluginInstalled), "announced again: %s", e)
	}
}
