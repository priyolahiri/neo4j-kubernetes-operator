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

// #608, the Aura Fleet Management items of the #606 and #613 walks:
//   - an enable on a running deployment wrote two controllerrevisions in the
//     same second, the conf change's restart stamp and then the merge's write
//     (operations.md id 226);
//   - the record of Fleet Management's NEO4J_PLUGINS entry stayed after
//     spec.env took the variable back (id 227);
//   - plugin-only mode logged "neither tokenSecretRef nor provision" at info on
//     every pass (id 145).

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// fakeClientWith is a fake API server holding objs whose StatefulSet writes
// writes counts.
func fakeClientWith(t *testing.T, writes *templateWrites, objs ...client.Object) client.WithWatch {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(objs...).WithInterceptorFuncs(writes.funcs()).Build()
}

// enableRoll turns Fleet Management on for a cluster settled with it off and
// runs one pass: it returns the StatefulSet writes and rolls the pass made,
// and the config-hash stamp before it.
func enableRoll(t *testing.T, h *fleetClusterHarness) (all, rolls int, stampBefore string) {
	t.Helper()
	stampBefore = h.server().Spec.Template.Annotations["neo4j.neo4j.com/config-hash"]
	fleetToggle(t, h.c, h.cluster, fleetOn)
	all, rolls = h.settledPasses(1)
	return all, rolls, stampBefore
}

// Turning Aura Fleet Management on for a running cluster without spec.env
// NEO4J_PLUGINS: one write, the conf change's restart stamp, carrying the
// name and its record. It was the stamp, then the merge: two revisions.
func TestClusterReconcile_EnablingFleetManagementIsOneRoll(t *testing.T) {
	for name, settle := range map[string]func(*fleetClusterHarness){
		"every server Ready":  func(h *fleetClusterHarness) { h.ready() },
		"the servers forming": func(*fleetClusterHarness) {},
	} {
		t.Run(name, func(t *testing.T) {
			h := newFleetClusterHarness(t, nil)
			h.created()
			settle(h)
			all, rolls, stampBefore := enableRoll(t, h)

			assert.Equal(t, 1, rolls, "one roll")
			assert.Equal(t, 1, all, "one write")
			sts := h.server()
			assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts))
			assert.True(t, fleetEntryRecorded(sts), "recorded as Fleet Management's in the same write (id 222)")
			assert.NotEqual(t, stampBefore, sts.Spec.Template.Annotations["neo4j.neo4j.com/config-hash"], "the write is the conf change's restart stamp")
			all, _ = h.settledPasses(repeatPasses)
			assert.Zero(t, all, "then nothing")
			assert.Empty(t, eventsWithReason(h.events(), EventReasonAuraFleetPluginPatchFailed))
		})
	}
}

// The conf write of the enable put off — the ConfigMap manager's one-second
// debounce, or a ConfigMap read from a cache that has not seen it: the merge
// waits for the conf roll, whose restart stamp carries the name. It wrote the
// name alone first, and the conf's roll followed: two.
func TestClusterReconcile_TheMergeWaitsForTheConfRoll(t *testing.T) {
	h := newFleetClusterHarness(t, nil)
	h.created()
	h.ready()
	fleetToggle(t, h.c, h.cluster, fleetOn)
	h.writes.reset()

	// A manager that wrote this cluster's ConfigMap under a second ago.
	cm := NewConfigMapManager(h.c)
	cm.LiveConfig = nil
	cm.lastUpdateTime[h.cluster.Namespace+"/"+h.cluster.Name] = time.Now()
	h.r.ConfigMapManager = cm
	_, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.cluster)})
	require.NoError(t, err)
	all, _ := h.writes.counts()
	assert.Zero(t, all, "the conf write was put off, and the merge waits for its roll")

	_, rolls := h.settledPasses(1)
	assert.Equal(t, 1, rolls, "the conf's roll, with the name")
	sts := h.server()
	assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts))
	assert.True(t, fleetEntryRecorded(sts))
}

// The merge reads the StatefulSet from a cache that has not seen the restart
// stamp, which carried the name: it writes against the API server, so it adds
// nothing a second time and meets no Conflict (no
// AuraFleetManagementPluginPatchFailed).
func TestMergeFleetManagementPlugin_ACacheThatHasNotSeenTheStamp(t *testing.T) {
	ctx := context.Background()
	cluster := fleetPluginCluster()
	cluster.Spec.AuraFleetManagement = fleetOn
	writes := &templateWrites{}
	live := fakeClientWith(t, writes, cluster, selfHealSTS())
	stale := liveSTS(t, live, "c-server")

	// The stamp's write, as the API server has it.
	stamped := stale.DeepCopy()
	require.NoError(t, addFleetEntryInWrite(ctx, stamped))
	require.NoError(t, live.Update(ctx, stamped))
	writes.reset()

	cached := interceptor.NewClient(live, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if sts, ok := obj.(*appsv1.StatefulSet); ok && key.Name == "c-server" {
				stale.DeepCopyInto(sts)
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	r := &Neo4jEnterpriseClusterReconciler{Client: cached, APIReader: live, Scheme: newTestScheme()}
	require.NoError(t, r.mergeFleetManagementPlugin(ctx, cluster))
	all, _ := writes.counts()
	assert.Zero(t, all, "listed on the API server: nothing to write")
	assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, liveSTS(t, live, "c-server")))
}

// The standalone's enable: the StatefulSet write that carries its new conf
// hash adds the name — one roll — also when that pass read its ConfigMap from
// a cache that had not seen the new conf: the merge waits for the write that
// carries it, where it added the name alone first (two rolls).
func TestStandaloneReconcile_EnablingFleetManagementIsOneRoll(t *testing.T) {
	t.Run("a whole pass", func(t *testing.T) {
		h := newFleetStandaloneHarness(t, fleetOff)
		h.pass()
		fleetToggle(t, h.c, h.standalone, fleetOn)
		all, rolls := h.settledPasses(1)
		assert.Equal(t, 1, all)
		assert.Equal(t, 1, rolls)
		sts := h.server()
		assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts))
		assert.True(t, fleetEntryRecorded(sts))
		all, _ = h.settledPasses(repeatPasses)
		assert.Zero(t, all)
	})

	t.Run("a ConfigMap the cache has not seen rewritten", func(t *testing.T) {
		ctx := context.Background()
		h := newFleetStandaloneHarness(t, fleetOff)
		h.pass()
		fleetToggle(t, h.c, h.standalone, fleetOn)
		h.writes.reset()

		// The StatefulSet write stamps the old conf, and changes nothing.
		require.NoError(t, h.r.reconcileStatefulSet(ctx, h.standalone))
		require.NoError(t, h.r.mergeFleetManagementPlugin(ctx, h.standalone))
		all, _ := h.writes.counts()
		assert.Zero(t, all, "the merge waits for the write that carries the new conf")

		require.NoError(t, h.r.reconcileConfigMap(ctx, h.standalone))
		require.NoError(t, h.r.reconcileStatefulSet(ctx, h.standalone))
		require.NoError(t, h.r.mergeFleetManagementPlugin(ctx, h.standalone))
		all, rolls := h.writes.counts()
		assert.Equal(t, 1, all)
		assert.Equal(t, 1, rolls, "the new conf hash and the name, one write")
		sts := h.server()
		assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts))
		assert.True(t, fleetEntryRecorded(sts))
	})
}

// spec.env takes NEO4J_PLUGINS back while Fleet Management is on: the
// variable is the deployment's own again, which carries no record, so the
// template write that takes it back drops the record — no extra write, no
// extra roll. It stayed until the next disable. A record a build before this
// left on such a variable goes with a metadata patch, which restarts nothing.
func TestClusterReconcile_TheRecordGoesWhenSpecEnvTakesNeo4jPluginsBack(t *testing.T) {
	h := newFleetClusterHarnessWith(t, fleetOn, withSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
	h.created()
	h.ready()
	h.setSpecEnv(nil)
	h.settledPasses(1)
	require.True(t, fleetEntryRecorded(h.server()), "Fleet Management's while spec.env does not set it (id 223)")

	h.setSpecEnv([]corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["apoc"]`}})
	all, rolls := h.settledPasses(1)
	assert.Equal(t, 1, rolls, "the roll for the user's list")
	assert.Equal(t, 1, all, "and no other write")
	sts := h.server()
	assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, sts))
	assert.Contains(t, readOwnedEnvVarNames(sts), "NEO4J_PLUGINS")
	assert.False(t, fleetEntryRecorded(sts), "the deployment's own list carries no record")
	all, _ = h.settledPasses(repeatPasses)
	assert.Zero(t, all)

	// As a build before this change left it.
	h.editServer(func(sts *appsv1.StatefulSet) { setFleetEntryRecord(sts, true) })
	all, rolls = h.settledPasses(1)
	assert.Equal(t, 1, all, "a metadata patch")
	assert.Zero(t, rolls, "which restarts nothing")
	assert.False(t, fleetEntryRecorded(h.server()))
}

func TestStandaloneReconcile_TheRecordGoesWhenSpecEnvTakesNeo4jPluginsBack(t *testing.T) {
	h := newFleetStandaloneHarness(t, fleetOn)
	h.standaloneWith(standaloneSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
	h.pass()
	h.standaloneWith(func(s *neo4jv1beta1.Neo4jEnterpriseStandalone) { s.Spec.Env = nil })
	h.settledPasses(1)
	require.Equal(t, `["fleet-management"]`, neo4jPlugins(t, h.server()))
	require.True(t, fleetEntryRecorded(h.server()))

	h.standaloneWith(standaloneSpecEnvPlugins(corev1.EnvVar{Value: `["apoc"]`}))
	all, rolls := h.settledPasses(1)
	assert.Equal(t, 1, rolls)
	assert.Equal(t, 1, all, "the record goes in the same write")
	sts := h.server()
	assert.Equal(t, `["apoc","fleet-management"]`, neo4jPlugins(t, sts))
	assert.False(t, fleetEntryRecorded(sts))
	all, _ = h.settledPasses(repeatPasses)
	assert.Zero(t, all)

	// As a build before this change left it.
	sts = h.server()
	setFleetEntryRecord(sts, true)
	require.NoError(t, h.c.Update(context.Background(), sts))
	all, rolls = h.settledPasses(1)
	assert.Equal(t, 1, all)
	assert.Zero(t, rolls)
	assert.False(t, fleetEntryRecorded(h.server()))
}

// Plugin-only mode — neither tokenSecretRef nor provision — is said once at
// info and at V(1) on the passes after, on both kinds; turned off and on
// again, it is said once more. It was an info line on every pass.
func TestFleetRegistrationDeferred_IsLoggedOnce(t *testing.T) {
	const line = "neither tokenSecretRef nor provision"
	check := func(t *testing.T, pass func(context.Context), toggle func(*neo4jv1beta1.AuraFleetManagementSpec)) {
		t.Helper()
		sink := newLevelCaptureSink()
		ctx := log.IntoContext(context.Background(), logr.New(sink))
		for range 3 {
			pass(ctx)
		}
		assert.Len(t, sink.mentioning(0, line), 1, "at info once")
		assert.Len(t, sink.mentioning(1, line), 2, "at V(1) on the passes after")

		toggle(fleetOff)
		pass(ctx)
		toggle(fleetOn)
		pass(ctx)
		assert.Len(t, sink.mentioning(0, line), 2, "turned on again: said once more")
	}

	t.Run("cluster", func(t *testing.T) {
		h := newFleetClusterHarness(t, fleetOn)
		h.created()
		check(t, func(ctx context.Context) {
			h.r.ConfigMapManager = NewConfigMapManager(h.c)
			h.r.ConfigMapManager.LiveConfig = nil
			_, err := h.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.cluster)})
			require.NoError(t, err)
		}, func(spec *neo4jv1beta1.AuraFleetManagementSpec) { fleetToggle(t, h.c, h.cluster, spec) })
	})

	t.Run("standalone", func(t *testing.T) {
		h := newFleetStandaloneHarness(t, fleetOn)
		check(t, func(ctx context.Context) {
			_, err := h.r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.standalone)})
			require.NoError(t, err)
		}, func(spec *neo4jv1beta1.AuraFleetManagementSpec) { fleetToggle(t, h.c, h.standalone, spec) })
	})

	t.Run("a token source ends it", func(t *testing.T) {
		var said lastAnnounced
		sink := newLevelCaptureSink()
		logger := logr.New(sink)
		assert.False(t, logFleetRegistrationDeferred(logger, &said, "ns/c", false))
		assert.True(t, logFleetRegistrationDeferred(logger, &said, "ns/c", true), "registration goes on")
		assert.False(t, logFleetRegistrationDeferred(logger, &said, "ns/c", false))
		assert.Len(t, sink.mentioning(0, line), 2, "plugin-only again: said again")
	})
}
