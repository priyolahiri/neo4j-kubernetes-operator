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

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// stsWrite is one write of the server StatefulSet: who made it, whether it
// changed the pod template (a roll), and the annotations it changed.
type stsWrite struct {
	by          string
	roll        bool
	annotations []string
}

func (w stsWrite) String() string {
	return fmt.Sprintf("%s roll=%v %v", w.by, w.roll, w.annotations)
}

// clusterAndPluginHarness runs the cluster controller's whole pass and a
// Neo4jPlugin's pass on one fake API server, for cluster "fleet" in "prod" on
// 5.26.32 with Aura Fleet Management on and a Managed GDS plugin, and records
// every write of the server StatefulSet with the pass that made it. The fake
// moves the StatefulSet's generation when its spec changes, as the API server
// does.
type clusterAndPluginHarness struct {
	t       *testing.T
	c       client.Client
	r       *Neo4jEnterpriseClusterReconciler
	pr      *Neo4jPluginReconciler
	cluster client.ObjectKey
	by      string
	writes  []stsWrite
}

func newClusterAndPluginHarness(t *testing.T, specEnv []corev1.EnvVar) *clusterAndPluginHarness {
	t.Helper()
	scheme := noEffectTestScheme(t)
	cluster := minimalCluster("fleet", "prod")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.Spec.Image.Tag = "5.26.32-enterprise"
	cluster.Spec.AuraFleetManagement = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
	cluster.Spec.Env = specEnv
	gds := gdsPlugin()
	gds.Namespace, gds.Spec.ClusterRef = "prod", "fleet"
	objs := []client.Object{cluster, standardStorageClass(), gds}
	for i := range int(cluster.Spec.Topology.Servers) {
		objs = append(objs, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("fleet-server-%d", i), Namespace: "prod",
				Labels: map[string]string{"app.kubernetes.io/name": "neo4j", "app.kubernetes.io/instance": "fleet"}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		})
	}
	h := &clusterAndPluginHarness{t: t, cluster: client.ObjectKeyFromObject(cluster), by: "setup"}
	stored := func(ctx context.Context, c client.WithWatch, obj client.Object) *appsv1.StatefulSet {
		s := &appsv1.StatefulSet{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), s); err != nil {
			return nil
		}
		return s
	}
	note := func(before, after *appsv1.StatefulSet) {
		if before == nil || after == nil {
			return
		}
		var changed []string
		for k, v := range after.Annotations {
			if before.Annotations[k] != v {
				changed = append(changed, k)
			}
		}
		for k := range before.Annotations {
			if _, ok := after.Annotations[k]; !ok {
				changed = append(changed, k)
			}
		}
		sort.Strings(changed)
		h.writes = append(h.writes, stsWrite{by: h.by, roll: !equality.Semantic.DeepEqual(before.Spec.Template, after.Spec.Template), annotations: changed})
	}
	h.c = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}, &neo4jv1beta1.Neo4jPlugin{}, &appsv1.StatefulSet{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				sts, ok := obj.(*appsv1.StatefulSet)
				if !ok {
					return c.Update(ctx, obj, opts...)
				}
				before := stored(ctx, c, obj)
				if before != nil && !equality.Semantic.DeepEqual(before.Spec, sts.Spec) {
					sts.Generation = before.Generation + 1
				}
				if err := c.Update(ctx, obj, opts...); err != nil {
					return err
				}
				note(before, stored(ctx, c, obj))
				return nil
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*appsv1.StatefulSet); !ok {
					return c.Patch(ctx, obj, patch, opts...)
				}
				before := stored(ctx, c, obj)
				if err := c.Patch(ctx, obj, patch, opts...); err != nil {
					return err
				}
				after := stored(ctx, c, obj)
				if before != nil && after != nil && !equality.Semantic.DeepEqual(before.Spec, after.Spec) {
					after.Generation = before.Generation + 1
					if err := c.Update(ctx, after); err != nil {
						return err
					}
				}
				note(before, after)
				return nil
			},
		}).Build()
	h.r = &Neo4jEnterpriseClusterReconciler{Client: h.c, Scheme: scheme, Recorder: record.NewFakeRecorder(1000),
		RequeueAfter: time.Minute, Validator: validation.NewClusterValidator(h.c)}
	h.pr = &Neo4jPluginReconciler{Client: h.c, Scheme: scheme, Recorder: record.NewFakeRecorder(1000), RequeueAfter: 30 * time.Second,
		bolt: &pluginBolt{functional: func(context.Context, *DeploymentInfo) bool { return true }}}
	return h
}

func (h *clusterAndPluginHarness) sts() *appsv1.StatefulSet {
	h.t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(h.t, h.c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "fleet-server"}, sts))
	return sts
}

// clusterPass runs the cluster's Reconcile, with a ConfigMap manager of its
// own (it debounces writes a second apart, which a test's passes are).
func (h *clusterAndPluginHarness) clusterPass(by string) {
	h.t.Helper()
	h.by = by
	cm := NewConfigMapManager(h.c)
	cm.LiveConfig = nil
	h.r.ConfigMapManager = cm
	_, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.cluster})
	require.NoError(h.t, err)
}

// pluginPass runs GDS's Reconcile with the cluster Ready at its generation:
// its servers cannot be reached here, so the cluster's own passes never write
// Ready; the status they wrote is put back after.
func (h *clusterAndPluginHarness) pluginPass(by string) {
	h.t.Helper()
	ctx := context.Background()
	h.by = by
	cl := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(h.t, h.c.Get(ctx, h.cluster, cl))
	saved := cl.Status.DeepCopy()
	cl.Status.Phase, cl.Status.ObservedGeneration = neo4jv1beta1.PhaseReady, cl.Generation
	require.NoError(h.t, h.c.Status().Update(ctx, cl))
	_, err := h.pr.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "prod", Name: "gds"}})
	require.NoError(h.t, err)
	require.NoError(h.t, h.c.Get(ctx, h.cluster, cl))
	cl.Status = *saved
	require.NoError(h.t, h.c.Status().Update(ctx, cl))
}

// rolledOut reports every server on the latest template and ready.
func (h *clusterAndPluginHarness) rolledOut() {
	h.t.Helper()
	sts := h.sts()
	if sts.UID == "" {
		sts.UID = "fleet-server-uid"
		require.NoError(h.t, h.c.Update(context.Background(), sts))
		sts = h.sts()
	}
	sts.Status = rolledOutStatus(*sts.Spec.Replicas, sts.Generation)
	require.NoError(h.t, h.c.Status().Update(context.Background(), sts))
}

// midRoll reports the StatefulSet part way through rolling its latest
// template: one server replaced, one not ready.
func (h *clusterAndPluginHarness) midRoll() {
	h.t.Helper()
	sts := h.sts()
	n := *sts.Spec.Replicas
	sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: sts.Generation, Replicas: n, ReadyReplicas: n - 1,
		AvailableReplicas: n - 1, UpdatedReplicas: 1, CurrentReplicas: n - 1, CurrentRevision: "old", UpdateRevision: "new"}
	require.NoError(h.t, h.c.Status().Update(context.Background(), sts))
}

func (h *clusterAndPluginHarness) setSpecEnv(env []corev1.EnvVar) {
	h.t.Helper()
	cl := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(h.t, h.c.Get(context.Background(), h.cluster, cl))
	require.NoError(h.t, patchObject(context.Background(), h.c, cl, func() { cl.Spec.Env = env }))
}

// since returns the writes made after the first n.
func (h *clusterAndPluginHarness) since(n int) []stsWrite {
	return append([]stsWrite(nil), h.writes[n:]...)
}

// #619 S8: on the v1.24.0 journey's GDS cluster (Aura Fleet Management on), a
// metadata-only write of the server StatefulSet by the operator's field
// manager came 19 s after the write that took spec.env NEO4J_PLUGINS back
// (rule 227); the walk could not say which. The candidates are the writers of
// the StatefulSet's records — a plugin's record upkeep (recordPluginEnv, the
// metadata-only branch of writePluginToTarget), Aura Fleet Management's
// (fleetRecordUpkeep) and the cluster's owned-env record — and each writes
// only when its record differs from what it would write. Here the whole
// sequence, cluster and plugin passes alike, with the servers settled and
// part way through the roll, and with the plugin settled or still waiting for
// its own roll: emptying spec.env is one write (id 233), setting it again is
// one write, and nothing writes the StatefulSet after either.
func TestClusterAndPlugin_SpecEnvNeo4jPluginsEmptiedAndSetAgain(t *testing.T) {
	list := []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["apoc","graph-data-science"]`}}
	for name, pluginMidRoll := range map[string]bool{"the plugin settled": false, "the plugin waiting for its own roll": true} {
		t.Run(name, func(t *testing.T) {
			h := newClusterAndPluginHarness(t, list)
			h.clusterPass("cluster: create")
			h.rolledOut()
			for i := range 3 {
				h.pluginPass(fmt.Sprintf("gds: install %d", i))
				h.rolledOut()
			}
			h.clusterPass("cluster")
			h.pluginPass("gds")
			require.Equal(t, neo4jv1beta1.PhaseReady, getPluginIn(t, h.c, "prod", "gds").Status.Phase)

			// Emptied: the cluster's write keeps graph-data-science and adds
			// fleet-management, each recorded as its own (ids 223, 233).
			n := len(h.writes)
			h.setSpecEnv(nil)
			h.clusterPass("cluster: emptied")
			for i := range 3 {
				h.pluginPass(fmt.Sprintf("gds: after emptied %d", i))
				if !pluginMidRoll {
					h.rolledOut()
				}
			}
			h.clusterPass("cluster: after emptied")
			got := h.since(n)
			require.Len(t, got, 1, "one write: %v", got)
			assert.Equal(t, "cluster: emptied", got[0].by)
			assert.True(t, got[0].roll)
			assert.JSONEq(t, `["graph-data-science","fleet-management"]`, neo4jPlugins(t, h.sts()))
			assert.True(t, fleetEntryRecorded(h.sts()))

			// Set again: the write that takes it back rolls the servers and
			// drops Fleet Management's record (id 227); nothing writes after
			// it, through a roll in progress or done.
			n = len(h.writes)
			h.setSpecEnv(list)
			h.clusterPass("cluster: set again")
			for i := range 2 {
				h.pluginPass(fmt.Sprintf("gds: before the roll %d", i))
			}
			h.midRoll()
			for i := range 3 {
				h.clusterPass(fmt.Sprintf("cluster: mid-roll %d", i))
				h.pluginPass(fmt.Sprintf("gds: mid-roll %d", i))
			}
			h.rolledOut()
			for i := range 3 {
				h.pluginPass(fmt.Sprintf("gds: rolled out %d", i))
				h.clusterPass(fmt.Sprintf("cluster: rolled out %d", i))
			}
			got = h.since(n)
			require.Len(t, got, 1, "one write, and no metadata-only write after it: %v", got)
			assert.Equal(t, "cluster: set again", got[0].by)
			assert.True(t, got[0].roll)
			sts := h.sts()
			assert.JSONEq(t, `["apoc","graph-data-science","fleet-management"]`, neo4jPlugins(t, sts))
			assert.False(t, fleetEntryRecorded(sts))
			assert.Equal(t, "graph-data-science", sts.Annotations[pluginEntriesAnnotation("gds")],
				"the plugin's record of its name stays: it installs it, whoever's variable it is")
			assert.Equal(t, neo4jv1beta1.PhaseReady, getPluginIn(t, h.c, "prod", "gds").Status.Phase)
		})
	}
}

func getPluginIn(t *testing.T, c client.Client, ns, name string) *neo4jv1beta1.Neo4jPlugin {
	t.Helper()
	p := &neo4jv1beta1.Neo4jPlugin{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, p))
	return p
}
