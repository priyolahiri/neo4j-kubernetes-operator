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

// Turning Aura Fleet Management off took nothing off the servers: both
// controllers merged fleet-management into NEO4J_PLUGINS only while it was
// enabled, and the merge only adds, so the JAR stayed for good (#606, moved
// from #582). The entry Fleet Management's own path put there — and only that
// one — now goes, in the roll the same disable's conf change makes
// (operations.md id 222).

import (
	"context"
	"sync"
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
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// templateWrites counts the writes of StatefulSets: all of them, and those
// that change the pod template — each a roll of the servers.
type templateWrites struct {
	mu         sync.Mutex
	all, rolls int
}

func (w *templateWrites) counts() (all, rolls int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.all, w.rolls
}

func (w *templateWrites) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.all, w.rolls = 0, 0
}

func (w *templateWrites) funcs() interceptor.Funcs {
	stored := func(ctx context.Context, c client.WithWatch, obj client.Object) *corev1.PodTemplateSpec {
		sts := &appsv1.StatefulSet{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), sts); err != nil {
			return nil
		}
		return &sts.Spec.Template
	}
	note := func(ctx context.Context, c client.WithWatch, obj client.Object, before *corev1.PodTemplateSpec) {
		after := stored(ctx, c, obj)
		w.mu.Lock()
		defer w.mu.Unlock()
		w.all++
		if before != nil && after != nil && !equality.Semantic.DeepEqual(*before, *after) {
			w.rolls++
		}
	}
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*appsv1.StatefulSet); ok {
				w.mu.Lock()
				w.all++
				w.mu.Unlock()
			}
			return nil
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); !ok {
				return c.Update(ctx, obj, opts...)
			}
			before := stored(ctx, c, obj)
			if err := c.Update(ctx, obj, opts...); err != nil {
				return err
			}
			note(ctx, c, obj, before)
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
			note(ctx, c, obj, before)
			return nil
		},
	}
}

// fleetToggle writes spec.auraFleetManagement of the deployment key names:
// enabled, disabled (enabled: false), or the block removed (nil).
func fleetToggle(t *testing.T, c client.Client, obj client.Object, spec *neo4jv1beta1.AuraFleetManagementSpec) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), obj))
	require.NoError(t, patchObject(ctx, c, obj, func() {
		switch o := obj.(type) {
		case *neo4jv1beta1.Neo4jEnterpriseCluster:
			o.Spec.AuraFleetManagement = spec
		case *neo4jv1beta1.Neo4jEnterpriseStandalone:
			o.Spec.AuraFleetManagement = spec
		}
	}))
}

var (
	fleetOn  = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: true}
	fleetOff = &neo4jv1beta1.AuraFleetManagementSpec{Enabled: false}
)

// fleetClusterHarness runs the cluster controller's whole pass on a fake API
// server, for cluster "fleet" in namespace "prod" on 5.26.32, whose entrypoint
// installs fleet-management from the image.
type fleetClusterHarness struct {
	t       *testing.T
	c       client.Client
	r       *Neo4jEnterpriseClusterReconciler
	writes  *templateWrites
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster
}

func newFleetClusterHarness(t *testing.T, fleet *neo4jv1beta1.AuraFleetManagementSpec, objs ...client.Object) *fleetClusterHarness {
	return newFleetClusterHarnessWith(t, fleet, nil, objs...)
}

// newFleetClusterHarnessWith is newFleetClusterHarness with the cluster's
// fixture changed by mutate before the fake API server holds it.
func newFleetClusterHarnessWith(t *testing.T, fleet *neo4jv1beta1.AuraFleetManagementSpec,
	mutate func(*neo4jv1beta1.Neo4jEnterpriseCluster), objs ...client.Object) *fleetClusterHarness {
	t.Helper()
	scheme := noEffectTestScheme(t)
	cluster := minimalCluster("fleet", "prod")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.Spec.Image.Tag = "5.26.32-enterprise"
	cluster.Spec.AuraFleetManagement = fleet
	if mutate != nil {
		mutate(cluster)
	}
	writes := &templateWrites{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objs, cluster, standardStorageClass())...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).WithInterceptorFuncs(writes.funcs()).Build()
	r := &Neo4jEnterpriseClusterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(1000), RequeueAfter: time.Minute,
		Validator: validation.NewClusterValidator(c),
	}
	return &fleetClusterHarness{t: t, c: c, r: r, writes: writes, cluster: cluster}
}

// pass runs one Reconcile. Each pass gets a ConfigMap manager of its own: the
// manager skips a ConfigMap write within a second of its last (a debounce),
// which a test's passes always are.
func (h *fleetClusterHarness) pass() {
	h.t.Helper()
	cm := NewConfigMapManager(h.c)
	cm.LiveConfig = nil
	h.r.ConfigMapManager = cm
	_, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.cluster)})
	require.NoError(h.t, err)
}

func (h *fleetClusterHarness) server() *appsv1.StatefulSet {
	h.t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(h.t, h.c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "fleet-server"}, sts))
	return sts
}

// created runs the first pass, which creates the server StatefulSet, and gives
// it the UID the API server would: the fake client sets none, which the
// controller reads as "not created yet".
func (h *fleetClusterHarness) created() {
	h.t.Helper()
	h.pass()
	sts := h.server()
	sts.UID = "fleet-server-uid"
	require.NoError(h.t, h.c.Update(context.Background(), sts))
	h.writes.reset()
}

// editServer changes the server StatefulSet by hand, as an earlier operator
// or a user left it.
func (h *fleetClusterHarness) editServer(edit func(*appsv1.StatefulSet)) {
	h.t.Helper()
	sts := h.server()
	edit(sts)
	require.NoError(h.t, h.c.Update(context.Background(), sts))
	h.writes.reset()
}

func (h *fleetClusterHarness) conf() string {
	h.t.Helper()
	cm := &corev1.ConfigMap{}
	require.NoError(h.t, h.c.Get(context.Background(), client.ObjectKey{Namespace: "prod", Name: "fleet-config"}, cm))
	return cm.Data["neo4j.conf"]
}

// The cluster's disable: one write — the ConfigMap's restart stamp for the
// conf change — rolls the servers and takes the entry and its record away;
// later passes write nothing; turning it on again adds both back.
func TestClusterReconcile_DisablingFleetManagementTakesItsEntryOut(t *testing.T) {
	for name, off := range map[string]*neo4jv1beta1.AuraFleetManagementSpec{"enabled: false": fleetOff, "block removed": nil} {
		t.Run(name, func(t *testing.T) {
			h := newFleetClusterHarness(t, fleetOn)
			h.created()
			sts := h.server()
			assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts))
			assert.Equal(t, "fleet-management", sts.Annotations[fleetEntriesAnnotation], "recorded in the write that creates it")
			for range repeatPasses {
				h.pass()
			}
			all, _ := h.writes.counts()
			require.Zero(t, all, "a settled cluster writes nothing")
			stampBefore := sts.Spec.Template.Annotations["neo4j.neo4j.com/config-hash"]

			fleetToggle(t, h.c, h.cluster, off)
			h.pass()

			all, rolls := h.writes.counts()
			assert.Equal(t, 1, rolls, "one roll: the conf change's, with the entry taken out in the same write")
			assert.Equal(t, 1, all)
			sts = h.server()
			assert.Equal(t, `[]`, neo4jPlugins(t, sts), "the entry is gone")
			assert.NotContains(t, sts.Annotations, fleetEntriesAnnotation, "and its record")
			assert.NotEqual(t, stampBefore, sts.Spec.Template.Annotations["neo4j.neo4j.com/config-hash"],
				"the write is the conf change's restart stamp")
			assert.NotContains(t, h.conf(), resources.FleetManagementProcedures, "the conf change of the same disable")

			h.writes.reset()
			for range repeatPasses {
				h.pass()
			}
			all, _ = h.writes.counts()
			assert.Zero(t, all, "nothing more to write")

			fleetToggle(t, h.c, h.cluster, fleetOn)
			h.pass()
			sts = h.server()
			assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts), "turned on again: added again")
			assert.Equal(t, "fleet-management", sts.Annotations[fleetEntriesAnnotation], "and recorded again")
		})
	}
}

// A name a Neo4jPlugin puts in NEO4J_PLUGINS is not Fleet Management's to
// take: its record lists it, or a Managed fleet-management plugin of the
// cluster adds it (one not written yet has no record). The record goes, the
// entry stays — the plugin's own removal takes it later.
func TestClusterReconcile_DisablingFleetManagementKeepsAPluginsEntry(t *testing.T) {
	managed := func() *neo4jv1beta1.Neo4jPlugin {
		p := selfHealPlugin("fleet-plugin", "fleet", neo4jv1beta1.PhaseReady, "")
		p.Namespace = "prod"
		p.Spec.Name, p.Spec.Config = "fleet-management", nil
		return p
	}
	cases := map[string]struct {
		objs   []client.Object
		record bool
	}{
		"the plugin's record lists it": {record: true},
		"a Managed plugin adds it":     {objs: []client.Object{managed()}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newFleetClusterHarness(t, fleetOn, tc.objs...)
			h.created()
			if tc.record {
				h.editServer(func(sts *appsv1.StatefulSet) {
					sts.Annotations[pluginEntriesAnnotation("fleet-plugin")] = "fleet-management"
				})
			}
			require.True(t, fleetEntryRecorded(h.server()))

			fleetToggle(t, h.c, h.cluster, fleetOff)
			h.pass()

			sts := h.server()
			assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts), "the plugin's entry stays")
			assert.NotContains(t, sts.Annotations, fleetEntriesAnnotation, "Fleet Management's record goes")
			_, rolls := h.writes.counts()
			assert.Equal(t, 1, rolls, "the conf change's roll alone")
		})
	}
}

// An entry with no record: a cluster an earlier operator gave the name. While
// Fleet Management is on, a pass adopts it with a metadata patch (no roll),
// unless a Neo4jPlugin claims the name; turned off then, it goes. On a cluster
// whose Fleet Management was already off when the operator was upgraded, it
// is left: it cannot be told from one a user put there (a by-hand step).
func TestClusterReconcile_AnEntryWithoutARecord(t *testing.T) {
	forget := func(sts *appsv1.StatefulSet) { delete(sts.Annotations, fleetEntriesAnnotation) }

	t.Run("adopted while enabled, then taken out", func(t *testing.T) {
		h := newFleetClusterHarness(t, fleetOn)
		h.created()
		h.editServer(forget)

		h.pass()
		all, rolls := h.writes.counts()
		assert.Equal(t, 1, all, "one write: the record")
		assert.Zero(t, rolls, "a metadata patch restarts nothing")
		assert.True(t, fleetEntryRecorded(h.server()))

		fleetToggle(t, h.c, h.cluster, fleetOff)
		h.pass()
		assert.Equal(t, `[]`, neo4jPlugins(t, h.server()))
	})

	t.Run("not adopted while a plugin claims it", func(t *testing.T) {
		p := selfHealPlugin("fleet-plugin", "fleet", neo4jv1beta1.PhaseReady, "")
		p.Namespace = "prod"
		p.Spec.Name, p.Spec.Config = "fleet-management", nil
		h := newFleetClusterHarness(t, fleetOn, p)
		h.created()
		h.editServer(forget)

		h.pass()
		all, _ := h.writes.counts()
		assert.Zero(t, all)
		assert.False(t, fleetEntryRecorded(h.server()))

		fleetToggle(t, h.c, h.cluster, fleetOff)
		h.pass()
		assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, h.server()), "the plugin's")
	})

	t.Run("already off: left", func(t *testing.T) {
		h := newFleetClusterHarness(t, fleetOn)
		h.created()
		h.editServer(forget)
		fleetToggle(t, h.c, h.cluster, fleetOff)

		h.pass()
		for range repeatPasses {
			h.pass()
		}
		assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, h.server()), "unrecorded: left for a by-hand removal")
		_, rolls := h.writes.counts()
		assert.Equal(t, 1, rolls, "the conf change's roll alone")
	})
}

// A NEO4J_PLUGINS the cluster's own spec.env sets is the user's: Fleet
// Management never records a name in it, and never takes one out.
func TestClusterReconcile_DisablingFleetManagementLeavesTheUsersOwnList(t *testing.T) {
	h := newFleetClusterHarnessWith(t, fleetOn, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
		c.Spec.Env = []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["fleet-management"]`}}
	})
	h.created()
	sts := h.server()
	require.Contains(t, readOwnedEnvVarNames(sts), "NEO4J_PLUGINS")
	assert.False(t, fleetEntryRecorded(sts), "the user's list: not recorded")

	fleetToggle(t, h.c, h.cluster, fleetOff)
	h.pass()
	assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, h.server()))

	// A record against the user's own list goes; the list stays.
	sts = h.server()
	setFleetEntryRecord(sts, true)
	changed, rolls := dropFleetEntry(context.Background(), sts, false)
	assert.True(t, changed)
	assert.False(t, rolls)
	assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts))
	assert.False(t, fleetEntryRecorded(sts))
}

// On 2026.03 and later Fleet Manager is built in: the merge adds nothing and
// keeps an entry an earlier operator wrote (id 192). That entry is adopted
// while enabled and goes with the disable's roll — it only logs "ERROR: could
// not query null" at each start.
func TestClusterReconcile_DisablingFleetManagementOnABuiltInImage(t *testing.T) {
	h := newFleetClusterHarnessWith(t, fleetOn, func(c *neo4jv1beta1.Neo4jEnterpriseCluster) {
		c.Spec.Image.Tag = "2026.08.1-enterprise"
	})
	h.created()
	require.NotContains(t, neo4jPlugins(t, h.server()), "fleet-management", "nothing added on a built-in image")

	// As an earlier operator left it.
	h.editServer(func(sts *appsv1.StatefulSet) {
		c := neo4jContainer(&sts.Spec.Template.Spec)
		c.Env = append(c.Env, corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["fleet-management"]`})
	})
	h.pass()
	all, rolls := h.writes.counts()
	assert.Equal(t, 1, all)
	assert.Zero(t, rolls, "kept while enabled: nothing restarts")
	assert.True(t, fleetEntryRecorded(h.server()))

	h.writes.reset()
	fleetToggle(t, h.c, h.cluster, fleetOff)
	h.pass()
	_, rolls = h.writes.counts()
	assert.Equal(t, 1, rolls, "the conf change's roll carries it")
	assert.Equal(t, `[]`, neo4jPlugins(t, h.server()))
}

// The removal no conf roll carries: the cluster's write of it waits while one
// is due (the ConfigMap's restart stamp takes it), then goes alone, once.
func TestRemoveFleetManagementPlugin_WaitsForTheConfRoll(t *testing.T) {
	ctx := context.Background()
	cluster := fleetPluginCluster()
	cluster.Spec.AuraFleetManagement = fleetOff
	sts := selfHealSTS(corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["apoc","fleet-management"]`})
	sts.Annotations = map[string]string{fleetEntriesAnnotation: "fleet-management", pluginEntriesAnnotation("apoc"): "apoc"}
	enabled := cluster.DeepCopy()
	enabled.Spec.AuraFleetManagement = fleetOn
	staleConf := resources.BuildConfigMapForEnterprise(enabled)

	writes := &templateWrites{}
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(cluster, sts, staleConf).
		WithInterceptorFuncs(writes.funcs()).Build()
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: newTestScheme(), ConfigMapManager: NewConfigMapManager(c)}

	r.reconcileFleetEntry(ctx, cluster)
	all, _ := writes.counts()
	assert.Zero(t, all, "the conf still has Fleet Management's lines: its restart stamp carries the removal")

	settled := resources.BuildConfigMapForEnterprise(cluster)
	live := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(settled), live))
	live.Data = settled.Data
	require.NoError(t, c.Update(ctx, live))

	r.reconcileFleetEntry(ctx, cluster)
	r.reconcileFleetEntry(ctx, cluster)
	all, rolls := writes.counts()
	assert.Equal(t, 1, all, "written once, alone")
	assert.Equal(t, 1, rolls)
	got := liveSTS(t, c, "c-server")
	assert.Equal(t, `["apoc"]`, neo4jPlugins(t, got), "only Fleet Management's entry goes")
	assert.False(t, fleetEntryRecorded(got))
	assert.Equal(t, "apoc", got.Annotations[pluginEntriesAnnotation("apoc")], "the plugin's record is left alone")
}

// The removal and the plugin controller leave each other's entries alone: a
// plugin's write after the removal does not put fleet-management back, and
// a fleet-management plugin installed while Fleet Management was on keeps its
// entry through the disable, until its own removal.
func TestFleetRemoval_AndThePluginControllerLeaveEachOthersEntries(t *testing.T) {
	ctx := context.Background()
	disabledCluster := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		c := fleetPluginCluster()
		c.Spec.AuraFleetManagement = fleetOff
		return c
	}
	settledConf := func(c *neo4jv1beta1.Neo4jEnterpriseCluster) client.Object {
		return resources.BuildConfigMapForEnterprise(c)
	}

	t.Run("an APOC write after the removal", func(t *testing.T) {
		cluster := disabledCluster()
		apoc := selfHealPlugin("apoc", "c", "", "")
		r, _, _, _ := gateReconciler(t, nil, append([]client.Object{apoc, cluster, settledConf(cluster),
			selfHealSTS(corev1.EnvVar{Name: "NEO4J_PLUGINS", Value: `["fleet-management"]`})}, readyClusterPods()...)...)
		sts := liveSTS(t, r.Client, "c-server")
		sts.Annotations = map[string]string{fleetEntriesAnnotation: "fleet-management"}
		require.NoError(t, r.Update(ctx, sts))
		rollOut(t, r.Client, "c-server")

		clusters := &Neo4jEnterpriseClusterReconciler{Client: r.Client, Scheme: newTestScheme(), ConfigMapManager: NewConfigMapManager(r.Client)}
		clusters.reconcileFleetEntry(ctx, cluster)
		rollOut(t, r.Client, "c-server")
		installAndRoll(t, r, "apoc", "c-server")
		reconcilePlugin(t, r, "apoc")

		v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
		assert.Equal(t, `["apoc"]`, v, "the plugin writes its own name, not Fleet Management's back")
	})

	t.Run("a fleet-management plugin's entry through the disable", func(t *testing.T) {
		cluster := disabledCluster()
		sts := fleetPluginSTS("fleetManagement.*")
		sts.Annotations[fleetEntriesAnnotation] = "fleet-management"
		r, _, _, _ := gateReconciler(t, nil, append([]client.Object{fleetPlugin(), cluster, settledConf(cluster), sts}, readyClusterPods()...)...)

		clusters := &Neo4jEnterpriseClusterReconciler{Client: r.Client, Scheme: newTestScheme(), ConfigMapManager: NewConfigMapManager(r.Client)}
		clusters.reconcileFleetEntry(ctx, cluster)
		got := liveSTS(t, r.Client, "c-server")
		assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, got), "the plugin's")
		assert.False(t, fleetEntryRecorded(got))

		// The plugin's removal, with Fleet Management off, takes it.
		plugin := getPlugin(t, r, "fleet")
		require.NoError(t, patchObject(ctx, r.Client, plugin, func() { plugin.Spec.Enabled = false }))
		rollOut(t, r.Client, "c-server")
		reconcilePlugin(t, r, "fleet")
		v, _ := envValueOf(t, r, "c-server", "NEO4J_PLUGINS")
		assert.Equal(t, `[]`, v)
	})
}

// fleetStandaloneHarness runs the standalone controller's whole pass on a fake
// API server, for standalone "single" in namespace "dev" on 5.26.32.
type fleetStandaloneHarness struct {
	t          *testing.T
	c          client.Client
	r          *Neo4jEnterpriseStandaloneReconciler
	writes     *templateWrites
	standalone *neo4jv1beta1.Neo4jEnterpriseStandalone
}

func newFleetStandaloneHarness(t *testing.T, fleet *neo4jv1beta1.AuraFleetManagementSpec, objs ...client.Object) *fleetStandaloneHarness {
	t.Helper()
	scheme := noEffectTestScheme(t)
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "dev", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.32-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
			AuraFleetManagement:    fleet,
		},
	}
	writes := &templateWrites{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objs, standalone, standardStorageClass())...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).WithInterceptorFuncs(writes.funcs()).Build()
	r := &Neo4jEnterpriseStandaloneReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(1000), RequeueAfter: time.Minute,
		Validator: validation.NewStandaloneValidator(),
	}
	return &fleetStandaloneHarness{t: t, c: c, r: r, writes: writes, standalone: standalone}
}

func (h *fleetStandaloneHarness) pass() {
	h.t.Helper()
	_, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.standalone)})
	require.NoError(h.t, err)
}

func (h *fleetStandaloneHarness) server() *appsv1.StatefulSet {
	h.t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(h.t, h.c.Get(context.Background(), client.ObjectKeyFromObject(h.standalone), sts))
	return sts
}

// The standalone's disable: one write — the template write that carries the
// new conf hash — rolls the pod and takes the entry and its record away;
// later passes write nothing; turning it on again adds both back.
func TestStandaloneReconcile_DisablingFleetManagementTakesItsEntryOut(t *testing.T) {
	for name, off := range map[string]*neo4jv1beta1.AuraFleetManagementSpec{"enabled: false": fleetOff, "block removed": nil} {
		t.Run(name, func(t *testing.T) {
			h := newFleetStandaloneHarness(t, fleetOn)
			h.pass()
			sts := h.server()
			assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts))
			assert.Equal(t, "fleet-management", sts.Annotations[fleetEntriesAnnotation], "recorded in the write that creates it")
			h.writes.reset()
			for range repeatPasses {
				h.pass()
			}
			all, _ := h.writes.counts()
			require.Zero(t, all)
			stampBefore := sts.Spec.Template.Annotations[standaloneConfigHashAnnotation]

			fleetToggle(t, h.c, h.standalone, off)
			h.pass()

			all, rolls := h.writes.counts()
			assert.Equal(t, 1, all, "one write")
			assert.Equal(t, 1, rolls, "one roll: the conf change's, with the entry taken out in it")
			sts = h.server()
			assert.Equal(t, `[]`, neo4jPlugins(t, sts))
			assert.NotContains(t, sts.Annotations, fleetEntriesAnnotation)
			assert.NotEqual(t, stampBefore, sts.Spec.Template.Annotations[standaloneConfigHashAnnotation], "the write carries the new conf")

			h.writes.reset()
			for range repeatPasses {
				h.pass()
			}
			all, _ = h.writes.counts()
			assert.Zero(t, all)

			fleetToggle(t, h.c, h.standalone, fleetOn)
			h.pass()
			sts = h.server()
			assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts), "turned on again: added again")
			assert.Equal(t, "fleet-management", sts.Annotations[fleetEntriesAnnotation])
		})
	}
}

// A fleet-management plugin's entry stays through a standalone's disable, and
// an entry with no record follows the cluster's rules: adopted while enabled,
// left when the standalone's Fleet Management was already off.
func TestStandaloneReconcile_DisablingFleetManagementKeepsWhatIsNotItsOwn(t *testing.T) {
	t.Run("a plugin's record lists it", func(t *testing.T) {
		h := newFleetStandaloneHarness(t, fleetOn)
		h.pass()
		sts := h.server()
		sts.Annotations[pluginEntriesAnnotation("fleet-plugin")] = "fleet-management"
		require.NoError(t, h.c.Update(context.Background(), sts))

		fleetToggle(t, h.c, h.standalone, fleetOff)
		h.pass()
		sts = h.server()
		assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, sts))
		assert.False(t, fleetEntryRecorded(sts))
	})

	t.Run("adopted while enabled", func(t *testing.T) {
		h := newFleetStandaloneHarness(t, fleetOn)
		h.pass()
		sts := h.server()
		delete(sts.Annotations, fleetEntriesAnnotation)
		require.NoError(t, h.c.Update(context.Background(), sts))
		h.writes.reset()

		h.pass()
		_, rolls := h.writes.counts()
		assert.Zero(t, rolls)
		assert.True(t, fleetEntryRecorded(h.server()))

		fleetToggle(t, h.c, h.standalone, fleetOff)
		h.pass()
		assert.Equal(t, `[]`, neo4jPlugins(t, h.server()))
	})

	t.Run("already off: left", func(t *testing.T) {
		h := newFleetStandaloneHarness(t, fleetOn)
		h.pass()
		sts := h.server()
		delete(sts.Annotations, fleetEntriesAnnotation)
		require.NoError(t, h.c.Update(context.Background(), sts))
		fleetToggle(t, h.c, h.standalone, fleetOff)

		h.pass()
		assert.Equal(t, `["fleet-management"]`, neo4jPlugins(t, h.server()))
	})
}

// A standalone writes its StatefulSet with the conf hash of the ConfigMap it
// reads, which a cache may not have seen rewritten yet: the removal waits for
// the write that carries the new conf, so the pod restarts once, not twice.
func TestStandaloneReconcile_TheRemovalWaitsForTheNewConf(t *testing.T) {
	ctx := context.Background()
	h := newFleetStandaloneHarness(t, fleetOn)
	h.pass()
	fleetToggle(t, h.c, h.standalone, fleetOff)
	h.writes.reset()

	// The ConfigMap as a cache that has not seen the disable's write has it.
	require.NoError(t, h.r.reconcileStatefulSet(ctx, h.standalone))
	all, _ := h.writes.counts()
	assert.Zero(t, all, "the conf hash is still the old conf's: nothing is written")
	assert.True(t, fleetEntryRecorded(h.server()))

	require.NoError(t, h.r.reconcileConfigMap(ctx, h.standalone))
	require.NoError(t, h.r.reconcileStatefulSet(ctx, h.standalone))
	all, rolls := h.writes.counts()
	assert.Equal(t, 1, all)
	assert.Equal(t, 1, rolls, "the new conf hash and the removal, one write")
	assert.Equal(t, `[]`, neo4jPlugins(t, h.server()))
}

func TestFleetEntryClaimed(t *testing.T) {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}}
	managed := func(name string, mutate func(*neo4jv1beta1.Neo4jPlugin)) neo4jv1beta1.Neo4jPlugin {
		p := selfHealPlugin(name, "c", "", "")
		p.Spec.Name, p.Spec.Config = "fleet-management", nil
		if mutate != nil {
			mutate(p)
		}
		return *p
	}
	const tag = "5.26.32-enterprise"
	assert.True(t, fleetEntryClaimed(sts, []neo4jv1beta1.Neo4jPlugin{managed("f", nil)}, "c", tag))
	assert.True(t, fleetEntryClaimed(sts, []neo4jv1beta1.Neo4jPlugin{managed("f", func(p *neo4jv1beta1.Neo4jPlugin) {
		p.Spec.Name = "apoc"
		p.Spec.Dependencies = []neo4jv1beta1.PluginDependency{{Name: "fleet-management"}}
	})}, "c", tag), "a dependency")
	assert.False(t, fleetEntryClaimed(sts, []neo4jv1beta1.Neo4jPlugin{managed("f", func(p *neo4jv1beta1.Neo4jPlugin) {
		p.Spec.ClusterRef = "other"
	})}, "c", tag), "another target's")
	assert.False(t, fleetEntryClaimed(sts, []neo4jv1beta1.Neo4jPlugin{managed("f", func(p *neo4jv1beta1.Neo4jPlugin) {
		p.Spec.Enabled = false
	})}, "c", tag), "a disabled plugin without a record")
	assert.False(t, fleetEntryClaimed(sts, []neo4jv1beta1.Neo4jPlugin{managed("f", func(p *neo4jv1beta1.Neo4jPlugin) {
		p.Spec.InstallMode = PluginInstallModeVerifiedDownload
	})}, "c", tag), "one that never touches NEO4J_PLUGINS")
	assert.False(t, fleetEntryClaimed(sts, []neo4jv1beta1.Neo4jPlugin{managed("f", func(p *neo4jv1beta1.Neo4jPlugin) {
		now := metav1.Now()
		p.DeletionTimestamp = &now
	})}, "c", tag), "one being deleted")

	sts.Annotations[pluginEntriesAnnotation("disabled")] = "fleet-management"
	assert.True(t, fleetEntryClaimed(sts, nil, "c", tag), "a plugin's record, a disabled plugin's too")
}
