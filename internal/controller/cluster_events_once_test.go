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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Cluster events that repeated, doubled or said the wrong thing (#527, #531,
// #549, #553). Each runs the emitter over several passes, or over a copy of
// the cluster older than what the API server holds, as the informer cache
// hands a pass.

// TopologyPlacementCalculated was emitted on every pass of a zoned cluster.
// It is announced when the zones are first calculated and when they change.
func TestClusterReconcile_TopologyPlacementIsAnnouncedWhenTheZonesChange(t *testing.T) {
	ctx := context.Background()
	scheme := noEffectTestScheme(t)
	cluster := minimalCluster("zoned", "prod")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.Spec.Topology.Servers = 3
	cluster.Spec.Topology.AvailabilityZones = []string{"zone-a", "zone-b", "zone-c"}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, standardStorageClass()).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()
	rec := record.NewFakeRecorder(200)
	r := &Neo4jEnterpriseClusterReconciler{
		Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Minute,
		Validator: validation.NewClusterValidator(c), ConfigMapManager: NewConfigMapManager(c),
		TopologyScheduler: NewTopologyScheduler(c),
	}
	key := client.ObjectKeyFromObject(cluster)
	placements := func() []string {
		t.Helper()
		return eventsWithReason(drainEvents(rec), EventReasonTopologyPlacementCalc)
	}

	for _, err := range reconcileTimes(t, r, key, repeatPasses) {
		require.NoError(t, err)
	}
	got := placements()
	require.Len(t, got, 1, "announced once, not on every pass")
	assert.Contains(t, got[0], "across 3 zones (zone-a, zone-b, zone-c)")

	live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(ctx, key, live))
	live.Spec.Topology.AvailabilityZones = []string{"zone-a", "zone-b"}
	require.NoError(t, c.Update(ctx, live))
	for _, err := range reconcileTimes(t, r, key, repeatPasses) {
		require.NoError(t, err)
	}
	got = placements()
	require.Len(t, got, 1, "other zones are announced, once")
	assert.Contains(t, got[0], "across 2 zones (zone-a, zone-b)")
}

// UpgradeDeferred was emitted every 15s while a scale-down drain held an
// upgrade back. It is announced once per held target; a new target, or a
// later drain, is announced again.
func TestStartRollingUpgrade_TheDrainDeferralIsAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	cluster := clusterForUpgrade("defer", "default", "5.26.0-enterprise", "5.26.1-enterprise", 2)
	cluster.Annotations = map[string]string{ScaleDownDrainingServersAnnotation: "id-a,id-b"}
	fc := fake.NewClientBuilder().WithScheme(makeUpgradeScheme()).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(20)
	r := &Neo4jEnterpriseClusterReconciler{Client: fc, Recorder: rec}
	deferred := func() []string { return eventsWithReason(drainEvents(rec), EventReasonUpgradeDeferred) }

	for range repeatPasses {
		_, err := r.startRollingUpgrade(ctx, cluster)
		require.NoError(t, err)
	}
	got := deferred()
	require.Len(t, got, 1, "the upgrade machine asks every 15s; the deferral is news once")
	assert.Contains(t, got[0], "5.26.1-enterprise")

	cluster.Spec.Image.Tag = "5.26.2-enterprise"
	for range repeatPasses {
		_, err := r.startRollingUpgrade(ctx, cluster)
		require.NoError(t, err)
	}
	got = deferred()
	require.Len(t, got, 1, "a new target held back is announced")
	assert.Contains(t, got[0], "5.26.2-enterprise")

	// The drain ends and a later one holds the same target back again.
	require.NoError(t, r.setDrainServers(ctx, cluster, nil))
	require.NoError(t, r.setDrainServers(ctx, cluster, []string{"id-c"}))
	_, err := r.startRollingUpgrade(ctx, cluster)
	require.NoError(t, err)
	assert.Len(t, deferred(), 1, "each drain announces what it holds back")
}

// ClusterFormationStarted was emitted twice for every new cluster: the pass
// after the one that wrote Forming read the cluster from a cache that had not
// seen that write yet, and judged from its copy. It is announced by the write
// that enters Forming.
func TestReconcileServerShortfall_FormationStartedIsAnnouncedOnceFromAStaleCopy(t *testing.T) {
	ctx := context.Background()
	r, rec := shortfallReconciler(t, availCluster("Initializing"))
	unreachable := formationCheck{message: "Waiting for Neo4j to accept connections"}

	stale := refetch(t, r) // phase Initializing, as both passes read it
	r.reconcileServerShortfall(ctx, stale.DeepCopy(), unreachable, unreachable.message)
	r.reconcileServerShortfall(ctx, stale.DeepCopy(), unreachable, unreachable.message)
	assert.Equal(t, neo4jv1beta1.PhaseForming, refetch(t, r).Status.Phase)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonClusterFormationStarted), 1)

	// A copy from before the cluster formed, read after it did: a rollout
	// takes it to Forming, which is not formation starting.
	formed := availCluster(neo4jv1beta1.PhaseReady,
		cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady),
		cond(ConditionTypeClusterFormed, metav1.ConditionTrue, ConditionReasonFormationComplete))
	r, rec = shortfallReconciler(t, formed)
	beforeFormation := refetch(t, r)
	beforeFormation.Status = neo4jv1beta1.Neo4jEnterpriseClusterStatus{Phase: "Initializing"}
	r.reconcileServerShortfall(ctx, beforeFormation, unreachable, unreachable.message)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonClusterFormationStarted),
		"the cluster the API server holds has formed")
}

// The #444 messages of a formed cluster short of a server carried the
// formation check's "Cluster formation in progress" while the phase read
// Ready, and during a pod's termination window "3 of 3 servers available.
// Waiting for every server pod to be updated…". They say what Neo4j and
// Kubernetes report.
func TestReconcileServerShortfall_OutageMessagesSayWhatTheServersReport(t *testing.T) {
	ctx := context.Background()
	ready := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		return availCluster(neo4jv1beta1.PhaseReady, cond(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReady))
	}
	pods := func(t *testing.T, r *Neo4jEnterpriseClusterReconciler, notReady int, terminating int) {
		t.Helper()
		for i := range 3 {
			pod := serverPod("c-server-"+strconv.Itoa(i), "u"+strconv.Itoa(i), i != notReady, 0)
			if i == terminating {
				pod.Finalizers = []string{"test/hold"}
			}
			require.NoError(t, r.Create(ctx, &pod))
			if i == terminating {
				require.NoError(t, r.Delete(ctx, &pod))
			}
		}
	}

	t.Run("a server down", func(t *testing.T) {
		r, _ := shortfallReconciler(t, ready())
		pods(t, r, 2, -1)
		f := formationCheck{message: "Cluster formation in progress: 2/3 servers visible", servers: availServers(3, 2)}
		r.reconcileServerShortfall(ctx, refetch(t, r), f, f.message)

		c := refetch(t, r)
		assert.Equal(t, neo4jv1beta1.PhaseReady, c.Status.Phase)
		want := "2 of 3 servers available; unavailable: c-server-2 (Unavailable)"
		assert.Equal(t, "Neo4j cluster is serving. "+want, c.Status.Message)
		degraded := findCondition(c.Status.Conditions, ConditionTypeDegraded)
		require.NotNil(t, degraded)
		assert.Equal(t, want, degraded.Message)
		assert.NotContains(t, strings.ToLower(degraded.Message), "formation")
	})

	t.Run("a pod terminating that Neo4j still lists", func(t *testing.T) {
		r, _ := shortfallReconciler(t, ready())
		pods(t, r, -1, 1)
		f := formationCheck{message: "Cluster formation complete: 3/3 servers available", servers: availServers(3, 3)}
		r.reconcileServerShortfall(ctx, refetch(t, r), f,
			"Waiting for every server pod to be updated and ready: 3/3 pods updated, 2/3 ready")

		c := refetch(t, r)
		assert.Equal(t, neo4jv1beta1.PhaseReady, c.Status.Phase)
		want := "Neo4j still lists 3 of 3 servers available; server pods not ready: c-server-1 (terminating)"
		assert.Equal(t, "Neo4j cluster is serving. "+want, c.Status.Message)
		assert.Equal(t, want, findCondition(c.Status.Conditions, ConditionTypeDegraded).Message)
	})

	t.Run("a cluster still forming keeps the formation check's words", func(t *testing.T) {
		r, _ := shortfallReconciler(t, availCluster(neo4jv1beta1.PhaseForming))
		f := formationCheck{message: "Cluster formation in progress: 2/3 servers visible", servers: availServers(3, 2)}
		r.reconcileServerShortfall(ctx, refetch(t, r), f, f.message)
		assert.Equal(t, f.message, refetch(t, r).Status.Message)
	})
}

func TestOutageDetail(t *testing.T) {
	oneDown := shortfallAssessment{Available: 2, Expected: 3, Unavailable: []string{"c-server-2 (Unavailable)"}}
	assert.Equal(t, "2 of 3 servers available; unavailable: c-server-2 (Unavailable)",
		outageDetail(oneDown, []string{"c-server-2 (not ready)"}), "Neo4j names the server; that is enough")
	allListed := shortfallAssessment{Available: 3, Expected: 3}
	assert.Equal(t, "Neo4j still lists 3 of 3 servers available; server pods not ready: c-server-0 (missing), c-server-1 (not ready)",
		outageDetail(allListed, []string{"c-server-0 (missing)", "c-server-1 (not ready)"}))
	assert.Equal(t, "3 of 3 servers available", outageDetail(allListed, nil))
}

// "Property sharding validation failed: property sharding version validation
// failed: …": the reconciler and the error it wrapped both said it (#549).
func TestClusterReconcile_PropertyShardingFailureSaysSoOnce(t *testing.T) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ps", Namespace: "default", Generation: 1},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
			Storage:                neo4jv1beta1.StorageSpec{Size: "1Gi"},
			PropertySharding:       &neo4jv1beta1.PropertyShardingSpec{Enabled: true},
		},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(cluster).
		WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(20)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, RequeueAfter: time.Minute}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	require.NoError(t, err)
	got := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cluster), got))
	assert.Equal(t, "Failed", got.Status.Phase)
	assert.Equal(t, "Property sharding validation failed: property sharding requires Neo4j 2025.12+ Enterprise, got 5.26.0-enterprise",
		got.Status.Message)
	events := eventsWithReason(drainEvents(rec), EventReasonPropertyShardingFailed)
	require.Len(t, events, 1)
	assert.Equal(t, 1, strings.Count(strings.ToLower(events[0]), "validation failed"), events[0])
}

// AuraFleetManagementPluginPatchFailed greeted every new cluster: its first
// pass merges the plugin before it writes the server StatefulSet (#531). The
// standalone's pass reads the StatefulSet it just created from a cache that
// has not seen it. Neither is a failure; the pass that finds the StatefulSet
// adds the plugin.
func TestReconcileAuraFleetManagement_NoStatefulSetYetIsNotAFailure(t *testing.T) {
	ctx := context.Background()
	scheme := auraTestScheme(t)
	plugins := func(t *testing.T, c client.Client, name string) string {
		t.Helper()
		sts := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: testNS, Name: name}, sts))
		for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
			if e.Name == "NEO4J_PLUGINS" {
				return e.Value
			}
		}
		return ""
	}

	t.Run("cluster", func(t *testing.T) {
		cl := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cl", Namespace: testNS},
			Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
		}
		c := newAuraFakeClient(t, scheme, cl)
		rec := record.NewFakeRecorder(20)
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}

		require.NoError(t, r.reconcileAuraFleetManagement(ctx, cl))
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonAuraFleetPluginPatchFailed))

		require.NoError(t, c.Create(ctx, fleetTestStatefulSet("cl-server")))
		require.NoError(t, r.reconcileAuraFleetManagement(ctx, cl))
		assert.Contains(t, plugins(t, c, "cl-server"), "fleet-management")
	})

	t.Run("standalone", func(t *testing.T) {
		sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: testNS},
			Spec:       neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
		}
		c := newAuraFakeClient(t, scheme, sa)
		rec := record.NewFakeRecorder(20)
		r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: rec}

		require.NoError(t, r.reconcileAuraFleetManagement(ctx, sa))
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonAuraFleetPluginPatchFailed))

		require.NoError(t, c.Create(ctx, fleetTestStatefulSet("sa")))
		require.NoError(t, r.reconcileAuraFleetManagement(ctx, sa))
		assert.Contains(t, plugins(t, c, "sa"), "fleet-management")
	})

	t.Run("a StatefulSet that cannot take the plugin is still a failure", func(t *testing.T) {
		cl := &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cl", Namespace: testNS},
			Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{AuraFleetManagement: fleetTokenRef("fleet-token")},
		}
		sts := fleetTestStatefulSet("cl-server")
		sts.Spec.Template.Spec.Containers[0].Name = "not-neo4j"
		c := newAuraFakeClient(t, scheme, cl, sts)
		rec := record.NewFakeRecorder(20)
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}

		require.NoError(t, r.reconcileAuraFleetManagement(ctx, cl))
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonAuraFleetPluginPatchFailed), 1)
	})
}

// ClusterFormationFailed was documented and never emitted: the formation check
// it was raised from returned an error on no path (#531). The reason is gone;
// the events guide must list only reasons the operator has.
func TestEventsGuide_ListsOnlyReasonsTheOperatorHas(t *testing.T) {
	values := eventReasonValues(t)
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "user_guide", "guides", "kubernetes-events.md"))
	require.NoError(t, err)

	firstCell := regexp.MustCompile(`^\| ([^|]+) \|`)
	name := regexp.MustCompile("`([A-Za-z]+)`")
	var documented int
	for _, line := range strings.Split(string(doc), "\n") {
		m := firstCell.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		for _, reason := range name.FindAllStringSubmatch(m[1], -1) {
			documented++
			assert.True(t, values[reason[1]], "the events guide lists %q, which no constant in events.go names", reason[1])
		}
	}
	require.Greater(t, documented, 100, "the guide's tables were read")
	assert.False(t, values["ClusterFormationFailed"], "nothing emits ClusterFormationFailed")
}

// eventReasonValues is every string constant events.go declares.
func eventReasonValues(t *testing.T) map[string]bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "events.go", nil, 0)
	require.NoError(t, err)
	values := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if ok && lit.Kind == token.STRING {
			if v, err := strconv.Unquote(lit.Value); err == nil {
				values[v] = true
			}
		}
		return true
	})
	return values
}
