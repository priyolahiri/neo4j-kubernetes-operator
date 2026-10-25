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

// #565: the shape #559 fixed for users, roles, bindings and auth rules (knowledge
// rule 158) was still in the database kinds. A Neo4jDatabase,
// Neo4jCompositeDatabase, Neo4jDatabaseAlias or Neo4jReplicaDatabase whose
// target restarted went Pending, and the pass that found the target back moved
// it into Ready again and announced DatabaseReady (CompositeDatabaseReady,
// AliasReady, ReplicaReady); a Neo4jShardedDatabase wrote Waiting over Ready on
// every rollout of its cluster and announced ShardedDatabaseReady after it. The
// composite, alias and replica also announced from the pass's own copy. Each
// now stays Ready (Replicating) with ClusterNotReady=True while its target is
// away, and announces Ready once, from the write that enters it (knowledge
// rule 183).

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// staleOnce serves stale for the next Get of its key and type, once, as an
// informer cache that has not caught up with the last write would.
type staleOnce struct {
	client.Client
	stale client.Object
}

func (s *staleOnce) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if s.stale != nil && key == client.ObjectKeyFromObject(s.stale) && reflect.TypeOf(obj) == reflect.TypeOf(s.stale) {
		reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(s.stale.DeepCopyObject()).Elem())
		s.stale = nil
		return nil
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

// errRefusedBolt is what connecting to a target that reads Ready but does not
// accept connections yet returns.
var errRefusedBolt = errors.New("ConnectivityError: dial tcp 10.0.0.7:7687: connect: connection refused")

// targetWaitCase is one dependent kind behind a scripted target.
type targetWaitCase struct {
	readyReason string // the event that announces the kind's Ready
	readyPhase  string // the phase it is Ready in
	reconcile   func() error
	restarted   func() error // a pass by a new reconciler on the same API server
	setReady    func(bool)   // the target's readiness, as its own controller writes it
	refuse      func(error)  // what connecting to the target returns (nil: it answers)
	staleNext   func()       // the next pass reads the object as it was created
	status      func() (phase string, conditions []metav1.Condition)
	events      func() []string
	// backdateWait moves the start of the recorded target wait to before
	// targetUnreachableGrace (Neo4jDatabase only).
	backdateWait func()
}

func clusterNotReadyCondition(t *testing.T, conditions []metav1.Condition) *metav1.Condition {
	t.Helper()
	c := meta.FindStatusCondition(conditions, ConditionTypeClusterNotReady)
	require.NotNil(t, c, "ClusterNotReady is recorded")
	return c
}

// checkATargetRestartIsNotATransition: Ready and announced once; three
// restarts of the target keep it Ready with ClusterNotReady=True and announce
// nothing; a pass on a copy from before the first write, and a restarted
// operator, announce nothing either.
func checkATargetRestartIsNotATransition(t *testing.T, tc targetWaitCase) {
	t.Helper()
	require.NoError(t, tc.reconcile())
	require.Len(t, eventsWithReason(tc.events(), tc.readyReason), 1, "the first Ready is announced")
	phase, _ := tc.status()
	require.Equal(t, tc.readyPhase, phase)

	for range targetRestarts {
		tc.setReady(false)
		require.NoError(t, tc.reconcile())
		phase, conditions := tc.status()
		assert.Equal(t, tc.readyPhase, phase, "a Ready object stays Ready while its target restarts")
		ready := meta.FindStatusCondition(conditions, ConditionTypeReady)
		require.NotNil(t, ready)
		assert.Equal(t, metav1.ConditionTrue, ready.Status)
		c := clusterNotReadyCondition(t, conditions)
		assert.Equal(t, metav1.ConditionTrue, c.Status, "ClusterNotReady says it waits")
		assert.Equal(t, ConditionReasonClusterNotReady, c.Reason)

		tc.setReady(true)
		require.NoError(t, tc.reconcile())
		_, conditions = tc.status()
		assert.Equal(t, metav1.ConditionFalse, clusterNotReadyCondition(t, conditions).Status)
	}
	assert.Empty(t, eventsWithReason(tc.events(), tc.readyReason), "a target restart is not a transition")

	tc.staleNext()
	require.NoError(t, tc.reconcile())
	assert.Empty(t, eventsWithReason(tc.events(), tc.readyReason), "announced from the pass's copy")

	require.NoError(t, tc.restarted())
	assert.Empty(t, eventsWithReason(tc.events(), tc.readyReason), "a restarted operator stays quiet")
}

// checkATargetThatRefusesBoltIsAnOutage: the target reads Ready but refuses
// connections — a standalone a few seconds after it restarts. The object stays
// Ready with ClusterNotReady=True, reason ClusterUnreachable; the pass returns
// no error and emits nothing; Ready is not announced when the target answers.
func checkATargetThatRefusesBoltIsAnOutage(t *testing.T, tc targetWaitCase) {
	t.Helper()
	require.NoError(t, tc.reconcile())
	require.Len(t, eventsWithReason(tc.events(), tc.readyReason), 1)

	for range targetRestarts {
		tc.refuse(errRefusedBolt)
		require.NoError(t, tc.reconcile(), "an outage is waited through, not returned as an error")
		phase, conditions := tc.status()
		assert.Equal(t, tc.readyPhase, phase)
		c := clusterNotReadyCondition(t, conditions)
		assert.Equal(t, metav1.ConditionTrue, c.Status)
		assert.Equal(t, ConditionReasonClusterUnreachable, c.Reason)
		assert.Empty(t, tc.events(), "no event while the target restarts")

		tc.refuse(nil)
		require.NoError(t, tc.reconcile())
	}
	assert.Empty(t, eventsWithReason(tc.events(), tc.readyReason))
}

func databaseTargetWaitCase(t *testing.T) targetWaitCase {
	t.Helper()
	ctx := context.Background()
	server := &databaseServer{}
	db := &neo4jv1beta1.Neo4jDatabase{
		ObjectMeta: repeatedWarningsMeta("orders", DatabaseFinalizer),
		Spec:       neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "sa", Name: "orders"},
	}
	base := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(db.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(db, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	c := &staleOnce{Client: base}
	rec := record.NewFakeRecorder(100)
	var refuse error
	newReconciler := func() *Neo4jDatabaseReconciler {
		return &Neo4jDatabaseReconciler{Client: c, Scheme: base.Scheme(), Recorder: rec,
			newNeo4jClient: func(*neo4jv1beta1.Neo4jEnterpriseCluster, *neo4jv1beta1.Neo4jEnterpriseStandalone) (databaseNeo4jClient, error) {
				if refuse != nil {
					return nil, refuse
				}
				return server, nil
			}}
	}
	r := newReconciler()
	key := client.ObjectKeyFromObject(db)
	return targetWaitCase{
		readyReason: EventReasonDatabaseReady, readyPhase: neo4jv1beta1.PhaseReady,
		reconcile: func() error { return reconcileTimes(t, r, key, 1)[0] },
		restarted: func() error { return reconcileTimes(t, newReconciler(), key, 1)[0] },
		setReady:  func(ready bool) { setStandaloneReady(t, base, ready) },
		refuse:    func(err error) { refuse = err },
		staleNext: func() { c.stale = db.DeepCopy() },
		status: func() (string, []metav1.Condition) {
			got := &neo4jv1beta1.Neo4jDatabase{}
			require.NoError(t, base.Get(ctx, key, got))
			return got.Status.Phase, got.Status.Conditions
		},
		events: func() []string { return drainEvents(rec) },
		backdateWait: func() {
			got := &neo4jv1beta1.Neo4jDatabase{}
			require.NoError(t, base.Get(ctx, key, got))
			c := findCondition(got.Status.Conditions, ConditionTypeClusterNotReady)
			require.NotNil(t, c)
			c.LastTransitionTime = metav1.NewTime(time.Now().Add(-targetUnreachableGrace - time.Minute))
			require.NoError(t, base.Status().Update(ctx, got))
		},
	}
}

// compositeServer is a scripted Neo4j for the Neo4jCompositeDatabase reconcile:
// one composite with no constituents.
type compositeServer struct{ exists bool }

func (s *compositeServer) ShowCompositeDatabase(_ context.Context, name string) (*neo4jclient.CompositeInfo, error) {
	if !s.exists {
		return nil, nil
	}
	return &neo4jclient.CompositeInfo{Name: name, CurrentStatus: "online", RequestedStatus: "online"}, nil
}

func (s *compositeServer) ShowAliases(context.Context) ([]neo4jclient.AliasInfo, error) {
	return nil, nil
}

func (s *compositeServer) GetDatabaseInfo(context.Context, string) (*neo4jclient.DatabaseInfo, error) {
	return nil, nil
}

func (s *compositeServer) CreateCompositeDatabase(context.Context, string, bool, bool, string) error {
	s.exists = true
	return nil
}

func (s *compositeServer) AlterCompositeDatabaseLanguage(context.Context, string, string) error {
	return nil
}

func (s *compositeServer) CreateCompositeConstituent(context.Context, string, string, string) error {
	return nil
}

func (s *compositeServer) AlterCompositeConstituent(context.Context, string, string, string) error {
	return nil
}

func (s *compositeServer) DropCompositeConstituent(context.Context, string, string) error { return nil }

func (s *compositeServer) CreateRemoteConstituent(context.Context, string, string, string, string, neo4jclient.RemoteConstituentAuth) error {
	return nil
}

func (s *compositeServer) AlterRemoteConstituent(context.Context, string, string, string, string, neo4jclient.RemoteConstituentAuth) error {
	return nil
}

func (s *compositeServer) Close() error { return nil }

func compositeTargetWaitCase(t *testing.T) targetWaitCase {
	t.Helper()
	ctx := context.Background()
	server := &compositeServer{}
	cd := &neo4jv1beta1.Neo4jCompositeDatabase{
		ObjectMeta: repeatedWarningsMeta("sales", Neo4jCompositeDatabaseFinalizer),
		Spec:       neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "sa", Name: "sales"},
	}
	base := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(cd.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(cd, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	c := &staleOnce{Client: base}
	rec := record.NewFakeRecorder(100)
	var refuse error
	newReconciler := func() *Neo4jCompositeDatabaseReconciler {
		return &Neo4jCompositeDatabaseReconciler{Client: c, Scheme: base.Scheme(), Recorder: rec,
			newNeo4jClient: func(ResolvedTarget) (compositeNeo4jClient, error) {
				if refuse != nil {
					return nil, refuse
				}
				return server, nil
			}}
	}
	r := newReconciler()
	key := client.ObjectKeyFromObject(cd)
	return targetWaitCase{
		readyReason: EventReasonCompositeReady, readyPhase: neo4jv1beta1.PhaseReady,
		reconcile: func() error { return reconcileTimes(t, r, key, 1)[0] },
		restarted: func() error { return reconcileTimes(t, newReconciler(), key, 1)[0] },
		setReady:  func(ready bool) { setStandaloneReady(t, base, ready) },
		refuse:    func(err error) { refuse = err },
		staleNext: func() { c.stale = cd.DeepCopy() },
		status: func() (string, []metav1.Condition) {
			got := &neo4jv1beta1.Neo4jCompositeDatabase{}
			require.NoError(t, base.Get(ctx, key, got))
			return got.Status.Phase, got.Status.Conditions
		},
		events: func() []string { return drainEvents(rec) },
	}
}

// aliasServer is a scripted Neo4j for the Neo4jDatabaseAlias reconcile: every
// target database exists, and one alias.
type aliasServer struct{ alias *neo4jclient.AliasInfo }

func (s *aliasServer) GetDatabaseInfo(_ context.Context, name string) (*neo4jclient.DatabaseInfo, error) {
	return &neo4jclient.DatabaseInfo{Name: name}, nil
}

func (s *aliasServer) ShowAlias(context.Context, string) (*neo4jclient.AliasInfo, error) {
	return s.alias, nil
}

func (s *aliasServer) CreateAlias(_ context.Context, name, target string) error {
	s.alias = &neo4jclient.AliasInfo{Name: name, Database: target}
	return nil
}

func (s *aliasServer) AlterAliasTarget(_ context.Context, name, target string) error {
	s.alias = &neo4jclient.AliasInfo{Name: name, Database: target}
	return nil
}

func (s *aliasServer) Close() error { return nil }

func aliasTargetWaitCase(t *testing.T) targetWaitCase {
	t.Helper()
	ctx := context.Background()
	server := &aliasServer{}
	alias := &neo4jv1beta1.Neo4jDatabaseAlias{
		ObjectMeta: repeatedWarningsMeta("latest", Neo4jDatabaseAliasFinalizer),
		Spec:       neo4jv1beta1.Neo4jDatabaseAliasSpec{ClusterRef: "sa", Name: "latest", TargetDatabase: "orders"},
	}
	base := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(alias.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(alias, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	c := &staleOnce{Client: base}
	rec := record.NewFakeRecorder(100)
	var refuse error
	newReconciler := func() *Neo4jDatabaseAliasReconciler {
		return &Neo4jDatabaseAliasReconciler{Client: c, Scheme: base.Scheme(), Recorder: rec,
			newNeo4jClient: func(ResolvedTarget) (aliasNeo4jClient, error) {
				if refuse != nil {
					return nil, refuse
				}
				return server, nil
			}}
	}
	r := newReconciler()
	key := client.ObjectKeyFromObject(alias)
	return targetWaitCase{
		readyReason: EventReasonAliasReady, readyPhase: neo4jv1beta1.PhaseReady,
		reconcile: func() error { return reconcileTimes(t, r, key, 1)[0] },
		restarted: func() error { return reconcileTimes(t, newReconciler(), key, 1)[0] },
		setReady:  func(ready bool) { setStandaloneReady(t, base, ready) },
		refuse:    func(err error) { refuse = err },
		staleNext: func() { c.stale = alias.DeepCopy() },
		status: func() (string, []metav1.Condition) {
			got := &neo4jv1beta1.Neo4jDatabaseAlias{}
			require.NoError(t, base.Get(ctx, key, got))
			return got.Status.Phase, got.Status.Conditions
		},
		events: func() []string { return drainEvents(rec) },
	}
}

func replicaTargetWaitCase(t *testing.T, ns string) targetWaitCase {
	t.Helper()
	h := newLagHarness(t, ns)
	h.server.replicating(lagTestDB, 3)
	created := &neo4jv1beta1.Neo4jReplicaDatabase{}
	require.NoError(t, h.c.Get(context.Background(), client.ObjectKey{Namespace: h.ns, Name: h.name}, created))
	c := &staleOnce{Client: h.c}
	h.r.Client = c
	rec, ok := h.r.Recorder.(*record.FakeRecorder)
	require.True(t, ok)
	return targetWaitCase{
		readyReason: EventReasonReplicaReady, readyPhase: neo4jv1beta1.ReplicaPhaseReplicating,
		reconcile: h.reconcile,
		restarted: func() error {
			saved := h.r
			defer func() { h.r = saved }()
			h.r = &Neo4jReplicaDatabaseReconciler{Client: c, Recorder: rec, newNeo4jClient: saved.newNeo4jClient}
			return h.reconcile()
		},
		setReady:  h.setClusterReady,
		refuse:    func(err error) { h.connErr = err },
		staleNext: func() { c.stale = created.DeepCopy() },
		status: func() (string, []metav1.Condition) {
			got := &neo4jv1beta1.Neo4jReplicaDatabase{}
			require.NoError(t, h.c.Get(context.Background(), client.ObjectKey{Namespace: h.ns, Name: h.name}, got))
			return got.Status.Phase, got.Status.Conditions
		},
		events: func() []string { return drainEvents(rec) },
	}
}

// setShardingClusterReady moves the sharding cluster between Ready and the
// Forming a rollout puts it in.
func setShardingClusterReady(t *testing.T, c client.Client, ready bool) {
	t.Helper()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "sc"}, cluster))
	cluster.Status.Phase = neo4jv1beta1.PhaseReady
	if !ready {
		cluster.Status.Phase = neo4jv1beta1.PhaseForming
	}
	require.NoError(t, c.Status().Update(context.Background(), cluster))
}

func shardedTargetWaitCase(t *testing.T) targetWaitCase {
	t.Helper()
	ctx := context.Background()
	server := &shardedServer{name: "products"}
	sd := productsShardedDB()
	r, rec := shardedTestReconciler(t, server, shardingCluster(), sd)
	base := r.Client
	var refuse error
	r.newNeo4jClient = func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) {
		if refuse != nil {
			return nil, refuse
		}
		return server, nil
	}
	// As in production, the pass reads its object from the API server
	// (APIReader); the stale copy then reaches only the status writers'
	// re-reads, which a conflict sends back to the API server.
	c := &staleOnce{Client: base}
	r.Client = c
	r.APIReader = base
	key := client.ObjectKeyFromObject(sd)
	return targetWaitCase{
		readyReason: EventReasonShardedDatabaseReady, readyPhase: neo4jv1beta1.PhaseReady,
		reconcile: func() error { return reconcileSharded(t, r, sd) },
		restarted: func() error {
			again := &Neo4jShardedDatabaseReconciler{Client: c, Scheme: r.Scheme, Recorder: rec,
				APIReader: base, newNeo4jClient: r.newNeo4jClient}
			return reconcileSharded(t, again, sd)
		},
		setReady:  func(ready bool) { setShardingClusterReady(t, base, ready) },
		refuse:    func(err error) { refuse = err },
		staleNext: func() { c.stale = sd.DeepCopy() },
		status: func() (string, []metav1.Condition) {
			got := &neo4jv1beta1.Neo4jShardedDatabase{}
			require.NoError(t, base.Get(ctx, key, got))
			return got.Status.Phase, got.Status.Conditions
		},
		events: func() []string { return drainEvents(rec) },
	}
}

func TestDatabaseKinds_ATargetRestartIsNotATransition(t *testing.T) {
	t.Run("Neo4jDatabase", func(t *testing.T) { checkATargetRestartIsNotATransition(t, databaseTargetWaitCase(t)) })
	t.Run("Neo4jCompositeDatabase", func(t *testing.T) { checkATargetRestartIsNotATransition(t, compositeTargetWaitCase(t)) })
	t.Run("Neo4jDatabaseAlias", func(t *testing.T) { checkATargetRestartIsNotATransition(t, aliasTargetWaitCase(t)) })
	t.Run("Neo4jReplicaDatabase", func(t *testing.T) {
		checkATargetRestartIsNotATransition(t, replicaTargetWaitCase(t, "tw-replica-restart"))
	})
	t.Run("Neo4jShardedDatabase", func(t *testing.T) { checkATargetRestartIsNotATransition(t, shardedTargetWaitCase(t)) })
}

func TestDatabaseKinds_ATargetThatRefusesBoltIsAnOutage(t *testing.T) {
	t.Run("Neo4jDatabase", func(t *testing.T) { checkATargetThatRefusesBoltIsAnOutage(t, databaseTargetWaitCase(t)) })
	t.Run("Neo4jCompositeDatabase", func(t *testing.T) { checkATargetThatRefusesBoltIsAnOutage(t, compositeTargetWaitCase(t)) })
	t.Run("Neo4jDatabaseAlias", func(t *testing.T) { checkATargetThatRefusesBoltIsAnOutage(t, aliasTargetWaitCase(t)) })
	t.Run("Neo4jReplicaDatabase", func(t *testing.T) {
		checkATargetThatRefusesBoltIsAnOutage(t, replicaTargetWaitCase(t, "tw-replica-refuse"))
	})
	t.Run("Neo4jShardedDatabase", func(t *testing.T) { checkATargetThatRefusesBoltIsAnOutage(t, shardedTargetWaitCase(t)) })
}

// An object not yet Ready still waits in its kind's waiting phase while the
// target is away: only Ready at its generation is kept.
func TestDatabaseKinds_AnObjectNotYetReadyStillWaits(t *testing.T) {
	cases := map[string]struct {
		tc      targetWaitCase
		waiting string
	}{
		"Neo4jDatabase":          {databaseTargetWaitCase(t), neo4jv1beta1.PhasePending},
		"Neo4jCompositeDatabase": {compositeTargetWaitCase(t), neo4jv1beta1.PhasePending},
		"Neo4jDatabaseAlias":     {aliasTargetWaitCase(t), neo4jv1beta1.PhasePending},
		"Neo4jReplicaDatabase":   {replicaTargetWaitCase(t, "tw-replica-new"), neo4jv1beta1.ReplicaPhasePending},
		"Neo4jShardedDatabase":   {shardedTargetWaitCase(t), neo4jv1beta1.PhaseWaiting},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			c.tc.setReady(false)
			require.NoError(t, c.tc.reconcile())
			phase, conditions := c.tc.status()
			assert.Equal(t, c.waiting, phase)
			assert.Equal(t, metav1.ConditionTrue, clusterNotReadyCondition(t, conditions).Status)

			c.tc.setReady(true)
			require.NoError(t, c.tc.reconcile())
			phase, _ = c.tc.status()
			assert.Equal(t, c.tc.readyPhase, phase)
			assert.Len(t, eventsWithReason(c.tc.events(), c.tc.readyReason), 1, "its first Ready is announced")
		})
	}
}

// The pass now writes ClusterNotReady=False after it connects, before the
// data-import marker, which is written from the pass's copy. Unless that copy
// takes the first write, the marker's write conflicts and the next pass
// imports the initial data a second time.
func TestDatabaseReconcile_InitialDataIsImportedOnceAfterTheHostAnswered(t *testing.T) {
	server := &databaseServer{}
	db := ordersDatabase("orders")
	db.Spec.InitialData = &neo4jv1beta1.InitialDataSpec{Source: "cypher",
		CypherStatements: []string{"CREATE (:Marker {at: 'import'})"}}
	r, _ := databaseTestReconciler(t, server, readyDatabaseHost(), db)
	key := client.ObjectKeyFromObject(db)

	for _, err := range reconcileTimes(t, r, key, 3) {
		require.NoError(t, err)
	}
	imports := 0
	for _, s := range server.statements {
		if s == "CREATE (:Marker {at: 'import'})" {
			imports++
		}
	}
	assert.Equal(t, 1, imports, "%q", server.statements)
	got := &neo4jv1beta1.Neo4jDatabase{}
	require.NoError(t, r.Get(context.Background(), key, got))
	require.NotNil(t, got.Status.DataImported)
	assert.Equal(t, metav1.ConditionFalse, clusterNotReadyCondition(t, got.Status.Conditions).Status)
}

// An outage that outlasts targetUnreachableGrace is the database's own
// failure, announced once; the pass that reaches the host again announces
// Ready, a return from that failure.
func TestDatabaseReconcile_AnUnreachableHostIsReportedAfterTheGrace(t *testing.T) {
	tc := databaseTargetWaitCase(t)
	require.NoError(t, tc.reconcile())
	tc.events()

	tc.refuse(errRefusedBolt)
	require.NoError(t, tc.reconcile())
	phase, _ := tc.status()
	require.Equal(t, neo4jv1beta1.PhaseReady, phase, "within the grace the database stays Ready")

	tc.backdateWait() // the outage began longer ago than the grace
	for range 3 {
		require.NoError(t, tc.reconcile())
	}
	phase, _ = tc.status()
	assert.Equal(t, neo4jv1beta1.PhaseFailed, phase)
	assert.Len(t, eventsWithReason(tc.events(), EventReasonConnectionFailed), 1, "announced once")

	tc.refuse(nil)
	require.NoError(t, tc.reconcile())
	phase, _ = tc.status()
	assert.Equal(t, neo4jv1beta1.PhaseReady, phase)
	assert.Len(t, eventsWithReason(tc.events(), EventReasonDatabaseReady), 1, "back from its own failure: announced")
}
