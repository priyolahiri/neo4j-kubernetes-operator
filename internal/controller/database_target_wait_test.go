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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-logr/logr"
	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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
	// failStatements makes the kind's first statement of a pass answer err
	// (nil: it answers), with the connection itself made.
	failStatements func(error)
	// errorLogs returns the ERROR lines the passes logged.
	errorLogs func() []string
}

// backdateWaitOf moves the ClusterNotReady condition of the object at key to
// before targetUnreachableGrace, as an outage that began long ago left it.
func backdateWaitOf[T client.Object](t *testing.T, c client.Client, key client.ObjectKey, obj T, conditions func(T) []metav1.Condition) func() {
	return func() {
		t.Helper()
		require.NoError(t, c.Get(context.Background(), key, obj))
		cond := findCondition(conditions(obj), ConditionTypeClusterNotReady)
		require.NotNil(t, cond, "the wait is recorded")
		cond.LastTransitionTime = metav1.NewTime(time.Now().Add(-targetUnreachableGrace - time.Minute))
		require.NoError(t, c.Status().Update(context.Background(), obj))
	}
}

// loggedPass reconciles key once with a context whose logger is sink.
func loggedPass(sink levelCaptureSink, r reconcile.Reconciler, key client.ObjectKey) error {
	_, err := r.Reconcile(log.IntoContext(context.Background(), logr.New(sink)), ctrl.Request{NamespacedName: key})
	return err
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
	sink := newLevelCaptureSink()
	return targetWaitCase{
		failStatements: func(err error) { server.err = err },
		errorLogs:      sink.errors,
		readyReason:    EventReasonDatabaseReady, readyPhase: neo4jv1beta1.PhaseReady,
		reconcile: func() error { return loggedPass(sink, r, key) },
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
		backdateWait: backdateWaitOf(t, base, key, &neo4jv1beta1.Neo4jDatabase{},
			func(o *neo4jv1beta1.Neo4jDatabase) []metav1.Condition { return o.Status.Conditions }),
	}
}

// compositeServer is a scripted Neo4j for the Neo4jCompositeDatabase reconcile:
// one composite with no constituents.
type compositeServer struct {
	exists bool
	err    error // what every SHOW DATABASE answers, when set
}

func (s *compositeServer) ShowCompositeDatabase(_ context.Context, name string) (*neo4jclient.CompositeInfo, error) {
	if s.err != nil {
		return nil, s.err
	}
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
	sink := newLevelCaptureSink()
	return targetWaitCase{
		failStatements: func(err error) { server.err = err },
		errorLogs:      sink.errors,
		readyReason:    EventReasonCompositeReady, readyPhase: neo4jv1beta1.PhaseReady,
		reconcile: func() error { return loggedPass(sink, r, key) },
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
		backdateWait: backdateWaitOf(t, base, key, &neo4jv1beta1.Neo4jCompositeDatabase{},
			func(o *neo4jv1beta1.Neo4jCompositeDatabase) []metav1.Condition { return o.Status.Conditions }),
	}
}

// aliasServer is a scripted Neo4j for the Neo4jDatabaseAlias reconcile: every
// target database exists, and one alias.
type aliasServer struct {
	alias     *neo4jclient.AliasInfo
	err       error // what every SHOW DATABASE answers, when set
	noTargets bool  // no target database exists
}

func (s *aliasServer) GetDatabaseInfo(_ context.Context, name string) (*neo4jclient.DatabaseInfo, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.noTargets {
		return nil, nil
	}
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
	sink := newLevelCaptureSink()
	return targetWaitCase{
		failStatements: func(err error) { server.err = err },
		errorLogs:      sink.errors,
		readyReason:    EventReasonAliasReady, readyPhase: neo4jv1beta1.PhaseReady,
		reconcile: func() error { return loggedPass(sink, r, key) },
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
		backdateWait: backdateWaitOf(t, base, key, &neo4jv1beta1.Neo4jDatabaseAlias{},
			func(o *neo4jv1beta1.Neo4jDatabaseAlias) []metav1.Condition { return o.Status.Conditions }),
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
	sink := newLevelCaptureSink()
	key := client.ObjectKey{Namespace: h.ns, Name: h.name}
	return targetWaitCase{
		failStatements: func(err error) {
			h.server.infoErr = err
			if err == nil {
				h.server.replicating(lagTestDB, 3)
			}
		},
		errorLogs:   sink.errors,
		readyReason: EventReasonReplicaReady, readyPhase: neo4jv1beta1.ReplicaPhaseReplicating,
		reconcile: func() error { return loggedPass(sink, h.r, key) },
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
		backdateWait: backdateWaitOf(t, h.c, key, &neo4jv1beta1.Neo4jReplicaDatabase{},
			func(o *neo4jv1beta1.Neo4jReplicaDatabase) []metav1.Condition { return o.Status.Conditions }),
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
	sink := newLevelCaptureSink()
	return targetWaitCase{
		failStatements: func(err error) { server.err = err },
		errorLogs:      sink.errors,
		readyReason:    EventReasonShardedDatabaseReady, readyPhase: neo4jv1beta1.PhaseReady,
		reconcile: func() error { return loggedPass(sink, r, key) },
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
		backdateWait: backdateWaitOf(t, base, key, &neo4jv1beta1.Neo4jShardedDatabase{},
			func(o *neo4jv1beta1.Neo4jShardedDatabase) []metav1.Condition { return o.Status.Conditions }),
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

// errLostMajority is what a cluster that has lost the majority of its servers,
// and still reads Ready, answers a statement (the v1.23 pre-release journey,
// on 2026.08.1 and 5.26.21).
var errLostMajority = &neo4jdriver.Neo4jError{Code: "Neo.TransientError.Statement.RemoteExecutionTransientError",
	Msg: "Failed to obtain connection towards READ server"}

// errNotALeader is a statement Neo4j asks to be retried whose code is not a
// Neo.TransientError.
var errNotALeader = &neo4jdriver.Neo4jError{Code: "Neo.ClientError.Cluster.NotALeader",
	Msg: "No write operations are allowed directly on this database"}

// passBound is the longest a pass against a scripted server may take: the
// sharded database's readiness loop once held the worker thirty seconds a pass
// while its cluster could not serve it.
const passBound = 5 * time.Second

// checkATransientIsAWait: the target reads Ready, the connection is made, and
// the kind's statements answer an error Neo4j asks to be retried. The object
// stays Ready with ClusterNotReady=True (ClusterUnreachable); the pass returns
// no error, logs no ERROR and emits nothing; Ready is not announced when the
// statements answer again.
func checkATransientIsAWait(t *testing.T, tc targetWaitCase, transient error) {
	t.Helper()
	require.NoError(t, tc.reconcile())
	require.Len(t, eventsWithReason(tc.events(), tc.readyReason), 1)

	for range targetRestarts {
		tc.failStatements(transient)
		for range 3 {
			start := time.Now()
			require.NoError(t, tc.reconcile(), "a transient is waited out, not returned as an error")
			assert.Less(t, time.Since(start), passBound, "the wait is a requeue, not a pass that holds the worker")
		}
		phase, conditions := tc.status()
		assert.Equal(t, tc.readyPhase, phase, "a Ready object stays Ready")
		ready := meta.FindStatusCondition(conditions, ConditionTypeReady)
		require.NotNil(t, ready)
		assert.Equal(t, metav1.ConditionTrue, ready.Status)
		c := clusterNotReadyCondition(t, conditions)
		assert.Equal(t, metav1.ConditionTrue, c.Status)
		assert.Equal(t, ConditionReasonClusterUnreachable, c.Reason)
		assert.Empty(t, tc.events(), "no Warning and no event while the cluster cannot serve")

		tc.failStatements(nil)
		require.NoError(t, tc.reconcile())
		_, conditions = tc.status()
		assert.Equal(t, metav1.ConditionFalse, clusterNotReadyCondition(t, conditions).Status)
		assert.Empty(t, tc.events(), "the recovery is not a transition")
	}
	assert.Empty(t, tc.errorLogs(), "a cluster that lost its majority is not an ERROR")
}

// The v1.23 pre-release journey: a cluster that lost two of its three
// servers still read Ready, and every database, alias and composite on it
// logged an ERROR about ten times a second, emitted CreationFailed,
// AliasFailed or CompositeDatabaseFailed in bursts, and announced its Ready
// again after recovery (knowledge rule 210).
func TestDatabaseKinds_ATransientFromAClusterThatLostItsMajorityIsAWait(t *testing.T) {
	t.Run("Neo4jDatabase", func(t *testing.T) { checkATransientIsAWait(t, databaseTargetWaitCase(t), errLostMajority) })
	t.Run("Neo4jCompositeDatabase", func(t *testing.T) {
		checkATransientIsAWait(t, compositeTargetWaitCase(t), errLostMajority)
	})
	t.Run("Neo4jDatabaseAlias", func(t *testing.T) { checkATransientIsAWait(t, aliasTargetWaitCase(t), errLostMajority) })
	t.Run("Neo4jReplicaDatabase", func(t *testing.T) {
		checkATransientIsAWait(t, replicaTargetWaitCase(t, "tw-replica-transient"), errLostMajority)
	})
	t.Run("Neo4jShardedDatabase", func(t *testing.T) { checkATransientIsAWait(t, shardedTargetWaitCase(t), errLostMajority) })
	t.Run("Neo4jShardedDatabase/NotALeader", func(t *testing.T) {
		checkATransientIsAWait(t, shardedTargetWaitCase(t), errNotALeader)
	})
}

// checkATransientThatLastsIsReported: a transient that outlasts
// targetUnreachableGrace is the object's failure, announced once with the
// kind's reason, as an unreachable target's is.
func checkATransientThatLastsIsReported(t *testing.T, tc targetWaitCase, transient error, reason string) {
	t.Helper()
	require.NoError(t, tc.reconcile())
	tc.events()

	tc.failStatements(transient)
	require.NoError(t, tc.reconcile())
	phase, _ := tc.status()
	require.Equal(t, tc.readyPhase, phase, "within the grace the object stays Ready")
	tc.backdateWait()
	for range 3 {
		start := time.Now()
		require.NoError(t, tc.reconcile())
		assert.Less(t, time.Since(start), passBound, "a failed object waits by requeue too")
	}
	phase, _ = tc.status()
	assert.Equal(t, "Failed", phase)
	events := tc.events()
	assert.Len(t, eventsWithReason(events, reason), 1, "announced once: %v", events)
}

func TestDatabaseKinds_ATransientThatLastsIsReportedAfterTheGrace(t *testing.T) {
	t.Run("Neo4jDatabase", func(t *testing.T) {
		checkATransientThatLastsIsReported(t, databaseTargetWaitCase(t), errLostMajority, EventReasonConnectionFailed)
	})
	t.Run("Neo4jCompositeDatabase", func(t *testing.T) {
		checkATransientThatLastsIsReported(t, compositeTargetWaitCase(t), errLostMajority, EventReasonConnectionFailed)
	})
	t.Run("Neo4jDatabaseAlias", func(t *testing.T) {
		checkATransientThatLastsIsReported(t, aliasTargetWaitCase(t), errLostMajority, EventReasonConnectionFailed)
	})
	t.Run("Neo4jReplicaDatabase", func(t *testing.T) {
		checkATransientThatLastsIsReported(t, replicaTargetWaitCase(t, "tw-replica-lasting"), errLostMajority, EventReasonConnectionFailed)
	})
	t.Run("Neo4jShardedDatabase", func(t *testing.T) {
		checkATransientThatLastsIsReported(t, shardedTargetWaitCase(t), errLostMajority, EventReasonClientCreationFailed)
	})
}

// A pass whose statements answered and that then waits on something other
// than the target — an alias whose target database does not exist yet — clears
// the target wait: the target served it. Written after the connection, as
// before rule 210, this came for free; written only before the Ready write, a
// wait that followed an outage kept ClusterNotReady=True.
func TestAliasReconcile_AWaitOnTheDatabaseIsNotATargetWait(t *testing.T) {
	ctx := context.Background()
	server := &aliasServer{}
	alias := &neo4jv1beta1.Neo4jDatabaseAlias{
		ObjectMeta: repeatedWarningsMeta("latest", Neo4jDatabaseAliasFinalizer),
		Spec:       neo4jv1beta1.Neo4jDatabaseAliasSpec{ClusterRef: "sa", Name: "latest", TargetDatabase: "orders"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(alias.DeepCopy(), authTargetStandalone()).
		WithStatusSubresource(alias, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	r := &Neo4jDatabaseAliasReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(100),
		newNeo4jClient: func(ResolvedTarget) (aliasNeo4jClient, error) { return server, nil }}
	key := client.ObjectKeyFromObject(alias)
	conditions := func() []metav1.Condition {
		got := &neo4jv1beta1.Neo4jDatabaseAlias{}
		require.NoError(t, c.Get(ctx, key, got))
		return got.Status.Conditions
	}

	server.err = errLostMajority
	reconcileTimes(t, r, key, 1)
	require.Equal(t, metav1.ConditionTrue, clusterNotReadyCondition(t, conditions()).Status)

	server.err, server.noTargets = nil, true
	reconcileTimes(t, r, key, 1)
	assert.Equal(t, metav1.ConditionFalse, clusterNotReadyCondition(t, conditions()).Status,
		"the target answered; the alias waits on its database")
}

func TestTargetAway(t *testing.T) {
	for _, err := range []error{
		errRefusedBolt, errLostMajority, errNotALeader,
		fmt.Errorf("failed to create database: %w", errLostMajority),
		errors.New("Neo.TransientError.Statement.RemoteExecutionTransientError: Failed to obtain connection towards READ server"),
	} {
		assert.True(t, targetAway(err), "%v", err)
	}
	for _, err := range []error{
		nil,
		errors.New("Neo.ClientError.Statement.SyntaxError: Invalid input"),
		errors.New("Neo.TransientError.Transaction.Terminated: the transaction was terminated"),
		&neo4jdriver.Neo4jError{Code: "Neo.ClientError.Security.Forbidden", Msg: "Permission denied"},
	} {
		assert.False(t, targetAway(err), "%v", err)
	}
}
