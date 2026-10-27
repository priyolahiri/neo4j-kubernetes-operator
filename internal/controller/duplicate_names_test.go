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

// Two CRs of one kind whose names in Neo4j differ only by case are one name
// there (operations.md id 217): the oldest owns it, the other is Failed with
// one DuplicateName Warning and acts on nothing, and a deletion leaves the
// database to a CR that still names it.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

var claimEpoch = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

// createdAt gives m a creation time minutes after claimEpoch and a UID.
func createdAt(m metav1.ObjectMeta, minutes int, uid string) metav1.ObjectMeta {
	m.CreationTimestamp = metav1.NewTime(claimEpoch.Add(time.Duration(minutes) * time.Minute))
	m.UID = types.UID(uid)
	return m
}

func dbClaim(name, target, dbName string, minutes int, uid string, deleting bool) nameClaim {
	db := &neo4jv1beta1.Neo4jDatabase{ObjectMeta: createdAt(metav1.ObjectMeta{Name: name, Namespace: "default"}, minutes, uid),
		Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: target, Name: dbName}}
	if deleting {
		now := metav1.Now()
		db.DeletionTimestamp = &now
	}
	cl, ok := databaseNames.claim(db)
	if !ok {
		panic("not a Neo4jDatabase")
	}
	return cl
}

func TestNameOwner_TheOldestLiveClaimOfANameAsNeo4jReadsItOwnsIt(t *testing.T) {
	older := dbClaim("orders", "ec", "orders", 0, "b", false)
	newer := dbClaim("orders-copy", "ec", "Orders", 1, "a", false)
	all := []nameClaim{older, newer}

	assert.Equal(t, "orders", nameOwner(older, all))
	assert.Equal(t, "orders", nameOwner(newer, all), "Neo4j's names ignore case")

	t.Run("another target or another name contests nothing", func(t *testing.T) {
		elsewhere := dbClaim("orders-b", "other", "orders", 0, "c", false)
		other := dbClaim("sales", "ec", "sales", 0, "d", false)
		late := dbClaim("orders-late", "ec", "ORDERS", 5, "e", false)
		assert.Equal(t, "orders-late", nameOwner(late, []nameClaim{late, elsewhere, other}))
	})

	t.Run("a CR being deleted contests nothing", func(t *testing.T) {
		going := dbClaim("orders", "ec", "orders", 0, "b", true)
		assert.Equal(t, "orders-copy", nameOwner(newer, []nameClaim{going, newer}))
	})

	t.Run("the same creation time: the lower UID", func(t *testing.T) {
		x := dbClaim("x", "ec", "orders", 0, "b", false)
		y := dbClaim("y", "ec", "orders", 0, "a", false)
		assert.Equal(t, "y", nameOwner(x, []nameClaim{x, y}))
		assert.Equal(t, "y", nameOwner(y, []nameClaim{x, y}))
	})

	t.Run("otherLiveClaim: the live CR a deletion leaves the name to", func(t *testing.T) {
		going := dbClaim("orders", "ec", "orders", 0, "b", true)
		assert.Equal(t, "orders-copy", otherLiveClaim(going, []nameClaim{going, newer}))
		assert.Empty(t, otherLiveClaim(going, []nameClaim{going}))
		goingToo := dbClaim("orders-copy", "ec", "Orders", 1, "a", true)
		assert.Empty(t, otherLiveClaim(going, []nameClaim{going, goingToo}), "two deletions: the last one drops it")
	})
}

// The watch wakes the other CRs of a name when one comes, goes or is renamed:
// the plugin's survivor stayed Failed until the operator restarted (id 195).
func TestNameClaims_TheWatchWakesTheOthersOfTheName(t *testing.T) {
	older := &neo4jv1beta1.Neo4jDatabase{ObjectMeta: createdAt(metav1.ObjectMeta{Name: "orders", Namespace: "default"}, 0, "a"),
		Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "ec", Name: "orders"}}
	newer := &neo4jv1beta1.Neo4jDatabase{ObjectMeta: createdAt(metav1.ObjectMeta{Name: "orders-copy", Namespace: "default"}, 1, "b"),
		Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "ec", Name: "Orders"}}
	unrelated := &neo4jv1beta1.Neo4jDatabase{ObjectMeta: metav1.ObjectMeta{Name: "sales", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "ec", Name: "sales"}}
	elsewhere := &neo4jv1beta1.Neo4jDatabase{ObjectMeta: metav1.ObjectMeta{Name: "orders-b", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "other", Name: "orders"}}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(newer, unrelated, elsewhere).Build()

	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()
	databaseNames.othersOfTheName(c).Delete(context.Background(), event.DeleteEvent{Object: older}, q)
	require.Equal(t, 1, q.Len())
	got, _ := q.Get()
	assert.Equal(t, client.ObjectKeyFromObject(newer), got.NamespacedName)

	statusOnly := older.DeepCopy()
	statusOnly.Status.Phase = "Ready"
	renamed := older.DeepCopy()
	renamed.Generation = 2
	deleting := older.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	p := nameClaimChanged
	assert.False(t, p.Update(event.UpdateEvent{ObjectOld: older, ObjectNew: statusOnly}), "a status write wakes no one")
	assert.True(t, p.Update(event.UpdateEvent{ObjectOld: older, ObjectNew: renamed}))
	assert.True(t, p.Update(event.UpdateEvent{ObjectOld: older, ObjectNew: deleting}))
}

// A Neo4jDatabase named "Orders" beside an older one named "orders" on the same
// cluster: both reported Ready on one database, and deleting either dropped it.
func TestDatabaseReconcile_ACaseOnlyDuplicateIsFailedAndActsOnNothing(t *testing.T) {
	ctx := context.Background()
	older := ordersDatabase("orders")
	older.ObjectMeta = createdAt(older.ObjectMeta, 0, "a")
	newer := ordersDatabase("Orders")
	newer.ObjectMeta = createdAt(newer.ObjectMeta, 1, "b")
	newer.Name = "orders-copy"
	server := &databaseServer{}
	r, rec := databaseTestReconciler(t, server, readyDatabaseHost(), older, newer)
	r.RequeueAfter = 30 * time.Second

	for range repeatPasses {
		for _, db := range []*neo4jv1beta1.Neo4jDatabase{older, newer} {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(db)})
			require.NoError(t, err)
		}
	}

	assert.Equal(t, neo4jv1beta1.PhaseReady, databasePhase(t, r, client.ObjectKeyFromObject(older)))
	got := &neo4jv1beta1.Neo4jDatabase{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(newer), got))
	assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
	assert.Contains(t, got.Status.Message, `"orders" (older) already gives database "Orders"`)
	dup := eventsWithReason(drainEvents(rec), EventReasonDuplicateName)
	require.Len(t, dup, 1, "announced once")
	assert.True(t, strings.HasPrefix(dup[0], "Warning "))
	assert.Contains(t, dup[0], `"orders"`)
	creates := 0
	for _, s := range server.statements {
		if strings.HasPrefix(s, "CREATE DATABASE") {
			creates++
		}
	}
	assert.Equal(t, 1, creates, "the duplicate sends Neo4j nothing: %q", server.statements)

	t.Run("deleting the owner leaves the database to the other, which takes it", func(t *testing.T) {
		require.NoError(t, r.Delete(ctx, older))
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(older)})
		require.NoError(t, err)
		assert.True(t, finalizerReleased(t, r.Client, older, DatabaseFinalizer))
		for _, s := range server.statements {
			assert.False(t, strings.HasPrefix(s, "DROP"), "Orders still names it: %q", server.statements)
		}
		kept := eventsWithReason(drainEvents(rec), EventReasonDuplicateNameKept)
		require.Len(t, kept, 1)
		assert.Contains(t, kept[0], `Neo4jDatabase "orders-copy" still gives it`)

		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(newer)})
		require.NoError(t, err)
		assert.Equal(t, neo4jv1beta1.PhaseReady, databasePhase(t, r, client.ObjectKeyFromObject(newer)))
	})
}

func TestShardedReconcile_ACaseOnlyDuplicateIsFailedAndAdoptsNothing(t *testing.T) {
	ctx := context.Background()
	server := &shardedServer{name: "products"}
	older := productsShardedDB()
	older.ObjectMeta = createdAt(older.ObjectMeta, 0, "a")
	newer := productsShardedDB()
	newer.ObjectMeta = createdAt(newer.ObjectMeta, 1, "b")
	newer.Name, newer.Spec.Name = "products-upper", "Products"
	r, rec := shardedTestReconciler(t, server, shardingCluster(), older, newer)

	for range repeatPasses {
		require.NoError(t, reconcileSharded(t, r, older))
		require.NoError(t, reconcileSharded(t, r, newer))
	}

	got := &neo4jv1beta1.Neo4jShardedDatabase{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(older), got))
	assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(newer), got))
	assert.Equal(t, "Failed", got.Status.Phase)
	assert.Contains(t, got.Status.Message, `"products" (older) already gives sharded database "Products"`)
	dup := eventsWithReason(drainEvents(rec), EventReasonDuplicateName)
	require.Len(t, dup, 1, "announced once")
	assert.True(t, strings.HasPrefix(dup[0], "Warning "))
	assert.Equal(t, 1, server.count("CREATE DATABASE"), "the duplicate sends Neo4j nothing: %q", server.statements)

	t.Run("deleting the owner hands the family to the other", func(t *testing.T) {
		require.NoError(t, r.Delete(ctx, older))
		require.NoError(t, reconcileSharded(t, r, newer))
		require.NoError(t, reconcileSharded(t, r, newer))
		require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(newer), got))
		assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase, got.Status.Message)
		assert.Zero(t, server.count("DROP"), "nothing is dropped: %q", server.statements)
	})
}

// countingCompositeServer counts the composites it creates.
type countingCompositeServer struct {
	compositeServer
	creates int
}

func (s *countingCompositeServer) CreateCompositeDatabase(ctx context.Context, name string, a, b bool, l string) error {
	s.creates++
	return s.compositeServer.CreateCompositeDatabase(ctx, name, a, b, l)
}

func TestCompositeReconcile_ACaseOnlyDuplicateIsFailed(t *testing.T) {
	ctx := context.Background()
	older := &neo4jv1beta1.Neo4jCompositeDatabase{
		ObjectMeta: createdAt(repeatedWarningsMeta("sales", Neo4jCompositeDatabaseFinalizer), 0, "a"),
		Spec:       neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "sa", Name: "sales"},
	}
	newer := &neo4jv1beta1.Neo4jCompositeDatabase{
		ObjectMeta: createdAt(repeatedWarningsMeta("sales-upper", Neo4jCompositeDatabaseFinalizer), 1, "b"),
		Spec:       neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "sa", Name: "Sales"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(older, newer, authTargetStandalone()).
		WithStatusSubresource(older, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	server := &countingCompositeServer{}
	r := &Neo4jCompositeDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
		newNeo4jClient: func(ResolvedTarget) (compositeNeo4jClient, error) { return server, nil }}

	for range repeatPasses {
		for _, cd := range []*neo4jv1beta1.Neo4jCompositeDatabase{older, newer} {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cd)})
			require.NoError(t, err)
		}
	}

	got := &neo4jv1beta1.Neo4jCompositeDatabase{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(older), got))
	assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(newer), got))
	assert.Equal(t, neo4jv1beta1.PhaseFailed, got.Status.Phase)
	assert.Contains(t, got.Status.Message, `"sales" (older) already gives composite database "Sales"`)
	dup := eventsWithReason(drainEvents(rec), EventReasonDuplicateName)
	require.Len(t, dup, 1, "announced once")
	assert.True(t, strings.HasPrefix(dup[0], "Warning "))
	assert.Equal(t, 1, server.creates)
}

// countingAliasServer counts the aliases it creates or re-points.
type countingAliasServer struct {
	aliasServer
	writes int
}

func (s *countingAliasServer) CreateAlias(ctx context.Context, name, target string) error {
	s.writes++
	return s.aliasServer.CreateAlias(ctx, name, target)
}

func (s *countingAliasServer) AlterAliasTarget(ctx context.Context, name, target string) error {
	s.writes++
	return s.aliasServer.AlterAliasTarget(ctx, name, target)
}

func (s *countingAliasServer) ShowAlias(_ context.Context, name string) (*neo4jclient.AliasInfo, error) {
	if s.alias != nil && strings.EqualFold(s.alias.Name, name) {
		return s.alias, nil
	}
	return nil, nil
}

// Two aliases spelled `latest` and `Latest` for different databases: each pass
// re-pointed the one alias between them.
func TestAliasReconcile_ACaseOnlyDuplicateIsFailed(t *testing.T) {
	ctx := context.Background()
	older := &neo4jv1beta1.Neo4jDatabaseAlias{
		ObjectMeta: createdAt(repeatedWarningsMeta("latest", Neo4jDatabaseAliasFinalizer), 0, "a"),
		Spec:       neo4jv1beta1.Neo4jDatabaseAliasSpec{ClusterRef: "sa", Name: "latest", TargetDatabase: "orders"},
	}
	newer := &neo4jv1beta1.Neo4jDatabaseAlias{
		ObjectMeta: createdAt(repeatedWarningsMeta("latest-upper", Neo4jDatabaseAliasFinalizer), 1, "b"),
		Spec:       neo4jv1beta1.Neo4jDatabaseAliasSpec{ClusterRef: "sa", Name: "Latest", TargetDatabase: "orders-v2"},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(older, newer, authTargetStandalone()).
		WithStatusSubresource(older, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	server := &countingAliasServer{}
	r := &Neo4jDatabaseAliasReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
		newNeo4jClient: func(ResolvedTarget) (aliasNeo4jClient, error) { return server, nil }}

	for range repeatPasses {
		for _, a := range []*neo4jv1beta1.Neo4jDatabaseAlias{older, newer} {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(a)})
			require.NoError(t, err)
		}
	}

	got := &neo4jv1beta1.Neo4jDatabaseAlias{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(older), got))
	assert.Equal(t, neo4jv1beta1.PhaseReady, got.Status.Phase)
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(newer), got))
	assert.Equal(t, "Failed", got.Status.Phase)
	assert.Contains(t, got.Status.Message, `"latest" (older) already gives alias "Latest"`)
	dup := eventsWithReason(drainEvents(rec), EventReasonDuplicateName)
	require.Len(t, dup, 1, "announced once")
	assert.Equal(t, 1, server.writes, "the alias is created once and never re-pointed")
	assert.Equal(t, "orders", server.alias.Database)
}

// A deletion of a CR whose name another live CR still gives leaves the name in
// Neo4j to it, for every kind whose deletion drops something.
func TestDeletion_LeavesANameAnotherCRStillGives(t *testing.T) {
	twinOf := func(obj client.Object) client.Object {
		twin, ok := obj.DeepCopyObject().(client.Object)
		require.True(t, ok)
		twin.SetName(obj.GetName() + "-upper")
		twin.SetDeletionTimestamp(nil)
		twin.SetUID("twin")
		switch o := twin.(type) {
		case *neo4jv1beta1.Neo4jDatabase:
			o.Spec.Name = strings.ToUpper(o.Spec.Name)
		case *neo4jv1beta1.Neo4jCompositeDatabase:
			o.Spec.Name = strings.ToUpper(o.Spec.Name)
		case *neo4jv1beta1.Neo4jDatabaseAlias:
			o.Spec.Name = strings.ToUpper(o.Spec.Name)
		default:
			return nil
		}
		return twin
	}
	tested := 0
	for _, k := range deletionKinds() {
		obj := k.obj(time.Now())
		twin := twinOf(obj)
		if twin == nil {
			continue
		}
		tested++
		t.Run(k.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, twin, authTargetStandalone()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			var connErr error
			r, sent := k.setup(c, rec, &connErr)

			for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), 1) {
				require.NoError(t, err)
			}
			assert.True(t, finalizerReleased(t, c, obj, k.finalizer))
			assert.Zero(t, sent(), "%s still gives the name: nothing is dropped", twin.GetName())
			events := drainEvents(rec)
			assert.Empty(t, eventsWithReason(events, k.dropped))
			kept := eventsWithReason(events, EventReasonDuplicateNameKept)
			require.Len(t, kept, 1, "%q", events)
			assert.True(t, strings.HasPrefix(kept[0], "Normal "))
			assert.Contains(t, kept[0], `"`+twin.GetName()+`" still gives it`)
		})

		t.Run(k.name+"/a twin in another cluster contests nothing", func(t *testing.T) {
			elsewhere := twinOf(obj)
			switch o := elsewhere.(type) {
			case *neo4jv1beta1.Neo4jDatabase:
				o.Spec.ClusterRef = "other"
			case *neo4jv1beta1.Neo4jCompositeDatabase:
				o.Spec.ClusterRef = "other"
			case *neo4jv1beta1.Neo4jDatabaseAlias:
				o.Spec.ClusterRef = "other"
			}
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, elsewhere, authTargetStandalone()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			var connErr error
			r, sent := k.setup(c, rec, &connErr)

			for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), 1) {
				require.NoError(t, err)
			}
			assert.Equal(t, 1, sent(), "dropped as before")
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonDuplicateNameKept))
		})
	}
	assert.Equal(t, 3, tested, "the database, composite and alias deletions")
}
