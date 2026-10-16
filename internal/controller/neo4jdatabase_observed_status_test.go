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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// databaseServer is a scripted Neo4j for the Neo4jDatabase reconcile. rows is
// what SHOW DATABASES lists, one row per copy, in the order the server returns
// them — which Neo4j does not keep stable between calls. DatabaseExists
// answers as the real client does, through neo4j.FindDatabase. CREATE behaves
// as Neo4j's does: it stores the name lower-cased, is a no-op for a name that
// exists (in any case) under IF NOT EXISTS, and is refused without it.
type databaseServer struct {
	rows       []neo4j.DatabaseInfo
	statements []string
}

func (s *databaseServer) DatabaseExists(ctx context.Context, name string) (bool, error) {
	databases, err := s.GetDatabases(ctx)
	if err != nil {
		return false, err
	}
	return neo4j.FindDatabase(databases, name) != nil, nil
}

func (s *databaseServer) GetDatabases(context.Context) ([]neo4j.DatabaseInfo, error) {
	return append([]neo4j.DatabaseInfo{{Name: "system", Status: "online", RequestedStatus: "online", Address: "server-0:7687"}}, s.rows...), nil
}

func (s *databaseServer) create(name string, ifNotExists bool) error {
	s.statements = append(s.statements, "CREATE DATABASE "+name)
	for _, row := range s.rows {
		if strings.EqualFold(row.Name, name) {
			if ifNotExists {
				return nil
			}
			return fmt.Errorf("failed to create the specified database '%s': database already exists", strings.ToLower(name))
		}
	}
	s.rows = append(s.rows, neo4j.DatabaseInfo{Name: strings.ToLower(name), Status: "online", RequestedStatus: "online", Address: "server-0:7687"})
	return nil
}

func (s *databaseServer) CreateDatabaseWithLanguage(_ context.Context, name string, _ map[string]string, _, ifNotExists bool, _ string) error {
	return s.create(name, ifNotExists)
}

func (s *databaseServer) CreateDatabaseWithTopology(_ context.Context, name string, _, _ int32, _ map[string]string, _, ifNotExists bool, _ string) error {
	return s.create(name, ifNotExists)
}

func (s *databaseServer) CreateDatabaseFromSeedURI(_ context.Context, name, _ string, _ *neo4jv1beta1.SeedConfiguration, _ map[string]string, _, ifNotExists bool, _ string) error {
	return s.create(name, ifNotExists)
}

func (s *databaseServer) CreateDatabaseFromSeedURIWithTopology(_ context.Context, name, _ string, _, _ int32, _ *neo4jv1beta1.SeedConfiguration, _ map[string]string, _, ifNotExists bool, _ string) error {
	return s.create(name, ifNotExists)
}

func (s *databaseServer) PrepareCloudCredentials(context.Context, client.Client, *neo4jv1beta1.Neo4jDatabase) error {
	return nil
}

func (s *databaseServer) ExecuteCypher(_ context.Context, _, statement string) error {
	s.statements = append(s.statements, statement)
	return nil
}

func (s *databaseServer) DropDatabase(_ context.Context, name string) error {
	s.statements = append(s.statements, "DROP DATABASE "+name)
	return nil
}

func (s *databaseServer) Close() error { return nil }

// copies lists one online copy of name per address.
func copies(name string, addresses ...string) []neo4j.DatabaseInfo {
	out := make([]neo4j.DatabaseInfo, 0, len(addresses))
	for _, a := range addresses {
		out = append(out, neo4j.DatabaseInfo{Name: name, Status: "online", RequestedStatus: "online", Address: a})
	}
	return out
}

func databaseTestReconciler(t *testing.T, server *databaseServer, objs ...client.Object) (*Neo4jDatabaseReconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := noEffectTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jDatabase{}, &neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()
	rec := record.NewFakeRecorder(50)
	return &Neo4jDatabaseReconciler{
		Client: c, Scheme: scheme, Recorder: rec,
		newNeo4jClient: func(*neo4jv1beta1.Neo4jEnterpriseCluster, *neo4jv1beta1.Neo4jEnterpriseStandalone) (databaseNeo4jClient, error) {
			return server, nil
		},
	}, rec
}

func readyDatabaseHost() *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image:    neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
		},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{Phase: neo4jv1beta1.PhaseReady},
	}
}

func ordersDatabase(name string) *neo4jv1beta1.Neo4jDatabase {
	return &neo4jv1beta1.Neo4jDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default", Generation: 1, Finalizers: []string{DatabaseFinalizer}},
		Spec:       neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "ec", Name: name, IfNotExists: true},
	}
}

// status.state and status.servers are what SHOW DATABASES says on the pass
// that wrote them, and follow it: they were set on the reconcile's own copy of
// the object and lost, because the status write re-reads the object first, so
// they reached the API only through the first pass's dataImported write —
// never at all for a database with neither initialData nor seedURI.
func TestDatabaseReconcile_StatusFollowsShowDatabases(t *testing.T) {
	server := &databaseServer{rows: copies("orders", "server-2:7687", "server-0:7687", "server-1:7687")}
	db := ordersDatabase("orders")
	r, _ := databaseTestReconciler(t, server, readyDatabaseHost(), db)
	ctx := context.Background()
	status := func() neo4jv1beta1.Neo4jDatabaseStatus {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(db)})
		require.NoError(t, err)
		got := &neo4jv1beta1.Neo4jDatabase{}
		require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(db), got))
		return got.Status
	}

	got := status()
	assert.Equal(t, "Ready", got.Phase)
	assert.Equal(t, "online", got.State)
	assert.Equal(t, []string{"server-0:7687", "server-1:7687", "server-2:7687"}, got.Servers,
		"sorted: SHOW DATABASES lists the copies in no fixed order")

	// The database is stopped.
	for i := range server.rows {
		server.rows[i].Status, server.rows[i].RequestedStatus = "offline", "offline"
	}
	assert.Equal(t, "offline", status().State, "a database that went offline must not keep reporting online")

	// It is started again on different servers.
	server.rows = copies("orders", "server-3:7687", "server-1:7687", "server-4:7687")
	got = status()
	assert.Equal(t, "online", got.State)
	assert.Equal(t, []string{"server-1:7687", "server-3:7687", "server-4:7687"}, got.Servers,
		"a database that moved must not keep reporting the servers it left")

	// One copy is being rebuilt. Whatever order the rows come back in, the
	// state is that copy's.
	server.rows[2].Status = "store copying"
	assert.Equal(t, "store copying", status().State)
	server.rows[0], server.rows[2] = server.rows[2], server.rows[0]
	assert.Equal(t, "store copying", status().State)
}

// A seeded database's first pass wrote dataImported from the reconcile's own
// copy, so its first observation did reach the API — and then stayed there.
func TestDatabaseReconcile_SeededDatabaseStatusDoesNotGoStale(t *testing.T) {
	server := &databaseServer{rows: copies("orders", "server-0:7687")}
	db := ordersDatabase("orders")
	db.Spec.SeedURI = "s3://bucket/orders.backup"
	r, _ := databaseTestReconciler(t, server, readyDatabaseHost(), db)
	ctx := context.Background()
	status := func() neo4jv1beta1.Neo4jDatabaseStatus {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(db)})
		require.NoError(t, err)
		got := &neo4jv1beta1.Neo4jDatabase{}
		require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(db), got))
		return got.Status
	}

	got := status()
	require.NotNil(t, got.DataImported)
	assert.Equal(t, "online", got.State)

	server.rows[0].Status, server.rows[0].RequestedStatus = "offline", "offline"
	server.rows[0].Address = "server-1:7687"
	got = status()
	assert.Equal(t, "offline", got.State)
	assert.Equal(t, []string{"server-1:7687"}, got.Servers)
}

// A status write is a watch event, which reconciles the database again. A
// pass that observes nothing new must not write, or the periodic reconcile
// feeds itself; rows in a new order are not new.
func TestDatabaseReconcile_WritesStatusOnlyWhenItChanges(t *testing.T) {
	server := &databaseServer{rows: copies("orders", "server-0:7687", "server-1:7687", "server-2:7687")}
	server.rows[1].Status = "starting"
	db := ordersDatabase("orders")
	r, _ := databaseTestReconciler(t, server, readyDatabaseHost(), db)
	ctx := context.Background()
	resourceVersion := func() string {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(db)})
		require.NoError(t, err)
		got := &neo4jv1beta1.Neo4jDatabase{}
		require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(db), got))
		return got.ResourceVersion
	}

	settled := resourceVersion()
	assert.Equal(t, settled, resourceVersion(), "nothing changed: no write")
	server.rows[0], server.rows[2] = server.rows[2], server.rows[0]
	server.rows[0], server.rows[1] = server.rows[1], server.rows[0]
	assert.Equal(t, settled, resourceVersion(), "the same copies in another order: no write")

	server.rows[0].Status = "online"
	assert.NotEqual(t, settled, resourceVersion(), "a copy came online: written")
}

// Neo4j lower-cases database names, and the validator admits capitals in
// spec.name, so SHOW DATABASES lists `Orders` as `orders`. The observation
// matches the name ignoring case, or such a database would never report a
// state or a server.
func TestDatabaseReconcile_ObservesAMixedCaseName(t *testing.T) {
	server := &databaseServer{}
	db := ordersDatabase("Orders")
	r, _ := databaseTestReconciler(t, server, readyDatabaseHost(), db)
	ctx := context.Background()

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(db)})
	require.NoError(t, err)
	got := &neo4jv1beta1.Neo4jDatabase{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(db), got))
	assert.Equal(t, "online", got.Status.State)
	assert.Equal(t, []string{"server-0:7687"}, got.Status.Servers)
}

func TestObserveDatabase(t *testing.T) {
	listing := []neo4j.DatabaseInfo{
		{Name: "system", Status: "online", Address: "server-0:7687"},
		{Name: "orders2", Status: "offline", Address: "server-9:7687"},
		{Name: "orders", Status: "online", RequestedStatus: "online", Address: "server-1:7687"},
		{Name: "orders", Status: "quarantined", RequestedStatus: "online", Address: "server-0:7687"},
		{Name: "orders", Status: "starting", RequestedStatus: "online", Address: "server-2:7687"},
	}

	got := observeDatabase("orders", listing)
	assert.True(t, got.observed)
	assert.Equal(t, "quarantined", got.state, "the first status other than online, by name")
	assert.Equal(t, []string{"server-0:7687", "server-1:7687", "server-2:7687"}, got.servers,
		"only the database's own copies; a name sharing its prefix is another database")

	assert.False(t, observeDatabase("invoices", listing).observed, "not listed: nothing observed")

	status := neo4jv1beta1.Neo4jDatabaseStatus{State: "online", Servers: []string{"server-1:7687"}}
	databaseObservation{}.apply(&status)
	assert.Equal(t, neo4jv1beta1.Neo4jDatabaseStatus{State: "online", Servers: []string{"server-1:7687"}}, status,
		"a pass that observed nothing keeps the last reading")
}
