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
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// shardedServer is a scripted Neo4j holding at most one sharded family. It
// answers CREATE DATABASE the way the Operations Manual says Neo4j does: with
// IF NOT EXISTS, nothing happens when the name exists; without it, the
// command fails ("create-databases.adoc": "the default behavior [is] to fail
// with an error if the database already exists").
type shardedServer struct {
	name       string
	exists     bool
	statements []string
}

func (s *shardedServer) GetDatabases(context.Context) ([]neo4j.DatabaseInfo, error) {
	out := []neo4j.DatabaseInfo{{Name: "system", Status: "online", RequestedStatus: "online"}}
	if s.exists {
		for _, name := range []string{s.name, s.name + "-g000", s.name + "-p000", s.name + "-p001"} {
			out = append(out, neo4j.DatabaseInfo{Name: name, Status: "online", RequestedStatus: "online"})
		}
	}
	return out, nil
}

func (s *shardedServer) ExecuteCypher(_ context.Context, _, statement string) error {
	s.statements = append(s.statements, statement)
	if strings.Contains(statement, "DROP DATABASE") {
		s.exists = false
	}
	return nil
}

func (s *shardedServer) ExecuteCypherWithParams(_ context.Context, _, statement string, _ map[string]any) error {
	s.statements = append(s.statements, statement)
	if !strings.Contains(statement, "CREATE DATABASE") {
		return nil
	}
	if s.exists {
		if strings.Contains(statement, "IF NOT EXISTS") {
			return nil
		}
		return fmt.Errorf("Neo.ClientError.Database.ExistingDatabaseFound: Failed to create the specified database '%s': Database already exists.", s.name)
	}
	s.exists = true
	return nil
}

func (s *shardedServer) PrepareCloudCredentialsForShardedDatabase(context.Context, client.Client, *neo4jv1beta1.Neo4jShardedDatabase) error {
	return nil
}

func (s *shardedServer) Close() error { return nil }

func (s *shardedServer) count(verb string) int {
	n := 0
	for _, st := range s.statements {
		if strings.Contains(st, verb) {
			n++
		}
	}
	return n
}

func shardedTestReconciler(t *testing.T, server *shardedServer, objs ...client.Object) (*Neo4jShardedDatabaseReconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := noEffectTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jShardedDatabase{}, &neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()
	rec := record.NewFakeRecorder(50)
	return &Neo4jShardedDatabaseReconciler{
		Client: c, Scheme: scheme, Recorder: rec,
		newNeo4jClient: func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) { return server, nil },
	}, rec
}

func shardingCluster() *neo4jv1beta1.Neo4jEnterpriseCluster {
	return &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "sc", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image:            neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
			Topology:         neo4jv1beta1.TopologyConfiguration{Servers: 3},
			PropertySharding: &neo4jv1beta1.PropertyShardingSpec{Enabled: true},
		},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			Phase:                 neo4jv1beta1.PhaseReady,
			PropertyShardingReady: ptr.To(true),
		},
	}
}

func productsShardedDB() *neo4jv1beta1.Neo4jShardedDatabase {
	return &neo4jv1beta1.Neo4jShardedDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "products", Namespace: "default", Generation: 1},
		Spec: neo4jv1beta1.Neo4jShardedDatabaseSpec{
			ClusterRef:            "sc",
			Name:                  "products",
			DefaultCypherLanguage: "25",
			PropertySharding: neo4jv1beta1.PropertyShardingConfiguration{
				PropertyShards:        2,
				GraphShard:            neo4jv1beta1.DatabaseTopology{Primaries: 1},
				PropertyShardTopology: neo4jv1beta1.PropertyShardTopology{Replicas: 1},
			},
		},
	}
}

func reconcileSharded(t *testing.T, r *Neo4jShardedDatabaseReconciler, sd *neo4jv1beta1.Neo4jShardedDatabase) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(sd)})
	return err
}

// The sharded controller ran CREATE DATABASE ... IF NOT EXISTS on every pass
// for the life of the CR, and announced ShardedDatabaseReady on every pass
// (v1.20.0 and v1.21.0 journeys). The family is created once; the SHOW
// DATABASES the reconcile already runs says when it exists.
func TestShardedReconcile_CreatesOnceAndAnnouncesReadyOnce(t *testing.T) {
	server := &shardedServer{name: "products"}
	sd := productsShardedDB()
	r, rec := shardedTestReconciler(t, server, shardingCluster(), sd)

	for range 3 {
		require.NoError(t, reconcileSharded(t, r, sd))
	}

	assert.Equal(t, 1, server.count("CREATE DATABASE"), "CREATE runs while the family is absent, then never again")
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonShardedDatabaseReady), 1)

	got := &neo4jv1beta1.Neo4jShardedDatabase{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(sd), got))
	assert.Equal(t, "Ready", got.Status.Phase)
	require.NotNil(t, got.Status.GraphShard, "shard status is still read every pass")
	assert.True(t, got.Status.GraphShard.Ready)
}

// A family created outside the operator, or by an earlier operator, is not
// created again either.
func TestShardedReconcile_ExistingFamilyIsNotCreated(t *testing.T) {
	server := &shardedServer{name: "products", exists: true}
	sd := productsShardedDB()
	r, _ := shardedTestReconciler(t, server, shardingCluster(), sd)

	require.NoError(t, reconcileSharded(t, r, sd))
	assert.Zero(t, server.count("CREATE DATABASE"))
}

// replaceExisting needs ifNotExists: false, so the CREATE that every pass
// sent after a destructive restore had no IF NOT EXISTS, and Neo4j refused
// it: the pass after the restore failed with "already exists" and turned the
// CR Failed. The integration spec stops at the generation stamp, before that
// pass. The destructive pass itself still drops and creates.
func TestShardedReconcile_StaysReadyAfterADestructiveRestore(t *testing.T) {
	server := &shardedServer{name: "products", exists: true}
	sd := productsShardedDB()
	sd.Generation = 2
	sd.Spec.ReplaceExisting = true
	sd.Spec.Force = true
	sd.Spec.IfNotExists = ptr.To(false)
	sd.Spec.SeedURI = "s3://backups/products/"
	sd.Status.LastDestructiveRestoreGeneration = 1
	r, rec := shardedTestReconciler(t, server, shardingCluster(), sd)

	require.NoError(t, reconcileSharded(t, r, sd), "the destructive pass")
	require.Equal(t, 1, server.count("DROP DATABASE"))
	require.Equal(t, 1, server.count("CREATE DATABASE"), "the destructive pass recreates the family it dropped")
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonShardedDatabaseReady), 1,
		"a recreated database is announced, though it was Ready before")

	for range 2 {
		require.NoError(t, reconcileSharded(t, r, sd), "the passes after the restore")
	}
	assert.Equal(t, 1, server.count("DROP DATABASE"), "the generation gate holds (rule 64)")
	assert.Equal(t, 1, server.count("CREATE DATABASE"), "no CREATE against the family that now exists")

	got := &neo4jv1beta1.Neo4jShardedDatabase{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(sd), got))
	assert.Equal(t, "Ready", got.Status.Phase)
	assert.Equal(t, int64(2), got.Status.LastDestructiveRestoreGeneration)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonShardedDatabaseReady))
}

// A sharded database waiting for its seedBackupRef's first Succeeded run said
// so on every pass; v1.20.0's journey saw that exhaust the object's event
// budget, so SeedCredsAutoInherited never reached the API.
func TestShardedReconcile_AnnouncesTheSeedWaitOnce(t *testing.T) {
	server := &shardedServer{name: "products"}
	sd := productsShardedDB()
	sd.Spec.SeedBackupRef = "products-backup"
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "products-backup", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:     "sc",
			ShardedDatabase: "products",
			Storage: neo4jv1beta1.StorageLocation{
				Type: "s3", Bucket: "backups",
				Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws"},
			},
		},
	}
	r, rec := shardedTestReconciler(t, server, shardingCluster(), sd, backup)

	for range 3 {
		require.NoError(t, reconcileSharded(t, r, sd))
	}
	events := drainEvents(rec)
	assert.Len(t, eventsWithReason(events, EventReasonSeedBackupPending), 1)
	assert.Zero(t, server.count("CREATE DATABASE"), "still waiting for the seed")
}

// The same for the wait on the PVC seed proxy, which the fake client never
// reports Ready.
func TestShardedReconcile_AnnouncesTheSeedProxyWaitOnce(t *testing.T) {
	server := &shardedServer{name: "sdata2"}
	sd := seedTestShardedDB()
	sd.Spec.SeedSourceDatabase = "" // the fallback validator wants a URI beside it; the PVC path drops it anyway
	sd.Spec.DefaultCypherLanguage = "25"
	sd.Spec.PropertySharding = productsShardedDB().Spec.PropertySharding
	backup := seedTestPVCBackup(
		neo4jv1beta1.ShardArtifact{ShardName: "sdata-g000", Filename: "sdata-g000-2026-10-06T13-46-10.backup", Type: "FULL"},
		neo4jv1beta1.ShardArtifact{ShardName: "sdata-p000", Filename: "sdata-p000-2026-10-06T13-46-09.backup", Type: "FULL"},
	)
	r, rec := shardedTestReconciler(t, server, shardingCluster(), sd, backup)

	for range 3 {
		require.NoError(t, reconcileSharded(t, r, sd))
	}
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonSeedProxyStarting), 1)
	assert.Zero(t, server.count("CREATE DATABASE"), "still waiting for the proxy")
}
