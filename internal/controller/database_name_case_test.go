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

// Neo4j database names "are case-insensitive and normalized to lowercase"
// (Operations Manual 5 and /current/, "Naming rules for databases"), and so
// are alias and composite names and a user's home database. The validators
// admit capitals in every name a spec gives. So a name from a spec and the
// same name from SHOW DATABASES, SHOW ALIASES, SHOW USERS or neo4j-admin's
// output are compared ignoring case (knowledge operations.md id 146). Each
// test below fails with its comparison made exact again.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// A Neo4jDatabase named `Orders` is listed as `orders`. DatabaseExists matched
// exactly, so every pass decided it was absent and sent CREATE again: under
// IF NOT EXISTS a no-op on every RequeueAfter, without it refused with
// "already exists", which turned the database Failed on the second pass.
func TestDatabaseReconcile_MixedCaseNameIsCreatedOnce(t *testing.T) {
	for _, ifNotExists := range []bool{true, false} {
		t.Run(fmt.Sprintf("ifNotExists=%v", ifNotExists), func(t *testing.T) {
			server := &databaseServer{}
			db := ordersDatabase("Orders")
			db.Spec.IfNotExists = ifNotExists
			r, _ := databaseTestReconciler(t, server, readyDatabaseHost(), db)
			ctx := context.Background()

			for pass := 1; pass <= 3; pass++ {
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(db)})
				require.NoError(t, err, "pass %d", pass)
			}

			assert.Equal(t, []string{"CREATE DATABASE Orders"}, server.statements,
				"one CREATE, on the first pass; the database keeps the name as spec writes it")
			got := &neo4jv1beta1.Neo4jDatabase{}
			require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(db), got))
			assert.Equal(t, "Ready", got.Status.Phase)
			assert.Equal(t, "online", got.Status.State)
		})
	}
}

// namedReplicaServer answers GetDatabaseInfo by name, as the real client does
// (neo4jclient.FindDatabase), and refuses CREATE REPLICA DATABASE for a name
// that exists in any case, as Neo4j does: the statement has no IF NOT EXISTS.
type namedReplicaServer struct {
	rows    []neo4jclient.DatabaseInfo
	created []string
}

func (s *namedReplicaServer) GetDatabaseInfo(_ context.Context, name string) (*neo4jclient.DatabaseInfo, error) {
	return neo4jclient.FindDatabase(s.rows, name), nil
}

func (s *namedReplicaServer) create(name string) error {
	s.created = append(s.created, name)
	for _, row := range s.rows {
		if strings.EqualFold(row.Name, name) {
			return fmt.Errorf("failed to create the specified database '%s': database already exists", strings.ToLower(name))
		}
	}
	s.rows = append(s.rows, neo4jclient.DatabaseInfo{Name: strings.ToLower(name), Type: neo4jclient.DatabaseTypeReplica,
		Status: "online", RequestedStatus: "online"})
	return nil
}

func (s *namedReplicaServer) CreateReplicaDatabaseFromBackup(_ context.Context, name string, _ neo4jclient.ReplicaBackupSource) error {
	return s.create(name)
}

func (s *namedReplicaServer) CreateReplicaDatabaseFromNetwork(_ context.Context, name string, _ neo4jclient.ReplicaNetworkSource) error {
	return s.create(name)
}

func (s *namedReplicaServer) Close() error { return nil }

// A replica named `Ledger` is listed as `ledger`. GetDatabaseInfo matched
// exactly, so the pass after the replica was created found it absent and
// created it again, which Neo4j refused: the replica went Failed.
func TestReplicaReconcile_MixedCaseNameIsCreatedOnce(t *testing.T) {
	const ns = "replica-name-case"
	replica := newReplicaForMetrics("ledger-replica", ns, "dr-cluster", "Ledger", "")
	replica.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
		Mode:      neo4jv1beta1.ReplicaSourceModeNetwork,
		Addresses: []string{"upstream.example.com:7688"},
	}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "dr-cluster", Namespace: ns},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.0-enterprise"},
		},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			Conditions: []metav1.Condition{{
				Type: ConditionTypeReady, Status: metav1.ConditionTrue, Reason: "ClusterReady",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}
	fc := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithObjects(replica, cluster).WithStatusSubresource(replica, cluster).Build()
	server := &namedReplicaServer{}
	r := &Neo4jReplicaDatabaseReconciler{
		Client:   fc,
		Recorder: record.NewFakeRecorder(100),
		newNeo4jClient: func(ResolvedTarget, client.Client) (replicaNeo4jClient, error) {
			return server, nil
		},
	}
	t.Cleanup(func() { withdrawReplicaLag(replica) })

	for pass := 1; pass <= 3; pass++ {
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: replica.Name}})
		require.NoError(t, err, "pass %d", pass)
	}

	assert.Equal(t, []string{"Ledger"}, server.created, "one CREATE REPLICA DATABASE")
	got := &neo4jv1beta1.Neo4jReplicaDatabase{}
	require.NoError(t, fc.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: replica.Name}, got))
	assert.NotEqual(t, neo4jv1beta1.ReplicaPhaseFailed, got.Status.Phase, got.Status.Message)
}

// A point-in-time restore decides online or offline from whether the database
// exists. Restoring `AsOf` over the existing `asof`, an exact match said
// absent: the restore went online and its CREATE DATABASE, which a point in
// time needs, failed on the existing database. It is the same database, so a
// standalone takes the offline path and a cluster is told why it cannot.
func TestDecidePointInTimePath_MixedCaseNameOverAnExistingDatabase(t *testing.T) {
	listing := []neo4jclient.DatabaseInfo{{Name: "system"}, {Name: "asof"}}
	existsInListing := func(_ context.Context, _ restoreTarget, name string) (bool, error) {
		return neo4jclient.FindDatabase(listing, name) != nil, nil
	}

	t.Run("standalone: offline, with the reason", func(t *testing.T) {
		restore := pitRestore("s3")
		restore.Spec.Database = "AsOf"
		standalone := minimalStandaloneForRestore("sa", "ns")
		standalone.Spec.Image.Tag = "2026.08.1-enterprise"
		r := statusRestoreReconciler(t, restore, standalone)

		_, done, err := r.decidePointInTimePathWith(context.Background(), restore, existsInListing)
		require.NoError(t, err)
		require.False(t, done)
		assert.Contains(t, standaloneOfflineReason(restore, nil), `database "AsOf" exists`)
	})

	t.Run("cluster: Failed with the reason, not an online CREATE", func(t *testing.T) {
		restore := pitRestore("s3")
		restore.Spec.Database = "AsOf"
		restore.Spec.InstanceRef = "c"
		cluster := minimalClusterForRestore("c", "ns")
		cluster.Spec.Image.Tag = "2026.08.1-enterprise"
		r := statusRestoreReconciler(t, restore, cluster)

		_, done, err := r.decidePointInTimePathWith(context.Background(), restore, existsInListing)
		require.NoError(t, err)
		require.True(t, done)
		got := &neo4jv1beta1.Neo4jRestore{}
		require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "r", Namespace: "ns"}, got))
		assert.Equal(t, StatusFailed, got.Status.Phase)
		assert.Contains(t, got.Status.Message, `database "AsOf" exists`)
	})
}

// The guard that sends a sharded database's restore to Neo4jShardedDatabase
// matched spec.name exactly, so a restore of `Products` passed it into the
// recreate path dbms.recreateDatabase cannot do for a sharded database.
func TestValidateRestore_ShardedGuardIgnoresCase(t *testing.T) {
	sharded := &neo4jv1beta1.Neo4jShardedDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "catalogue", Namespace: "default"},
		Spec:       neo4jv1beta1.Neo4jShardedDatabaseSpec{ClusterRef: "c", Name: "products"},
	}
	for _, name := range []string{"Products", "Catalogue"} {
		restore := &neo4jv1beta1.Neo4jRestore{
			ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jRestoreSpec{
				InstanceRef: "c",
				Database:    name,
				Source:      neo4jv1beta1.RestoreSource{Type: "storage", Storage: &renamePVC, BackupPath: "nightly"},
			},
		}
		err := newResolvedSourceReconciler(t, sharded).validateRestore(context.Background(), restore)
		if assert.Error(t, err, name) {
			assert.Contains(t, err.Error(), "is a Neo4jShardedDatabase")
		}
	}
}

// neo4j-admin names artifacts after the database as Neo4j stores it,
// lower-cased; a restore may spell it with capitals.
func TestRestoreArtifactLookup_IgnoresCase(t *testing.T) {
	artifacts := []neo4jv1beta1.DatabaseArtifact{
		{Database: "system", Filename: "system-2026-10-06T12-00-00.backup"},
		{Database: "orders", Filename: "orders-2026-10-06T12-00-00.backup", Type: "FULL"},
	}
	assert.Equal(t, "orders-2026-10-06T12-00-00.backup", artifactForDatabase(artifacts, "Orders"))
	assert.Empty(t, artifactForDatabase(artifacts, "order"))

	restore := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{DatabaseArtifacts: artifacts})
	restore.Spec.Database = "Orders"
	got := restoreSeedArtifacts(restore, resolvedBackupSnapshot(restore))
	require.Len(t, got, 1, "the database's entry in an all-databases backup")
	assert.Equal(t, "orders-2026-10-06T12-00-00.backup", got[0].Filename)

	// A sourceDatabase that is another spelling of the target restores under
	// the same name, which neo4j-admin does from a directory.
	sameName := minimalRestore("r", "ns", "sa")
	sameName.Spec.Database = "orders"
	sameName.Spec.Source.SourceDatabase = "Orders"
	path, err := standaloneRestoreFromPath(sameName, "s3://bucket/backups", false)
	require.NoError(t, err)
	assert.Equal(t, shellQuote("s3://bucket/backups"), path)

	// The single artifact of a one-database backup holds `orders`;
	// sourceDatabase `Orders` names it.
	single := pinnedBackupRestore("s3", neo4jv1beta1.ResolvedRestoreSource{ArtifactFilename: "orders-2026-10-06T12-00-00.backup"})
	single.Spec.Database = "restored"
	single.Spec.Source.SourceDatabase = "Orders"
	file, err := standaloneRestoreArtifact(single, "s3://bucket/backups")
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/backups/orders-2026-10-06T12-00-00.backup", file)
}

// SHOW ALIASES lists composite `Cineasts`'s constituent `Latest` as
// `cineasts.latest`, composite `cineasts`. Matched exactly, none of a
// capitalised composite's constituents was found, so CREATE ALIAS (IF NOT
// EXISTS) and a ConstituentAdded event were sent for each on every pass.
func TestClassifyAliases_IgnoresCase(t *testing.T) {
	live := []neo4jclient.AliasInfo{
		{Name: "cineasts.latest", Composite: "cineasts", Database: "movies", Location: "local"},
		{Name: "cineasts.fake", Database: "movies"},
		{Name: "other.latest", Composite: "other", Database: "movies"},
	}
	existing, lookalikes := classifyAliases(live, "Cineasts")
	require.Contains(t, existing, "latest")
	assert.Len(t, existing, 1)
	assert.Equal(t, []string{"cineasts.fake"}, lookalikes)

	// The constituent spec calls `Latest` is the one listed, and neither
	// drifted nor unwanted.
	spec := []neo4jv1beta1.CompositeConstituent{{Name: "Latest", TargetDatabase: "Movies"}}
	current, present := existing[strings.ToLower(spec[0].Name)]
	require.True(t, present)
	assert.False(t, localConstituentDrifted(current, spec[0]), "`Movies` is the listed `movies`")
	assert.Empty(t, undesiredConstituents(existing, spec),
		"enforceConstituents dropped `latest` as not in spec, and the next pass created it again")
}

func TestUndesiredConstituents(t *testing.T) {
	existing := map[string]neo4jclient.AliasInfo{"latest": {}, "old": {}, "archive": {}}
	assert.Equal(t, []string{"archive", "old"},
		undesiredConstituents(existing, []neo4jv1beta1.CompositeConstituent{{Name: "LATEST"}}),
		"sorted, by the key classifyAliases gives them")
}

func TestConstituentDrift_IgnoresTheTargetsCase(t *testing.T) {
	local := neo4jclient.AliasInfo{Database: "movies", Location: "local"}
	assert.False(t, localConstituentDrifted(local, neo4jv1beta1.CompositeConstituent{TargetDatabase: "Movies"}))
	assert.True(t, localConstituentDrifted(local, neo4jv1beta1.CompositeConstituent{TargetDatabase: "films"}))
	assert.True(t, localConstituentDrifted(neo4jclient.AliasInfo{Database: "movies", Location: "remote"},
		neo4jv1beta1.CompositeConstituent{TargetDatabase: "movies"}), "remote to local is a real change")

	remote := neo4jclient.AliasInfo{Database: "movies", Location: "remote", URL: "neo4j+s://far:7687", User: "reader"}
	c := neo4jv1beta1.CompositeConstituent{TargetDatabase: "Movies", Remote: &neo4jv1beta1.RemoteConstituent{URL: "neo4j+s://far:7687"}}
	assert.False(t, remoteConstituentDrifted(remote, c, neo4jclient.RemoteConstituentAuth{Username: "reader"}))
}

// A composite spelled `Cineasts` is blocked by the alias `cineasts.x` as
// surely as one spelled in lower case.
func TestAliasInNamespace_IgnoresCase(t *testing.T) {
	aliases := []neo4jclient.AliasInfo{{Name: "cineastsx"}, {Name: "cineasts.x"}}
	assert.Equal(t, "cineasts.x", aliasInNamespace(aliases, "Cineasts"))
	assert.Empty(t, aliasInNamespace(aliases, "movies"))
}

// The alias controller re-pointed an alias whose target spec spells `Orders`
// on every pass, with an AliasRetargeted event, since SHOW ALIASES lists the
// target as `orders`.
func TestAliasTargetDiffers(t *testing.T) {
	assert.False(t, aliasTargetDiffers("orders", "Orders"))
	assert.True(t, aliasTargetDiffers("orders", "orders-dr"))
}

// SHOW USERS lists the home database spec spells `Orders` as `orders`; the
// user controller re-sent ALTER USER … SET HOME DATABASE on every pass.
func TestComputeAlter_HomeDatabaseIgnoresCase(t *testing.T) {
	r := &Neo4jUserReconciler{}
	user := &neo4jv1beta1.Neo4jUser{Spec: neo4jv1beta1.Neo4jUserSpec{HomeDatabase: "Orders"}}

	opts := r.computeAlter(user, &neo4jclient.UserInfo{HomeDatabase: "orders"}, "", "", false)
	assert.True(t, opts.IsEmpty(), "nothing to change")

	opts = r.computeAlter(user, &neo4jclient.UserInfo{HomeDatabase: "sales"}, "", "", false)
	assert.False(t, opts.IsEmpty(), "another database: SET HOME DATABASE")
}

// A GRAPH grant on a composite is inert, and the role controller warns about
// it — only when the privilege spelled the composite's name in lower case.
func TestCompositesAmong_IgnoresCase(t *testing.T) {
	databases := []neo4jclient.DatabaseInfo{
		{Name: "travel", Type: neo4jclient.CompositeDatabaseType},
		{Name: "orders", Type: "standard"},
	}
	assert.Equal(t, []string{"Travel"}, compositesAmong([]string{"Travel", "Orders"}, databases),
		"as the privilege wrote it")
}

// A Neo4jBackup of `Orders` whose artifact neo4j-admin names `orders-…`: the
// run captured no artifact (and so no type), which a later restore needs.
func TestBackupLogParsers_IgnoreCase(t *testing.T) {
	log := "Start full backup of database 'orders'.\n" +
		"Finished artifact creation 'orders-2026-10-06T07-36-03.backup' for database 'orders', took 38ms.\n"
	assert.Equal(t, "orders-2026-10-06T07-36-03.backup", parseStandardArtifactFromLog(log, "Orders"))

	run := neo4jv1beta1.BackupRun{ArtifactFilename: "orders-2026-10-06T07-36-03.backup"}
	recordArtifactTypes(&run, log, "Orders")
	assert.Equal(t, backupArtifactFull, run.ArtifactType)
}

// neo4j-admin matches the backup glob `Products*` ignoring case, so it takes
// `productsales` along; the guard that refuses such a glob compared the name
// as written and matched nothing.
func TestShardedGlobPoisoning_IgnoresCase(t *testing.T) {
	dbs := []neo4jclient.DatabaseInfo{
		{Name: "system"}, {Name: "products-g000"}, {Name: "products-p000"}, {Name: "products-p000"},
		{Name: "productsales"}, {Name: "products"},
	}
	assert.Equal(t, []string{"productsales"}, shardedGlobPoisoning("Products", dbs))
	assert.Equal(t, []string{"productsales"}, shardedGlobPoisoning("products", dbs))
	assert.Empty(t, shardedGlobPoisoning("Products", dbs[:4]), "the family alone")
}

// An all-databases backup catalogues the sharded family as neo4j-admin listed
// it; a Neo4jShardedDatabase may spell its seed source with capitals.
func TestFindFamilyArtifacts_IgnoresCase(t *testing.T) {
	run := &neo4jv1beta1.BackupRun{ShardedFamilies: []neo4jv1beta1.ShardedFamilyArtifacts{{
		Family:         "products",
		ShardArtifacts: []neo4jv1beta1.ShardArtifact{{ShardName: "products-g000", Filename: "products-g000-2026-10-06T07-36-03.backup"}},
	}}}
	got := findFamilyArtifacts(run, "Products")
	require.Len(t, got, 1)
	assert.Equal(t, "products-g000", got[0].ShardName)
	assert.Nil(t, findFamilyArtifacts(run, "product"))
}
