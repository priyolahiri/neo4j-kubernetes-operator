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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Knowledge rule 145. A Warning about a condition that lasts — a precondition
// the user has to put in place, a refusal, a failure the controller retries —
// was emitted on every pass that met it: every RequeueAfter, every backoff
// retry, every resync. Each one spends the object's event budget (client-go's
// spam filter: a burst of 25 per object, then one every five minutes), so the
// repeats crowd out the events that report real changes. The status write that
// records the condition now says whether it is news, and the Warning follows
// it; an advisory that stays true goes through advisoryWarnings. These run each
// emitter over several passes: one event, and none from a restarted operator.

const repeatPasses = 3

// reconcileTimes runs r over key n times and returns the errors it returned.
func reconcileTimes(t *testing.T, r reconcile.Reconciler, key client.ObjectKey, n int) []error {
	t.Helper()
	var errs []error
	for range n {
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
		errs = append(errs, err)
	}
	return errs
}

// --- Neo4jEnterpriseCluster ------------------------------------------------

func TestClusterReconcile_PreconditionFailuresAreAnnouncedOnce(t *testing.T) {
	base := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		return &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default", Generation: 1},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
				Topology:               neo4jv1beta1.TopologyConfiguration{Servers: 3},
				Storage:                neo4jv1beta1.StorageSpec{Size: "1Gi"},
			},
		}
	}
	cases := []struct {
		name      string
		reason    string
		validator bool
		cluster   func() *neo4jv1beta1.Neo4jEnterpriseCluster
	}{
		{
			name: "server role hint", reason: EventReasonServerRoleFailed, validator: true,
			cluster: func() *neo4jv1beta1.Neo4jEnterpriseCluster {
				c := base()
				c.Spec.Topology.ServerRoles = []neo4jv1beta1.ServerRoleHint{{ServerIndex: 0, ModeConstraint: "LEADER"}}
				return c
			},
		},
		{
			name: "property sharding", reason: EventReasonPropertyShardingFailed,
			cluster: func() *neo4jv1beta1.Neo4jEnterpriseCluster {
				c := base()
				c.Spec.PropertySharding = &neo4jv1beta1.PropertyShardingSpec{Enabled: true}
				return c
			},
		},
		{
			name: "missing StorageClass", reason: EventReasonStorageClassNotFound,
			cluster: func() *neo4jv1beta1.Neo4jEnterpriseCluster {
				c := base()
				c.Spec.Storage.ClassName = "fast-ssd"
				return c
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := tc.cluster()
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(cluster).
				WithStatusSubresource(cluster).Build()
			rec := record.NewFakeRecorder(100)
			newReconciler := func() *Neo4jEnterpriseClusterReconciler {
				r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, RequeueAfter: time.Minute}
				if tc.validator {
					r.Validator = validation.NewClusterValidator(c)
				}
				return r
			}
			key := client.ObjectKeyFromObject(cluster)

			for _, err := range reconcileTimes(t, newReconciler(), key, repeatPasses) {
				require.NoError(t, err)
			}
			events := drainEvents(rec)
			assert.Len(t, eventsWithReason(events, tc.reason), 1, "announced once, not on every pass: %q", events)
			live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			require.NoError(t, c.Get(context.Background(), key, live))
			assert.Equal(t, "Failed", live.Status.Phase)

			reconcileTimes(t, newReconciler(), key, 1)
			assert.Empty(t, eventsWithReason(drainEvents(rec), tc.reason), "a restarted operator reads the failure from status")
		})
	}
}

// A storage expansion the StorageClass refuses was retried by writing
// Expanding and then Failed on every pass — two status writes, each a watch
// event that ran the next pass at once — with StorageExpansionStarted and
// StorageExpansionFailed each time. A retry keeps the failure in status.
func TestClusterStorageExpansion_ARefusedExpansionIsAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	scheme := newStorageTestScheme()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default", Generation: 1},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
			Storage:  neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "20Gi"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, makeStorageClass("standard", false), makeStatefulSet("ec-server", "default"),
			makePVC("ec", "default", "ec-server", "data", "standard", "10Gi", 0, true)).
		WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(100)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}
	key := client.ObjectKeyFromObject(cluster)

	pass := func() string {
		t.Helper()
		live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, c.Get(ctx, key, live)) // what the pass reads first
		_, err := r.reconcileStorageExpansion(ctx, live)
		require.Error(t, err)
		require.NoError(t, c.Get(ctx, key, live))
		return live.ResourceVersion
	}
	pass()
	afterFirst := pass()
	assert.Equal(t, afterFirst, pass(), "a retry writes no status: the phase no longer flips Expanding/Failed")
	events := drainEvents(rec)
	assert.Len(t, eventsWithReason(events, EventReasonStorageExpansionFailed), 1, "%q", events)
	assert.Len(t, eventsWithReason(events, EventReasonStorageExpansionStarted), 1, "%q", events)

	// A new generation (the user edits the spec) is a new attempt.
	live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, c.Get(ctx, key, live))
	live.Spec.Storage.Size = "30Gi"
	live.Generation = 2
	require.NoError(t, c.Update(ctx, live))
	pass()
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonStorageExpansionStarted), 1)
}

func TestClusterStorageExpansion_AShrinkIsAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	scheme := newStorageTestScheme()
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ec", Namespace: "default", Generation: 1},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: 3},
			Storage:  neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "5Gi"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, makeStorageClass("standard", true),
			makePVC("ec", "default", "ec-server", "data", "standard", "10Gi", 0, true)).
		WithStatusSubresource(cluster).Build()
	rec := record.NewFakeRecorder(100)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}
	for range repeatPasses {
		live := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cluster), live))
		_, err := r.reconcileStorageExpansion(ctx, live)
		require.Error(t, err)
	}
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonStorageExpansionFailed), 1)
}

// --- Advisory warnings that stay true (knowledge rule 102) -----------------

func TestAdvisoryEmitters_AnnounceOnceWhileTheyApply(t *testing.T) {
	ctx := context.Background()

	// The Route and the MCP Route both report RouteAPINotFound, each on every
	// pass. This pins the scoping the two call sites rely on — with one shared
	// set, each check's pass would make the other's warning look new, and both
	// would repeat on every pass. TestReconcileRoute_NoRouteAPI drives the call
	// sites themselves.
	t.Run("RouteAPINotFound, the Route and the MCP Route kept apart", func(t *testing.T) {
		rec := record.NewFakeRecorder(20)
		var w advisoryWarnings
		cluster := minimalCluster("ec", "default")
		for range repeatPasses {
			w.recordScoped(rec, cluster, EventReasonRouteAPINotFound, "route",
				[]string{"route.openshift.io/v1 not available; skipping Route reconciliation"})
			w.recordScoped(rec, cluster, EventReasonRouteAPINotFound, "mcp-route",
				[]string{"route.openshift.io/v1 not available; skipping MCP Route reconciliation"})
		}
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonRouteAPINotFound), 2,
			"one for the Route, one for the MCP Route")

		w.recordScoped(rec, cluster, EventReasonRouteAPINotFound, "route", nil)
		w.recordScoped(rec, cluster, EventReasonRouteAPINotFound, "route",
			[]string{"route.openshift.io/v1 not available; skipping Route reconciliation"})
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonRouteAPINotFound), 1,
			"a warning that stopped applying and came back is announced again")
	})

	t.Run("MCPApocMissing, forgotten once APOC is there", func(t *testing.T) {
		cluster := minimalCluster("ec", "default")
		cluster.Spec.MCP = &neo4jv1beta1.MCPServerSpec{Enabled: true}
		standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
			Spec:       neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{MCP: &neo4jv1beta1.MCPServerSpec{Enabled: true}},
		}
		c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(cluster, standalone).Build()
		rec := record.NewFakeRecorder(20)
		cr := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
		sr := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
		for range repeatPasses {
			cr.warnIfMCPMissingAPOC(ctx, cluster)
			sr.warnIfMCPMissingAPOC(ctx, standalone)
		}
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonMCPApocMissing), 2, "one per deployment")

		apoc := &neo4jv1beta1.Neo4jPlugin{
			ObjectMeta: metav1.ObjectMeta{Name: "apoc", Namespace: "default"},
			Spec:       neo4jv1beta1.Neo4jPluginSpec{ClusterRef: "ec", Name: "apoc", Enabled: true},
		}
		require.NoError(t, c.Create(ctx, apoc))
		cr.warnIfMCPMissingAPOC(ctx, cluster)
		require.NoError(t, c.Delete(ctx, apoc))
		cr.warnIfMCPMissingAPOC(ctx, cluster)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonMCPApocMissing), 1,
			"a warning that stopped applying and came back is announced again")
	})

	t.Run("TopologyZoneDiscoveryDegraded", func(t *testing.T) {
		rec := record.NewFakeRecorder(20)
		r := &Neo4jEnterpriseClusterReconciler{Recorder: rec}
		cluster := minimalCluster("ec", "default")
		for range repeatPasses {
			r.recordZoneDiscovery(cluster, &TopologyPlacement{ZoneDiscoveryDegraded: true})
		}
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonTopologyZoneDiscoveryDegraded), 1)
	})

	t.Run("UpgradeFailed while the pre-upgrade check blocks the upgrade", func(t *testing.T) {
		standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
				Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
				UpgradeStrategy: &neo4jv1beta1.UpgradeStrategySpec{
					PreUpgradeHealthCheck: true, AutoPauseOnFailure: true,
				},
			},
			Status: neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: "Ready", Version: "5.26.0-enterprise"},
		}
		// No admin Secret: the check cannot connect, on every pass.
		c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(standalone).Build()
		rec := record.NewFakeRecorder(20)
		r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
		for range repeatPasses {
			assert.True(t, r.preUpgradeHealthCheck(ctx, standalone), "the upgrade stays blocked")
		}
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonUpgradeFailed), 1)
	})

	t.Run("BackupValidateUnsupported", func(t *testing.T) {
		rec := record.NewFakeRecorder(20)
		r := &Neo4jBackupReconciler{Recorder: rec}
		backup := &neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
			Spec:       neo4jv1beta1.Neo4jBackupSpec{Options: &neo4jv1beta1.BackupOptions{Validate: ptr.To(true)}},
		}
		cluster := minimalCluster("ec", "default")
		for range repeatPasses {
			r.warnValidateUnsupported(backup, cluster)
		}
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonBackupValidateUnsupported), 1,
			"the CronJob path reaches it on every scheduled pass")
	})

	t.Run("ServiceAccountAnnotationConflict between two backups", func(t *testing.T) {
		withIdentity := func(name, role string) *neo4jv1beta1.Neo4jBackup {
			return &neo4jv1beta1.Neo4jBackup{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec: neo4jv1beta1.Neo4jBackupSpec{Storage: neo4jv1beta1.StorageLocation{Type: "s3",
					Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws", Identity: &neo4jv1beta1.CloudIdentity{
						AutoCreate: &neo4jv1beta1.AutoCreateSpec{Annotations: map[string]string{"eks.amazonaws.com/role-arn": role}},
					}}}},
			}
		}
		a, b := withIdentity("a", "arn:aws:iam::1:role/a"), withIdentity("b", "arn:aws:iam::1:role/b")
		c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).Build()
		rec := record.NewFakeRecorder(50)
		r := &Neo4jBackupReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
		for range repeatPasses {
			require.NoError(t, r.ensureBackupServiceAccount(ctx, a))
			require.NoError(t, r.ensureBackupServiceAccount(ctx, b))
		}
		conflicts := eventsWithReason(drainEvents(rec), EventReasonServiceAccountAnnotationConflict)
		assert.Len(t, conflicts, 2, "the two CRs overwrite each other on every pass; each says so once: %q", conflicts)
	})
}

// --- Neo4jEnterpriseStandalone ---------------------------------------------

func TestStandaloneReconcile_PreconditionFailuresAreAnnouncedOnce(t *testing.T) {
	newStandalone := func() *neo4jv1beta1.Neo4jEnterpriseStandalone {
		return &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default", Generation: 1, Finalizers: []string{StandaloneFinalizer}},
			Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
				AcceptLicenseAgreement: "eval",
				Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
				Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "5Gi"},
			},
		}
	}
	run := func(t *testing.T, objs []client.Object, reasons ...string) {
		t.Helper()
		standalone := newStandalone()
		scheme := newStandaloneTestScheme()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objs, standalone)...).
			WithStatusSubresource(standalone).Build()
		rec := record.NewFakeRecorder(100)
		newReconciler := func() *Neo4jEnterpriseStandaloneReconciler {
			return &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: rec,
				RequeueAfter: time.Minute, Validator: validation.NewStandaloneValidator()}
		}
		key := client.ObjectKeyFromObject(standalone)
		reconcileTimes(t, newReconciler(), key, repeatPasses)
		events := drainEvents(rec)
		for _, reason := range reasons {
			assert.Len(t, eventsWithReason(events, reason), 1, "%s announced once: %q", reason, events)
		}
		live := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		require.NoError(t, c.Get(context.Background(), key, live))
		assert.Equal(t, "Failed", live.Status.Phase)

		reconcileTimes(t, newReconciler(), key, 1)
		events = drainEvents(rec)
		for _, reason := range reasons {
			assert.Empty(t, eventsWithReason(events, reason), "a restarted operator reads the failure from status")
		}
	}

	t.Run("missing StorageClass", func(t *testing.T) {
		run(t, nil, EventReasonStorageClassNotFound)
	})
	t.Run("storage shrink, retried with backoff", func(t *testing.T) {
		run(t, []client.Object{makeStorageClass("standard", true), makeStandalonePVC("sa", "default", "standard", "10Gi", 0, true)},
			EventReasonStorageExpansionFailed, EventReasonReconcileFailed)
	})
}

// --- Neo4jDatabase -----------------------------------------------------------

func TestDatabaseReconcile_WaitsAreAnnouncedOnce(t *testing.T) {
	t.Run("host not found: Pending, one ClusterNotFound", func(t *testing.T) {
		db := ordersDatabase("orders")
		r, rec := databaseTestReconciler(t, &databaseServer{}, db)
		key := client.ObjectKeyFromObject(db)
		reconcileTimes(t, r, key, repeatPasses)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound), 1)
		assert.Equal(t, "Pending", databasePhase(t, r, key))

		restarted, _ := databaseTestReconciler(t, &databaseServer{})
		restarted.Client, restarted.Recorder = r.Client, rec
		reconcileTimes(t, restarted, key, 1)
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound))
	})

	t.Run("seed credentials not on the host: Pending, one SeedCredsMissing, then it carries on", func(t *testing.T) {
		db := ordersDatabase("orders")
		db.Spec.SeedURI = "s3://backups/orders.backup"
		db.Spec.SeedCredentials = &neo4jv1beta1.SeedCredentials{SecretRef: "s3-creds"}
		server := &databaseServer{}
		r, rec := databaseTestReconciler(t, server, readyDatabaseHost(), db)
		key := client.ObjectKeyFromObject(db)
		reconcileTimes(t, r, key, repeatPasses)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonSeedCredsMissing), 1)
		assert.Equal(t, "Pending", databasePhase(t, r, key), "a precondition the user can fix is a wait, not a failure")
		assert.Empty(t, server.statements, "nothing is asked of Neo4j while it waits")

		host := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "ec", Namespace: "default"}, host))
		host.Spec.ExtraEnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "s3-creds"}}}}
		require.NoError(t, r.Update(context.Background(), host))
		reconcileTimes(t, r, key, 1)
		assert.Equal(t, "Ready", databasePhase(t, r, key))
		assert.Len(t, server.statements, 1, "created once the host has the Secret")
	})
}

func databasePhase(t *testing.T, r *Neo4jDatabaseReconciler, key client.ObjectKey) string {
	t.Helper()
	db := &neo4jv1beta1.Neo4jDatabase{}
	require.NoError(t, r.Get(context.Background(), key, db))
	return db.Status.Phase
}

// --- Neo4jShardedDatabase ----------------------------------------------------

// failingShardedServer refuses SHOW DATABASES the way a server does for a
// reason that will not go away by itself.
type failingShardedServer struct{ shardedServer }

func (s *failingShardedServer) GetDatabases(context.Context) ([]neo4jclient.DatabaseInfo, error) {
	return nil, errors.New("Neo.ClientError.Security.Forbidden: permission denied")
}

func TestShardedReconcile_LastingFailuresAreAnnouncedOnce(t *testing.T) {
	type tc struct {
		name    string
		reason  string
		phase   string
		objs    func() []client.Object
		client  func(server *shardedServer) func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error)
		mutate  func(*neo4jv1beta1.Neo4jShardedDatabase)
		errored bool
	}
	scripted := func(server *shardedServer) func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) {
		return func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) { return server, nil }
	}
	cases := []tc{
		{
			name: "cluster not found is a wait", reason: EventReasonClusterNotFound, phase: "Pending",
			objs: func() []client.Object { return nil }, client: scripted,
		},
		{
			name: "cluster without property sharding", reason: EventReasonClusterNotReady, phase: "Failed",
			objs: func() []client.Object {
				c := shardingCluster()
				c.Spec.PropertySharding = nil
				return []client.Object{c}
			},
			client: scripted,
		},
		{
			name: "client cannot be created", reason: EventReasonClientCreationFailed, phase: "Failed", errored: true,
			objs: func() []client.Object { return []client.Object{shardingCluster()} },
			client: func(*shardedServer) func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) {
				return func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) {
					return nil, errors.New("admin Secret sc-admin-secret not found")
				}
			},
		},
		{
			name: "seedBackupRef names no backup", reason: EventReasonSeedBackupResolutionFailed, phase: "Failed",
			objs:   func() []client.Object { return []client.Object{shardingCluster()} },
			client: scripted,
			mutate: func(sd *neo4jv1beta1.Neo4jShardedDatabase) { sd.Spec.SeedBackupRef = "no-such-backup" },
		},
		{
			name: "reconcile refused by the server", reason: EventReasonReconcileFailed, phase: "Failed", errored: true,
			objs: func() []client.Object { return []client.Object{shardingCluster()} },
			client: func(*shardedServer) func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) {
				return func(*neo4jv1beta1.Neo4jEnterpriseCluster) (shardedNeo4jClient, error) {
					return &failingShardedServer{}, nil
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sd := productsShardedDB()
			if c.mutate != nil {
				c.mutate(sd)
			}
			server := &shardedServer{name: "products"}
			r, rec := shardedTestReconciler(t, server, append(c.objs(), sd)...)
			r.newNeo4jClient = c.client(server)
			key := client.ObjectKeyFromObject(sd)
			for _, err := range reconcileTimes(t, r, key, repeatPasses) {
				if c.errored {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
			events := drainEvents(rec)
			assert.Len(t, eventsWithReason(events, c.reason), 1, "announced once, not on every pass: %q", events)
			assert.Equal(t, c.phase, shardedPhase(t, r, key))

			restarted := *r
			reconcileTimes(t, &restarted, key, 1)
			assert.Empty(t, eventsWithReason(drainEvents(rec), c.reason), "a restarted operator reads it from status")
		})
	}
}

func TestShardedReconcile_AMissingClusterIsAWaitThatEnds(t *testing.T) {
	sd := productsShardedDB()
	server := &shardedServer{name: "products"}
	r, rec := shardedTestReconciler(t, server, sd)
	key := client.ObjectKeyFromObject(sd)
	reconcileTimes(t, r, key, repeatPasses)
	assert.Equal(t, "Pending", shardedPhase(t, r, key))
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound), 1)

	require.NoError(t, r.Create(context.Background(), shardingCluster()))
	reconcileTimes(t, r, key, 1)
	assert.Equal(t, "Ready", shardedPhase(t, r, key))
	assert.Equal(t, 1, server.count("CREATE DATABASE"))
}

// A cloud seedBackupRef whose cluster lacks the credentials Secret waited in
// phase Failed while it retried every RequeueAfter, with SeedCredsMissing (a
// raw reason string) on every pass. It waits in Pending, says so once, and
// creates the database once the cluster has the Secret.
func TestShardedReconcile_MissingSeedCredentialsAreAWait(t *testing.T) {
	ctx := context.Background()
	sd := productsShardedDB()
	sd.Spec.SeedBackupRef = "products-backup"
	backup := &neo4jv1beta1.Neo4jBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "products-backup", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jBackupSpec{
			InstanceRef:     "sc",
			ShardedDatabase: "products",
			Storage: neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "backups",
				Cloud: &neo4jv1beta1.CloudBlock{Provider: "aws", CredentialsSecretRef: "s3-creds"}},
		},
		Status: neo4jv1beta1.Neo4jBackupStatus{History: []neo4jv1beta1.BackupRun{{
			RunID: "products-backup-1", Status: "Succeeded", BackupsPath: "products-backup",
		}}},
	}
	server := &shardedServer{name: "products"}
	r, rec := shardedTestReconciler(t, server, shardingCluster(), sd, backup)
	key := client.ObjectKeyFromObject(sd)

	for _, err := range reconcileTimes(t, r, key, repeatPasses) {
		require.NoError(t, err)
	}
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonSeedCredsMissing), 1)
	assert.Equal(t, "Pending", shardedPhase(t, r, key), "a precondition the user can fix is a wait, not a failure")
	assert.Zero(t, server.count("CREATE DATABASE"))

	restarted := *r
	reconcileTimes(t, &restarted, key, 1)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonSeedCredsMissing))

	// The user projects the Secret; the servers roll out with it.
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "sc", Namespace: "default"}, cluster))
	cluster.Spec.ExtraEnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
		LocalObjectReference: corev1.LocalObjectReference{Name: "s3-creds"}}}}
	require.NoError(t, r.Update(ctx, cluster))
	three := int32(3)
	require.NoError(t, r.Create(ctx, &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sc-server", Namespace: "default", Generation: 2},
		Spec: appsv1.StatefulSetSpec{Replicas: &three, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "neo4j", EnvFrom: cluster.Spec.ExtraEnvFrom}}}}},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 2, UpdatedReplicas: 3, ReadyReplicas: 3},
	}))
	reconcileTimes(t, r, key, 1)
	assert.Equal(t, 1, server.count("CREATE DATABASE"), "seeded once the cluster has the credentials")
	assert.Equal(t, "Ready", shardedPhase(t, r, key))
}

func shardedPhase(t *testing.T, r *Neo4jShardedDatabaseReconciler, key client.ObjectKey) string {
	t.Helper()
	sd := &neo4jv1beta1.Neo4jShardedDatabase{}
	require.NoError(t, r.Get(context.Background(), key, sd))
	return sd.Status.Phase
}

// --- Neo4jReplicaDatabase ----------------------------------------------------

func newBackupModeReplicaHarness(t *testing.T, ns, tag string) *lagHarness {
	t.Helper()
	h := newLagHarness(t, ns)
	replica := &neo4jv1beta1.Neo4jReplicaDatabase{}
	require.NoError(t, h.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: h.name}, replica))
	replica.Spec.Source = neo4jv1beta1.ReplicaSourceSpec{
		Mode:    neo4jv1beta1.ReplicaSourceModeBackup,
		PullURI: "s3://chains/ledger/",
	}
	require.NoError(t, h.c.Update(context.Background(), replica))
	if tag != "" {
		cluster := h.cluster()
		cluster.Spec.Image.Tag = tag
		require.NoError(t, h.c.Update(context.Background(), cluster))
	}
	return h
}

func (h *lagHarness) phase() string {
	h.t.Helper()
	replica := &neo4jv1beta1.Neo4jReplicaDatabase{}
	require.NoError(h.t, h.c.Get(context.Background(), types.NamespacedName{Namespace: h.ns, Name: h.name}, replica))
	return replica.Status.Phase
}

func (h *lagHarness) events(reason string) []string {
	h.t.Helper()
	rec, ok := h.r.Recorder.(*record.FakeRecorder)
	require.True(h.t, ok)
	return eventsWithReason(drainEvents(rec), reason)
}

// A backup-mode replica whose downstream servers have no AWS_REGION reported
// Failed while it retried every two minutes on its own. It waits in Pending,
// says so once, and creates the replica once the servers have the region.
func TestReplicaReconcile_MissingRegionIsAWait(t *testing.T) {
	h := newBackupModeReplicaHarness(t, "rep-region", "")
	for range repeatPasses {
		require.NoError(t, h.reconcile())
	}
	assert.Len(t, h.events(EventReasonReplicaObjectStoreEnvMissing), 1)
	assert.Equal(t, neo4jv1beta1.ReplicaPhasePending, h.phase(), "a precondition the user can fix is a wait, not a failure")
	assert.Zero(t, h.server.created)

	restarted := &Neo4jReplicaDatabaseReconciler{Client: h.r.Client, Recorder: h.r.Recorder, newNeo4jClient: h.r.newNeo4jClient}
	_, err := restarted.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: h.ns, Name: h.name}})
	require.NoError(t, err)
	assert.Empty(t, h.events(EventReasonReplicaObjectStoreEnvMissing), "a restarted operator reads the wait from status")

	cluster := h.cluster()
	cluster.Spec.Env = []corev1.EnvVar{{Name: "AWS_REGION", Value: "eu-west-1"}}
	require.NoError(t, h.c.Update(context.Background(), cluster))
	require.NoError(t, h.reconcile())
	assert.Equal(t, 1, h.server.created, "the replica carries on once the servers have the region")
}

func TestReplicaReconcile_LastingFailuresAreAnnouncedOnce(t *testing.T) {
	t.Run("downstream too old", func(t *testing.T) {
		h := newBackupModeReplicaHarness(t, "rep-old", "2026.01.0-enterprise")
		for range repeatPasses {
			require.NoError(t, h.reconcile())
		}
		assert.Len(t, h.events(EventReasonReplicaVersionTooOld), 1)
		assert.Equal(t, neo4jv1beta1.ReplicaPhaseFailed, h.phase())
	})
	t.Run("cannot connect, retried with backoff", func(t *testing.T) {
		h := newBackupModeReplicaHarness(t, "rep-conn", "")
		h.connErr = errors.New("dial tcp 10.0.0.1:7687: connect: connection refused")
		for range repeatPasses {
			require.Error(t, h.reconcile())
		}
		assert.Len(t, h.events(EventReasonConnectionFailed), 1)
	})
}

// --- Neo4jBackup -------------------------------------------------------------

func TestBackupReconcile_LastingWarningsAreAnnouncedOnce(t *testing.T) {
	t.Run("invalid spec", func(t *testing.T) {
		backup := &neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default", Finalizers: []string{BackupFinalizer}},
			Spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef: "ec", AllDatabases: true, Database: "orders",
				Storage: neo4jv1beta1.StorageLocation{Type: "s3", Bucket: "b"},
			},
		}
		r := newBackupTestReconcilerWithStatus(t, backup)
		rec := record.NewFakeRecorder(50)
		r.Recorder = rec
		key := client.ObjectKeyFromObject(backup)
		reconcileTimes(t, r, key, repeatPasses)
		assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonBackupFailed), 1)
		live := &neo4jv1beta1.Neo4jBackup{}
		require.NoError(t, r.Get(context.Background(), key, live))
		assert.Equal(t, neo4jv1beta1.PhaseInvalid, live.Status.Phase)

		restarted := &Neo4jBackupReconciler{Client: r.Client, Scheme: r.Scheme, Recorder: rec}
		reconcileTimes(t, restarted, key, 1)
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonBackupFailed))
	})

	t.Run("replication source on a PVC", func(t *testing.T) {
		backup := &neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "chain", Namespace: "default", Finalizers: []string{BackupFinalizer}},
			Spec: neo4jv1beta1.Neo4jBackupSpec{
				InstanceRef: "not-created-yet", Database: "orders", Mode: validation.BackupModeReplicationSource,
				Schedule: "0 * * * *",
				Storage:  neo4jv1beta1.StorageLocation{Type: "pvc", PVC: &neo4jv1beta1.PVCSpec{Name: "backups"}},
			},
		}
		r := newBackupTestReconcilerWithStatus(t, backup)
		rec := record.NewFakeRecorder(50)
		r.Recorder = rec
		reconcileTimes(t, r, client.ObjectKeyFromObject(backup), repeatPasses)
		events := drainEvents(rec)
		assert.Len(t, eventsWithReason(events, EventReasonReplicationSourceCaveat), 1, "%q", events)
	})
}

// The sharded-run warnings were emitted while a run was being built, inside the
// conflict retry and before the run was known to be recorded: again on each
// conflict, on every pass that deferred the run for its Pod log, and on every
// pass for a run the ten-entry cap drops and the next pass rediscovers. They
// are now collected and emitted for the runs the write kept. (The paths
// themselves need a scripted Pod log, which the fake clientset cannot give.)
func TestRunWarnings_EmitOnlyForRecordedRuns(t *testing.T) {
	backup := &neo4jv1beta1.Neo4jBackup{ObjectMeta: metav1.ObjectMeta{Name: "all", Namespace: "default"}}
	kept := neo4jv1beta1.BackupRun{RunID: "kept"}
	trimmed := neo4jv1beta1.BackupRun{RunID: "trimmed"}
	var warnings runWarnings
	warnings.add(&kept, EventReasonBackupShardedExcluded, "kept run captured products")
	warnings.add(&trimmed, EventReasonBackupShardedExcluded, "trimmed run captured products")
	warnings.add(&kept, EventReasonBackupShardedDifferential, "")

	rec := record.NewFakeRecorder(10)
	warnings.emit(rec, backup, []neo4jv1beta1.BackupRun{kept})
	events := drainEvents(rec)
	require.Len(t, events, 1, "only the run the write kept, and no empty warning: %q", events)
	assert.Contains(t, events[0], "kept run")

	warnings.emit(rec, backup, nil)
	assert.Len(t, drainEvents(rec), 2, "nil: every collected warning (the one-shot path, once recorded)")
}

// --- Neo4jRestore ------------------------------------------------------------

// Both advisories ran on every pass of a restore waiting for its target's seed
// credentials (Rule 138), which lasts until the user acts.
func TestRestoreReconcile_AdvisoriesWhileWaitingAreAnnouncedOnce(t *testing.T) {
	t.Run("RestoreFromChainParent", func(t *testing.T) {
		f := newSeedConfigWaitFixture(t, false)
		require.NoError(t, f.r.Create(f.ctx, &neo4jv1beta1.Neo4jBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "b-diff", Namespace: "ns"},
			Spec:       neo4jv1beta1.Neo4jBackupSpec{ChainFromBackup: "b"},
			Status:     neo4jv1beta1.Neo4jBackupStatus{History: []neo4jv1beta1.BackupRun{{RunID: "d1", Status: "Succeeded"}}},
		}))
		for range repeatPasses {
			f.reconcile()
		}
		assert.Equal(t, neo4jv1beta1.PhasePending, f.restore().Status.Phase, "precondition: the restore waits")
		assert.Len(t, eventsWithReason(drainEvents(f.recorder), EventReasonRestoreFromChainParent), 1)
	})

	t.Run("RestoreShardedDatabasesNotCovered", func(t *testing.T) {
		f := newSeedConfigWaitFixture(t, true)
		restore := f.restore()
		restore.Status.ResolvedSource.ShardedDatabasesExcluded = []string{"products"}
		require.NoError(t, f.r.Status().Update(f.ctx, restore))
		for range repeatPasses {
			f.reconcile()
		}
		assert.Equal(t, neo4jv1beta1.PhasePending, f.restore().Status.Phase, "precondition: the restore waits")
		assert.Len(t, eventsWithReason(drainEvents(f.recorder), EventReasonRestoreShardedNotCovered), 1)
	})
}

// --- Neo4jPlugin --------------------------------------------------------------

func TestPluginReconcile_DuplicateIsAnnouncedOnce(t *testing.T) {
	older := &neo4jv1beta1.Neo4jPlugin{
		ObjectMeta: metav1.ObjectMeta{Name: "apoc", Namespace: "default", UID: "uid-older", Finalizers: []string{PluginFinalizer},
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour))},
		Spec: neo4jv1beta1.Neo4jPluginSpec{ClusterRef: "ec", Name: "apoc", Enabled: true},
	}
	newer := older.DeepCopy()
	newer.Name, newer.UID = "apoc-again", "uid-newer"
	newer.CreationTimestamp = metav1.NewTime(time.Now())
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(older, newer).
		WithStatusSubresource(&neo4jv1beta1.Neo4jPlugin{}).Build()
	rec := record.NewFakeRecorder(50)
	newReconciler := func() *Neo4jPluginReconciler {
		return &Neo4jPluginReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec}
	}
	key := client.ObjectKeyFromObject(newer)
	reconcileTimes(t, newReconciler(), key, repeatPasses)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonPluginDuplicate), 1)

	reconcileTimes(t, newReconciler(), key, 1)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonPluginDuplicate), "a restarted operator reads it from status")
}
