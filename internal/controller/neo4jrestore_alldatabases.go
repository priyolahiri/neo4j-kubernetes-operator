/*
Copyright 2025.

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
	stderrors "errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// startAllDatabasesRestore drives an all-databases restore (spec.allDatabases):
// it restores every USER database recorded in the resolved backup's per-database
// artifact map (the system database is always excluded), reporting per-database
// progress in status.databaseResults. It restores ONE database per reconcile
// pass and requeues — never blocking the worker on the asynchronous seed (the
// same non-blocking contract the single-database cluster path holds, #218/#227).
//
// This release supports CLOUD-backed CLUSTER targets (the canonical #222 DR
// case). Standalone all-databases restore and PVC-backed cluster all-databases
// restore are rejected with an actionable message (tracked for a follow-up);
// users can restore those databases individually with spec.database.
func (r *Neo4jRestoreReconciler) startAllDatabasesRestore(
	ctx context.Context,
	restore *neo4jv1beta1.Neo4jRestore,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	isTrueCluster bool,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// The resolved source (pinned by ensureResolvedBackupSource, issue #188)
	// carries the per-database artifact map. Without it we can't enumerate.
	snap := resolvedBackupSnapshot(restore)
	if snap == nil || snap.Storage == nil {
		r.updateRestoreStatus(ctx, restore, StatusPending, "Waiting for the backup source to resolve before all-databases restore")
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}

	dbs := userDatabasesFromArtifacts(snap.DatabaseArtifacts)
	if len(dbs) == 0 {
		msg := "all-databases restore: the resolved backup recorded no per-database artifacts. The source must be an all-databases backup (spec.allDatabases / target.kind=Cluster) whose per-database map was captured in status.history[*].databaseArtifacts"
		r.updateRestoreStatus(ctx, restore, StatusFailed, msg)
		return ctrl.Result{}, fmt.Errorf("%s", msg)
	}

	// Bounded scope for this release.
	if !isTrueCluster {
		msg := "all-databases restore currently supports cluster targets only; for a standalone, restore each database individually with spec.database"
		r.updateRestoreStatus(ctx, restore, StatusFailed, msg)
		return ctrl.Result{}, fmt.Errorf("%s", msg)
	}
	storage := *snap.Storage
	if storage.Type != "s3" && storage.Type != "gcs" && storage.Type != "azure" {
		msg := fmt.Sprintf("all-databases restore currently supports cloud-backed backups (s3/gcs/azure); storage type %q is not yet supported — restore each database individually with spec.database", storage.Type)
		r.updateRestoreStatus(ctx, restore, StatusFailed, msg)
		return ctrl.Result{}, fmt.Errorf("%s", msg)
	}

	// Initialize per-database results once (idempotent across reconciles).
	r.ensureDatabaseResults(restore, dbs)

	// ONE-TIME: the SERVER pods fetch each seed, so cloud credentials (and any
	// custom S3 endpoint) must be projected onto the cluster and rolled out
	// BEFORE we seed any database. This is per-cluster, not per-database, so it
	// runs once and gates the whole all-databases restore (#190/#252).
	if res, ready, err := r.ensureClusterSeedConfigReady(ctx, restore, cluster, storage); !ready {
		return res, err
	}

	neo4jClient, err := r.createNeo4jClient(ctx, cluster)
	if err != nil {
		logger.Error(err, "Failed to connect to cluster for all-databases restore")
		r.updateRestoreStatus(ctx, restore, StatusPending, fmt.Sprintf("Waiting to connect to cluster: %v", err))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}
	defer func() { _ = neo4jClient.Close() }()

	imageTag := fmt.Sprintf("%s:%s", cluster.Spec.Image.Repo, cluster.Spec.Image.Tag)
	version, vErr := neo4j.GetImageVersion(imageTag)
	if vErr != nil {
		version = &neo4j.Version{Major: 5, Minor: 26, Patch: 0}
	}

	dirURI, err := buildSeedURIFromBackupStorage(storage, snap.BackupPath)
	if err != nil {
		r.updateRestoreStatus(ctx, restore, StatusFailed, err.Error())
		return ctrl.Result{}, err
	}
	dirURI = strings.TrimRight(dirURI, "/")

	// Drive ONE database action per pass, then requeue (non-blocking).
	for i := range restore.Status.DatabaseResults {
		res := &restore.Status.DatabaseResults[i]
		if res.Phase == StatusCompleted || res.Phase == StatusFailed {
			continue
		}
		db := res.Database
		fname := filenameForDB(snap.DatabaseArtifacts, db)
		if fname == "" {
			r.markDatabaseResult(ctx, restore, db, StatusFailed,
				"no .backup artifact filename recorded for this database in the source backup")
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
		seedURI := dirURI + "/" + fname

		switch res.Phase {
		case "", StatusPending:
			// Issue the create/recreate exactly ONCE, then flip to Running and
			// persist BEFORE returning — re-issuing would wipe a partially-seeded
			// database. Re-entry finds Running and only polls (below).
			exists, exErr := neo4jClient.DatabaseExists(ctx, db)
			if exErr != nil {
				r.updateRestoreStatus(ctx, restore, StatusPending, fmt.Sprintf("Checking database %q: %v", db, exErr))
				return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
			}
			if exists {
				// Recreating an existing database is destructive — gate on the
				// same explicit opt-in as the single-database path (#218).
				if !restore.Spec.Force && (restore.Spec.Options == nil || !restore.Spec.Options.ReplaceExisting) {
					r.markDatabaseResult(ctx, restore, db, StatusFailed,
						"database already exists; set spec.force=true (or spec.options.replaceExisting=true) to overwrite it during an all-databases restore")
					return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
				}
				applied, rErr := neo4jClient.RecreateDatabaseWithSeedURI(ctx, version, db, seedURI)
				if rErr != nil {
					r.markDatabaseResult(ctx, restore, db, StatusFailed, fmt.Sprintf("recreateDatabase failed: %v", rErr))
					return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
				}
				if !applied {
					r.markDatabaseResult(ctx, restore, db, StatusFailed,
						fmt.Sprintf("Neo4j %d.%d does not support dbms.recreateDatabase; DROP DATABASE %q and re-run", version.Major, version.Minor, db))
					return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
				}
			} else if cErr := neo4jClient.CreateDatabaseWithSeedURIOptions(ctx, db, seedURI, true); cErr != nil {
				r.markDatabaseResult(ctx, restore, db, StatusFailed, fmt.Sprintf("CREATE DATABASE with seedURI failed: %v", cErr))
				return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
			}
			r.Recorder.Event(restore, corev1.EventTypeNormal, EventReasonRestoreStarted,
				fmt.Sprintf("All-databases restore: seeding %q from %s", db, seedURI))
			r.markDatabaseResult(ctx, restore, db, StatusRunning, "seeding from backup")
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil

		case StatusRunning:
			online, total, _, stErr := neo4jClient.DatabaseOnlineState(ctx, db)
			if stErr == nil && total > 0 && online == total {
				r.markDatabaseResult(ctx, restore, db, StatusCompleted, "online")
				return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
			}
			// Still seeding — requeue without blocking.
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
	}

	// No database is still pending/running — aggregate the terminal outcome.
	failed := 0
	for i := range restore.Status.DatabaseResults {
		if restore.Status.DatabaseResults[i].Phase == StatusFailed {
			failed++
		}
	}
	if failed > 0 {
		r.updateRestoreStatus(ctx, restore, StatusFailed,
			fmt.Sprintf("%d of %d databases failed to restore; see status.databaseResults", failed, len(restore.Status.DatabaseResults)))
		return ctrl.Result{}, nil
	}
	r.updateRestoreStatus(ctx, restore, StatusCompleted,
		fmt.Sprintf("Restored %d databases", len(restore.Status.DatabaseResults)))
	return ctrl.Result{}, nil
}

// ensureClusterSeedConfigReady projects the cloud seed credentials / custom
// endpoint onto the cluster once and gates on the resulting rolling restart, so
// every per-database seed runs against pods that can reach the backup store.
// Returns ready=false (with the result the caller must return) until the config
// has rolled out.
func (r *Neo4jRestoreReconciler) ensureClusterSeedConfigReady(
	ctx context.Context,
	restore *neo4jv1beta1.Neo4jRestore,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
	storage neo4jv1beta1.StorageLocation,
) (ctrl.Result, bool, error) {
	credsSecret := ""
	if storage.Cloud != nil {
		credsSecret = storage.Cloud.CredentialsSecretRef
	}
	hasCustomEndpoint := storage.Type == "s3" && storage.Cloud != nil && storage.Cloud.EndpointURL != ""

	projected, missing, projErr := r.projectClusterSeedConfig(ctx, cluster, credsSecret, storage.Cloud)
	if projErr != nil {
		if stderrors.Is(projErr, errSeedConfigNotAutoInherited) {
			msg := fmt.Sprintf("cluster %q's server pods can't reach the seed source: missing %s. The server JVM fetches each seed itself. Provide these on the cluster CR, or set annotation %s=\"true\" to let the operator inject them (one rolling restart).",
				cluster.Name, strings.Join(missing, "; "), AutoInheritSeedCredsAnnotation)
			r.updateRestoreStatus(ctx, restore, StatusFailed, msg)
			r.Recorder.Event(restore, corev1.EventTypeWarning, EventReasonSeedEndpointNotProjected, msg)
			return ctrl.Result{}, false, nil
		}
		r.updateRestoreStatus(ctx, restore, StatusPending,
			fmt.Sprintf("Retrying projection of seed configuration onto cluster %q: %v", cluster.Name, projErr))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, false, nil
	}
	if projected {
		r.updateRestoreStatus(ctx, restore, StatusPending,
			fmt.Sprintf("Projected seed configuration onto cluster %q; waiting for the rolling restart", cluster.Name))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, false, nil
	}
	if credsSecret != "" {
		rolled, rErr := r.seedCredsRolledOut(ctx, cluster, credsSecret)
		if rErr != nil || !rolled {
			r.updateRestoreStatus(ctx, restore, StatusPending,
				fmt.Sprintf("Waiting for cluster %q server pods to roll out seed credentials Secret %q", cluster.Name, credsSecret))
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, false, nil
		}
	}
	if hasCustomEndpoint && clusterSpecEnvHasSeedEndpoint(cluster) {
		rolled, rErr := r.specEnvEndpointRolledOut(ctx, cluster)
		if rErr != nil || !rolled {
			r.updateRestoreStatus(ctx, restore, StatusPending,
				fmt.Sprintf("Waiting for cluster %q server pods to roll out the S3 endpoint", cluster.Name))
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, false, nil
		}
	}
	return ctrl.Result{}, true, nil
}

// userDatabasesFromArtifacts returns the restorable user-database names from a
// per-database artifact map, excluding the system database (never user-restored).
func userDatabasesFromArtifacts(arts []neo4jv1beta1.DatabaseArtifact) []string {
	var out []string
	for _, a := range arts {
		if strings.EqualFold(a.Database, "system") {
			continue
		}
		out = append(out, a.Database)
	}
	return out
}

// filenameForDB returns the `.backup` filename recorded for a database.
func filenameForDB(arts []neo4jv1beta1.DatabaseArtifact, db string) string {
	for _, a := range arts {
		if a.Database == db {
			return a.Filename
		}
	}
	return ""
}

// ensureDatabaseResults seeds status.databaseResults with a Pending entry per
// database the first time, preserving any results already recorded.
func (r *Neo4jRestoreReconciler) ensureDatabaseResults(restore *neo4jv1beta1.Neo4jRestore, dbs []string) {
	existing := map[string]bool{}
	for i := range restore.Status.DatabaseResults {
		existing[restore.Status.DatabaseResults[i].Database] = true
	}
	for _, db := range dbs {
		if !existing[db] {
			restore.Status.DatabaseResults = append(restore.Status.DatabaseResults,
				neo4jv1beta1.DatabaseRestoreResult{Database: db, Phase: StatusPending})
		}
	}
}

// markDatabaseResult sets a database's per-DB phase/message in memory and
// durably persists status.databaseResults (refetch + RetryOnConflict — the
// project's status-write pattern; updateRestoreStatus copies only
// phase/message/conditions and would drop databaseResults, #227 item 4).
func (r *Neo4jRestoreReconciler) markDatabaseResult(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore, db, phase, message string) {
	for i := range restore.Status.DatabaseResults {
		if restore.Status.DatabaseResults[i].Database == db {
			restore.Status.DatabaseResults[i].Phase = phase
			restore.Status.DatabaseResults[i].Message = message
			if phase == StatusCompleted || phase == StatusFailed {
				now := metav1.Now()
				restore.Status.DatabaseResults[i].CompletionTime = &now
			}
			break
		}
	}
	results := restore.Status.DatabaseResults
	_ = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := &neo4jv1beta1.Neo4jRestore{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(restore), latest); err != nil {
			return err
		}
		latest.Status.DatabaseResults = results
		if latest.Status.Phase != StatusCompleted && latest.Status.Phase != StatusFailed {
			latest.Status.Phase = StatusRunning
		}
		return r.Status().Update(ctx, latest)
	})
}
