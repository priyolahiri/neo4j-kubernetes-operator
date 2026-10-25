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
	stderrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
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
// This path drives clusters and standalones alike (cloud s3/gcs/azure or
// PVC-backed backups; the PVC path uses the in-cluster seed proxy). Only a
// point-in-time standalone restore still takes the offline Job (rule 108).
func (r *Neo4jRestoreReconciler) startAllDatabasesRestore(
	ctx context.Context,
	restore *neo4jv1beta1.Neo4jRestore,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	target, err := r.getRestoreTarget(ctx, restore)
	if err != nil {
		r.updateRestoreStatus(ctx, restore, StatusFailed, fmt.Sprintf("Target lookup failed: %v", err))
		return ctrl.Result{}, err
	}

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

	// Surface (once) any property-sharded databases this restore's per-database
	// loop does not recreate — sharded DBs need the SET GRAPH/PROPERTY SHARDS
	// CREATE clauses only Neo4jShardedDatabase emits. They are NOT lost: an
	// all-databases backup taken by a recent operator catalogues each family's
	// shard artifacts (status.shardedFamilies), so each is restorable FROM THIS
	// BACKUP by pointing its Neo4jShardedDatabase CR's seedBackupRef at it.
	// Checked before any per-database result is recorded — which includes
	// every pass that waits for seed credentials, a rollout or the proxy — so
	// it goes through advisoryWarnings: announced once, not on each of those
	// passes (knowledge rules 102 and 145).
	if len(snap.ShardedDatabasesExcluded) > 0 && len(restore.Status.DatabaseResults) == 0 {
		seedHint := "its Neo4jShardedDatabase CR (spec.seedBackupRef)"
		if snap.BackupRef != "" {
			seedHint = fmt.Sprintf("its Neo4jShardedDatabase CR with spec.seedBackupRef: %s", snap.BackupRef)
		}
		r.advisoryWarnings.record(r.Recorder, restore, EventReasonRestoreShardedNotCovered, []string{
			fmt.Sprintf("all-databases restore does not recreate property-sharded database(s) %s in its per-database loop — restore each via %s", strings.Join(snap.ShardedDatabasesExcluded, ", "), seedHint),
		})
		logger.Info("all-databases restore: sharded databases restore via their Neo4jShardedDatabase CRs (seedBackupRef → this backup)",
			"shardedDatabases", snap.ShardedDatabasesExcluded, "backupRef", snap.BackupRef)
	}

	storage := *snap.Storage

	// Pre-restore hooks, once per attempt, before anything is seeded. The
	// annotation guard matters here: this function is re-entered on every
	// requeue while it waits for credentials or the proxy.
	if restore.Spec.Options != nil && restore.Spec.Options.PreRestore != nil &&
		restore.Annotations[AnnotationRestorePreHooksRan] != "true" {
		if err := r.runRestoreHooks(ctx, restore, cluster, restore.Spec.Options.PreRestore, hookPhasePreRestore); err != nil {
			logger.Error(err, "Pre-restore hooks failed")
			r.updateRestoreStatus(ctx, restore, StatusFailed, fmt.Sprintf("Pre-restore hooks failed: %v", err))
			return ctrl.Result{}, err
		}
		if err := r.markRestorePreHooksRan(ctx, restore); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Initialize per-database results once (idempotent across reconciles).
	r.ensureDatabaseResults(restore, dbs)

	// ONE-TIME, storage-specific seed setup the per-database loop depends on,
	// plus a per-database seedURI builder. The server pods fetch each seed, so
	// this gates the whole restore until ready: one rolling restart for cloud
	// credentials/endpoint (#190/#252), or one proxy rollout for PVC (#227).
	var seedURIFor func(filename string) string
	switch storage.Type {
	case "s3", "gcs", "azure":
		if res, ready, err := r.ensureClusterSeedConfigReady(ctx, restore, target, storage); !ready {
			return res, err
		}
		dirURI, err := buildSeedURIFromBackupStorage(storage, snap.BackupPath)
		if err != nil {
			r.updateRestoreStatus(ctx, restore, StatusFailed, err.Error())
			return ctrl.Result{}, err
		}
		dirURI = strings.TrimRight(dirURI, "/")
		seedURIFor = func(filename string) string { return dirURI + "/" + filename }
	case "pvc":
		// Every database the backup did not record as a full backup has its
		// chain merged by the proxy first (rule 109).
		var seeds []seedArtifact
		for _, a := range snap.DatabaseArtifacts {
			if !strings.EqualFold(a.Database, "system") {
				seeds = append(seeds, newSeedArtifact(snap.BackupPath, a.Filename, a.Type, a.Database))
			}
		}
		merge := newSeedMergePlan(target.image(), restore.Spec.Options, seeds)
		if res, ready, err := r.ensurePVCSeedProxyReady(ctx, restore, storage, target.podLabels(), merge); !ready {
			return res, err
		}
		seedURIFor = func(filename string) string {
			return pvcSeedProxyURL(restore.Name, restore.Namespace, snap.BackupPath, filename)
		}
	default:
		msg := fmt.Sprintf("all-databases restore does not support storage type %q (expected s3, gcs, azure, or pvc)", storage.Type)
		r.updateRestoreStatus(ctx, restore, StatusFailed, msg)
		return ctrl.Result{}, fmt.Errorf("%s", msg)
	}

	neo4jClient, err := r.allDatabasesClient(target)
	if err != nil {
		logger.Error(err, "Failed to connect for all-databases restore", "kind", target.kind())
		r.updateRestoreStatus(ctx, restore, StatusPending, fmt.Sprintf("Waiting to connect to %s %q: %v", target.kind(), target.name(), err))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	}
	defer func() { _ = neo4jClient.Close() }()

	imageTag := fmt.Sprintf("%s:%s", cluster.Spec.Image.Repo, cluster.Spec.Image.Tag)
	version, vErr := neo4j.GetImageVersion(imageTag)
	if vErr != nil {
		version = &neo4j.Version{Major: 5, Minor: 26, Patch: 0}
	}

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
		seedURI := seedURIFor(fname)

		switch res.Phase {
		case "", StatusPending:
			// Issue the create/recreate exactly ONCE, then flip to Running and
			// persist BEFORE returning — re-issuing would wipe a partially-seeded
			// database. Re-entry finds Running and only polls (below).
			//
			// "Persist before returning" is necessary but NOT sufficient: the
			// re-entry reads through the informer CACHE, which lags the write. A
			// second reconcile can therefore still see Pending and issue the
			// recreate again. That is not theoretical — it was reproduced, and the
			// result is a permanently bricked database: two concurrent seeds race
			// on /data/databases/<db>/temp-copy/<db>/database_lock and Neo4j parks
			// the database offline with "Unable to obtain lock on file", where it
			// stays forever (requestedStatus online, currentStatus offline). So
			// confirm against the API SERVER, uncached, before doing it.
			if !r.issueGuardAllowsRecreate(ctx, restore, db) {
				logger.Info("all-databases restore: recreate already issued for this database (uncached re-check); skipping",
					"database", db)
				return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
			}
			exists, exErr := neo4jClient.DatabaseExists(ctx, db)
			if exErr != nil {
				r.updateRestoreStatus(ctx, restore, StatusPending, fmt.Sprintf("Checking database %q: %v", db, exErr))
				return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
			}
			if exists {
				// Recreating an existing database is destructive — gate on the
				// same explicit opt-in as the single-database path (#218).
				if !restoreOverwriteConfirmed(restore) {
					r.markDatabaseResult(ctx, restore, db, StatusFailed,
						"database already exists; set spec.options.replaceExisting=true to overwrite it during an all-databases restore")
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
			// Recorded in status.databaseResults; the restore's start is
			// announced once, by the write that makes it Running (#573).
			r.markDatabaseResult(ctx, restore, db, StatusRunning, "seeding from backup")
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil

		case StatusRunning:
			online, total, _, stErr := neo4jClient.DatabaseOnlineState(ctx, db)
			if stErr == nil && total > 0 && online == total {
				r.markDatabaseResult(ctx, restore, db, StatusCompleted, "online")
				return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
			}
			// Still seeding — requeue without blocking, but RECORD WHY.
			//
			// This branch used to discard stErr and the counts entirely, so a
			// database that never came online — or a SHOW DATABASE that failed on
			// every poll — left the restore in Running forever with no message, no
			// event and no log line. That is exactly how the 2026-07-31 extended run
			// failed: 12 minutes of silence, nothing to distinguish "seeding" from
			// "the status query has been erroring since the first poll". The message
			// is persisted only when it CHANGES, so polling still costs no writes
			// once the state is steady.
			progress := seedingProgressMessage(online, total, stErr)
			r.noteDatabaseProgress(ctx, restore, db, progress)
			// Also announce it, as the restore's progress summary. The status
			// message records only the latest state, so it cannot tell "slow"
			// from "stuck": one sample of "0/1 allocations online" looks the
			// same either way. The summary says how long the database has
			// shown this progress, and how far the restore has got, once on
			// the first poll and then every seedProgressRefresh — one event per
			// restore, not one per database change, so every one fits the
			// progress recorder's budget whatever the number of databases
			// (knowledge id 166).
			r.seedProgress.announce(r.progressRecorder(), restore, db, progress)
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
		}
	}

	// No database is still pending/running — aggregate the terminal outcome.
	return r.finishAllDatabasesRestore(ctx, restore, cluster)
}

// finishAllDatabasesRestore aggregates the per-database results once every
// database is terminal: Failed if any database failed, otherwise the
// post-restore hooks run and the restore is Completed. Either way the restore
// is over, so it is stamped with status.completionTime (the online loop never
// set it, so `kubectl get neo4jrestore` showed no end time).
//
// The outcome is announced (RestoreCompleted / RestoreFailed) from the status
// write, once: this path used to end in status alone, with no event.
func (r *Neo4jRestoreReconciler) finishAllDatabasesRestore(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (ctrl.Result, error) {
	r.seedProgress.forget(restore)
	completion := metav1.Now()
	restore.Status.CompletionTime = &completion
	failed := 0
	for i := range restore.Status.DatabaseResults {
		if restore.Status.DatabaseResults[i].Phase == StatusFailed {
			failed++
		}
	}
	if failed > 0 {
		msg := fmt.Sprintf("%d of %d databases failed to restore; see status.databaseResults", failed, len(restore.Status.DatabaseResults))
		if r.transitionRestoreStatus(ctx, restore, StatusFailed, msg) {
			r.Recorder.Event(restore, corev1.EventTypeWarning, EventReasonRestoreFailed, "All-databases restore failed: "+msg)
		}
		return ctrl.Result{}, nil
	}
	if restore.Spec.Options != nil && restore.Spec.Options.PostRestore != nil {
		if err := r.runRestoreHooks(ctx, restore, cluster, restore.Spec.Options.PostRestore, hookPhasePostRestore); err != nil {
			log.FromContext(ctx).Error(err, "Post-restore hooks failed")
			r.updateRestoreStatus(ctx, restore, StatusFailed, fmt.Sprintf("Post-restore hooks failed: %v", err))
			return ctrl.Result{}, err
		}
	}
	msg := fmt.Sprintf("Restored %d databases", len(restore.Status.DatabaseResults))
	if r.transitionRestoreStatus(ctx, restore, StatusCompleted, msg) {
		r.Recorder.Event(restore, corev1.EventTypeNormal, EventReasonRestoreCompleted, "All-databases restore completed: "+msg)
	}
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
	target restoreTarget,
	storage neo4jv1beta1.StorageLocation,
) (ctrl.Result, bool, error) {
	credsSecret := ""
	if storage.Cloud != nil {
		credsSecret = storage.Cloud.CredentialsSecretRef
	}
	hasCustomEndpoint := storage.Type == "s3" && storage.Cloud != nil && storage.Cloud.EndpointURL != ""

	projected, missing, projErr := r.projectSeedConfig(ctx, target, credsSecret, storage.Cloud)
	if projErr != nil {
		if stderrors.Is(projErr, errSeedConfigNotAutoInherited) {
			return r.awaitSeedConfig(ctx, restore, target, missing), false, nil
		}
		r.updateRestoreStatus(ctx, restore, StatusPending,
			fmt.Sprintf("Retrying projection of seed configuration onto %s %q: %v", target.kind(), target.name(), projErr))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, false, nil
	}
	if projected {
		r.updateRestoreStatus(ctx, restore, StatusPending,
			fmt.Sprintf("Projected seed configuration onto %s %q; waiting for the rolling restart", target.kind(), target.name()))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, false, nil
	}
	if credsSecret != "" {
		rolled, rErr := r.seedCredsRolledOut(ctx, target, credsSecret)
		if rErr != nil || !rolled {
			msg := fmt.Sprintf("Waiting for %s %q pods to roll out seed credentials Secret %q", target.kind(), target.name(), credsSecret)
			if rErr != nil {
				msg += fmt.Sprintf(" (rollout check pending: %v)", rErr)
			}
			r.updateRestoreStatus(ctx, restore, StatusPending, msg)
			// A transient rollout-check error is treated as "not rolled out yet":
			// this function drives all waiting via status + RequeueAfter and never
			// propagates transient errors (cf. the projErr handling above). The
			// error is surfaced in the status message set just above.
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, false, nil
		}
	}
	if hasCustomEndpoint && envHasSeedEndpoint(target.env()) {
		rolled, rErr := r.specEnvEndpointRolledOut(ctx, target)
		if rErr != nil || !rolled {
			msg := fmt.Sprintf("Waiting for %s %q pods to roll out the S3 endpoint", target.kind(), target.name())
			if rErr != nil {
				msg += fmt.Sprintf(" (rollout check pending: %v)", rErr)
			}
			r.updateRestoreStatus(ctx, restore, StatusPending, msg)
			// Transient rollout-check error → keep waiting; this function retries
			// via RequeueAfter and never propagates transient errors. The error is
			// surfaced in the status message set just above.
			return ctrl.Result{RequeueAfter: r.RequeueAfter}, false, nil
		}
	}
	if res, ready := r.awaitSeedIdentity(ctx, restore, target, storage); !ready {
		return res, false, nil
	}
	return ctrl.Result{}, true, nil
}

// ensurePVCSeedProxyReady spawns (idempotently) the in-cluster HTTP proxy in
// front of the backup PVC and gates on its readiness, so the per-database loop
// can build seedURIs against it via pvcSeedProxyURL. Mirrors the single-database
// PVC setup in resolveClusterPVCRestoreURI (bounded wait + diagnosis on a proxy
// that never starts, #227). Returns ready=false (with the result to return)
// until the proxy Deployment is Ready.
func (r *Neo4jRestoreReconciler) ensurePVCSeedProxyReady(
	ctx context.Context,
	restore *neo4jv1beta1.Neo4jRestore,
	storage neo4jv1beta1.StorageLocation,
	peers map[string]string,
	merge *seedMergePlan,
) (ctrl.Result, bool, error) {
	logger := log.FromContext(ctx)
	if storage.PVC == nil || storage.PVC.Name == "" {
		msg := "PVC-backed all-databases restore requires storage.pvc.name to be set"
		r.updateRestoreStatus(ctx, restore, StatusFailed, msg)
		return ctrl.Result{}, false, fmt.Errorf("%s", msg)
	}

	proxyAvailable, err := ensurePVCSeedProxyResources(ctx, r.Client, r.Scheme, restore, restore.Name, storage.PVC.Name, merge)
	if err != nil {
		r.updateRestoreStatus(ctx, restore, StatusFailed, fmt.Sprintf("ensure PVC seed proxy: %v", err))
		return ctrl.Result{}, false, fmt.Errorf("ensure PVC seed proxy: %w", err)
	}
	// Restrict the proxy (which serves the whole backup PVC) to the target's
	// Neo4j pods (#219). Best-effort: only enforcing CNIs apply it.
	if npErr := ensurePVCSeedProxyNetworkPolicy(ctx, r.Client, r.Scheme, restore, restore.Name, peers); npErr != nil {
		logger.Error(npErr, "Failed to ensure seed-proxy NetworkPolicy (non-fatal)")
	}

	if !proxyAvailable {
		// Bounded wait (#227): a proxy that can never start (RWO backup PVC
		// attached elsewhere, unpullable image) must not requeue forever. A
		// failed merge never heals: fail at once with its reason.
		if why := pvcSeedProxyMergeFailure(ctx, r.Client, restore.Namespace, restore.Name); why != "" {
			msg := "the seed proxy could not merge the backup chain: " + why
			r.updateRestoreStatus(ctx, restore, StatusFailed, msg)
			r.Recorder.Event(restore, corev1.EventTypeWarning, EventReasonRestoreFailed, msg)
			return ctrl.Result{}, false, nil
		}
		waitStart, haveAnchor := r.seedProxyWaitStart(ctx, restore)
		diagnosis := pvcSeedProxyDiagnosis(ctx, r.Client, restore.Namespace, restore.Name)
		budget := seedProxyBudget(restore, merge != nil)
		if haveAnchor && time.Since(waitStart) > budget {
			msg := fmt.Sprintf("backup-seed-proxy Deployment did not become Ready within %s: %s — a common cause is the backup PVC (%s) being ReadWriteOnce and still attached elsewhere; fix the cause and re-trigger by bumping the spec",
				budget, diagnosis, storage.PVC.Name)
			r.updateRestoreStatus(ctx, restore, StatusFailed, msg)
			r.Recorder.Event(restore, corev1.EventTypeWarning, EventReasonRestoreFailed, msg)
			return ctrl.Result{}, false, nil
		}
		r.updateRestoreStatus(ctx, restore, StatusPending,
			fmt.Sprintf("Waiting for backup-seed-proxy Deployment to become Ready (%s)", diagnosis))
		return ctrl.Result{RequeueAfter: r.RequeueAfter}, false, nil
	}
	// Proxy is up — drop the wait anchor so any later wait starts fresh.
	if err := r.clearSeedProxyWaitStarted(ctx, restore); err != nil {
		logger.V(1).Info("Failed to clear seed-proxy wait anchor (non-fatal)", "error", err.Error())
	}
	return ctrl.Result{}, true, nil
}

// buildAllDatabasesRestoreCommand builds the offline `neo4j-admin database
// restore` command for a STANDALONE all-databases restore: one restore
// invocation per user database (system excluded), each from its exact .backup
// file in the resolved source, sharing one writable temp dir reset between
// databases. The instance is scaled to 0 while this Job runs (the standalone
// Job machinery, reused via createRestoreJob → buildRestoreCommand).
func (r *Neo4jRestoreReconciler) buildAllDatabasesRestoreCommand(restore *neo4jv1beta1.Neo4jRestore, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (string, error) {
	snap := resolvedBackupSnapshot(restore)
	if snap == nil || snap.Storage == nil {
		return "", fmt.Errorf("all-databases restore: resolved backup source not available")
	}
	dbs := userDatabasesFromArtifacts(snap.DatabaseArtifacts)
	if len(dbs) == 0 {
		return "", fmt.Errorf("all-databases restore: the resolved backup recorded no per-database artifacts")
	}

	imageTag := fmt.Sprintf("%s:%s", cluster.Spec.Image.Repo, cluster.Spec.Image.Tag)
	version, err := neo4j.GetImageVersion(imageTag)
	if err != nil {
		version = &neo4j.Version{Major: 5, Minor: 26, Patch: 0}
	}

	// dir is the chain-root directory (cloud URI like s3://.../<chain-root>, or
	// the local PVC mount /backup/<chain-root>). resolveRestoreSource has
	// normalized Source to type=storage before buildRestoreCommand runs, so
	// buildRestoreFromPath yields that directory; the exact per-database file is
	// dir + "/" + <filename> (we have the precise filename from the artifact map,
	// so no `ls` glob is needed).
	dir := strings.TrimRight(r.buildRestoreFromPath(restore), "/")
	overwrite := restoreOverwriteConfirmed(restore)

	var b strings.Builder
	b.WriteString("set -e; ")
	for _, db := range dbs {
		fname := filenameForDB(snap.DatabaseArtifacts, db)
		if fname == "" {
			return "", fmt.Errorf("all-databases restore: no artifact filename recorded for database %q", db)
		}
		// Reset the writable temp dir per database (neo4j-admin requires it
		// empty; the backup mount is ReadOnly), then restore that database.
		b.WriteString("rm -rf /tmp/restore-tmp && mkdir -p /tmp/restore-tmp && ")
		b.WriteString(neo4j.GetRestoreCommand(version, db, shellQuote(dir+"/"+fname)))
		if overwrite {
			b.WriteString(" --overwrite-destination=true")
		}
		b.WriteString(" --temp-path=/tmp/restore-tmp; ")
	}
	return b.String(), nil
}

// registerAllDatabasesAfterRestore brings every restored user database online
// after a standalone all-databases restore Job completes (the offline restore
// writes store files; the database must be CREATE'd/START'ed to be served), and
// records per-database results in status.databaseResults.
func (r *Neo4jRestoreReconciler) registerAllDatabasesAfterRestore(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore) error {
	snap := resolvedBackupSnapshot(restore)
	if snap == nil {
		return fmt.Errorf("all-databases restore: resolved source missing during registration")
	}
	dbs := userDatabasesFromArtifacts(snap.DatabaseArtifacts)
	r.ensureDatabaseResults(restore, dbs)

	neo4jClient, err := r.newStandaloneRestoreClient(ctx, restore)
	if err != nil {
		return fmt.Errorf("failed to create Neo4j client: %w", err)
	}
	defer func() { _ = neo4jClient.Close() }()

	var firstErr error
	for _, db := range dbs {
		exists, exErr := neo4jClient.DatabaseExists(ctx, db)
		if exErr == nil {
			if exists {
				exErr = neo4jClient.StartDatabase(ctx, db, false)
			} else {
				exErr = neo4jClient.CreateDatabase(ctx, db, nil, false, false)
			}
		}
		if exErr != nil {
			if firstErr == nil {
				firstErr = exErr
			}
			r.markDatabaseResult(ctx, restore, db, StatusFailed, fmt.Sprintf("register after restore: %v", exErr))
			continue
		}
		r.markDatabaseResult(ctx, restore, db, StatusCompleted, "restored and online")
	}
	return firstErr
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
//
// It announces, from the write, what an all-databases restore says in events
// (#573): RestoreStarted once, when the write moves the restore into Running,
// on the restore's own recorder; and RestoreDatabaseFailed when it moves a
// database into Failed, from the progress recorder. A database starting or
// finishing is recorded in status.databaseResults only: one RestoreStarted per
// database (30 for a 30-database restore) outran the progress source's event
// budget, and 13 of 30 reached the API server.
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
	restoreEntered, databaseFailed := false, false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		restoreEntered, databaseFailed = false, false
		latest := &neo4jv1beta1.Neo4jRestore{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(restore), latest); err != nil {
			return err
		}
		before := ""
		for _, res := range latest.Status.DatabaseResults {
			if res.Database == db {
				before = res.Phase
			}
		}
		wasRunning := latest.Status.Phase == StatusRunning
		latest.Status.DatabaseResults = results
		if latest.Status.Phase != StatusCompleted && latest.Status.Phase != StatusFailed {
			latest.Status.Phase = StatusRunning
		}
		if err := r.Status().Update(ctx, latest); err != nil {
			return err
		}
		restoreEntered = !wasRunning && latest.Status.Phase == StatusRunning
		databaseFailed = phase == StatusFailed && before != StatusFailed
		return nil
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to record a database's restore result", "database", db)
		return
	}
	if restoreEntered {
		r.Recorder.Event(restore, corev1.EventTypeNormal, EventReasonRestoreStarted, allDatabasesRestoreStartedMessage(results))
	}
	if databaseFailed {
		r.progressRecorder().Event(restore, corev1.EventTypeWarning, EventReasonRestoreDatabaseFailed,
			fmt.Sprintf("All-databases restore: database %q failed: %s", db, message))
	}
}

// allDatabasesRestoreStartedMessage is the one RestoreStarted of an
// all-databases restore: how many databases it restores, and the first few by
// name.
func allDatabasesRestoreStartedMessage(results []neo4jv1beta1.DatabaseRestoreResult) string {
	const named = 10
	names := make([]string, 0, named)
	for i := range results {
		if i == named {
			break
		}
		names = append(names, fmt.Sprintf("%q", results[i].Database))
	}
	list := strings.Join(names, ", ")
	if len(results) > named {
		list += fmt.Sprintf(" and %d more", len(results)-named)
	}
	return fmt.Sprintf("All-databases restore started: %d database(s), %s; each one's progress is in status.databaseResults",
		len(results), list)
}

// seedingProgressMessage describes an in-progress (or failing) seed poll. Kept
// separate so it can be unit-tested without a live Neo4j.
func seedingProgressMessage(online, total int, stErr error) string {
	if stErr != nil {
		// A failing poll is NOT progress. Say so plainly: silence here previously
		// looked identical to a healthy long seed.
		return fmt.Sprintf("seeding from backup; last status poll FAILED: %v", stErr)
	}
	if total == 0 {
		// Mid-recreate: SHOW DATABASE reports no allocations for the database yet.
		return "seeding from backup; database has no allocations yet (recreate in progress)"
	}
	return fmt.Sprintf("seeding from backup; %d/%d allocations online", online, total)
}

// noteDatabaseProgress updates a database's message only when it changed, so the
// poll loop does not issue a status write per reconcile.
func (r *Neo4jRestoreReconciler) noteDatabaseProgress(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore, db, message string) {
	for i := range restore.Status.DatabaseResults {
		if restore.Status.DatabaseResults[i].Database != db {
			continue
		}
		if restore.Status.DatabaseResults[i].Message == message {
			return // unchanged — nothing to persist
		}
		break
	}
	r.markDatabaseResult(ctx, restore, db, StatusRunning, message)
}

// issueGuardAllowsRecreate answers "is this database still un-issued?" against
// the API SERVER rather than the informer cache.
//
// The cached client is eventually consistent, so the status write that flips a
// database to Running may not be visible to the next reconcile — which would
// then issue a second, concurrent seed and brick the database (see the call
// site). An uncached read closes that window: controller-runtime never
// reconciles one object concurrently, so once the write has landed, the next
// reconcile is guaranteed to see it here.
//
// Fails OPEN when no APIReader is wired (tests, dev wiring): the cached phase
// has already been checked by the caller, so this can only ever be an extra
// guard, never the only one.
func (r *Neo4jRestoreReconciler) issueGuardAllowsRecreate(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore, db string) bool {
	if r.APIReader == nil {
		return true
	}
	fresh := &neo4jv1beta1.Neo4jRestore{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(restore), fresh); err != nil {
		// Cannot confirm. Refuse rather than risk a second concurrent seed: the
		// caller requeues, and a transient read failure costs one interval.
		log.FromContext(ctx).V(1).Info("all-databases restore: uncached re-check failed; deferring the recreate",
			"database", db, "error", err.Error())
		return false
	}
	for i := range fresh.Status.DatabaseResults {
		if fresh.Status.DatabaseResults[i].Database != db {
			continue
		}
		p := fresh.Status.DatabaseResults[i].Phase
		return p == "" || p == StatusPending
	}
	// No entry yet on the server — the seeding write has not landed at all, so
	// this is genuinely the first pass for this database.
	return true
}

// allDatabasesRestoreClient is the part of the Neo4j client the all-databases
// restore loop uses, so a test can drive the loop against a scripted server;
// production always supplies a *neo4j.Client.
type allDatabasesRestoreClient interface {
	DatabaseExists(ctx context.Context, databaseName string) (bool, error)
	RecreateDatabaseWithSeedURI(ctx context.Context, version *neo4j.Version, databaseName, seedURI string) (bool, error)
	CreateDatabaseWithSeedURIOptions(ctx context.Context, databaseName, seedURI string, ifNotExists bool) error
	DatabaseOnlineState(ctx context.Context, databaseName string) (online, total int, diag string, err error)
	Close() error
}

// allDatabasesClient connects to the restore's target for the all-databases
// loop.
func (r *Neo4jRestoreReconciler) allDatabasesClient(target restoreTarget) (allDatabasesRestoreClient, error) {
	if r.newAllDatabasesClient != nil {
		return r.newAllDatabasesClient(target)
	}
	c, err := r.targetClient(target)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// seedProgressRefresh is how often an all-databases restore announces its
// progress: one RestoreSeedProgress per restore on its first seed poll, then
// one every ten minutes while it seeds. The progress recorder's budget
// (client-go: a burst of 25 per source, object and event type, then one every
// five minutes) refills twice as fast as this spends it, and one message every
// ten minutes never reaches the aggregator's ten similar events in ten
// minutes, so every summary reaches the API server, whatever the number of
// databases (knowledge operations.md id 166). Inside the API server's one-hour
// event TTL.
const seedProgressRefresh = 10 * time.Minute

// seedProgressEvents remembers, per restore, when its last progress summary
// went out and how long the database it is seeding has shown its current
// progress, so a restore announces one summary every seedProgressRefresh — not
// one event per database and state, which on a 30-database restore was more
// than the budget allows (55 of 101 reached the API server, #573). The zero
// value is ready to use. State is in memory: an operator restart announces the
// next summary at once, which is fine.
type seedProgressEvents struct {
	mu        sync.Mutex
	last      map[string]seedProgressNote
	lastSweep time.Time
	// now replaces the clock in tests; nil means time.Now.
	now func() time.Time
}

type seedProgressNote struct {
	announced time.Time // when the last summary went out
	database  string    // the database being seeded
	progress  string    // its progress at the last poll
	since     time.Time // when it first showed that progress
	seen      time.Time // the last poll
}

// seedProgressKey names one restore; the UID makes a recreated restore start
// afresh.
func seedProgressKey(restore *neo4jv1beta1.Neo4jRestore) string {
	return fmt.Sprintf("%s/%s/%s", restore.Namespace, restore.Name, restore.UID)
}

// announce records a seed poll of database and emits the restore's progress
// summary on rec when none went out within seedProgressRefresh.
func (s *seedProgressEvents) announce(rec record.EventRecorder, restore *neo4jv1beta1.Neo4jRestore, database, progress string) {
	if rec == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	if s.last == nil {
		s.last = map[string]seedProgressNote{}
	}
	s.sweepLocked(now)
	key := seedProgressKey(restore)
	n := s.last[key]
	if n.database != database || n.progress != progress {
		n.database, n.progress, n.since = database, progress, now
	}
	n.seen = now
	if n.announced.IsZero() || now.Sub(n.announced) >= seedProgressRefresh {
		rec.Event(restore, corev1.EventTypeNormal, EventReasonRestoreSeedProgress,
			seedProgressSummary(restore.Status.DatabaseResults, database, progress, now.Sub(n.since)))
		n.announced = now
	}
	s.last[key] = n
}

// forget drops a restore whose databases are all done.
func (s *seedProgressEvents) forget(restore *neo4jv1beta1.Neo4jRestore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.last, seedProgressKey(restore))
}

// seedProgressSummary is a RestoreSeedProgress message: how far the restore
// has got, by status.databaseResults, and the database it is seeding, with its
// progress and how long it has shown it — which tells a slow seed from a stuck
// one.
func seedProgressSummary(results []neo4jv1beta1.DatabaseRestoreResult, database, progress string, unchanged time.Duration) string {
	restored, failed := 0, 0
	for i := range results {
		switch results[i].Phase {
		case StatusCompleted:
			restored++
		case StatusFailed:
			failed++
		}
	}
	return fmt.Sprintf("%d of %d database(s) restored, %d failed; seeding %q: %s, unchanged for %s",
		restored, len(results), failed, database, strings.TrimPrefix(progress, "seeding from backup; "),
		roughDuration(unchanged))
}

// roughDuration says d to the minute.
func roughDuration(d time.Duration) string {
	minutes := int(d.Round(time.Minute) / time.Minute)
	switch {
	case minutes < 1:
		return "under a minute"
	case minutes < 60:
		return fmt.Sprintf("%dm", minutes)
	default:
		return fmt.Sprintf("%dh%02dm", minutes/60, minutes%60)
	}
}

// sweepLocked drops, at most once per seedProgressRefresh, the restores no
// poll has seen for two refresh periods — deleted restores, mostly. A restore
// still seeding is polled on every requeue.
func (s *seedProgressEvents) sweepLocked(now time.Time) {
	if now.Sub(s.lastSweep) < seedProgressRefresh {
		return
	}
	s.lastSweep = now
	for key, n := range s.last {
		if now.Sub(n.seen) > 2*seedProgressRefresh {
			delete(s.last, key)
		}
	}
}
