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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// AnnotationPointInTimePath records, once per attempt, how a restore with a
// point in time runs: "online", or "offline: <reason>". Whether it can run
// online depends on whether the database exists, which the online restore
// itself changes, so the decision is made once and the routing reads it.
// Cleared with the other per-attempt markers when the spec changes.
const AnnotationPointInTimePath = "neo4j.com/point-in-time-path"

const pointInTimeOnline = "online"

// pointInTimeOnlineBlocker says why a restore with a point in time cannot run
// online, or "" when it can. Online is `CREATE DATABASE … OPTIONS {seedURI,
// seedRestoreUntil}`, and Neo4j takes seedRestoreUntil only:
//   - when creating a database, not in dbms.recreateDatabase;
//   - through the cloud and file seed providers, not over HTTP (the PVC proxy);
//   - on CalVer (Neo4j 5.26 has no such option);
//   - from the backup "containing the data up to that date" — the run the
//     restore resolved (restoreRunIndex), which must have started at or after
//     the point in time.
//
// https://neo4j.com/docs/operations-manual/current/database-administration/standard-databases/seed-from-uri/#seed-restore-until-option
func pointInTimeOnlineBlocker(restore *neo4jv1beta1.Neo4jRestore, snap *neo4jv1beta1.ResolvedRestoreSource, calver, exists bool) string {
	src := restore.Spec.Source
	switch {
	case src.Type == "pitr":
		return "source.type pitr replays separately stored transaction logs, which only neo4j-admin does"
	case restore.Spec.AllDatabases:
		return "an all-databases restore recreates the databases that exist, and Neo4j restores to a point in time only when creating a database"
	case src.Type != SourceTypeBackup:
		return fmt.Sprintf("source.type %q names a path, so the operator cannot tell which backup in it holds the point in time", src.Type)
	case !calver:
		return "Neo4j 5.26 cannot seed a database to a point in time (seedRestoreUntil arrived with CalVer)"
	case snap == nil || snap.Storage == nil:
		return "the backup's location is not resolved yet"
	case snap.Storage.Type == "pvc":
		return "the backup is on a PVC, which the seed proxy serves over HTTP, and Neo4j restores a seed to a point in time only from cloud storage"
	case !pointInTimeCovered(restore, snap):
		return fmt.Sprintf("no run of Neo4jBackup %q started at or after %s, so none is known to hold every transaction up to it",
			snap.BackupRef, src.PointInTime.UTC().Format(time.RFC3339))
	case snap.ArtifactFilename == "":
		return fmt.Sprintf("Neo4jBackup %q did not record the artifact of the run that holds the point in time", snap.BackupRef)
	case exists:
		return fmt.Sprintf("database %q exists, and Neo4j restores to a point in time only when creating a database (seedRestoreUntil is a CREATE DATABASE option); restore under a new name",
			restore.Spec.Database)
	}
	return ""
}

// pointInTimeOfflineReason reads the recorded decision: "" when the restore
// runs online, else why it does not.
func pointInTimeOfflineReason(restore *neo4jv1beta1.Neo4jRestore) string {
	path, ok := restore.Annotations[AnnotationPointInTimePath]
	switch {
	case !ok:
		return "a point-in-time restore runs online only once the operator has checked it can (CalVer, cloud storage, a new database)"
	case path == pointInTimeOnline:
		return ""
	}
	return strings.TrimPrefix(path, "offline: ")
}

// decidePointInTimePath decides, once per attempt, whether a restore with a
// point in time runs online, and records it. A cluster cannot restore offline,
// so a cluster restore that cannot run online fails with the reason. done=true
// means return res, err.
func (r *Neo4jRestoreReconciler) decidePointInTimePath(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore) (res ctrl.Result, done bool, err error) {
	return r.decidePointInTimePathWith(ctx, restore, r.targetDatabaseExists)
}

// decidePointInTimePathWith is decidePointInTimePath with the live existence
// check passed in.
func (r *Neo4jRestoreReconciler) decidePointInTimePathWith(
	ctx context.Context,
	restore *neo4jv1beta1.Neo4jRestore,
	databaseExists func(context.Context, restoreTarget, string) (bool, error),
) (res ctrl.Result, done bool, err error) {
	src := restore.Spec.Source
	if src.PointInTime == nil && src.Type != "pitr" {
		return ctrl.Result{}, false, nil
	}
	if _, decided := restore.Annotations[AnnotationPointInTimePath]; decided {
		return ctrl.Result{}, false, nil
	}
	target, err := r.getRestoreTarget(ctx, restore)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	calver := false
	image := target.image()
	if v, verr := neo4j.GetImageVersion(fmt.Sprintf("%s:%s", image.Repo, image.Tag)); verr == nil {
		calver = v.IsCalver
	}
	snap := resolvedBackupSnapshot(restore)
	why := pointInTimeOnlineBlocker(restore, snap, calver, false)
	if why == "" && target.isStandalone() && target.standalone.Annotations[RestoreInProgressAnnotation] == restore.Name {
		// Started offline by an operator that recorded no decision: the
		// instance is stopped, so there is nothing to ask, and it must finish
		// where it started.
		why = "this restore already holds the standalone stopped for an offline restore"
	}
	if why == "" {
		// Everything else allows it; only a live database is in the way.
		exists, existsErr := databaseExists(ctx, target, restore.Spec.Database)
		if existsErr != nil {
			r.updateRestoreStatus(ctx, restore, StatusPending,
				fmt.Sprintf("Checking whether database %q exists on %s %q before a point-in-time restore: %v", restore.Spec.Database, target.kind(), target.name(), existsErr))
			requeue := ctrl.Result{RequeueAfter: r.RequeueAfter}
			return requeue, true, nil
		}
		why = pointInTimeOnlineBlocker(restore, snap, calver, exists)
	}
	path := pointInTimeOnline
	if why != "" {
		path = "offline: " + why
	}
	if err := r.setRestoreAnnotation(ctx, restore, AnnotationPointInTimePath, path); err != nil {
		return ctrl.Result{}, true, err
	}
	if why != "" && !target.isStandalone() {
		msg := fmt.Sprintf("point-in-time restore into cluster %q cannot run: %s. A cluster restores only online", target.name(), why)
		r.updateRestoreStatus(ctx, restore, StatusFailed, msg)
		r.Recorder.Event(restore, corev1.EventTypeWarning, EventReasonRestoreFailed, msg)
		return ctrl.Result{}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// targetDatabaseExists asks the running target whether a database exists.
func (r *Neo4jRestoreReconciler) targetDatabaseExists(ctx context.Context, target restoreTarget, database string) (bool, error) {
	c, err := r.targetClient(target)
	if err != nil {
		return false, err
	}
	defer func() { _ = c.Close() }()
	return c.DatabaseExists(ctx, database)
}

// setRestoreAnnotation writes one annotation on the restore (refetch +
// RetryOnConflict) and mirrors it onto the in-memory object.
func (r *Neo4jRestoreReconciler) setRestoreAnnotation(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore, key, value string) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jRestore{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(restore), latest); err != nil {
			return err
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		if latest.Annotations[key] == value {
			return nil
		}
		latest.Annotations[key] = value
		return r.Update(ctx, latest)
	})
	if err != nil {
		return err
	}
	if restore.Annotations == nil {
		restore.Annotations = map[string]string{}
	}
	restore.Annotations[key] = value
	return nil
}
