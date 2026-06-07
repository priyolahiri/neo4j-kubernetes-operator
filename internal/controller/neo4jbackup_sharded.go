/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"fmt"
	"regexp"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/neo4j-partners/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/neo4j-partners/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/neo4j-partners/neo4j-kubernetes-operator/internal/validation"
)

// shardedShardNamePattern matches the per-shard naming convention Neo4j uses
// for property-sharded databases: {logical-name}-g000 (graph shard) and
// {logical-name}-p000…p{N-1} (property shards). Used by the glob-safety check
// to confirm every database matching the backup glob is actually a shard of the
// target sharded DB and not an unrelated database that happens to share the
// logical name as a prefix.
//
// Build the regex per-target since the logical name is user-supplied.
func shardedShardNamePattern(logicalName string) *regexp.Regexp {
	// QuoteMeta escapes any regex special chars in the user-supplied name so
	// e.g. a logical name containing "." can't widen the match.
	return regexp.MustCompile(`^` + regexp.QuoteMeta(logicalName) + `-(g|p)\d{3}$`)
}

// preflightAction tells the caller how to route after the sharded preflight.
type preflightAction int

const (
	preflightContinue preflightAction = iota // checks passed, continue normally
	preflightWait                            // not yet ready, requeue without changing phase to terminal
	preflightFail                            // terminal failure, set Phase=Failed and stop retrying
)

// shardedPreflightStatic runs the cheap, no-Neo4j-connection checks for a
// ShardedDatabase backup. Returns:
//   - preflightContinue if the backup is OK to proceed.
//   - preflightWait with a message if the sharded DB CR exists but is not yet
//     Ready. Caller should set status to "Pending" / "Waiting" and requeue.
//   - preflightFail with an error if the backup target is fundamentally
//     misconfigured (sharding disabled on cluster, version too old, sharded DB
//     CR missing, clusterRef mismatch). Caller should set Phase=Failed without
//     retrying.
//
// Returns preflightContinue immediately for non-ShardedDatabase kinds so the
// helper is safe to call unconditionally.
func (r *Neo4jBackupReconciler) shardedPreflightStatic(ctx context.Context, backup *neo4jv1beta1.Neo4jBackup, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (preflightAction, string, error) {
	if backup.Spec.Target.Kind != neo4jv1beta1.BackupTargetKindShardedDatabase {
		return preflightContinue, "", nil
	}

	if err := validation.IsClusterShardingReady(cluster); err != nil {
		return preflightFail, "", fmt.Errorf("cluster preflight failed: %w", err)
	}

	ns := backup.Spec.Target.Namespace
	if ns == "" {
		ns = backup.Namespace
	}
	shardedDB := &neo4jv1beta1.Neo4jShardedDatabase{}
	if err := r.Get(ctx, types.NamespacedName{Name: backup.Spec.Target.Name, Namespace: ns}, shardedDB); err != nil {
		if errors.IsNotFound(err) {
			return preflightFail, "", fmt.Errorf("Neo4jShardedDatabase %q not found in namespace %q", backup.Spec.Target.Name, ns)
		}
		return preflightFail, "", fmt.Errorf("failed to fetch Neo4jShardedDatabase %q: %w", backup.Spec.Target.Name, err)
	}

	if shardedDB.Spec.ClusterRef != backup.Spec.Target.ClusterRef {
		return preflightFail, "", fmt.Errorf("Neo4jShardedDatabase %q references cluster %q but backup target.clusterRef is %q", shardedDB.Name, shardedDB.Spec.ClusterRef, backup.Spec.Target.ClusterRef)
	}

	if shardedDB.Status.ShardingReady == nil || !*shardedDB.Status.ShardingReady {
		return preflightWait, fmt.Sprintf("Neo4jShardedDatabase %q is not yet Ready", shardedDB.Name), nil
	}

	return preflightContinue, "", nil
}

// shardedPreflightGlobSafety runs the expensive `SHOW DATABASES` check that
// guards against the neo4j-admin glob `{logical-name}*` pulling in unrelated
// databases (e.g. a backup for "products" silently including "productsales").
// Only call this for ShardedDatabase kinds AND only at Job-creation time —
// running it on every reconcile would issue a Bolt query every poll.
//
// Returns nil if the glob is safe. Returns an error naming the offending
// database(s) otherwise; the caller should route the backup to Phase=Failed.
func (r *Neo4jBackupReconciler) shardedPreflightGlobSafety(ctx context.Context, backup *neo4jv1beta1.Neo4jBackup, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error {
	if backup.Spec.Target.Kind != neo4jv1beta1.BackupTargetKindShardedDatabase {
		return nil
	}

	client, err := neo4jclient.NewClientForEnterprise(cluster, r.Client, cluster.Spec.Auth.AdminSecret)
	if err != nil {
		return fmt.Errorf("failed to open Neo4j client for glob-safety check: %w", err)
	}
	defer client.Close()

	dbs, err := client.GetDatabases(ctx)
	if err != nil {
		return fmt.Errorf("failed to list databases for glob-safety check: %w", err)
	}

	logical := backup.Spec.Target.Name
	shardPattern := shardedShardNamePattern(logical)
	prefix := logical
	var poisoning []string
	for _, db := range dbs {
		// SHOW DATABASES returns duplicate rows per database (one per role).
		// Filter by exact name match against the prefix and shard regex.
		if len(db.Name) < len(prefix) || db.Name[:len(prefix)] != prefix {
			continue
		}
		if db.Name == logical {
			// The "virtual" aggregated database — same name as the logical
			// sharded DB. Today this doesn't appear in SHOW DATABASES until
			// first access (per neo4jshardeddatabase_controller.go), but if a
			// future Neo4j release surfaces it we still want to allow it
			// since it's part of the same sharded family.
			continue
		}
		if shardPattern.MatchString(db.Name) {
			continue
		}
		// Deduplicate names that appear once per role row.
		if !contains(poisoning, db.Name) {
			poisoning = append(poisoning, db.Name)
		}
	}
	if len(poisoning) > 0 {
		return fmt.Errorf("backup glob %q would also match unrelated database(s): %v — rename or drop the conflicting database(s) before running this backup", logical+"*", poisoning)
	}
	return nil
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// applyShardedPreflight is the convenience wrapper used by the reconcile flow:
// runs the static preflight, translates results to status/result/error tuples
// the caller can return verbatim. Returns (done=true, result, err) when the
// caller should return immediately. Returns (false, _, _) to continue.
func (r *Neo4jBackupReconciler) applyShardedPreflight(ctx context.Context, backup *neo4jv1beta1.Neo4jBackup, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) (bool, ctrl.Result, error) {
	action, waitMsg, err := r.shardedPreflightStatic(ctx, backup, cluster)
	switch action {
	case preflightContinue:
		return false, ctrl.Result{}, nil
	case preflightWait:
		r.updateBackupStatus(ctx, backup, "Waiting", waitMsg)
		return true, ctrl.Result{RequeueAfter: r.RequeueAfter}, nil
	case preflightFail:
		r.updateBackupStatus(ctx, backup, "Failed", err.Error())
		// Returning nil error so controller-runtime doesn't requeue with
		// backoff. The terminal-phase guard in handleOneTimeBackup keeps the
		// CR pinned to Failed.
		return true, ctrl.Result{}, nil
	}
	return false, ctrl.Result{}, nil
}
