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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// restoreTarget is the deployment a Neo4jRestore writes into.
//
// The online restore path — `dbms.recreateDatabase` / `CREATE DATABASE …
// OPTIONS {seedURI}` against the running DBMS — was written for
// Neo4jEnterpriseCluster. A standalone takes it too (rule 108): its database is
// replaced while every other database stays online, where the neo4j-admin Job
// stopped the whole instance. These are the places the two differ: the Bolt
// Service, the StatefulSet name, the pods' labels, and which CR carries the
// spec.env / spec.extraEnvFrom the server needs to reach a cloud seed.
type restoreTarget struct {
	cluster    *neo4jv1beta1.Neo4jEnterpriseCluster    // nil for a standalone
	standalone *neo4jv1beta1.Neo4jEnterpriseStandalone // nil for a cluster
}

// getRestoreTarget resolves spec.instanceRef to the real cluster or standalone.
func (r *Neo4jRestoreReconciler) getRestoreTarget(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore) (restoreTarget, error) {
	key := types.NamespacedName{Name: restore.Spec.InstanceRef, Namespace: restore.Namespace}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := r.Get(ctx, key, cluster); err == nil {
		return restoreTarget{cluster: cluster}, nil
	} else if !apierrors.IsNotFound(err) {
		return restoreTarget{}, err
	}
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	if err := r.Get(ctx, key, standalone); err != nil {
		return restoreTarget{}, fmt.Errorf("target %q not found as Neo4jEnterpriseCluster or Neo4jEnterpriseStandalone: %w", restore.Spec.InstanceRef, err)
	}
	return restoreTarget{standalone: standalone}, nil
}

func (t restoreTarget) isStandalone() bool { return t.standalone != nil }

func (t restoreTarget) object() client.Object {
	if t.standalone != nil {
		return t.standalone
	}
	return t.cluster
}

func (t restoreTarget) name() string { return t.object().GetName() }

// kind names the target in status messages: "cluster" or "standalone".
func (t restoreTarget) kind() string {
	if t.standalone != nil {
		return "standalone"
	}
	return "cluster"
}

func (t restoreTarget) annotations() map[string]string { return t.object().GetAnnotations() }

func (t restoreTarget) env() []corev1.EnvVar {
	if t.standalone != nil {
		return t.standalone.Spec.Env
	}
	return t.cluster.Spec.Env
}

func (t restoreTarget) extraEnvFrom() []corev1.EnvFromSource {
	if t.standalone != nil {
		return t.standalone.Spec.ExtraEnvFrom
	}
	return t.cluster.Spec.ExtraEnvFrom
}

// statefulSetName is the StatefulSet running the target's Neo4j servers: a
// cluster's `<name>-server`, a standalone's `<name>`.
func (t restoreTarget) statefulSetName() string {
	if t.standalone != nil {
		return t.standalone.Name
	}
	return t.cluster.Name + "-server"
}

// podLabels selects the target's Neo4j pods — the peers allowed through the PVC
// seed proxy's NetworkPolicy. A standalone's pods carry `app: <name>` (its
// StatefulSet selector) and not the cluster's `neo4j.com/cluster` label.
func (t restoreTarget) podLabels() map[string]string {
	if t.standalone != nil {
		return map[string]string{"app": t.standalone.Name}
	}
	return clusterPodLabels(t.cluster.Name)
}

// clusterPodLabels selects a Neo4jEnterpriseCluster's server pods.
func clusterPodLabels(clusterName string) map[string]string {
	return map[string]string{"neo4j.com/cluster": clusterName}
}

// targetClient opens a Bolt client against the target's `<name>-client`
// Service (#215), with the admin Secret each kind resolves its own way.
func (r *Neo4jRestoreReconciler) targetClient(t restoreTarget) (*neo4j.Client, error) {
	if t.standalone != nil {
		return neo4j.NewClientForEnterpriseStandalone(t.standalone, r.Client, getStandaloneAdminSecretName(t.standalone))
	}
	return neo4j.NewClientForEnterprise(t.cluster, r.Client, restoreAdminSecretName(t.cluster))
}

// updateTargetSpec refetches the target's CR and applies mutate to its
// spec.env, spec.extraEnvFrom and annotations, writing only when mutate
// reports a change. Refetch-inside-RetryOnConflict is mandatory: the owning
// controller rewrites its CR every reconcile.
func (r *Neo4jRestoreReconciler) updateTargetSpec(
	ctx context.Context,
	t restoreTarget,
	mutate func(env *[]corev1.EnvVar, envFrom *[]corev1.EnvFromSource, annotations map[string]string) bool,
) (bool, error) {
	changed := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var latest client.Object
		var env *[]corev1.EnvVar
		var envFrom *[]corev1.EnvFromSource
		if t.standalone != nil {
			s := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
			latest, env, envFrom = s, &s.Spec.Env, &s.Spec.ExtraEnvFrom
		} else {
			c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			latest, env, envFrom = c, &c.Spec.Env, &c.Spec.ExtraEnvFrom
		}
		if err := r.Get(ctx, client.ObjectKeyFromObject(t.object()), latest); err != nil {
			return err
		}
		annotations := latest.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		changed = mutate(env, envFrom, annotations)
		if !changed {
			return nil
		}
		latest.SetAnnotations(annotations)
		return r.Update(ctx, latest)
	})
	return changed, err
}

// restoreRunsOffline reports whether a restore must take the offline
// neo4j-admin Job, which stops the instance. A cluster never does (rule 75);
// a standalone does whenever standaloneOfflineReason names a reason.
func restoreRunsOffline(restore *neo4jv1beta1.Neo4jRestore, isTrueCluster bool) bool {
	if isTrueCluster {
		return false
	}
	return standaloneOfflineReason(restore) != ""
}

// standaloneOfflineReason says why a restore into a standalone cannot run
// online, or "" when it can (rule 108). Online — `dbms.recreateDatabase` or
// `CREATE DATABASE … OPTIONS {seedURI}` against the running instance — only
// the database being restored is unavailable; the neo4j-admin Job stops the
// whole instance. Online needs the server to fetch the whole seed itself, so
// a standalone restores online only when the operator knows it can:
//   - not point-in-time: recreate has no restore-until option;
//   - from a Neo4jBackup (`source.type: backup`) that recorded the artifact
//     for every database restored: a `source.type: storage` path may be a
//     directory, which only neo4j-admin resolves;
//   - from a PVC, FULL artifacts only: the server fetches a PVC seed over
//     HTTP through the seed proxy, one file, and Neo4j refuses a DIFF on its
//     own ("not part of a valid backup chain") — `backupType: AUTO`, the
//     default, makes every scheduled run after the first a DIFF.
//
// neo4j-admin restores every one of these, so they keep the Job.
func standaloneOfflineReason(restore *neo4jv1beta1.Neo4jRestore) string {
	if restore.Spec.Source.Type == "pitr" || restore.Spec.Source.PointInTime != nil {
		return "a point-in-time restore cannot be seeded online (dbms.recreateDatabase has no restore-until option)"
	}
	if restore.Spec.Source.Type != SourceTypeBackup {
		return fmt.Sprintf("source.type %q gives a path, which may be a directory or part of a backup chain; only neo4j-admin resolves those", restore.Spec.Source.Type)
	}
	snap := resolvedBackupSnapshot(restore)
	if snap == nil {
		return "the backup's location is not resolved yet"
	}
	if snap.Storage.Type != "pvc" && (snap.Storage.Cloud == nil || snap.Storage.Cloud.CredentialsSecretRef == "") {
		return "the backup's cloud storage authenticates by pod identity (no credentialsSecretRef); online, the standalone's own pods fetch the seed and cannot carry a workload identity, while the restore Job's ServiceAccount can"
	}
	artifacts := restoreSeedArtifacts(restore, snap)
	if len(artifacts) == 0 {
		return fmt.Sprintf("Neo4jBackup %q did not record the artifact to seed from", snap.BackupRef)
	}
	for _, a := range artifacts {
		if a.Filename == "" {
			return fmt.Sprintf("Neo4jBackup %q did not record the artifact for database %q", snap.BackupRef, a.Database)
		}
		if snap.Storage.Type != "pvc" {
			continue
		}
		switch a.Type {
		case backupArtifactFull:
		case backupArtifactDiff:
			return fmt.Sprintf("%s is a differential (DIFF) backup on a PVC; the server would fetch it over HTTP as one file, without the rest of its chain", a.Filename)
		default:
			return fmt.Sprintf("Neo4jBackup %q did not record whether %s is a FULL or a DIFF backup (backups taken before the operator recorded it); a DIFF on a PVC cannot be seeded over HTTP", snap.BackupRef, a.Filename)
		}
	}
	return ""
}

// restoreSeedArtifacts lists the artifacts a `source.type: backup` restore
// seeds from, as the resolved backup recorded them: every user database of an
// all-databases restore, otherwise the one database restored — the run's
// single artifact, or its entry in an all-databases backup.
func restoreSeedArtifacts(restore *neo4jv1beta1.Neo4jRestore, snap *neo4jv1beta1.ResolvedRestoreSource) []neo4jv1beta1.DatabaseArtifact {
	var artifacts []neo4jv1beta1.DatabaseArtifact
	switch {
	case restore.Spec.AllDatabases:
		for _, a := range snap.DatabaseArtifacts {
			if !strings.EqualFold(a.Database, "system") {
				artifacts = append(artifacts, a)
			}
		}
	case snap.ArtifactFilename != "":
		artifacts = []neo4jv1beta1.DatabaseArtifact{{Database: restoreSourceDatabase(restore), Filename: snap.ArtifactFilename, Type: snap.ArtifactType}}
	default:
		db := restoreSourceDatabase(restore)
		for _, a := range snap.DatabaseArtifacts {
			if a.Database == db {
				artifacts = append(artifacts, a)
			}
		}
	}
	return artifacts
}

// pvcDiffSeedRefusal explains why an online restore cannot seed artifact from
// a PVC when the backup recorded it as a DIFF, or returns "" otherwise. The
// seed proxy serves the one file over HTTP, and Neo4j refuses a differential
// without the rest of its chain ("not part of a valid backup chain") — after
// creating the database, which is left offline. A cluster has no Job to fall
// back to, so it refuses before creating anything; a standalone never gets
// here with a DIFF (standaloneOfflineReason sends it to the Job).
func pvcDiffSeedRefusal(storageType, filename, artifactType string) string {
	if storageType != "pvc" || artifactType != backupArtifactDiff {
		return ""
	}
	return fmt.Sprintf("%s is a differential (DIFF) backup on a PVC, which cannot be seeded online: the seed proxy serves it to the server over HTTP as one file, and Neo4j refuses a differential without the rest of its chain. Restore from a FULL backup (a Neo4jBackup with spec.options.backupType: FULL), or keep the backups this restores from on cloud storage, whose seed provider reads the whole chain", filename)
}

// restoreOnJobPath is restoreRunsOffline plus one case it cannot see: a Job
// restore that already holds the standalone stopped — started by an earlier
// operator, which sent every standalone restore to the Job. Finishing it
// online would talk Bolt to an instance with no pods, and nothing would bring
// the instance back.
func (r *Neo4jRestoreReconciler) restoreOnJobPath(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore, isTrueCluster bool) bool {
	if isTrueCluster {
		return false
	}
	if restoreRunsOffline(restore, isTrueCluster) {
		return true
	}
	sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.InstanceRef, Namespace: restore.Namespace}, sa); err != nil {
		return false
	}
	return sa.Annotations[RestoreInProgressAnnotation] == restore.Name
}
