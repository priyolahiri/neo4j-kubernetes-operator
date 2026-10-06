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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
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

// image is the Neo4j image the target's servers run; the seed proxy merges
// backup chains with the same version's neo4j-admin.
func (t restoreTarget) image() neo4jv1beta1.ImageSpec {
	if t.standalone != nil {
		return t.standalone.Spec.Image
	}
	return t.cluster.Spec.Image
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
func restoreRunsOffline(restore *neo4jv1beta1.Neo4jRestore, isTrueCluster bool, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) bool {
	if isTrueCluster {
		return false
	}
	return standaloneOfflineReason(restore, standalone) != ""
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
//   - from cloud storage, with a credentialsSecretRef — or a standalone whose
//     pod runs under a workload identity (spec.podServiceAccountAnnotations).
//     Otherwise its pod holds no cloud identity; the restore Job's
//     ServiceAccount can. standalone is the restore's target, nil if unknown.
//
// A differential on a PVC restores online: the seed proxy merges its chain
// into one full artifact first (rule 109). neo4j-admin restores every case
// above, so they keep the Job.
func standaloneOfflineReason(restore *neo4jv1beta1.Neo4jRestore, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) string {
	if restore.Spec.Source.Type == "pitr" || restore.Spec.Source.PointInTime != nil {
		if why := pointInTimeOfflineReason(restore); why != "" {
			return why
		}
	}
	if restore.Spec.Source.Type != SourceTypeBackup {
		return fmt.Sprintf("source.type %q gives a path, which may be a directory or part of a backup chain; only neo4j-admin resolves those", restore.Spec.Source.Type)
	}
	snap := resolvedBackupSnapshot(restore)
	if snap == nil {
		return "the backup's location is not resolved yet"
	}
	if snap.Storage.Type != "pvc" && (snap.Storage.Cloud == nil || snap.Storage.Cloud.CredentialsSecretRef == "") && !standaloneHasPodIdentity(standalone) {
		return "the backup's cloud storage authenticates by pod identity (no credentialsSecretRef); online, the standalone's own pod fetches the seed, and it has no workload identity (set spec.podServiceAccountAnnotations on the standalone to give it one), while the restore Job's ServiceAccount can"
	}
	artifacts := restoreSeedArtifacts(restore, snap)
	if len(artifacts) == 0 {
		return fmt.Sprintf("Neo4jBackup %q did not record the artifact to seed from", snap.BackupRef)
	}
	for _, a := range artifacts {
		if a.Filename == "" {
			return fmt.Sprintf("Neo4jBackup %q did not record the artifact for database %q", snap.BackupRef, a.Database)
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

// restoreOnJobPath is restoreRunsOffline, pinned to the path a restore
// started on — the standalone's pod identity is live spec, and removing it
// mid-restore must not move the restore. A Job restore that holds the
// standalone stopped stays on the Job (including one started by an earlier
// operator, which sent every standalone restore there: finishing it online
// would talk Bolt to an instance with no pods, and nothing would bring the
// instance back). An online restore already issued stays online.
func (r *Neo4jRestoreReconciler) restoreOnJobPath(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore, isTrueCluster bool) bool {
	if isTrueCluster {
		return false
	}
	standalone := r.restoreStandalone(ctx, restore)
	if standalone != nil && standalone.Annotations[RestoreInProgressAnnotation] == restore.Name {
		return true
	}
	if _, issued := restore.Annotations[AnnotationCypherRestoreIssued]; issued || len(restore.Status.DatabaseResults) > 0 {
		return false
	}
	return restoreRunsOffline(restore, false, standalone)
}

// restoreStandalone fetches the restore's target standalone; nil for a
// cluster target or one that cannot be read.
func (r *Neo4jRestoreReconciler) restoreStandalone(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore) *neo4jv1beta1.Neo4jEnterpriseStandalone {
	s := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	if err := r.Get(ctx, types.NamespacedName{Name: restore.Spec.InstanceRef, Namespace: restore.Namespace}, s); err != nil {
		return nil
	}
	return s
}

// standaloneIdentityRolledOut reports whether a standalone's pod runs under its
// workload-identity ServiceAccount: the StatefulSet's template names it, and
// the pod has been recreated from that template and is Ready. Identity
// webhooks inject at pod admission, so a pod admitted before the field was set
// cannot fetch a seed by pod identity.
func (r *Neo4jRestoreReconciler) standaloneIdentityRolledOut(ctx context.Context, s *neo4jv1beta1.Neo4jEnterpriseStandalone) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: s.Name, Namespace: s.Namespace}, sts); err != nil {
		return false, err
	}
	tmpl := sts.Spec.Template
	if tmpl.Spec.ServiceAccountName != standaloneServiceAccountName(s) ||
		tmpl.Annotations[standalonePodIdentityAnnotation] != podIdentityHash(s.Spec.PodServiceAccountAnnotations) {
		return false, nil
	}
	desired := int32(1)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}
	return sts.Status.ObservedGeneration == sts.Generation &&
		sts.Status.UpdatedReplicas == desired &&
		sts.Status.ReadyReplicas == desired, nil
}

// awaitSeedIdentity holds an online restore from cloud storage without a
// credentialsSecretRef into a standalone until its pod runs under the workload
// identity (spec.podServiceAccountAnnotations): seeding earlier fails inside
// Neo4j with no retry. ready=false means return res — the restore is Pending.
// Clusters need no wait: their pods always run under their ServiceAccount.
func (r *Neo4jRestoreReconciler) awaitSeedIdentity(ctx context.Context, restore *neo4jv1beta1.Neo4jRestore, target restoreTarget, storage neo4jv1beta1.StorageLocation) (res ctrl.Result, ready bool) {
	if !target.isStandalone() || storage.Type == "pvc" || (storage.Cloud != nil && storage.Cloud.CredentialsSecretRef != "") {
		return ctrl.Result{}, true
	}
	if ok, err := r.standaloneIdentityRolledOut(ctx, target.standalone); err != nil || !ok {
		msg := fmt.Sprintf("Waiting for standalone %q's pod to run under ServiceAccount %q, which carries its workload identity", target.name(), standaloneServiceAccountName(target.standalone))
		if err != nil {
			msg += fmt.Sprintf(" (rollout check pending: %v)", err)
		}
		r.updateRestoreStatus(ctx, restore, StatusPending, msg)
		requeue := ctrl.Result{RequeueAfter: r.RequeueAfter}
		return requeue, false
	}
	return ctrl.Result{}, true
}
