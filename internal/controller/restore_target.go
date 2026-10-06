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

// targetClient opens a Bolt client against the target: a cluster's
// `<name>-client` routing Service, a standalone's `<name>-service` (#187).
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
// neo4j-admin Job, which stops the instance: only a point-in-time restore into
// a standalone (rule 108). `dbms.recreateDatabase` has no restore-until option,
// so a point-in-time restore over an existing database cannot be seeded
// online. Everything else — every cluster restore, and a standalone's restore
// of one database or all of them — runs online against the live DBMS, where
// only the database being restored is unavailable.
func restoreRunsOffline(restore *neo4jv1beta1.Neo4jRestore, isTrueCluster bool) bool {
	if isTrueCluster {
		return false
	}
	return restore.Spec.Source.Type == "pitr" || restore.Spec.Source.PointInTime != nil
}
