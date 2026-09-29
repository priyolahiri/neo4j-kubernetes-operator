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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// serverCypherLanguageFacts reads what the stamp depends on from the cluster:
// whether the deployment's StatefulSet already exists (sts.UID != "", the
// codebase's existence check), and db.query.default_language in the
// operator's current ConfigMap.
func serverCypherLanguageFacts(
	ctx context.Context, c client.Client, namespace, stsName, cmName string,
) (exists bool, running string, err error) {
	sts := &appsv1.StatefulSet{}
	switch err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: stsName}, sts); {
	case err == nil:
		exists = sts.UID != ""
	case apierrors.IsNotFound(err):
	default:
		return false, "", err
	}
	cm := &corev1.ConfigMap{}
	switch err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: cmName}, cm); {
	case err == nil:
		running = resources.Neo4jConfSettings(cm.Data["neo4j.conf"])[resources.ServerCypherLanguageKey]
	case apierrors.IsNotFound(err):
	default:
		return false, "", err
	}
	return exists, running, nil
}

// stampClusterCypherLanguage resolves and records
// status.effectiveCypherLanguage BEFORE the ConfigMap is built, so the builder
// — which reads the stamp — writes the resolved value on the very reconcile
// that creates the deployment. If the status write is lost, the next reconcile
// finds the StatefulSet and the ConfigMap carrying the value and records the
// same thing (StampServerCypherLanguage's "existing, never stamped" row).
func (r *Neo4jEnterpriseClusterReconciler) stampClusterCypherLanguage(
	ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster,
) error {
	exists, running, err := serverCypherLanguageFacts(ctx, r.Client, cluster.Namespace,
		cluster.Name+"-server", cluster.Name+"-config")
	if err != nil {
		return err
	}
	var shardingConfig map[string]string
	if cluster.Spec.PropertySharding != nil {
		shardingConfig = cluster.Spec.PropertySharding.Config
	}
	stamp := resources.StampServerCypherLanguage(resources.ServerCypherLanguageInputs{
		Spec:    cluster.Spec.ServerDefaultCypherLanguage,
		Legacy:  resources.LegacyServerCypherLanguage(cluster.Spec.Config, shardingConfig),
		Stamped: cluster.Status.EffectiveCypherLanguage,
		CalVer:  resources.IsCalverImage(cluster.Spec.Image.Tag),
		Exists:  exists,
		Running: running,
	})
	if stamp == cluster.Status.EffectiveCypherLanguage {
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), latest); err != nil {
			return err
		}
		latest.Status.EffectiveCypherLanguage = stamp
		return r.Status().Update(ctx, latest)
	}); err != nil {
		return err
	}
	cluster.Status.EffectiveCypherLanguage = stamp
	return nil
}

// stampStandaloneCypherLanguage is stampClusterCypherLanguage for a
// standalone, whose StatefulSet is named after the CR.
func (r *Neo4jEnterpriseStandaloneReconciler) stampStandaloneCypherLanguage(
	ctx context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone,
) error {
	exists, running, err := serverCypherLanguageFacts(ctx, r.Client, standalone.Namespace,
		standalone.Name, standalone.Name+"-config")
	if err != nil {
		return err
	}
	stamp := resources.StampServerCypherLanguage(resources.ServerCypherLanguageInputs{
		Spec:    standalone.Spec.ServerDefaultCypherLanguage,
		Legacy:  resources.LegacyServerCypherLanguage(standalone.Spec.Config),
		Stamped: standalone.Status.EffectiveCypherLanguage,
		CalVer:  resources.IsCalverImage(standalone.Spec.Image.Tag),
		Exists:  exists,
		Running: running,
	})
	if stamp == standalone.Status.EffectiveCypherLanguage {
		return nil
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(standalone), latest); err != nil {
			return err
		}
		latest.Status.EffectiveCypherLanguage = stamp
		return r.Status().Update(ctx, latest)
	}); err != nil {
		return err
	}
	standalone.Status.EffectiveCypherLanguage = stamp
	return nil
}
