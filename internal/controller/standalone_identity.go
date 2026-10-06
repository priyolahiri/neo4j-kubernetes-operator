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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// standalonePodIdentityAnnotation stamps the pod template with a hash of
// spec.podServiceAccountAnnotations, so changing them rolls the pod: identity
// webhooks (IRSA, GKE Workload Identity) inject at pod admission, and a running
// pod never sees an annotation added to its ServiceAccount later.
const standalonePodIdentityAnnotation = "neo4j.com/pod-identity-hash"

// standaloneHasPodIdentity reports whether the standalone's pod runs under a
// workload identity the operator gave it (spec.podServiceAccountAnnotations).
// A nil standalone has none.
func standaloneHasPodIdentity(s *neo4jv1beta1.Neo4jEnterpriseStandalone) bool {
	return s != nil && len(s.Spec.PodServiceAccountAnnotations) > 0
}

// standaloneServiceAccountName is the ServiceAccount the operator creates for
// a standalone with a pod identity.
func standaloneServiceAccountName(s *neo4jv1beta1.Neo4jEnterpriseStandalone) string {
	return s.Name + "-neo4j"
}

// podIdentityHash is a stable hash of the identity annotations.
func podIdentityHash(annotations map[string]string) string {
	keys := make([]string, 0, len(annotations))
	for k := range annotations {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%s\n", k, annotations[k])
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// applyStandalonePodIdentity points the pod template at the operator-managed
// ServiceAccount and stamps the identity hash. Without a pod identity the
// template is left as it always was — the namespace's default ServiceAccount
// — so standalones that do not use the field are not restarted by it.
func applyStandalonePodIdentity(s *neo4jv1beta1.Neo4jEnterpriseStandalone, tmpl *corev1.PodTemplateSpec) {
	if !standaloneHasPodIdentity(s) {
		return
	}
	tmpl.Spec.ServiceAccountName = standaloneServiceAccountName(s)
	if tmpl.Annotations == nil {
		tmpl.Annotations = map[string]string{}
	}
	tmpl.Annotations[standalonePodIdentityAnnotation] = podIdentityHash(s.Spec.PodServiceAccountAnnotations)
}

// ensureStandaloneServiceAccount creates or updates the ServiceAccount a
// standalone with a pod identity runs under, before the StatefulSet that
// references it, so the pod is admitted with the identity. The operator owns
// only the annotation keys in the spec (removals honoured); foreign ones stay.
func (r *Neo4jEnterpriseStandaloneReconciler) ensureStandaloneServiceAccount(ctx context.Context, s *neo4jv1beta1.Neo4jEnterpriseStandalone) error {
	if !standaloneHasPodIdentity(s) {
		return nil
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: standaloneServiceAccountName(s), Namespace: s.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		if err := controllerutil.SetControllerReference(s, sa, r.Scheme); err != nil {
			return err
		}
		applyOwnedMetadata(sa, s.Spec.PodServiceAccountAnnotations, map[string]string{
			"app.kubernetes.io/name":       "neo4j",
			"app.kubernetes.io/instance":   s.Name,
			"app.kubernetes.io/managed-by": "neo4j-operator",
		})
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile ServiceAccount %s: %w", sa.Name, err)
	}
	return nil
}

// cleanupStandaloneServiceAccount deletes the ServiceAccount once the pod
// identity is removed from the spec and the StatefulSet no longer runs pods
// under it — a ServiceAccount bound to a cloud role should not outlive its
// use. One the operator did not create (no controller reference to this
// standalone) is left alone.
func (r *Neo4jEnterpriseStandaloneReconciler) cleanupStandaloneServiceAccount(ctx context.Context, s *neo4jv1beta1.Neo4jEnterpriseStandalone) error {
	if standaloneHasPodIdentity(s) {
		return nil
	}
	name := standaloneServiceAccountName(s)
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: s.Name, Namespace: s.Namespace}, sts); err == nil {
		if sts.Spec.Template.Spec.ServiceAccountName == name {
			return nil
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	sa := &corev1.ServiceAccount{}
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: s.Namespace}, sa); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if ref := metav1.GetControllerOf(sa); ref == nil || ref.UID != s.UID {
		return nil
	}
	if err := r.Delete(ctx, sa); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ServiceAccount %s: %w", name, err)
	}
	return nil
}
