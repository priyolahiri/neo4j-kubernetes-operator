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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// storageClassChangeScope keeps the spec.storage.className warning apart from
// the ValidationWarning set the validator reports on the same pass: each
// recordScoped scope is its own set (advisory_warnings.go).
const storageClassChangeScope = "storage-class"

// dataVolumeClasses maps each PVC to the StorageClass it names, "" for none.
func dataVolumeClasses(pvcs []corev1.PersistentVolumeClaim) map[string]string {
	classes := make(map[string]string, len(pvcs))
	for i := range pvcs {
		class := ""
		if pvcs[i].Spec.StorageClassName != nil {
			class = *pvcs[i].Spec.StorageClassName
		}
		classes[pvcs[i].Name] = class
	}
	return classes
}

// ClusterDataVolumeClasses maps each data volume (PVC) of the cluster named
// name to the StorageClass it names, found as the storage pass finds them.
// `kubectl neo4j validate --connect` reads them through this, so it says what
// the operator's advisory will say before the manifest is applied.
func ClusterDataVolumeClasses(ctx context.Context, c client.Client, namespace, name string) (map[string]string, error) {
	pvcs, err := (&Neo4jEnterpriseClusterReconciler{Client: c}).
		findPVCsForStatefulSet(ctx, namespace, name, fmt.Sprintf("%s-server", name), "data")
	if err != nil {
		return nil, err
	}
	return dataVolumeClasses(pvcs), nil
}

// StandaloneDataVolumeClasses is ClusterDataVolumeClasses for a standalone.
func StandaloneDataVolumeClasses(ctx context.Context, c client.Client, namespace, name string) (map[string]string, error) {
	pvcs, err := (&Neo4jEnterpriseStandaloneReconciler{Client: c}).findStandalonePVCs(ctx, namespace, name, "neo4j-data")
	if err != nil {
		return nil, err
	}
	return dataVolumeClasses(pvcs), nil
}

// recordStorageClassChange warns, as an advisory ValidationWarning, while
// spec.storage.className names a StorageClass the cluster's data volumes do
// not use: an edit of it changes nothing for volumes that exist (#532). A
// failed read is logged and changes nothing, so a warning already announced
// is neither repeated nor forgotten.
func (r *Neo4jEnterpriseClusterReconciler) recordStorageClassChange(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) {
	classes, err := ClusterDataVolumeClasses(ctx, r.Client, cluster.Namespace, cluster.Name)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to read the data volumes for the StorageClass check (non-fatal)")
		return
	}
	var warnings []string
	if w := validation.StorageClassChangeWarning(cluster.Spec.Storage.ClassName, classes); w != "" {
		warnings = append(warnings, w)
	}
	r.advisoryWarnings.recordScoped(r.Recorder, cluster, EventReasonValidationWarning, storageClassChangeScope, warnings)
}

// recordStorageClassChange is the cluster's check for a standalone.
func (r *Neo4jEnterpriseStandaloneReconciler) recordStorageClassChange(ctx context.Context, standalone *neo4jv1beta1.Neo4jEnterpriseStandalone) {
	classes, err := StandaloneDataVolumeClasses(ctx, r.Client, standalone.Namespace, standalone.Name)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to read the data volume for the StorageClass check (non-fatal)")
		return
	}
	var warnings []string
	if w := validation.StorageClassChangeWarning(standalone.Spec.Storage.ClassName, classes); w != "" {
		warnings = append(warnings, w)
	}
	r.advisoryWarnings.recordScoped(r.Recorder, standalone, EventReasonValidationWarning, storageClassChangeScope, warnings)
}
