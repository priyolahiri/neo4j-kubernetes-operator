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

package validation

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// StorageValidator validates Neo4j storage configuration
type StorageValidator struct{}

// NewStorageValidator creates a new storage validator
func NewStorageValidator() *StorageValidator {
	return &StorageValidator{}
}

// Validate validates the storage configuration
func (v *StorageValidator) Validate(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) field.ErrorList {
	var allErrs field.ErrorList
	storagePath := field.NewPath("spec", "storage")

	// An empty className is intentionally allowed: the PVC then inherits the
	// cluster's default StorageClass (see resources.StorageClassNamePtr). When a
	// className IS given, the reconciler verifies it exists at apply time and
	// surfaces an explicit error rather than leaving pods Pending indefinitely.

	if cluster.Spec.Storage.Size == "" {
		allErrs = append(allErrs, field.Required(
			storagePath.Child("size"),
			"storage size must be specified",
		))
	}

	if cluster.Spec.Storage.Size != "" {
		if err := storageSizeError(storagePath.Child("size"), cluster.Spec.Storage.Size); err != nil {
			allErrs = append(allErrs, err)
		}
	}

	return allErrs
}

// storageSizeError returns the error for a data-volume size that cannot be
// used, or nil. A size is usable when Kubernetes can parse it as a quantity and
// it is greater than zero; anything Kubernetes accepts beyond that (1.5Gi, 500M,
// 1e12) is accepted here too, because it is the apiserver, not this operator,
// that decides what a PVC request may be.
//
// Shared by StorageValidator (cluster) and StandaloneValidator, which had
// drifted in both directions: the cluster used a format regex that refused
// 100.5Gi and lowercase k yet admitted capital "5K", which is not a quantity at
// all and panicked the manager in resource.MustParse; the standalone checked
// only that the field was non-empty. The cluster also accepted "0", which no
// Neo4j store fits in. TestStandaloneAndClusterAgreeOnStorageSize pins parity.
func storageSizeError(fldPath *field.Path, size string) *field.Error {
	q, err := resource.ParseQuantity(size)
	if err != nil {
		return field.Invalid(fldPath, size,
			"storage size must be a valid Kubernetes quantity, e.g. '100Gi' or '1Ti'")
	}
	if q.Sign() <= 0 {
		return field.Invalid(fldPath, size, "storage size must be greater than zero")
	}
	return nil
}

// StorageClassChangeWarning reports a spec.storage.className that the data
// volumes already there do not use, or "". volumeClasses maps each data
// volume (its PVC name) to the StorageClass it names, "" for none.
//
// A volume keeps the StorageClass it was created with (a PVC's
// storageClassName cannot change), and so does the StatefulSet's volume claim
// template, so an edit of className changes nothing for a running deployment,
// and nothing said so (#532). Only a className that is set is compared: an
// empty one means the cluster's default StorageClass, which a volume created
// under it names already. Shared by the cluster and the standalone; the
// reconciler reads the PVCs and turns the result into an advisory
// ValidationWarning.
func StorageClassChangeWarning(className string, volumeClasses map[string]string) string {
	if className == "" {
		return ""
	}
	byClass := map[string][]string{}
	for volume, class := range volumeClasses {
		if class != className {
			byClass[class] = append(byClass[class], volume)
		}
	}
	if len(byClass) == 0 {
		return ""
	}
	classes := make([]string, 0, len(byClass))
	for class := range byClass {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	parts := make([]string, 0, len(classes))
	for _, class := range classes {
		volumes := byClass[class]
		sort.Strings(volumes)
		uses := fmt.Sprintf("StorageClass %q", class)
		if class == "" {
			uses = "no StorageClass"
		}
		verb := "use"
		if len(volumes) == 1 {
			verb = "uses"
		}
		parts = append(parts, fmt.Sprintf("%s %s %s", strings.Join(volumes, ", "), verb, uses))
	}
	return fmt.Sprintf("spec.storage.className %q has no effect on the data volumes that already exist (%s): a volume "+
		"keeps the StorageClass it was created with, and so does the StatefulSet's volume claim template. To move the "+
		"data to %q, back it up and restore it into a deployment created with that class; otherwise set "+
		"spec.storage.className to the class the volumes use", className, strings.Join(parts, "; "), className)
}
