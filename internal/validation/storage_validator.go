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
