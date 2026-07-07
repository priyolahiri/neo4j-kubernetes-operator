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
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// validateAuraInstanceRef confirms a referenced AuraInstance exists in the
// namespace. NotFound → an error on spec.auraInstanceRef; a transient get error
// → a warning (best-effort — don't block on a flaky read). Shared by the
// user/role/rolebinding validators so an Aura-targeted auth CR validates against
// the instance instead of a cluster/standalone.
func validateAuraInstanceRef(ctx context.Context, c client.Client, name, namespace string, errs *field.ErrorList, warnings *[]string) {
	if c == nil {
		return
	}
	path := field.NewPath("spec", "auraInstanceRef")
	ai := &neo4jv1beta1.AuraInstance{}
	err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, ai)
	switch {
	case err == nil:
		return
	case apierrors.IsNotFound(err):
		*errs = append(*errs, field.NotFound(path,
			fmt.Sprintf("no AuraInstance named %q in namespace %q", name, namespace)))
	default:
		*warnings = append(*warnings,
			fmt.Sprintf("could not verify auraInstanceRef %q: %v", name, err))
	}
}
