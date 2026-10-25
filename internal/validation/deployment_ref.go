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

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A reference to a deployment that does not exist yet is not a validation
// error. Applying a database, a user or a role together with its cluster is
// the common case (one kubectl apply -f dir/, one GitOps sync), and every
// reconciler that resolves a clusterRef waits for it: phase Pending, one
// ClusterNotFound event, and it carries on once the deployment is there
// (docs/knowledge/operations.md ids 145 and 182). A validator that refused the
// missing deployment ran first and made that wait unreachable: the object
// went ValidationFailed, or Failed, until a later pass found the cluster. So
// the validators report it as pending, which `kubectl neo4j validate
// --connect` prints as such and the reconcilers leave to their own wait.

// LookupDeployment reads the Neo4jEnterpriseCluster named ref in namespace,
// else the Neo4jEnterpriseStandalone, as the reconcilers resolve a clusterRef.
// Both nil with a nil error means neither exists. Any other read failure is
// returned.
func LookupDeployment(ctx context.Context, c client.Client, namespace, ref string) (
	*neo4jv1beta1.Neo4jEnterpriseCluster, *neo4jv1beta1.Neo4jEnterpriseStandalone, error) {
	key := types.NamespacedName{Name: ref, Namespace: namespace}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := c.Get(ctx, key, cluster); err == nil {
		return cluster, nil, nil
	} else if !errors.IsNotFound(err) {
		return nil, nil, err
	}
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	if err := c.Get(ctx, key, standalone); err == nil {
		return nil, standalone, nil
	} else if !errors.IsNotFound(err) {
		return nil, nil, err
	}
	return nil, nil, nil
}

// DeploymentPending is the pending finding for a reference at path that names
// no deployment yet.
func DeploymentPending(path *field.Path, ref, namespace string) string {
	return fmt.Sprintf("%s: waiting for Neo4jEnterpriseCluster or Neo4jEnterpriseStandalone %q in namespace %q; "+
		"the operator waits for it and carries on once it exists", path, ref, namespace)
}

// DeploymentUnreadable is the warning for a reference at path whose
// deployment could not be read: the rules that need it were not checked.
func DeploymentUnreadable(path *field.Path, ref string, err error) string {
	return fmt.Sprintf("could not read the deployment %s names (%q), so the rules that need it were not checked: %v", path, ref, err)
}
