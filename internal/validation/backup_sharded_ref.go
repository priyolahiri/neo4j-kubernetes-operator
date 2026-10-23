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
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// ResolveBackupShardedDatabase reads the Neo4jShardedDatabase that a
// ShardedDatabase-scope backup names in spec.shardedDatabase, the way the
// backup controller resolves it before it runs anything: by the CR's
// metadata.name in the backup's namespace — not by the logical database name,
// the CR's spec.name, which often differs — and on the backup's own
// spec.instanceRef.
//
// The backup controller (shardedPreflightStatic) and `kubectl neo4j validate
// --connect` share it, so the CLI refuses the reference the operator fails the
// backup for; the CLI used to pass it. refusal is that reference: no such CR
// (naming the CR whose spec.name it matches, the usual mistake), or a CR on
// another cluster. err is a read that failed for another reason. All three are
// nil for a backup of another scope.
func ResolveBackupShardedDatabase(ctx context.Context, c client.Reader, backup *neo4jv1beta1.Neo4jBackup) (sd *neo4jv1beta1.Neo4jShardedDatabase, refusal *field.Error, err error) {
	if backup.Spec.Scope() != neo4jv1beta1.BackupTargetKindShardedDatabase {
		return nil, nil, nil
	}
	path := field.NewPath("spec", "shardedDatabase")
	name, ns := backup.Spec.ShardedDatabase, backup.Namespace

	sd = &neo4jv1beta1.Neo4jShardedDatabase{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, sd); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, nil, err
		}
		detail := fmt.Sprintf("Neo4jShardedDatabase %q not found in namespace %q", name, ns)
		if cr := shardedDatabaseNamed(ctx, c, ns, name); cr != "" {
			detail += fmt.Sprintf("; Neo4jShardedDatabase %q has spec.name %q, and spec.shardedDatabase takes the CR's metadata.name: set it to %q",
				cr, name, cr)
		}
		return nil, field.Invalid(path, name, detail), nil
	}
	if sd.Spec.ClusterRef != backup.Spec.InstanceRef {
		return sd, field.Invalid(path, name, fmt.Sprintf(
			"Neo4jShardedDatabase %q references cluster %q but spec.instanceRef is %q",
			sd.Name, sd.Spec.ClusterRef, backup.Spec.InstanceRef)), nil
	}
	return sd, nil, nil
}

// shardedDatabaseNamed returns the name of the Neo4jShardedDatabase in ns whose
// logical database name is database, compared ignoring case as Neo4j compares
// database names, or "" when there is none or the list cannot be read.
func shardedDatabaseNamed(ctx context.Context, c client.Reader, ns, database string) string {
	list := &neo4jv1beta1.Neo4jShardedDatabaseList{}
	if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
		return ""
	}
	for i := range list.Items {
		if strings.EqualFold(list.Items[i].Spec.Name, database) {
			return list.Items[i].Name
		}
	}
	return ""
}
