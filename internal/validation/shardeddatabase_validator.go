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

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// ShardedDatabaseValidator validates Neo4jShardedDatabase resources
type ShardedDatabaseValidator struct {
	client client.Client
}

// NewShardedDatabaseValidator creates a new sharded database validator
func NewShardedDatabaseValidator(client client.Client) *ShardedDatabaseValidator {
	return &ShardedDatabaseValidator{
		client: client,
	}
}

// ShardedDatabaseValidationResult holds validation results including warnings
type ShardedDatabaseValidationResult struct {
	Errors   field.ErrorList
	Warnings []string
	// Pending names what the sharded database waits for rather than what is
	// wrong with it: a clusterRef that names no cluster yet. The reconciler
	// makes that a wait (phase Pending, ClusterNotFound), not a refusal
	// (operations.md id 145), and applies in either order are fine.
	Pending []string
	// ClusterReadErr is a failure to read the referenced cluster for any reason
	// other than its absence. The rules that need the cluster were not run.
	ClusterReadErr error
}

// Validate runs every rule and reports every finding. The rules on the spec
// alone always run; the rules that need the referenced cluster run when it can
// be read. A failed read used to return at once and drop the errors the spec
// rules had found, so a user fixed one error, applied again, and only then saw
// the rest (#532).
func (v *ShardedDatabaseValidator) Validate(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase) ShardedDatabaseValidationResult {
	result := &ShardedDatabaseValidationResult{
		Errors:   field.ErrorList{},
		Warnings: []string{},
	}

	v.validateBasicFields(shardedDB, result)
	v.validatePropertyShardingConfig(shardedDB, result)
	v.validateTopologyConfig(shardedDB, result)
	v.validateCypherLanguage(shardedDB, result)

	// Last: the rules that need the cluster.
	v.validateClusterReference(ctx, shardedDB, result)
	return *result
}

// ValidateShardedDatabase is Validate for the reconciler: an error when the
// spec is refused, or when the cluster could not be read. A cluster that does
// not exist is not an error here: the reconciler waits for it.
func (v *ShardedDatabaseValidator) ValidateShardedDatabase(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase) error {
	result := v.Validate(ctx, shardedDB)

	// The errors alone: the reconciler's status message says "Validation
	// failed" itself, and the prefix here doubled it.
	var errs []error
	if agg := result.Errors.ToAggregate(); agg != nil {
		errs = append(errs, agg.Errors()...)
	}
	if result.ClusterReadErr != nil {
		errs = append(errs, fmt.Errorf("failed to get cluster %s: %w", shardedDB.Spec.ClusterRef, result.ClusterReadErr))
	}
	return utilerrors.NewAggregate(errs)
}

// validateBasicFields validates required basic fields
func (v *ShardedDatabaseValidator) validateBasicFields(shardedDB *neo4jv1beta1.Neo4jShardedDatabase, result *ShardedDatabaseValidationResult) {
	specPath := field.NewPath("spec")

	if shardedDB.Spec.ClusterRef == "" {
		result.Errors = append(result.Errors, field.Required(specPath.Child("clusterRef"), "cluster reference is required"))
	}

	if shardedDB.Spec.Name == "" {
		result.Errors = append(result.Errors, field.Required(specPath.Child("name"), "database name is required"))
	} else {
		// Validate database name format
		if err := v.validateDatabaseName(shardedDB.Spec.Name); err != nil {
			result.Errors = append(result.Errors, field.Invalid(specPath.Child("name"), shardedDB.Spec.Name, err.Error()))
		}
	}
}

// validateClusterReference validates that the referenced cluster supports
// property sharding and has room for the shards. Without a clusterRef (already
// an error) or a client there is nothing to read; a cluster that does not exist
// yet is Pending; a read that fails otherwise is ClusterReadErr.
func (v *ShardedDatabaseValidator) validateClusterReference(ctx context.Context, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, result *ShardedDatabaseValidationResult) {
	specPath := field.NewPath("spec")
	if shardedDB.Spec.ClusterRef == "" || v.client == nil {
		return
	}

	// Get the referenced cluster
	var cluster neo4jv1beta1.Neo4jEnterpriseCluster
	clusterKey := types.NamespacedName{
		Name:      shardedDB.Spec.ClusterRef,
		Namespace: shardedDB.Namespace,
	}

	if err := v.client.Get(ctx, clusterKey, &cluster); err != nil {
		if errors.IsNotFound(err) {
			result.Pending = append(result.Pending, fmt.Sprintf(
				"waiting for Neo4jEnterpriseCluster %q in namespace %q (spec.clusterRef)", shardedDB.Spec.ClusterRef, shardedDB.Namespace))
			return
		}
		result.ClusterReadErr = err
		return
	}

	// Validate cluster supports property sharding
	if cluster.Spec.PropertySharding == nil || !cluster.Spec.PropertySharding.Enabled {
		result.Errors = append(result.Errors, field.Invalid(
			specPath.Child("clusterRef"),
			shardedDB.Spec.ClusterRef,
			"referenced cluster does not have property sharding enabled"))
	}

	// Check if cluster is ready
	if cluster.Status.Phase != "Ready" {
		result.Warnings = append(result.Warnings, fmt.Sprintf("referenced cluster %s is not ready (current phase: %s)", cluster.Name, cluster.Status.Phase))
	}

	// Check if property sharding is ready on the cluster
	if cluster.Status.PropertyShardingReady == nil || !*cluster.Status.PropertyShardingReady {
		result.Warnings = append(result.Warnings, fmt.Sprintf("property sharding is not ready on cluster %s", cluster.Name))
	}

	// Validate that the cluster has sufficient capacity for all shards
	v.validateClusterCapacity(&cluster, shardedDB, result)
}

// validatePropertyShardingConfig validates property sharding configuration
func (v *ShardedDatabaseValidator) validatePropertyShardingConfig(shardedDB *neo4jv1beta1.Neo4jShardedDatabase, result *ShardedDatabaseValidationResult) {
	shardingPath := field.NewPath("spec", "propertySharding")
	config := &shardedDB.Spec.PropertySharding

	// Validate property shards count
	if config.PropertyShards < 1 {
		result.Errors = append(result.Errors, field.Invalid(
			shardingPath.Child("propertyShards"),
			config.PropertyShards,
			"propertyShards must be at least 1"))
	} else if config.PropertyShards > 1000 {
		result.Errors = append(result.Errors, field.Invalid(
			shardingPath.Child("propertyShards"),
			config.PropertyShards,
			"propertyShards cannot exceed 1000"))
	}

	// Performance warnings
	if config.PropertyShards > 16 {
		result.Warnings = append(result.Warnings, "using more than 16 property shards may impact query performance")
	}

	// Validate seed options
	specPath := field.NewPath("spec")
	hasSeedSource := shardedDB.Spec.SeedURI != "" || len(shardedDB.Spec.SeedURIs) > 0 || shardedDB.Spec.SeedBackupRef != ""

	if shardedDB.Spec.SeedURI != "" && len(shardedDB.Spec.SeedURIs) > 0 {
		result.Errors = append(result.Errors, field.Invalid(
			specPath.Child("seedURI"),
			shardedDB.Spec.SeedURI,
			"seedURI and seedURIs cannot be specified together"))
	}

	// SeedBackupRef is mutually exclusive with the manual seed URI fields —
	// it materialises into a seedURI at reconcile time, so passing both lets
	// users contradict themselves without the operator picking a winner.
	if shardedDB.Spec.SeedBackupRef != "" && shardedDB.Spec.SeedURI != "" {
		result.Errors = append(result.Errors, field.Invalid(
			specPath.Child("seedBackupRef"),
			shardedDB.Spec.SeedBackupRef,
			"seedBackupRef and seedURI cannot be specified together"))
	}
	if shardedDB.Spec.SeedBackupRef != "" && len(shardedDB.Spec.SeedURIs) > 0 {
		result.Errors = append(result.Errors, field.Invalid(
			specPath.Child("seedBackupRef"),
			shardedDB.Spec.SeedBackupRef,
			"seedBackupRef and seedURIs cannot be specified together"))
	}

	if shardedDB.Spec.SeedSourceDatabase != "" && !hasSeedSource {
		result.Errors = append(result.Errors, field.Invalid(
			specPath.Child("seedSourceDatabase"),
			shardedDB.Spec.SeedSourceDatabase,
			"seedSourceDatabase requires seedURI, seedURIs, or seedBackupRef"))
	}

	if shardedDB.Spec.SeedConfig != nil && !hasSeedSource {
		result.Errors = append(result.Errors, field.Invalid(
			specPath.Child("seedConfig"),
			"",
			"seedConfig requires seedURI, seedURIs, or seedBackupRef"))
	}

	if shardedDB.Spec.SeedCredentials != nil && !hasSeedSource {
		result.Errors = append(result.Errors, field.Invalid(
			specPath.Child("seedCredentials"),
			"",
			"seedCredentials requires seedURI, seedURIs, or seedBackupRef"))
	}

	// Per-shard seedURIs keys are interpolated (backtick-escaped) into the
	// CREATE DATABASE OPTIONS; constrain them to a simple identifier set.
	for key := range shardedDB.Spec.SeedURIs {
		if !seedConfigKeyPattern.MatchString(key) {
			result.Errors = append(result.Errors, field.Invalid(
				specPath.Child("seedURIs").Key(key),
				key,
				"seedURIs key may contain only letters, digits, dots, underscores and dashes"))
		}
	}

	// seedConfig is serialised into the documented comma-separated seedConfig
	// OPTIONS string; restoreUntil maps to seedRestoreUntil. Constrain keys and
	// values so the serialisation is unambiguous and injection-safe (the same
	// rules as the standard Neo4jDatabase seed path).
	if shardedDB.Spec.SeedConfig != nil {
		scPath := specPath.Child("seedConfig")
		if ru := shardedDB.Spec.SeedConfig.RestoreUntil; ru != "" {
			switch {
			case strings.HasPrefix(ru, "txId:"):
				if !isValidRestoreUntilTxID(strings.TrimPrefix(ru, "txId:")) {
					result.Errors = append(result.Errors, field.Invalid(
						scPath.Child("restoreUntil"), ru,
						"txId: format requires a positive integer within int64 range (e.g., 'txId:12345')"))
				}
			case isRFC3339Timestamp(ru):
			default:
				result.Errors = append(result.Errors, field.Invalid(
					scPath.Child("restoreUntil"), ru,
					"restoreUntil must be an RFC3339 timestamp or a transaction ID (e.g., 'txId:12345')"))
			}
		}
		for key, value := range shardedDB.Spec.SeedConfig.Config {
			if !seedConfigKeyPattern.MatchString(key) {
				result.Errors = append(result.Errors, field.Invalid(
					scPath.Child("config").Key(key), key,
					"seedConfig key may contain only letters, digits, dots, underscores and dashes"))
			}
			if strings.ContainsAny(value, ",=") || cypherLiteralUnsafe(value) {
				result.Errors = append(result.Errors, field.Invalid(
					scPath.Child("config").Key(key), value,
					"seedConfig value may not contain ',', '=', quote, backtick or newline characters"))
			}
		}
	}

	// replaceExisting is the destructive drop-and-recreate path. Two safety
	// gates:
	//   1. Must be paired with force=true so an accidental flip can't
	//      destroy data (mirrors Neo4jRestore.spec.force semantics).
	//   2. Mutex with ifNotExists=true — those two settings contradict
	//      each other (one says "skip if exists", the other says "destroy
	//      if exists").
	if shardedDB.Spec.ReplaceExisting {
		if !shardedDB.Spec.Force {
			result.Errors = append(result.Errors, field.Invalid(
				specPath.Child("replaceExisting"),
				shardedDB.Spec.ReplaceExisting,
				"replaceExisting=true is destructive (DROP DATABASE DESTROY DATA) and requires force=true as confirmation"))
		}
		if shardedDB.Spec.IfNotExistsEffective() {
			result.Errors = append(result.Errors, field.Invalid(
				specPath.Child("ifNotExists"),
				shardedDB.Spec.IfNotExistsEffective(),
				"ifNotExists is implicitly true (the default) — mutually exclusive with replaceExisting=true (the former skips when present, the latter drops when present). Set ifNotExists=false explicitly."))
		}
		if !hasSeedSource {
			result.Errors = append(result.Errors, field.Invalid(
				specPath.Child("replaceExisting"),
				shardedDB.Spec.ReplaceExisting,
				"replaceExisting=true requires a seed source (seedURI, seedURIs, or seedBackupRef) — re-creating without data would leave the database empty"))
		}
	}

	if strings.HasSuffix(shardedDB.Spec.SeedURI, ".dump") {
		result.Warnings = append(result.Warnings,
			"Using dump file format. For better performance with large databases, consider using Neo4j backup format (.backup) instead.")
	}
	for _, uri := range shardedDB.Spec.SeedURIs {
		if strings.HasSuffix(uri, ".dump") {
			result.Warnings = append(result.Warnings,
				"Using dump file format. For better performance with large databases, consider using Neo4j backup format (.backup) instead.")
			break
		}
	}
}

// validateTopologyConfig validates database topology configuration
func (v *ShardedDatabaseValidator) validateTopologyConfig(shardedDB *neo4jv1beta1.Neo4jShardedDatabase, result *ShardedDatabaseValidationResult) {
	shardingPath := field.NewPath("spec", "propertySharding")

	// Validate graph shard topology. validateDatabaseTopology appends any errors
	// to result directly; the returned error is already reflected there.
	graphPath := shardingPath.Child("graphShard")
	_ = v.validateDatabaseTopology(&shardedDB.Spec.PropertySharding.GraphShard, graphPath, result)

	// Validate property shard topology
	propertyPath := shardingPath.Child("propertyShardTopology")
	if shardedDB.Spec.PropertySharding.PropertyShardTopology.Replicas < 1 {
		result.Errors = append(result.Errors, field.Invalid(
			propertyPath.Child("replicas"),
			shardedDB.Spec.PropertySharding.PropertyShardTopology.Replicas,
			"replicas must be at least 1"))
	}
}

// validateDatabaseTopology validates a database topology configuration
func (v *ShardedDatabaseValidator) validateDatabaseTopology(topology *neo4jv1beta1.DatabaseTopology, path *field.Path, result *ShardedDatabaseValidationResult) error {
	if topology.Primaries < 1 {
		result.Errors = append(result.Errors, field.Invalid(
			path.Child("primaries"),
			topology.Primaries,
			"primaries must be at least 1"))
	}

	if topology.Secondaries < 0 {
		result.Errors = append(result.Errors, field.Invalid(
			path.Child("secondaries"),
			topology.Secondaries,
			"secondaries cannot be negative"))
	}

	return nil
}

// validateClusterCapacity validates that the cluster has sufficient capacity for all shards
func (v *ShardedDatabaseValidator) validateClusterCapacity(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, shardedDB *neo4jv1beta1.Neo4jShardedDatabase, result *ShardedDatabaseValidationResult) {
	clusterServers := cluster.Spec.Topology.Servers
	shardingPath := field.NewPath("spec", "propertySharding")

	// Calculate total required servers for graph shard
	graphShard := &shardedDB.Spec.PropertySharding.GraphShard
	graphServers := graphShard.Primaries + graphShard.Secondaries

	if graphServers > clusterServers {
		result.Errors = append(result.Errors, field.Invalid(
			shardingPath.Child("graphShard"),
			fmt.Sprintf("primaries(%d) + secondaries(%d)", graphShard.Primaries, graphShard.Secondaries),
			fmt.Sprintf("graph shard requires %d servers but cluster only has %d", graphServers, clusterServers)))
	}

	// Calculate total required servers for property shards
	propertyReplicas := shardedDB.Spec.PropertySharding.PropertyShardTopology.Replicas

	if propertyReplicas > clusterServers {
		result.Errors = append(result.Errors, field.Invalid(
			shardingPath.Child("propertyShardTopology"),
			fmt.Sprintf("replicas(%d)", propertyReplicas),
			fmt.Sprintf("property shard requires %d servers but cluster only has %d", propertyReplicas, clusterServers)))
	}

	// Warn about resource utilization
	totalShards := int32(1) + shardedDB.Spec.PropertySharding.PropertyShards // 1 graph + N property shards
	if totalShards > clusterServers {
		result.Warnings = append(result.Warnings, fmt.Sprintf(
			"total shards (%d) exceeds cluster servers (%d) - shards will share servers",
			totalShards, clusterServers))
	}
}

// validateCypherLanguage validates Cypher language version
func (v *ShardedDatabaseValidator) validateCypherLanguage(shardedDB *neo4jv1beta1.Neo4jShardedDatabase, result *ShardedDatabaseValidationResult) {
	specPath := field.NewPath("spec")

	if shardedDB.Spec.DefaultCypherLanguage != "25" {
		result.Errors = append(result.Errors, field.Invalid(
			specPath.Child("defaultCypherLanguage"),
			shardedDB.Spec.DefaultCypherLanguage,
			"property sharding requires Cypher 25 (set to '25')"))
	}
}

// validateDatabaseName validates database name format and conventions
func (v *ShardedDatabaseValidator) validateDatabaseName(name string) error {
	if name == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	if len(name) > 63 {
		return fmt.Errorf("database name cannot exceed 63 characters")
	}

	// Check for reserved names
	reserved := []string{"system", "neo4j"}
	for _, r := range reserved {
		if strings.EqualFold(name, r) {
			return fmt.Errorf("database name '%s' is reserved", name)
		}
	}

	// Check for valid characters (alphanumeric, underscore, hyphen)
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') && char != '_' && char != '-' {
			return fmt.Errorf("database name contains invalid character '%c'", char)
		}
	}

	return nil
}
