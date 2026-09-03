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
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// MemoryValidator validates Neo4j memory configuration against Kubernetes resource limits
type MemoryValidator struct {
	recommender *resources.ResourceRecommender
}

// NewMemoryValidator creates a new memory validator
func NewMemoryValidator() *MemoryValidator {
	return &MemoryValidator{
		recommender: resources.NewResourceRecommender(),
	}
}

// Validate validates memory configuration consistency
func (v *MemoryValidator) Validate(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) field.ErrorList {
	allErrs := v.ValidateResourcesAndConfig(cluster.Spec.Resources, cluster.Spec.Config)

	// Topology-aware recommendations are cluster-only: they reason about the
	// number of servers, which a standalone does not have.
	if len(allErrs) == 0 && cluster.Spec.Resources != nil {
		if limit := cluster.Spec.Resources.Limits.Memory(); limit != nil && limit.Value() > 0 {
			allErrs = append(allErrs, v.validateMemoryAllocation(cluster, limit.Value())...)
		}
	}
	return allErrs
}

// ValidateResourcesAndConfig is the deployment-agnostic half of memory
// validation: Neo4j's own memory settings weighed against the container limit,
// and the Enterprise floor.
//
// It exists because these rules were reachable only through the CLUSTER
// validator, so the identical manifest — heap 2G and pagecache 1G against a
// 1Gi limit — was rejected as a Neo4jEnterpriseCluster and accepted as a
// Neo4jEnterpriseStandalone. The v1.15.0 release-verify journey then watched
// that standalone crash-loop on Neo4j's own "Invalid memory configuration -
// exceeds physical memory", which is exactly what this rule exists to catch
// before apply.
func (v *MemoryValidator) ValidateResourcesAndConfig(resources *corev1.ResourceRequirements, config map[string]string) field.ErrorList {
	var allErrs field.ErrorList

	if resources == nil {
		return allErrs
	}

	memoryLimit := resources.Limits.Memory()
	containerMemoryBytes := int64(0)
	if memoryLimit != nil {
		containerMemoryBytes = memoryLimit.Value()
	}

	// An absent limit is not a limit of zero. Reporting `Invalid value: "0b"`
	// on spec.resources.limits.memory named a field the user never set and a
	// value they never wrote; say what is actually required instead.
	if containerMemoryBytes == 0 {
		if hasAnyMemorySetting(resources, config) {
			allErrs = append(allErrs, field.Required(
				field.NewPath("spec", "resources", "limits", "memory"),
				fmt.Sprintf("set a memory limit: Neo4j Enterprise needs at least %s, and the operator sizes the JVM from this value",
					v.formatMemorySize(minEnterpriseMemoryBytes)),
			))
		}
		return allErrs
	}

	neo4jHeap := config["server.memory.heap.max_size"]
	neo4jPageCache := config["server.memory.pagecache.size"]
	transactionMemory := config["dbms.memory.transaction.total.max"]

	if neo4jHeap != "" || neo4jPageCache != "" || transactionMemory != "" {
		allErrs = append(allErrs, v.validateNeo4jMemorySettings(config, containerMemoryBytes, neo4jHeap, neo4jPageCache, transactionMemory)...)
	}

	// One message per problem: the container floor was previously reported
	// twice, by two rules with the same threshold and different wording.
	if len(allErrs) == 0 && containerMemoryBytes < minEnterpriseMemoryBytes {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec", "resources", "limits", "memory"),
			v.formatMemorySize(containerMemoryBytes),
			fmt.Sprintf("Neo4j Enterprise requires at least %s of memory", v.formatMemorySize(minEnterpriseMemoryBytes)),
		))
	}

	return allErrs
}

// minEnterpriseMemoryBytes is the floor below which Neo4j Enterprise will not
// run at all.
const minEnterpriseMemoryBytes = int64(1024 * 1024 * 1024)

// hasAnyMemorySetting reports whether the user expressed a memory intent at
// all. Someone who set no resources and no memory config is not asking for a
// sizing check; someone who set requests, or heap, is.
func hasAnyMemorySetting(resources *corev1.ResourceRequirements, config map[string]string) bool {
	if resources != nil {
		if req := resources.Requests.Memory(); req != nil && req.Value() > 0 {
			return true
		}
	}
	for _, k := range []string{
		"server.memory.heap.max_size",
		"server.memory.pagecache.size",
		"dbms.memory.transaction.total.max",
	} {
		if config[k] != "" {
			return true
		}
	}
	return false
}

// validateNeo4jMemorySettings validates explicit Neo4j memory configuration
func (v *MemoryValidator) validateNeo4jMemorySettings(config map[string]string, containerMemoryBytes int64, neo4jHeap, neo4jPageCache, transactionMem string) field.ErrorList {
	var allErrs field.ErrorList

	var totalNeo4jMemory int64
	var heapBytes, pageCacheBytes, transactionBytes int64
	var err error

	if neo4jHeap != "" {
		heapBytes, err = v.parseMemoryToBytes(neo4jHeap)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "config", "server.memory.heap.max_size"),
				neo4jHeap,
				fmt.Sprintf("invalid memory format: %v", err),
			))
			return allErrs
		}
		totalNeo4jMemory += heapBytes
	}

	if neo4jPageCache != "" {
		pageCacheBytes, err = v.parseMemoryToBytes(neo4jPageCache)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "config", "server.memory.pagecache.size"),
				neo4jPageCache,
				fmt.Sprintf("invalid memory format: %v", err),
			))
			return allErrs
		}
		totalNeo4jMemory += pageCacheBytes
	}

	// Validate transaction memory if specified
	if transactionMem != "" {
		transactionBytes, err = v.parseMemoryToBytes(transactionMem)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "config", "dbms.memory.transaction.total.max"),
				transactionMem,
				fmt.Sprintf("invalid memory format: %v", err),
			))
			return allErrs
		}

		// Transaction memory should not exceed heap size
		if heapBytes > 0 && transactionBytes > heapBytes {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "config", "dbms.memory.transaction.total.max"),
				transactionMem,
				fmt.Sprintf("transaction memory (%s) cannot exceed heap size (%s)",
					v.formatMemorySize(transactionBytes), v.formatMemorySize(heapBytes)),
			))
		}

		// Note: Transaction memory is allocated from heap, so don't add to total
	}

	// Add system memory overhead (typically 512MB-1GB)
	systemOverhead := containerMemoryBytes / 4 // 25% for system overhead
	if systemOverhead < 512*1024*1024 {        // Minimum 512MB
		systemOverhead = 512 * 1024 * 1024
	}
	totalNeo4jMemory += systemOverhead

	if totalNeo4jMemory > containerMemoryBytes {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec", "config"),
			config,
			fmt.Sprintf("Neo4j memory configuration (heap: %s, pagecache: %s) plus system overhead exceeds container memory limit (%s). Consider reducing memory settings or increasing container limits.",
				v.formatMemorySize(heapBytes),
				v.formatMemorySize(pageCacheBytes),
				v.formatMemorySize(containerMemoryBytes),
			),
		))
	}

	// Validate minimum heap size
	minHeapSize := int64(256 * 1024 * 1024) // 256MB
	if heapBytes > 0 && heapBytes < minHeapSize {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec", "config", "server.memory.heap.max_size"),
			neo4jHeap,
			fmt.Sprintf("heap size must be at least %s", v.formatMemorySize(minHeapSize)),
		))
	}

	// Validate minimum page cache size
	minPageCacheSize := int64(128 * 1024 * 1024) // 128MB
	if pageCacheBytes > 0 && pageCacheBytes < minPageCacheSize {
		allErrs = append(allErrs, field.Invalid(
			field.NewPath("spec", "config", "server.memory.pagecache.size"),
			neo4jPageCache,
			fmt.Sprintf("page cache size must be at least %s", v.formatMemorySize(minPageCacheSize)),
		))
	}

	return allErrs
}

// validateMemoryAllocation validates overall memory allocation strategy
func (v *MemoryValidator) validateMemoryAllocation(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, containerMemoryBytes int64) field.ErrorList {
	var allErrs field.ErrorList

	// The Enterprise floor is checked once, in ValidateResourcesAndConfig —
	// it used to be reported here a second time, in different words, so a
	// single undersized deployment produced two errors saying one thing.

	// Check if memory is sufficient for cluster size
	totalServers := cluster.Spec.Topology.Servers
	if totalServers > 3 {
		// For larger clusters (>3 nodes), require at least 2GB per node for basic operation
		minMemoryForClusterSize := int64(2 * 1024 * 1024 * 1024) // 2GB minimum for larger clusters
		if containerMemoryBytes < minMemoryForClusterSize {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "resources", "limits", "memory"),
				v.formatMemorySize(containerMemoryBytes),
				fmt.Sprintf("insufficient memory for cluster size (%d servers). Current: %s, Required minimum: %s",
					totalServers, v.formatMemorySize(containerMemoryBytes), v.formatMemorySize(minMemoryForClusterSize)),
			))
		}
	}

	// Recommendations are now advisory only - logged as events, not validation errors

	// Log optimization tips as events instead of validation errors
	// We'll emit these as events in the controller rather than blocking validation

	return allErrs
}

// GetOptimizationTips returns optimization tips for the cluster configuration
func (v *MemoryValidator) GetOptimizationTips(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) []string {
	recommendation := v.recommender.RecommendResourcesForTopology(cluster.Spec.Topology, cluster.Spec.Resources)
	return recommendation.OptimizationTips
}

// parseMemoryToBytes converts memory string to bytes
func (v *MemoryValidator) parseMemoryToBytes(memoryStr string) (int64, error) {
	if memoryStr == "" {
		return 0, nil
	}

	// Handle Neo4j format (e.g., "1g", "512m")
	memoryStr = strings.ToLower(strings.TrimSpace(memoryStr))

	var multiplier int64
	var numStr string

	if strings.HasSuffix(memoryStr, "g") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(memoryStr, "g")
	} else if strings.HasSuffix(memoryStr, "m") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(memoryStr, "m")
	} else if strings.HasSuffix(memoryStr, "k") {
		multiplier = 1024
		numStr = strings.TrimSuffix(memoryStr, "k")
	} else if strings.HasSuffix(memoryStr, "gi") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(memoryStr, "gi")
	} else if strings.HasSuffix(memoryStr, "mi") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(memoryStr, "mi")
	} else if strings.HasSuffix(memoryStr, "ki") {
		multiplier = 1024
		numStr = strings.TrimSuffix(memoryStr, "ki")
	} else {
		// Try parsing as Kubernetes resource quantity
		quantity, err := resource.ParseQuantity(memoryStr)
		if err != nil {
			return 0, fmt.Errorf("invalid memory format: %s", memoryStr)
		}
		return quantity.Value(), nil
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory format: %s", memoryStr)
	}

	return int64(num * float64(multiplier)), nil
}

// formatMemorySize formats bytes to human-readable string
func (v *MemoryValidator) formatMemorySize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1fGi", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1fMi", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1fKi", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%db", bytes)
	}
}
