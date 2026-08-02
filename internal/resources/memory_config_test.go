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

package resources

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestCalculateOptimalMemorySettings(t *testing.T) {
	tests := []struct {
		name           string
		cluster        *neo4jv1beta1.Neo4jEnterpriseCluster
		expectedHeap   string
		expectedPage   string
		testMemorySize string
	}{
		{
			name: "default memory limit (2Gi)",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					// No resources specified, should use default
				},
			},
			expectedHeap:   "1G",
			expectedPage:   "512M", // Adjusted for actual calculation
			testMemorySize: "2Gi",
		},
		{
			name: "high memory deployment (8Gi)",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("8Gi"),
						},
					},
				},
			},
			expectedHeap:   "5G",
			expectedPage:   "2G",
			testMemorySize: "8Gi",
		},
		{
			name: "low memory deployment (1Gi)",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
			expectedHeap:   "512M", // Heap size after reserving space for page cache
			expectedPage:   "256M", // Page cache for 1Gi (25% of 1Gi)
			testMemorySize: "1Gi",
		},
		{
			name: "very high memory deployment (16Gi)",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("16Gi"),
						},
					},
				},
			},
			expectedHeap:   "10G",
			expectedPage:   "5G",
			testMemorySize: "16Gi",
		},
		{
			name: "custom memory configuration",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("4Gi"),
						},
					},
					Config: map[string]string{
						"server.memory.heap.max_size":  "1G",
						"server.memory.pagecache.size": "512M",
					},
				},
			},
			expectedHeap:   "1G",
			expectedPage:   "512M",
			testMemorySize: "4Gi",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memoryConfig := GetMemoryConfigForCluster(tt.cluster)

			if memoryConfig.HeapMaxSize != tt.expectedHeap {
				t.Errorf("expected heap max size %s, got %s", tt.expectedHeap, memoryConfig.HeapMaxSize)
			}

			if memoryConfig.PageCacheSize != tt.expectedPage {
				t.Errorf("expected page cache size %s, got %s", tt.expectedPage, memoryConfig.PageCacheSize)
			}

			// Heap initial should equal heap max
			if memoryConfig.HeapInitialSize != memoryConfig.HeapMaxSize {
				t.Errorf("heap initial size %s should equal heap max size %s",
					memoryConfig.HeapInitialSize, memoryConfig.HeapMaxSize)
			}
		})
	}
}

func TestCalculateOptimalMemoryForNeo4j526Plus(t *testing.T) {
	tests := []struct {
		name         string
		memoryLimit  string
		expectedHeap string
		expectedPage string
	}{
		{
			name:         "high memory optimized (8Gi)",
			memoryLimit:  "8Gi",
			expectedHeap: "5G",
			expectedPage: "2G",
		},
		{
			name:         "low memory standard (1Gi)",
			memoryLimit:  "1Gi",
			expectedHeap: "512M",
			expectedPage: "256M",
		},
		{
			name:         "very high memory (32Gi)",
			memoryLimit:  "32Gi",
			expectedHeap: "19G",
			expectedPage: "10G",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse(tt.memoryLimit),
						},
					},
				},
			}

			memoryConfig := CalculateOptimalMemoryForNeo4j526Plus(cluster)

			if memoryConfig.HeapMaxSize != tt.expectedHeap {
				t.Errorf("expected heap max size %s, got %s", tt.expectedHeap, memoryConfig.HeapMaxSize)
			}

			if memoryConfig.PageCacheSize != tt.expectedPage {
				t.Errorf("expected page cache size %s, got %s", tt.expectedPage, memoryConfig.PageCacheSize)
			}
		})
	}
}

func TestFormatMemorySize(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{
			name:     "bytes",
			bytes:    512,
			expected: "512",
		},
		{
			name:     "kilobytes",
			bytes:    1024,
			expected: "1K",
		},
		{
			name:     "megabytes",
			bytes:    1024 * 1024,
			expected: "1M",
		},
		{
			name:     "gigabytes",
			bytes:    1024 * 1024 * 1024,
			expected: "1G",
		},
		{
			name:     "256 megabytes",
			bytes:    256 * 1024 * 1024,
			expected: "256M",
		},
		{
			name:     "1.5 gigabytes",
			bytes:    1536 * 1024 * 1024,
			expected: "2G", // Rounded up
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatMemorySize(tt.bytes)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestParseMemorySize(t *testing.T) {
	tests := []struct {
		name     string
		size     string
		expected int64
		hasError bool
	}{
		{
			name:     "kubernetes format Gi",
			size:     "2Gi",
			expected: 2 * 1024 * 1024 * 1024,
			hasError: false,
		},
		{
			name:     "kubernetes format Mi",
			size:     "512Mi",
			expected: 512 * 1024 * 1024,
			hasError: false,
		},
		{
			name:     "traditional format G",
			size:     "2G",
			expected: 2 * 1024 * 1024 * 1024,
			hasError: false,
		},
		{
			name:     "traditional format M",
			size:     "256M",
			expected: 256 * 1024 * 1024,
			hasError: false,
		},
		{
			name:     "traditional format K",
			size:     "1024K",
			expected: 1024 * 1024,
			hasError: false,
		},
		{
			name:     "plain bytes",
			size:     "1048576",
			expected: 1048576,
			hasError: false,
		},
		{
			name:     "empty string",
			size:     "",
			expected: 0,
			hasError: true,
		},
		{
			name:     "invalid format",
			size:     "invalid",
			expected: 0,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseMemorySize(tt.size)

			if tt.hasError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("expected %d, got %d", tt.expected, result)
				}
			}
		})
	}
}

func TestMemoryConfigConstraints(t *testing.T) {
	// Test minimum heap size constraint
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cluster",
		},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Resources: &corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceMemory: resource.MustParse("128Mi"), // Very low memory
				},
			},
		},
	}

	memoryConfig := CalculateOptimalMemorySettings(cluster)

	// Should enforce minimum heap size
	if memoryConfig.HeapMaxSize != "256M" {
		t.Errorf("expected minimum heap size 256M, got %s", memoryConfig.HeapMaxSize)
	}

	// Should enforce minimum page cache size (64M for very low memory)
	if memoryConfig.PageCacheSize != "64M" {
		t.Errorf("expected minimum page cache size 64M, got %s", memoryConfig.PageCacheSize)
	}
}

func TestMemoryConfigForDifferentTopologies(t *testing.T) {
	tests := []struct {
		name         string
		servers      int32
		memoryLimit  string
		expectedHeap string
	}{
		{
			name:         "small cluster",
			servers:      2,
			memoryLimit:  "4Gi",
			expectedHeap: "2G",
		},
		{
			name:         "multi server cluster",
			servers:      5,
			memoryLimit:  "4Gi",
			expectedHeap: "2G",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Topology: neo4jv1beta1.TopologyConfiguration{
						Servers: tt.servers,
					},
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse(tt.memoryLimit),
						},
					},
				},
			}

			memoryConfig := GetMemoryConfigForCluster(cluster)

			if memoryConfig.HeapMaxSize != tt.expectedHeap {
				t.Errorf("expected heap max size %s, got %s", tt.expectedHeap, memoryConfig.HeapMaxSize)
			}
		})
	}
}

// TestHeapCoherenceAcrossUserConfigMaps pins the fix for a crash-loop that
// shipped in examples/property_sharding/development-property-sharding.yaml.
//
// The example set server.memory.heap.max_size in spec.propertySharding.config —
// a map GetMemoryConfigForCluster did not read — so the operator still derived
// heap.initial_size from the memory LIMIT (60% of 8Gi = 5G) while the user's 4G
// max was appended to neo4j.conf afterwards and won. Every server then died with
// "Initial heap size set to a larger value than the maximum heap size".
//
// The invariant that matters is not which number wins but that initial <= max,
// whichever map the user wrote it in.
func TestHeapCoherenceAcrossUserConfigMaps(t *testing.T) {
	newCluster := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
		return &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				AcceptLicenseAgreement: "eval",
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
					Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
				},
			},
		}
	}

	t.Run("heap max in propertySharding.config drives initial too", func(t *testing.T) {
		cluster := newCluster()
		cluster.Spec.PropertySharding = &neo4jv1beta1.PropertyShardingSpec{
			Enabled: true,
			Config:  map[string]string{"server.memory.heap.max_size": "4G"},
		}

		cfg := GetMemoryConfigForCluster(cluster)

		if cfg.HeapMaxSize != "4G" {
			t.Errorf("user heap max ignored: got %q, want %q", cfg.HeapMaxSize, "4G")
		}
		if cfg.HeapInitialSize != "4G" {
			t.Errorf("initial heap not clamped to the user's max: got %q, want %q "+
				"(a larger initial than max makes the JVM refuse to start)",
				cfg.HeapInitialSize, "4G")
		}
	})

	t.Run("spec.config still wins over propertySharding.config", func(t *testing.T) {
		cluster := newCluster()
		cluster.Spec.Config = map[string]string{"server.memory.heap.max_size": "3G"}
		cluster.Spec.PropertySharding = &neo4jv1beta1.PropertyShardingSpec{
			Enabled: true,
			Config:  map[string]string{"server.memory.heap.max_size": "4G"},
		}

		cfg := GetMemoryConfigForCluster(cluster)

		if cfg.HeapMaxSize != "3G" || cfg.HeapInitialSize != "3G" {
			t.Errorf("spec.config should take precedence: got initial=%q max=%q, want both %q",
				cfg.HeapInitialSize, cfg.HeapMaxSize, "3G")
		}
	})

	t.Run("the shipped example manifest boots", func(t *testing.T) {
		// development-property-sharding.yaml, verbatim: 4Gi/8Gi with a 4G heap
		// max and a 1G page cache, both under propertySharding.config.
		cluster := newCluster()
		cluster.Spec.PropertySharding = &neo4jv1beta1.PropertyShardingSpec{
			Enabled: true,
			Config: map[string]string{
				"server.memory.heap.max_size":  "4G",
				"server.memory.pagecache.size": "1G",
			},
		}

		cfg := GetMemoryConfigForCluster(cluster)

		// Both values must be what the manifest asked for. Comparing initial
		// against max here would be vacuous: they are read from the same field
		// and agree even when the bug is present - the contradiction only shows
		// up in the RENDERED conf, which
		// TestPropertyShardingConfigCannotOverrideOperatorMemoryKeys covers.
		if cfg.HeapMaxSize != "4G" || cfg.HeapInitialSize != "4G" {
			t.Errorf("manifest heap not honoured: got initial=%q max=%q, want both %q",
				cfg.HeapInitialSize, cfg.HeapMaxSize, "4G")
		}
		if cfg.PageCacheSize != "1G" {
			t.Errorf("user page cache ignored: got %q, want %q", cfg.PageCacheSize, "1G")
		}
	})
}

// TestPropertyShardingConfigCannotOverrideOperatorMemoryKeys checks the RENDERED
// conf, not just the computed struct.
//
// The original bug was invisible at the struct level: the derived value was fine,
// and the contradiction only appeared because propertySharding.config was
// appended to neo4j.conf verbatim AFTER the operator's memory block, where
// DedupeNeo4jConf keeps the last occurrence.
func TestPropertyShardingConfigCannotOverrideOperatorMemoryKeys(t *testing.T) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
			},
			PropertySharding: &neo4jv1beta1.PropertyShardingSpec{
				Enabled: true,
				Config: map[string]string{
					"server.memory.heap.max_size": "4G",
					// A non-memory key must still pass through untouched.
					"dbms.logs.debug.level": "DEBUG",
				},
			},
		},
	}

	conf := buildNeo4jConfigForEnterprise(cluster)

	var initial, maxHeap string
	var maxCount int
	for _, line := range strings.Split(conf, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "server.memory.heap.initial_size="):
			initial = strings.TrimPrefix(line, "server.memory.heap.initial_size=")
		case strings.HasPrefix(line, "server.memory.heap.max_size="):
			maxHeap = strings.TrimPrefix(line, "server.memory.heap.max_size=")
			maxCount++
		}
	}

	if maxCount != 1 {
		t.Errorf("expected exactly one server.memory.heap.max_size line, got %d - "+
			"a duplicate means a raw user override was appended after the operator's block", maxCount)
	}
	if initial != maxHeap {
		t.Errorf("rendered conf is incoherent: initial_size=%q max_size=%q "+
			"(the JVM refuses to start when initial > max)", initial, maxHeap)
	}
	if !strings.Contains(conf, "dbms.logs.debug.level=DEBUG") {
		t.Error("non-memory propertySharding.config keys must still reach the rendered conf")
	}
}

// TestPropertyShardingConfigCannotOverrideTLSPosture covers what the exclusion
// list uniquely protects.
//
// The memory keys are read back through UserMemorySetting, so for those the two
// halves of the fix overlap. These two are different: nothing reads them back,
// so without the exclusion a value in propertySharding.config would be appended
// raw and silently downgrade the operator-managed TLS posture — the same class of
// bypass the spec.config path has excluded all along.
func TestPropertyShardingConfigCannotOverrideTLSPosture(t *testing.T) {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			AcceptLicenseAgreement: "eval",
			PropertySharding: &neo4jv1beta1.PropertyShardingSpec{
				Enabled: true,
				Config: map[string]string{
					"server.bolt.tls_level":           "DISABLED",
					"server.directories.certificates": "/tmp/evil",
				},
			},
		},
	}

	conf := buildNeo4jConfigForEnterprise(cluster)

	if strings.Contains(conf, "server.bolt.tls_level=DISABLED") {
		t.Error("propertySharding.config overrode the operator-managed server.bolt.tls_level")
	}
	if strings.Contains(conf, "/tmp/evil") {
		t.Error("propertySharding.config overrode the operator-managed certificates directory")
	}
}
