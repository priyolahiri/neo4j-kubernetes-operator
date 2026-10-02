package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestNewStorageValidator(t *testing.T) {
	validator := NewStorageValidator()
	assert.NotNil(t, validator)
}

func TestStorageValidator_Validate(t *testing.T) {
	tests := []struct {
		name       string
		cluster    *neo4jv1beta1.Neo4jEnterpriseCluster
		wantErrors bool
		errorCount int
	}{
		{
			name: "valid storage configuration",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Storage: neo4jv1beta1.StorageSpec{
						ClassName: "fast-ssd",
						Size:      "100Gi",
					},
				},
			},
			wantErrors: false,
		},
		{
			name: "valid storage with different size formats",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Storage: neo4jv1beta1.StorageSpec{
						ClassName: "standard",
						Size:      "1Ti",
					},
				},
			},
			wantErrors: false,
		},
		{
			// An empty className is valid: the PVC inherits the cluster's default
			// StorageClass. Existence of a named class is checked at apply time by
			// the reconciler, not here.
			name: "empty storage class name uses cluster default",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Storage: neo4jv1beta1.StorageSpec{
						Size: "100Gi",
					},
				},
			},
			wantErrors: false,
		},
		{
			name: "missing storage size",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Storage: neo4jv1beta1.StorageSpec{
						ClassName: "standard",
					},
				},
			},
			wantErrors: true,
			errorCount: 1,
		},
		{
			name: "missing size (empty class name is allowed)",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Storage:                neo4jv1beta1.StorageSpec{},
				},
			},
			wantErrors: true,
			errorCount: 1,
		},
		{
			name: "invalid storage size format",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Storage: neo4jv1beta1.StorageSpec{
						ClassName: "standard",
						Size:      "invalid-size",
					},
				},
			},
			wantErrors: true,
			errorCount: 1,
		},
		{
			name: "empty storage size",
			cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					AcceptLicenseAgreement: "eval",
					Storage: neo4jv1beta1.StorageSpec{
						ClassName: "standard",
						Size:      "",
					},
				},
			},
			wantErrors: true,
			errorCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewStorageValidator()
			errors := validator.Validate(tt.cluster)

			if tt.wantErrors {
				assert.NotEmpty(t, errors, "Expected validation errors but got none")
				if tt.errorCount > 0 {
					assert.Len(t, errors, tt.errorCount, "Expected %d errors but got %d", tt.errorCount, len(errors))
				}
			} else {
				assert.Empty(t, errors, "Expected no validation errors but got: %v", errors)
			}
		})
	}
}

func TestStorageValidator_isValidStorageSize(t *testing.T) {
	tests := []struct {
		name  string
		size  string
		valid bool
	}{
		{
			name:  "valid size in Gi",
			size:  "100Gi",
			valid: true,
		},
		{
			name:  "valid size in Ti",
			size:  "1Ti",
			valid: true,
		},
		{
			name:  "valid size in Mi",
			size:  "2048Mi",
			valid: true,
		},
		{
			name:  "valid size in Ki",
			size:  "1048576Ki",
			valid: true,
		},
		{
			name:  "valid size in G",
			size:  "100G",
			valid: true,
		},
		{
			name:  "valid size in T",
			size:  "1T",
			valid: true,
		},
		{
			name:  "valid size in M",
			size:  "2048M",
			valid: true,
		},
		{
			// Capital K is NOT a Kubernetes quantity suffix (decimal kilo is a
			// lowercase "k"; binary is "Ki"). The format regex alone admitted it,
			// and the StatefulSet builder's resource.MustParse then panicked the
			// manager. The validator must refuse what Kubernetes cannot parse.
			name:  "capital K is not a Kubernetes quantity suffix",
			size:  "1048576K",
			valid: false,
		},
		{
			name:  "valid size without unit",
			size:  "1073741824",
			valid: true,
		},
		{
			name:  "invalid size with invalid unit",
			size:  "100Zi",
			valid: false,
		},
		{
			name:  "invalid size with text",
			size:  "one-hundred-gigabytes",
			valid: false,
		},
		{
			name:  "invalid size with mixed text and numbers",
			size:  "100GB-storage",
			valid: false,
		},
		{
			name:  "empty size",
			size:  "",
			valid: false,
		},
		{
			name:  "size with only unit",
			size:  "Gi",
			valid: false,
		},
		{
			name:  "negative size",
			size:  "-100Gi",
			valid: false,
		},
		{
			// Kubernetes accepts a decimal quantity, so the operator does too:
			// the old format regex refused it although the apiserver would not.
			name:  "decimal quantity",
			size:  "100.5Gi",
			valid: true,
		},
		{
			name:  "decimal quantity in Gi",
			size:  "1.5Gi",
			valid: true,
		},
		{
			name:  "lowercase k is the decimal kilo suffix",
			size:  "500000000k",
			valid: true,
		},
		{
			name:  "exponent form",
			size:  "1e12",
			valid: true,
		},
		{
			name:  "zero holds no Neo4j store",
			size:  "0",
			valid: false,
		},
		{
			name:  "zero with a unit",
			size:  "0Gi",
			valid: false,
		},
		{
			name:  "size with spaces",
			size:  "100 Gi",
			valid: false,
		},
		{
			name:  "valid large size",
			size:  "999999Gi",
			valid: true,
		},
		{
			name:  "valid small size",
			size:  "1Ki",
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storageSizeError(field.NewPath("spec", "storage", "size"), tt.size)
			assert.Equal(t, tt.valid, err == nil, "storageSizeError(%q) = %v, want valid=%v", tt.size, err, tt.valid)
		})
	}
}

// A cluster and a standalone are the same Neo4j server with the same data
// volume, so they must accept the same sizes. They had drifted both ways: the
// cluster's format regex refused 100.5Gi yet admitted capital 5K (which panicked
// the manager), and accepted 0, while the standalone accepted anything non-empty.
// Runs each size through BOTH validators' real entry points.
func TestStandaloneAndClusterAgreeOnStorageSize(t *testing.T) {
	sizes := []string{"100Gi", "1.5Gi", "100.5Gi", "500000000k", "1e12", "2048M",
		"0", "0Gi", "-1Gi", "5K", "fifty", "10 Gi", "100Zi", "Gi"}

	for _, size := range sizes {
		t.Run(size, func(t *testing.T) {
			cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					Storage: neo4jv1beta1.StorageSpec{Size: size},
				},
			}
			clusterRefuses := hasFieldError(NewStorageValidator().Validate(cluster), "spec.storage.size")

			standalone := validStandalone()
			standalone.Spec.Storage.Size = size
			standaloneRefuses := hasFieldError(NewStandaloneValidator().ValidateCreate(standalone), "spec.storage.size")

			assert.Equal(t, clusterRefuses, standaloneRefuses,
				"size %q: cluster refuses=%v, standalone refuses=%v", size, clusterRefuses, standaloneRefuses)
		})
	}
}

func hasFieldError(errs field.ErrorList, path string) bool {
	for _, e := range errs {
		if e.Field == path {
			return true
		}
	}
	return false
}
