package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1alpha1 "github.com/neo4j-labs/neo4j-kubernetes-operator/api/v1alpha1"
)

func TestNewEditionValidator(t *testing.T) {
	validator := NewEditionValidator()
	assert.NotNil(t, validator)
}

func TestEditionValidator_Validate(t *testing.T) {
	tests := []struct {
		name       string
		cluster    *neo4jv1alpha1.Neo4jEnterpriseCluster
		wantErrors bool
	}{
		{
			name: "valid enterprise edition",
			cluster: &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "enterprise",
				},
			},
			wantErrors: false,
		},
		{
			name: "invalid community edition",
			cluster: &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "community",
				},
			},
			wantErrors: true,
		},
		{
			name: "invalid empty edition",
			cluster: &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "",
				},
			},
			wantErrors: true,
		},
		{
			name: "invalid unknown edition",
			cluster: &neo4jv1alpha1.Neo4jEnterpriseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
				Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
					Edition: "unknown",
				},
			},
			wantErrors: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewEditionValidator()
			errors := validator.Validate(tt.cluster)

			if tt.wantErrors {
				assert.NotEmpty(t, errors, "Expected validation errors but got none")
				// Verify error details
				assert.Contains(t, errors[0].Error(), "only 'enterprise' edition is supported")
				assert.Equal(t, "spec.edition", errors[0].Field)
			} else {
				assert.Empty(t, errors, "Expected no validation errors but got: %v", errors)
			}
		})
	}
}
