package resources_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	neo4jv1alpha1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1alpha1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

func TestBuildRouteForEnterprise_Disabled(t *testing.T) {
	cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{}

	route := resources.BuildRouteForEnterprise(cluster)
	require.Nil(t, route)
}

func TestBuildRouteForEnterprise_Enabled(t *testing.T) {
	cluster := &neo4jv1alpha1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "graph",
			Namespace: "ns",
		},
		Spec: neo4jv1alpha1.Neo4jEnterpriseClusterSpec{
			Service: &neo4jv1alpha1.ServiceSpec{
				Route: &neo4jv1alpha1.RouteSpec{
					Enabled:     true,
					Host:        "graph.example.com",
					Annotations: map[string]string{"route": "enabled"},
					Termination: "edge",
				},
			},
		},
	}

	route := resources.BuildRouteForEnterprise(cluster)
	require.NotNil(t, route)

	spec, found, _ := unstructured.NestedMap(route.Object, "spec")
	require.True(t, found)
	host, _, _ := unstructured.NestedString(spec, "host")
	require.Equal(t, "graph.example.com", host)
	targetPort, _, _ := unstructured.NestedString(spec, "port", "targetPort")
	require.Equal(t, "http", targetPort)
}

func TestBuildRouteForStandalone_TLS(t *testing.T) {
	standalone := &neo4jv1alpha1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "graph-standalone",
			Namespace: "ns",
		},
		Spec: neo4jv1alpha1.Neo4jEnterpriseStandaloneSpec{
			TLS: &neo4jv1alpha1.TLSSpec{Mode: resources.CertManagerMode},
			Route: &neo4jv1alpha1.RouteSpec{
				Enabled:                       true,
				Termination:                   "reencrypt",
				DestinationCACertificate:      "dest",
				InsecureEdgeTerminationPolicy: "Redirect",
			},
		},
	}

	route := resources.BuildRouteForStandalone(standalone)
	require.NotNil(t, route)

	tls, found, _ := unstructured.NestedMap(route.Object, "spec", "tls")
	require.True(t, found)
	require.Equal(t, "reencrypt", tls["termination"])
	require.Equal(t, "dest", tls["destinationCACertificate"])
}
