package resources

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	neo4jv1alpha1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1alpha1"
)

// BuildRouteForEnterprise builds an OpenShift Route for client access when enabled.
func BuildRouteForEnterprise(cluster *neo4jv1alpha1.Neo4jEnterpriseCluster) *unstructured.Unstructured {
	if cluster.Spec.Service == nil || cluster.Spec.Service.Route == nil || !cluster.Spec.Service.Route.Enabled {
		return nil
	}

	routeSpec := cluster.Spec.Service.Route
	targetPort := "http"
	if cluster.Spec.TLS != nil && cluster.Spec.TLS.Mode == CertManagerMode {
		targetPort = "https"
	}

	return buildRoute(
		fmt.Sprintf("%s-client-route", cluster.Name),
		cluster.Namespace,
		fmt.Sprintf("%s-client", cluster.Name),
		targetPort,
		routeSpec,
		getLabelsForEnterprise(cluster, "client"),
	)
}

// BuildRouteForStandalone builds an OpenShift Route for standalone access when enabled.
func BuildRouteForStandalone(standalone *neo4jv1alpha1.Neo4jEnterpriseStandalone) *unstructured.Unstructured {
	if standalone.Spec.Route == nil || !standalone.Spec.Route.Enabled {
		return nil
	}

	routeSpec := standalone.Spec.Route
	targetPort := "http"
	if standalone.Spec.TLS != nil && standalone.Spec.TLS.Mode == CertManagerMode {
		targetPort = "https"
	}

	labels := map[string]string{
		"app.kubernetes.io/name":       "neo4j",
		"app.kubernetes.io/instance":   standalone.Name,
		"app.kubernetes.io/component":  "standalone",
		"app.kubernetes.io/managed-by": "neo4j-operator",
	}

	return buildRoute(
		fmt.Sprintf("%s-route", standalone.Name),
		standalone.Namespace,
		fmt.Sprintf("%s-service", standalone.Name),
		targetPort,
		routeSpec,
		labels,
	)
}

func buildRoute(name, namespace, serviceName, targetPort string, spec *neo4jv1alpha1.RouteSpec, labels map[string]string) *unstructured.Unstructured {
	labelsCopy := make(map[string]string, len(labels))
	for k, v := range labels {
		labelsCopy[k] = v
	}

	annotations := map[string]string{}
	for k, v := range spec.Annotations {
		annotations[k] = v
	}

	route := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "route.openshift.io/v1",
			"kind":       "Route",
			"metadata": map[string]interface{}{
				"name":        name,
				"namespace":   namespace,
				"labels":      labelsCopy,
				"annotations": annotations,
			},
			"spec": map[string]interface{}{
				"to": map[string]interface{}{
					"kind":   "Service",
					"name":   serviceName,
					"weight": int64(100),
				},
				"port": map[string]interface{}{
					"targetPort": targetPort,
				},
			},
		},
	}

	routeSpec := route.Object["spec"].(map[string]interface{})
	if spec.Host != "" {
		routeSpec["host"] = spec.Host
	}

	// Configure TLS if provided
	if spec.Termination != "" {
		tls := map[string]interface{}{
			"termination": spec.Termination,
		}
		if spec.Certificate != "" {
			tls["certificate"] = spec.Certificate
		}
		if spec.Key != "" {
			tls["key"] = spec.Key
		}
		if spec.CaCertificate != "" {
			tls["caCertificate"] = spec.CaCertificate
		}
		if spec.DestinationCACertificate != "" {
			tls["destinationCACertificate"] = spec.DestinationCACertificate
		}
		if spec.InsecureEdgeTerminationPolicy != "" {
			tls["insecureEdgeTerminationPolicy"] = spec.InsecureEdgeTerminationPolicy
		}
		routeSpec["tls"] = tls
	}

	return route
}
