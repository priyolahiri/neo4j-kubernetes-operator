package resources_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// Unstructured content holds JSON types only (operations.md id 148). The Route
// builders put map[string]string labels and annotations and int / int32 ports
// into it, so Unstructured.DeepCopy — which reconcileRoute calls on every pass
// — panicked with "cannot deep copy map[string]string", and GetLabels read the
// labels as absent.

// fullRouteSpec sets every field the Route builders render.
func fullRouteSpec() *neo4jv1beta1.RouteSpec {
	return &neo4jv1beta1.RouteSpec{
		Enabled:     true,
		Host:        "neo4j.apps.example.com",
		Path:        "/browser",
		Annotations: map[string]string{"haproxy.router.openshift.io/timeout": "5m"},
		TargetPort:  7473,
		TLS: &neo4jv1beta1.RouteTLSSpec{
			Termination:                   "reencrypt",
			InsecureEdgeTerminationPolicy: "Redirect",
		},
	}
}

// builtRoutes returns every Route the operator builds: the Route and the MCP
// Route of a cluster with every field set, and of a standalone with none.
func builtRoutes() map[string]*unstructured.Unstructured {
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	cluster.Name, cluster.Namespace = "graph", "prod"
	cluster.Spec.Image = neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"}
	cluster.Spec.Service = &neo4jv1beta1.ServiceSpec{
		Annotations: map[string]string{"team": "graph"},
		Route:       fullRouteSpec(),
	}
	cluster.Spec.MCP = &neo4jv1beta1.MCPServerSpec{
		Enabled: true, Transport: "http",
		HTTP: &neo4jv1beta1.MCPHTTPConfig{Service: &neo4jv1beta1.MCPServiceSpec{
			Annotations: map[string]string{"team": "mcp"},
			Route:       fullRouteSpec(),
		}},
	}

	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	standalone.Name, standalone.Namespace = "single", "dev"
	standalone.Spec.Service = &neo4jv1beta1.ServiceSpec{Route: &neo4jv1beta1.RouteSpec{Enabled: true}}
	standalone.Spec.MCP = &neo4jv1beta1.MCPServerSpec{
		Enabled: true, Transport: "http",
		HTTP: &neo4jv1beta1.MCPHTTPConfig{Service: &neo4jv1beta1.MCPServiceSpec{
			Route: &neo4jv1beta1.RouteSpec{Enabled: true},
		}},
	}

	return map[string]*unstructured.Unstructured{
		"cluster Route":        resources.BuildRouteForEnterprise(cluster),
		"cluster MCP Route":    resources.BuildMCPRouteForCluster(cluster),
		"standalone Route":     resources.BuildRouteForStandalone(standalone),
		"standalone MCP Route": resources.BuildMCPRouteForStandalone(standalone),
	}
}

func TestBuiltRoutes_DeepCopy(t *testing.T) {
	for name, route := range builtRoutes() {
		t.Run(name, func(t *testing.T) {
			require.NotNil(t, route)
			var copied *unstructured.Unstructured
			require.NotPanics(t, func() { copied = route.DeepCopy() },
				"reconcileRoute deep-copies the Route on every pass")
			assert.Equal(t, route.Object, copied.Object)
		})
	}
}

// The JSON each builder rendered before its content was made JSON-typed,
// captured from the previous code: the fix changes the Go types, not one field
// the API server receives.
func TestBuiltRoutes_RenderTheSameJSON(t *testing.T) {
	want := map[string]string{
		"cluster Route":        `{"apiVersion":"route.openshift.io/v1","kind":"Route","metadata":{"annotations":{"haproxy.router.openshift.io/timeout":"5m","team":"graph"},"labels":{"app.kubernetes.io/component":"database","app.kubernetes.io/instance":"graph","app.kubernetes.io/managed-by":"neo4j-operator","app.kubernetes.io/name":"neo4j","app.kubernetes.io/part-of":"neo4j-cluster","app.kubernetes.io/version":"5.26.0-enterprise","neo4j.com/cluster":"graph","neo4j.com/role":"route","neo4j.com/service-type":"internals"},"name":"graph-client-route","namespace":"prod"},"spec":{"host":"neo4j.apps.example.com","path":"/browser","port":{"targetPort":7473},"tls":{"insecureEdgeTerminationPolicy":"Redirect","termination":"reencrypt"},"to":{"kind":"Service","name":"graph-client","weight":100}}}`,
		"cluster MCP Route":    `{"apiVersion":"route.openshift.io/v1","kind":"Route","metadata":{"annotations":{"haproxy.router.openshift.io/timeout":"5m","team":"mcp"},"labels":{"app.kubernetes.io/component":"mcp","app.kubernetes.io/instance":"graph","app.kubernetes.io/managed-by":"neo4j-operator","app.kubernetes.io/name":"neo4j","app.kubernetes.io/part-of":"neo4j-cluster","app.kubernetes.io/version":"latest","neo4j.com/cluster":"graph","neo4j.com/component":"mcp"},"name":"graph-mcp-route","namespace":"prod"},"spec":{"host":"neo4j.apps.example.com","path":"/browser","port":{"targetPort":7473},"tls":{"insecureEdgeTerminationPolicy":"Redirect","termination":"reencrypt"},"to":{"kind":"Service","name":"graph-mcp","weight":100}}}`,
		"standalone Route":     `{"apiVersion":"route.openshift.io/v1","kind":"Route","metadata":{"annotations":{},"labels":{"app.kubernetes.io/component":"route","app.kubernetes.io/instance":"single","app.kubernetes.io/managed-by":"neo4j-operator","app.kubernetes.io/name":"neo4j"},"name":"single-route","namespace":"dev"},"spec":{"path":"/","port":{"targetPort":7474},"to":{"kind":"Service","name":"single-client","weight":100}}}`,
		"standalone MCP Route": `{"apiVersion":"route.openshift.io/v1","kind":"Route","metadata":{"annotations":{},"labels":{"app.kubernetes.io/component":"mcp","app.kubernetes.io/instance":"single","app.kubernetes.io/managed-by":"neo4j-operator","app.kubernetes.io/name":"neo4j","app.kubernetes.io/part-of":"neo4j-standalone","app.kubernetes.io/version":"latest","neo4j.com/cluster":"single","neo4j.com/component":"mcp"},"name":"single-mcp-route","namespace":"dev"},"spec":{"path":"/mcp","port":{"targetPort":8080},"to":{"kind":"Service","name":"single-mcp","weight":100}}}`,
	}
	for name, route := range builtRoutes() {
		t.Run(name, func(t *testing.T) {
			got, err := json.Marshal(route.Object)
			require.NoError(t, err)
			assert.JSONEq(t, want[name], string(got))
		})
	}
}

// What the reconcilers read back through the accessors is what was rendered.
func TestBuiltRoutes_ReadThroughTheAccessors(t *testing.T) {
	routes := builtRoutes()

	route := routes["cluster Route"]
	assert.Equal(t, "route", route.GetLabels()["neo4j.com/role"])
	assert.Equal(t, map[string]string{"haproxy.router.openshift.io/timeout": "5m", "team": "graph"}, route.GetAnnotations())
	for field, want := range map[string]string{
		"spec.host":                              "neo4j.apps.example.com",
		"spec.path":                              "/browser",
		"spec.tls.termination":                   "reencrypt",
		"spec.tls.insecureEdgeTerminationPolicy": "Redirect",
		"spec.to.kind":                           "Service",
		"spec.to.name":                           "graph-client",
	} {
		got, found, err := unstructured.NestedString(route.Object, strings.Split(field, ".")...)
		require.NoError(t, err, field)
		assert.True(t, found, field)
		assert.Equal(t, want, got, field)
	}
	port, found, err := unstructured.NestedInt64(route.Object, "spec", "port", "targetPort")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(7473), port)
	weight, _, err := unstructured.NestedInt64(route.Object, "spec", "to", "weight")
	require.NoError(t, err)
	assert.Equal(t, int64(100), weight)

	standalone := routes["standalone Route"]
	assert.Equal(t, "single", standalone.GetLabels()["app.kubernetes.io/instance"])
	port, _, err = unstructured.NestedInt64(standalone.Object, "spec", "port", "targetPort")
	require.NoError(t, err)
	assert.Equal(t, int64(7474), port, "default targetPort")
	_, found, _ = unstructured.NestedFieldNoCopy(standalone.Object, "spec", "host")
	assert.False(t, found, "no host: OpenShift generates one")
	_, found, _ = unstructured.NestedFieldNoCopy(standalone.Object, "spec", "tls")
	assert.False(t, found)
}

// The ExternalSecrets the cluster controller applies as unstructured content.
func TestBuildExternalSecret_ContentIsJSON(t *testing.T) {
	esConfig := &neo4jv1beta1.ExternalSecretsConfig{
		Enabled:        true,
		SecretStoreRef: &neo4jv1beta1.SecretStoreRef{Name: "vault-store", Kind: "ClusterSecretStore"},
		Data: []neo4jv1beta1.ExternalSecretData{
			{SecretKey: "password", RemoteRef: &neo4jv1beta1.ExternalSecretRemoteRef{Key: "neo4j/admin", Property: "password"}},
		},
	}
	cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	cluster.Name, cluster.Namespace = "prod", "ns"
	cluster.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "cert-manager", ExternalSecrets: esConfig}
	cluster.Spec.Auth = &neo4jv1beta1.AuthSpec{ExternalSecrets: esConfig}

	for name, content := range map[string]map[string]any{
		"tls":  resources.BuildExternalSecretForTLS(cluster),
		"auth": resources.BuildExternalSecretForAuth(cluster),
	} {
		t.Run(name, func(t *testing.T) {
			require.NotNil(t, content)
			require.NotPanics(t, func() { runtime.DeepCopyJSON(content) })
			es := &unstructured.Unstructured{Object: content}
			assert.Equal(t, "external-secret", es.GetLabels()["neo4j.com/role"])
			data, found, err := unstructured.NestedSlice(es.Object, "spec", "data")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, data, 1)
			item, ok := data[0].(map[string]any)
			require.True(t, ok)
			key, _, _ := unstructured.NestedString(item, "remoteRef", "key")
			assert.Equal(t, "neo4j/admin", key)
		})
	}
}
