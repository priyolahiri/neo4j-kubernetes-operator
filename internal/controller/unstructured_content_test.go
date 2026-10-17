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

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// The OpenShift Route and the MCP Route, reconciled end to end against a
// client that serves route.openshift.io/v1 (operations.md id 148). The Route
// builders put map[string]string labels and int32 ports into unstructured
// content, so the route.DeepCopy() each reconcileRoute starts with panicked on
// every pass; the cluster and standalone controllers both reach it before they
// write the server StatefulSet, so a deployment with a Route never got one.

var routeTestGVK = schema.GroupVersionKind{Group: "route.openshift.io", Version: "v1", Kind: "Route"}

// routeTestScheme is the controllers' scheme plus route.openshift.io/v1,
// registered as unstructured: the operator has no typed Route.
func routeTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := noEffectTestScheme(t)
	scheme.AddKnownTypeWithName(routeTestGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(routeTestGVK.GroupVersion().WithKind("RouteList"), &unstructured.UnstructuredList{})
	return scheme
}

func getRoute(t *testing.T, c client.Client, namespace, name string) *unstructured.Unstructured {
	t.Helper()
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(routeTestGVK)
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, route))
	return route
}

func routeSpecForTest(host string) *neo4jv1beta1.RouteSpec {
	return &neo4jv1beta1.RouteSpec{
		Enabled:     true,
		Host:        host,
		Annotations: map[string]string{"haproxy.router.openshift.io/timeout": "5m"},
		TargetPort:  7473,
		TLS:         &neo4jv1beta1.RouteTLSSpec{Termination: "reencrypt", InsecureEdgeTerminationPolicy: "Redirect"},
	}
}

func mcpWithRoute(route *neo4jv1beta1.RouteSpec) *neo4jv1beta1.MCPServerSpec {
	return &neo4jv1beta1.MCPServerSpec{
		Enabled: true, Transport: "http",
		HTTP: &neo4jv1beta1.MCPHTTPConfig{Service: &neo4jv1beta1.MCPServiceSpec{Route: route}},
	}
}

// assertRoute checks a Route read back from the API: what the builder
// rendered, owned by the deployment.
func assertRoute(t *testing.T, route *unstructured.Unstructured, owner metav1.Object, service, host, path string, targetPort int64) {
	t.Helper()
	got := func(fields ...string) string {
		v, _, err := unstructured.NestedString(route.Object, fields...)
		require.NoError(t, err, fields)
		return v
	}
	assert.Equal(t, host, got("spec", "host"))
	assert.Equal(t, path, got("spec", "path"))
	assert.Equal(t, service, got("spec", "to", "name"))
	assert.Equal(t, "reencrypt", got("spec", "tls", "termination"))
	assert.Equal(t, "Redirect", got("spec", "tls", "insecureEdgeTerminationPolicy"))
	port, _, err := unstructured.NestedInt64(route.Object, "spec", "port", "targetPort")
	require.NoError(t, err)
	assert.Equal(t, targetPort, port)
	assert.Equal(t, owner.GetName(), route.GetLabels()["app.kubernetes.io/instance"])
	assert.Equal(t, "5m", route.GetAnnotations()["haproxy.router.openshift.io/timeout"])
	ref := metav1.GetControllerOf(route)
	require.NotNil(t, ref, "the Route is owned by the deployment")
	assert.Equal(t, owner.GetName(), ref.Name)
}

func TestReconcileRoute_Cluster(t *testing.T) {
	ctx := context.Background()
	scheme := routeTestScheme(t)
	cluster := minimalCluster("graph", "prod")
	cluster.Spec.Service = &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("neo4j.apps.example.com")}
	cluster.Spec.MCP = mcpWithRoute(routeSpecForTest("mcp.apps.example.com"))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	rec := record.NewFakeRecorder(20)
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}

	require.NotPanics(t, func() {
		require.NoError(t, r.reconcileRoute(ctx, cluster))
		require.NoError(t, r.reconcileMCPRoute(ctx, cluster))
	})
	assertRoute(t, getRoute(t, c, "prod", "graph-client-route"), cluster, "graph-client", "neo4j.apps.example.com", "/", 7473)
	assertRoute(t, getRoute(t, c, "prod", "graph-mcp-route"), cluster, "graph-mcp", "mcp.apps.example.com", "/mcp", 7473)

	// The next pass updates the Routes it created.
	cluster.Spec.Service.Route.Host = "graph.apps.example.com"
	cluster.Spec.MCP.HTTP.Service.Route.Host = "graph-mcp.apps.example.com"
	require.NotPanics(t, func() {
		require.NoError(t, r.reconcileRoute(ctx, cluster))
		require.NoError(t, r.reconcileMCPRoute(ctx, cluster))
	})
	assertRoute(t, getRoute(t, c, "prod", "graph-client-route"), cluster, "graph-client", "graph.apps.example.com", "/", 7473)
	assertRoute(t, getRoute(t, c, "prod", "graph-mcp-route"), cluster, "graph-mcp", "graph-mcp.apps.example.com", "/mcp", 7473)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRouteAPINotFound))
}

func TestReconcileRoute_Standalone(t *testing.T) {
	ctx := context.Background()
	scheme := routeTestScheme(t)
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "dev"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			Service: &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("neo4j.apps.example.com")},
			MCP:     mcpWithRoute(routeSpecForTest("mcp.apps.example.com")),
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone).Build()
	rec := record.NewFakeRecorder(20)
	r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: rec}

	for range 2 {
		require.NotPanics(t, func() {
			require.NoError(t, r.reconcileRoute(ctx, standalone))
			require.NoError(t, r.reconcileMCPRoute(ctx, standalone))
		})
	}
	assertRoute(t, getRoute(t, c, "dev", "single-route"), standalone, "single-client", "neo4j.apps.example.com", "/", 7473)
	assertRoute(t, getRoute(t, c, "dev", "single-mcp-route"), standalone, "single-mcp", "mcp.apps.example.com", "/mcp", 7473)
	assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonRouteAPINotFound))
}

// Without the Route API both call sites skip the Route, and each says so once
// — not on every pass.
func TestReconcileRoute_NoRouteAPI(t *testing.T) {
	ctx := context.Background()
	scheme := routeTestScheme(t)
	noRouteAPI := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == routeTestGVK {
				return &meta.NoKindMatchError{GroupKind: routeTestGVK.GroupKind(), SearchedVersions: []string{"v1"}}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
	cluster := minimalCluster("graph", "prod")
	cluster.Spec.Service = &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("")}
	cluster.Spec.MCP = mcpWithRoute(routeSpecForTest(""))
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "dev"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			Service: &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("")},
			MCP:     mcpWithRoute(routeSpecForTest("")),
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, standalone).WithInterceptorFuncs(noRouteAPI).Build()
	rec := record.NewFakeRecorder(20)
	cr := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: rec}
	sr := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: rec}

	for range repeatPasses {
		require.NotPanics(t, func() {
			require.NoError(t, cr.reconcileRoute(ctx, cluster))
			require.NoError(t, cr.reconcileMCPRoute(ctx, cluster))
			require.NoError(t, sr.reconcileRoute(ctx, standalone))
			require.NoError(t, sr.reconcileMCPRoute(ctx, standalone))
		})
	}
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonRouteAPINotFound), 4,
		"one for each Route of each deployment")
}

func standardStorageClass() *storagev1.StorageClass {
	return &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "standard"}, Provisioner: "kubernetes.io/no-provisioner"}
}

// The whole cluster pass: with both Routes configured it reaches the server
// StatefulSet. Before the fix it panicked at the Route step, which comes before
// the StatefulSet, so a cluster with a Route never got its servers;
// controller-runtime recovered the panic as a reconcile error and retried the
// pass with backoff, reaching the same panic every time.
func TestClusterReconcile_WithRoutesWritesTheStatefulSet(t *testing.T) {
	ctx := context.Background()
	scheme := routeTestScheme(t)
	cluster := minimalCluster("graph", "prod")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.Spec.Service = &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("neo4j.apps.example.com")}
	cluster.Spec.MCP = mcpWithRoute(routeSpecForTest("mcp.apps.example.com"))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, standardStorageClass()).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}).Build()
	r := &Neo4jEnterpriseClusterReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100),
		Validator: validation.NewClusterValidator(c), ConfigMapManager: NewConfigMapManager(c),
	}

	require.NotPanics(t, func() {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
		require.NoError(t, err)
	})
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "prod", Name: "graph-server"}, &appsv1.StatefulSet{}))
	assertRoute(t, getRoute(t, c, "prod", "graph-client-route"), cluster, "graph-client", "neo4j.apps.example.com", "/", 7473)
	assertRoute(t, getRoute(t, c, "prod", "graph-mcp-route"), cluster, "graph-mcp", "mcp.apps.example.com", "/mcp", 7473)
}

// The whole standalone pass: the same. Its Route is reconciled at the end of
// reconcileService and its MCP Route in reconcileMCP, both before the
// StatefulSet.
func TestStandaloneReconcile_WithRoutesWritesTheStatefulSet(t *testing.T) {
	ctx := context.Background()
	scheme := routeTestScheme(t)
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "dev", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
			Service:                &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("neo4j.apps.example.com")},
			MCP:                    mcpWithRoute(routeSpecForTest("mcp.apps.example.com")),
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone, standardStorageClass()).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	r := &Neo4jEnterpriseStandaloneReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100), Validator: validation.NewStandaloneValidator(),
	}

	require.NotPanics(t, func() {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(standalone)})
		require.NoError(t, err)
	})
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "dev", Name: "single"}, &appsv1.StatefulSet{}))
	assertRoute(t, getRoute(t, c, "dev", "single-route"), standalone, "single-client", "neo4j.apps.example.com", "/", 7473)
	assertRoute(t, getRoute(t, c, "dev", "single-mcp-route"), standalone, "single-mcp", "mcp.apps.example.com", "/mcp", 7473)
}

// The other hand-built unstructured content: the ServiceMonitors and the
// PrometheusRule held their lists as []map[string]any, the ExternalSecrets
// their labels as map[string]string and their data as []map[string]any —
// DeepCopy refuses both. The fake client deep-copies what it stores, as an
// informer cache does.
func TestHandBuiltObjects_ContentIsJSON(t *testing.T) {
	ctx := context.Background()
	scheme := routeTestScheme(t)
	getAs := func(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
		t.Helper()
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, obj))
		return obj
	}
	get := func(t *testing.T, c client.Client, kind, namespace, name string) *unstructured.Unstructured {
		t.Helper()
		return getAs(t, c, schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: kind}, namespace, name)
	}

	t.Run("standalone ServiceMonitor", func(t *testing.T) {
		standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "dev"}}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone).Build()
		r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme}
		for range 2 { // created, then updated
			require.NotPanics(t, func() { require.NoError(t, r.reconcileServiceMonitor(ctx, standalone)) })
		}
		endpoints, found, err := unstructured.NestedSlice(get(t, c, "ServiceMonitor", "dev", "single-monitoring").Object, "spec", "endpoints")
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, endpoints, 1)
		endpoint, ok := endpoints[0].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "metrics", endpoint["port"])
	})

	t.Run("cluster ServiceMonitor and PrometheusRule", func(t *testing.T) {
		cluster := minimalCluster("graph", "prod")
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		qm := NewQueryMonitor(c, scheme)
		require.NotPanics(t, func() {
			require.NoError(t, qm.setupMetricsCollection(ctx, cluster))
			require.NoError(t, qm.setupAlertingRules(ctx, cluster))
		})
		endpoints, _, err := unstructured.NestedSlice(get(t, c, "ServiceMonitor", "prod", "graph-monitoring").Object, "spec", "endpoints")
		require.NoError(t, err)
		require.Len(t, endpoints, 1)
		groups, _, err := unstructured.NestedSlice(get(t, c, "PrometheusRule", "prod", "graph-query-alerts").Object, "spec", "groups")
		require.NoError(t, err)
		require.Len(t, groups, 1)
		group, ok := groups[0].(map[string]any)
		require.True(t, ok)
		rules, _, err := unstructured.NestedSlice(group, "rules")
		require.NoError(t, err)
		assert.Len(t, rules, 2)
	})

	t.Run("cluster ExternalSecrets", func(t *testing.T) {
		esConfig := &neo4jv1beta1.ExternalSecretsConfig{
			Enabled:        true,
			SecretStoreRef: &neo4jv1beta1.SecretStoreRef{Name: "vault-store", Kind: "ClusterSecretStore"},
			Data: []neo4jv1beta1.ExternalSecretData{
				{SecretKey: "password", RemoteRef: &neo4jv1beta1.ExternalSecretRemoteRef{Key: "neo4j/admin"}},
			},
		}
		cluster := minimalCluster("graph", "prod")
		cluster.Spec.TLS = &neo4jv1beta1.TLSSpec{Mode: "cert-manager", ExternalSecrets: esConfig}
		cluster.Spec.Auth = &neo4jv1beta1.AuthSpec{ExternalSecrets: esConfig}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
		r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme}
		for range 2 { // created, then updated
			require.NotPanics(t, func() {
				require.NoError(t, r.createExternalSecretForTLS(ctx, cluster))
				require.NoError(t, r.createExternalSecretForAuth(ctx, cluster))
			})
		}
		esGVK := schema.GroupVersionKind{Group: "external-secrets.io", Version: "v1", Kind: "ExternalSecret"}
		for _, name := range []string{"graph-tls-external-secret", "graph-auth-external-secret"} {
			es := getAs(t, c, esGVK, "prod", name)
			assert.Equal(t, "external-secret", es.GetLabels()["neo4j.com/role"], name)
			data, _, err := unstructured.NestedSlice(es.Object, "spec", "data")
			require.NoError(t, err)
			assert.Len(t, data, 1, name)
		}
	})
}
