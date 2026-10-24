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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// #536: the Route writes. The standalone wrote its Route twice per pass, and
// every Route reconcile replaced spec wholesale, dropping what OpenShift's API
// server fills in — so each pass sent an update the server turned back into
// the same object.

// routeWrites counts the creates, updates and patches of Routes a client sends.
type routeWrites struct {
	mu sync.Mutex
	n  int
}

func (w *routeWrites) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

func (w *routeWrites) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.n = 0
}

func (w *routeWrites) note(obj client.Object) {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == routeTestGVK {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.n++
	}
}

func (w *routeWrites) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			w.note(obj)
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			w.note(obj)
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			w.note(obj)
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
}

// fillInLikeOpenShift does to a stored Route what OpenShift's API server does:
// it defaults wildcardPolicy, generates a host for a Route that names none and
// marks it so. It also adds a certificate by hand, as a user would.
func fillInLikeOpenShift(t *testing.T, c client.Client, namespace, name string) {
	t.Helper()
	route := getRoute(t, c, namespace, name)
	require.NoError(t, unstructured.SetNestedField(route.Object, "None", "spec", "wildcardPolicy"))
	if _, found, _ := unstructured.NestedString(route.Object, "spec", "host"); !found {
		require.NoError(t, unstructured.SetNestedField(route.Object, name+"-"+namespace+".apps.example.com", "spec", "host"))
		annotations := route.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[routeHostGeneratedAnnotation] = "true"
		route.SetAnnotations(annotations)
	}
	require.NoError(t, unstructured.SetNestedField(route.Object, "-----BEGIN CERTIFICATE-----", "spec", "tls", "certificate"))
	require.NoError(t, c.Update(context.Background(), route))
}

func assertFilledInKept(t *testing.T, c client.Client, namespace, name string) {
	t.Helper()
	route := getRoute(t, c, namespace, name)
	policy, _, _ := unstructured.NestedString(route.Object, "spec", "wildcardPolicy")
	assert.Equal(t, "None", policy, name)
	host, _, _ := unstructured.NestedString(route.Object, "spec", "host")
	assert.NotEmpty(t, host, name)
	cert, _, _ := unstructured.NestedString(route.Object, "spec", "tls", "certificate")
	assert.Equal(t, "-----BEGIN CERTIFICATE-----", cert, name)
}

// A Route the API server filled in is already right: the next passes leave it
// alone, and a spec change is applied without taking away what was filled in.
func TestReconcileRoute_LeavesWhatTheAPIServerFilledIn(t *testing.T) {
	ctx := context.Background()
	scheme := routeTestScheme(t)
	cluster := minimalCluster("graph", "prod")
	cluster.Spec.Service = &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("")} // OpenShift generates the host
	cluster.Spec.MCP = mcpWithRoute(routeSpecForTest("mcp.apps.example.com"))
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "prod"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			Service: &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("neo4j.apps.example.com")},
			MCP:     mcpWithRoute(routeSpecForTest("")),
		},
	}
	writes := &routeWrites{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, standalone).WithInterceptorFuncs(writes.funcs()).Build()
	cr := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	sr := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	pass := func() {
		t.Helper()
		require.NoError(t, cr.reconcileRoute(ctx, cluster))
		require.NoError(t, cr.reconcileMCPRoute(ctx, cluster))
		require.NoError(t, sr.reconcileRoute(ctx, standalone))
		require.NoError(t, sr.reconcileMCPRoute(ctx, standalone))
	}
	routes := []string{"graph-client-route", "graph-mcp-route", "single-route", "single-mcp-route"}

	pass()
	require.Equal(t, 4, writes.count(), "each Route created once")
	for _, name := range routes {
		fillInLikeOpenShift(t, c, "prod", name)
	}
	writes.reset()

	for range repeatPasses {
		pass()
	}
	assert.Zero(t, writes.count(), "a Route that is already right is not written again")
	for _, name := range routes {
		assertFilledInKept(t, c, "prod", name)
	}

	// A change the operator renders is written, once, and keeps the rest.
	cluster.Spec.Service.Route.TargetPort = 7474
	standalone.Spec.Service.Route.Path = "/browser"
	pass()
	pass()
	assert.Equal(t, 2, writes.count())
	for _, name := range routes {
		assertFilledInKept(t, c, "prod", name)
	}
	port, _, _ := unstructured.NestedInt64(getRoute(t, c, "prod", "graph-client-route").Object, "spec", "port", "targetPort")
	assert.Equal(t, int64(7474), port)
	path, _, _ := unstructured.NestedString(getRoute(t, c, "prod", "single-route").Object, "spec", "path")
	assert.Equal(t, "/browser", path)
}

func TestApplyDesiredRoute(t *testing.T) {
	desired := resources.BuildRouteForStandalone(&neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "dev"},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{Service: &neo4jv1beta1.ServiceSpec{Route: &neo4jv1beta1.RouteSpec{
			Enabled: true, TLS: &neo4jv1beta1.RouteTLSSpec{Termination: "edge"},
		}}},
	})
	live := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"host":           "single-route-dev.apps.example.com",
			"wildcardPolicy": "None",
			"path":           "/old",
			"to":             map[string]any{"kind": "Service", "name": "old", "weight": int64(100)},
			"tls": map[string]any{
				"termination":                   "reencrypt",
				"insecureEdgeTerminationPolicy": "Redirect",
				"certificate":                   "cert",
				"key":                           "key",
			},
		},
	}}
	live.SetAnnotations(map[string]string{routeHostGeneratedAnnotation: "true", "stale": "x"})

	applyDesiredRoute(live, desired)
	spec, _, _ := unstructured.NestedMap(live.Object, "spec")
	assert.Equal(t, map[string]any{
		"host":           "single-route-dev.apps.example.com",
		"wildcardPolicy": "None",
		"path":           "/",
		"to":             map[string]any{"kind": "Service", "name": "single-client", "weight": int64(100)},
		"port":           map[string]any{"targetPort": int64(7474)},
		"tls":            map[string]any{"termination": "edge", "certificate": "cert", "key": "key"},
	}, spec, "what the operator renders replaces the live value; the rest is kept")
	assert.Equal(t, map[string]string{routeHostGeneratedAnnotation: "true"}, live.GetAnnotations(),
		"the operator's annotations, plus the one the API server set")
	require.NotPanics(t, func() { _ = live.DeepCopy() }, "the content stays JSON (operations.md id 148)")

	// A host the user names wins; a Route without TLS loses its certificate.
	named := desired.DeepCopy()
	require.NoError(t, unstructured.SetNestedField(named.Object, "neo4j.example.com", "spec", "host"))
	unstructured.RemoveNestedField(named.Object, "spec", "tls")
	applyDesiredRoute(live, named)
	host, _, _ := unstructured.NestedString(live.Object, "spec", "host")
	assert.Equal(t, "neo4j.example.com", host)
	_, hasTLS, _ := unstructured.NestedMap(live.Object, "spec", "tls")
	assert.False(t, hasTLS)
}

// The standalone reconciled its Route at the end of reconcileService and then
// again, with an unconditional Get+Update, later in the same pass.
func TestStandaloneReconcile_WritesItsRouteOnlyWhenItChanges(t *testing.T) {
	ctx := context.Background()
	scheme := routeTestScheme(t)
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "dev", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
			Service:                &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("neo4j.apps.example.com")},
		},
	}
	writes := &routeWrites{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone, standardStorageClass()).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).WithInterceptorFuncs(writes.funcs()).Build()
	r := &Neo4jEnterpriseStandaloneReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100), Validator: validation.NewStandaloneValidator(),
	}
	key := client.ObjectKeyFromObject(standalone)

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	assert.Equal(t, 1, writes.count(), "the first pass creates the Route, and writes it no more")

	writes.reset()
	for _, err := range reconcileTimes(t, r, key, repeatPasses) {
		require.NoError(t, err)
	}
	assert.Zero(t, writes.count(), "a pass with nothing to change writes no Route")
	assertRoute(t, getRoute(t, c, "dev", "single-route"), standalone, "single-client", "neo4j.apps.example.com", "/", 7473)
}

// What a Kind cluster sees: an API server without route.openshift.io. The
// whole cluster and standalone passes, with a Route and an MCP Route asked
// for, reach their StatefulSet and say once that there is no Route API. The
// panic fixed by #534 fired here, before the NoMatch check; Route testing
// needs OpenShift only for what OpenShift does with the Route.
func TestReconcile_RoutesWithoutTheRouteAPI_APIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run via `make test-unit`")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	defer func() { _ = env.Stop() }()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))
	direct, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	// The manager's cache hands objects back with their kind set, which the
	// ConfigMap's owner reference is built from; a direct client does not.
	c := interceptor.NewClient(direct, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if _, typed := obj.(*unstructured.Unstructured); !typed {
				gvk, err := apiutil.GVKForObject(obj, scheme)
				if err != nil {
					return err
				}
				obj.GetObjectKind().SetGroupVersionKind(gvk)
			}
			return nil
		},
	})
	ctx := context.Background()
	require.NoError(t, c.Create(ctx, standardStorageClass()))

	cluster := minimalCluster("graph", "default")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.Spec.Service = &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("neo4j.apps.example.com")}
	cluster.Spec.MCP = mcpWithRoute(routeSpecForTest(""))
	require.NoError(t, c.Create(ctx, cluster))
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "single", Namespace: "default", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
			Service:                &neo4jv1beta1.ServiceSpec{Route: routeSpecForTest("")},
			MCP:                    mcpWithRoute(routeSpecForTest("")),
		},
	}
	require.NoError(t, c.Create(ctx, standalone))

	rec := record.NewFakeRecorder(200)
	cr := &Neo4jEnterpriseClusterReconciler{
		Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Minute,
		Validator: validation.NewClusterValidator(c), ConfigMapManager: NewConfigMapManager(c),
	}
	sr := &Neo4jEnterpriseStandaloneReconciler{
		Client: c, Scheme: scheme, Recorder: rec, RequeueAfter: time.Minute, Validator: validation.NewStandaloneValidator(),
	}
	for range 2 {
		require.NotPanics(t, func() {
			_, err := cr.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			require.NoError(t, err)
			_, err = sr.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(standalone)})
			require.NoError(t, err)
		})
	}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "graph-server"}, &appsv1.StatefulSet{}))
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "single"}, &appsv1.StatefulSet{}))
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonRouteAPINotFound), 4,
		"each Route of each deployment, once over two passes")
}
