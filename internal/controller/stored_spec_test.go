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

// The operator never changes the spec a user applied (operations.md id 150).
// Its defaults stay in the reconcile's own copy, and what it writes to a
// cluster or standalone is a patch of what it changed. An Update sent the whole
// object as the Go types encode it, which is not what the user applied: the
// first pass wrote every default into the stored spec, a false the CRD defaults
// to true came back true, and a quantity came back in another spelling.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// keepFinalizer is another controller's finalizer: it keeps a deleted object
// readable after the operator released its own, so the spec the deletion pass
// left behind can be compared too.
const keepFinalizer = "example.com/keep"

// appliedSpec is what a user applies. Every line is one the Go types cannot
// carry through an Update: no authenticationProviders, tls or service for
// defaults to fill in, false for three fields the CRD defaults to true, and a
// CPU request written "0.5", which a Quantity encodes as "500m".
const appliedSpec = `{
  "acceptLicenseAgreement": "eval",
  "image": {"repo": "neo4j", "tag": "2026.08.1-enterprise"},
  "storage": {"className": "standard", "size": "1Gi"},
  "resources": {"requests": {"cpu": "0.5", "memory": "2Gi"}, "limits": {"memory": "2Gi"}},
  "auth": {
    "adminSecret": "neo4j-admin-secret",
    "authorizationProviders": ["ldap", "native"],
    "ldap": {"host": "ldap://ldap.example.com:389"}
  },
  "monitoring": {"enabled": false},
  "upgradeStrategy": {"autoPauseOnFailure": false, "preUpgradeHealthCheck": false}
}`

// applyManifest creates the object from JSON, as kubectl apply does, so the
// Go types never see it on its way in.
func applyManifest(t *testing.T, c client.Client, kind, name string, extraSpec map[string]any) types.NamespacedName {
	t.Helper()
	spec := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(appliedSpec), &spec))
	for k, v := range extraSpec {
		spec[k] = v
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": neo4jv1beta1.GroupVersion.String(),
		"kind":       kind,
		"metadata": map[string]any{
			"name":        name,
			"namespace":   "default",
			"finalizers":  []any{keepFinalizer},
			"annotations": map[string]any{AutoInheritSeedCredsAnnotation: "true"},
		},
		"spec": spec,
	}}
	require.NoError(t, c.Create(context.Background(), u))
	return types.NamespacedName{Namespace: "default", Name: name}
}

// stored reads the object as the API server stores it, without the Go types.
func stored(t *testing.T, c client.Client, kind string, key types.NamespacedName) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(neo4jv1beta1.GroupVersion.WithKind(kind))
	require.NoError(t, c.Get(context.Background(), key, u))
	return u
}

func storedSpec(t *testing.T, c client.Client, kind string, key types.NamespacedName) map[string]any {
	t.Helper()
	spec, ok := stored(t, c, kind, key).Object["spec"].(map[string]any)
	require.True(t, ok, "%s %s has no spec", kind, key)
	return spec
}

// release lets a deleted object go: it drops the finalizer the test kept it
// with.
func release(t *testing.T, c client.Client, kind string, key types.NamespacedName) {
	t.Helper()
	u := stored(t, c, kind, key)
	base := u.DeepCopy()
	u.SetFinalizers(nil)
	require.NoError(t, c.Patch(context.Background(), u, client.MergeFrom(base)))
}

// seedEnvFrom and restoreEnv are what the two writers that change a spec on
// purpose add to it, as the API server stores them.
var (
	seedEnvFrom = []any{map[string]any{"secretRef": map[string]any{"name": "seed-creds"}}}
	restoreEnv  = []any{map[string]any{"name": "RESTORE_SEED", "value": "s3://bucket/seed"}}
)

func addRestoreEnv(env *[]corev1.EnvVar, _ *[]corev1.EnvFromSource, _ map[string]string) bool {
	*env = append(*env, corev1.EnvVar{Name: "RESTORE_SEED", Value: "s3://bucket/seed"})
	return true
}

// TestReconcile_LeavesTheSpecAsApplied_APIServer drives every writer of a
// cluster and a standalone against a real API server, with the CRDs and their
// defaults installed, and compares the stored spec after each with the one the
// apply stored. Without the patches, the first pass alone changes it.
func TestReconcile_LeavesTheSpecAsApplied_APIServer(t *testing.T) {
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
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, c.Create(ctx, &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "standard"},
		Provisioner: "kubernetes.io/no-provisioner",
	}))
	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "neo4j-admin-secret", Namespace: "default"},
		Data:       map[string][]byte{"username": []byte("neo4j"), "password": []byte("testpassword")},
	}))
	restores := &Neo4jRestoreReconciler{Client: c, Scheme: scheme}

	t.Run("cluster", func(t *testing.T) {
		const kind = "Neo4jEnterpriseCluster"
		key := applyManifest(t, c, kind, "applied", map[string]any{
			"topology": map[string]any{"servers": int64(3)},
		})
		want := storedSpec(t, c, kind, key)
		r := &Neo4jEnterpriseClusterReconciler{
			Client:       c,
			Scheme:       scheme,
			Recorder:     record.NewFakeRecorder(100),
			Validator:    validation.NewClusterValidator(c),
			RequeueAfter: time.Second,
		}
		get := func() *neo4jv1beta1.Neo4jEnterpriseCluster {
			cluster := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			require.NoError(t, c.Get(ctx, key, cluster))
			return cluster
		}

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.Contains(t, stored(t, c, kind, key).GetFinalizers(), ClusterFinalizer, "the first pass adds the finalizer")
		require.Equal(t, want, storedSpec(t, c, kind, key), "the first pass changed the stored spec")

		// The metadata writers write their annotation and nothing else.
		require.NoError(t, r.setDrainServers(ctx, get(), []string{"server-2"}))
		require.Equal(t, want, storedSpec(t, c, kind, key), "the scale-down annotation changed the stored spec")
		require.NoError(t, r.newFleetProvisioner().setDeploymentAnnotation(ctx, get(), "deployment-1"))
		require.Equal(t, want, storedSpec(t, c, kind, key), "the fleet annotation changed the stored spec")

		// The two that change the spec on purpose change only what they set.
		_, err = EnsureSeedCredsProjected(ctx, c, get(), "seed-creds")
		require.NoError(t, err)
		want["extraEnvFrom"] = seedEnvFrom
		require.Equal(t, want, storedSpec(t, c, kind, key), "projecting the seed credentials changed more than spec.extraEnvFrom")
		_, err = restores.updateTargetSpec(ctx, restoreTarget{cluster: get()}, addRestoreEnv)
		require.NoError(t, err)
		want["env"] = restoreEnv
		require.Equal(t, want, storedSpec(t, c, kind, key), "the restore changed more than spec.env")

		require.NoError(t, c.Delete(ctx, get()))
		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NotContains(t, stored(t, c, kind, key).GetFinalizers(), ClusterFinalizer, "the deletion pass releases the finalizer")
		require.Equal(t, want, storedSpec(t, c, kind, key), "the deletion pass changed the stored spec")
		release(t, c, kind, key)
	})

	t.Run("standalone", func(t *testing.T) {
		const kind = "Neo4jEnterpriseStandalone"
		key := applyManifest(t, c, kind, "applied", nil)
		want := storedSpec(t, c, kind, key)
		r := &Neo4jEnterpriseStandaloneReconciler{
			Client:       c,
			Scheme:       scheme,
			Recorder:     record.NewFakeRecorder(100),
			Validator:    validation.NewStandaloneValidator(),
			RequeueAfter: time.Second,
		}
		get := func() *neo4jv1beta1.Neo4jEnterpriseStandalone {
			standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
			require.NoError(t, c.Get(ctx, key, standalone))
			return standalone
		}

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.Contains(t, stored(t, c, kind, key).GetFinalizers(), StandaloneFinalizer, "the first pass adds the finalizer")
		require.Equal(t, want, storedSpec(t, c, kind, key), "the first pass changed the stored spec")

		require.NoError(t, r.newFleetProvisioner().setDeploymentAnnotation(ctx, get(), "deployment-1"))
		require.Equal(t, want, storedSpec(t, c, kind, key), "the fleet annotation changed the stored spec")

		_, err = EnsureSeedCredsProjected(ctx, c, get(), "seed-creds")
		require.NoError(t, err)
		want["extraEnvFrom"] = seedEnvFrom
		require.Equal(t, want, storedSpec(t, c, kind, key), "projecting the seed credentials changed more than spec.extraEnvFrom")
		_, err = restores.updateTargetSpec(ctx, restoreTarget{standalone: get()}, addRestoreEnv)
		require.NoError(t, err)
		want["env"] = restoreEnv
		require.Equal(t, want, storedSpec(t, c, kind, key), "the restore changed more than spec.env")

		require.NoError(t, c.Delete(ctx, get()))
		_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NotContains(t, stored(t, c, kind, key).GetFinalizers(), StandaloneFinalizer, "the deletion pass releases the finalizer")
		require.Equal(t, want, storedSpec(t, c, kind, key), "the deletion pass changed the stored spec")
		release(t, c, kind, key)
	})
}
