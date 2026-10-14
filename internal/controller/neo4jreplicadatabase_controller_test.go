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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A backup-mode replica needs the object-store credentials in the DOWNSTREAM
// SERVERS' environment, because the seed and the pull run there. Without them
// the failure surfaces deep inside the AWS SDK as "Unable to load region from
// any of the providers" — no mention of replicas, buckets or what to do — and
// the CR sat in Seeding while the create was retried about once a second.
// Checking first turns that into one actionable sentence.
func TestMissingObjectStoreEnv(t *testing.T) {
	withEnv := func(names ...string) ResolvedTarget {
		var env []corev1.EnvVar
		for _, n := range names {
			env = append(env, corev1.EnvVar{Name: n, Value: "x"})
		}
		return ResolvedTarget{
			Found:   true,
			Cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{Env: env}},
		}
	}

	live := func(names ...string) []corev1.EnvVar {
		var env []corev1.EnvVar
		for _, n := range names {
			env = append(env, corev1.EnvVar{Name: n, Value: "x"})
		}
		return env
	}

	t.Run("s3 without AWS_REGION is reported", func(t *testing.T) {
		missing := missingObjectStoreEnv(withEnv(), serverEnvironment{}, "s3://bucket/chain/", "s3://bucket/chain/seed.backup")
		require.Equal(t, []string{"AWS_REGION"}, missing)
	})

	t.Run("s3 with AWS_REGION is satisfied", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv("AWS_REGION"), serverEnvironment{}, "s3://bucket/chain/"))
	})

	// AWS_DEFAULT_REGION is the SDK's own equally-valid spelling of the same
	// setting. Accepting only AWS_REGION failed clusters that had already set
	// the region, with a message telling them to set the region.
	t.Run("AWS_DEFAULT_REGION satisfies it too", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv("AWS_DEFAULT_REGION"), serverEnvironment{}, "s3://bucket/chain/"))
	})

	// The operator's own credential projection writes to the StatefulSet, not
	// to spec.env — so does every plugin/fleet merge. A check that reads only
	// spec.env cannot see any of it and rejects a cluster that is correctly
	// configured.
	t.Run("the region is found on the live StatefulSet, not only spec.env", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv(), serverEnvironment{Env: live("AWS_REGION")}, "s3://bucket/chain/"))
	})

	// IRSA and instance profiles supply credentials without a key pair, so
	// demanding the key and secret would break working deployments. Only the
	// region is genuinely un-inferrable, which is why only it is required.
	t.Run("a key pair is NOT required", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv("AWS_REGION"), serverEnvironment{}, "s3://bucket/chain/"))
	})

	// Network mode reads over the wire; there is no bucket to authenticate to.
	t.Run("non-s3 sources need nothing", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv(), serverEnvironment{}, ""))
		require.Empty(t, missingObjectStoreEnv(withEnv(), serverEnvironment{}, "file:///backups/chain/"))
	})

	t.Run("a standalone target is read the same way", func(t *testing.T) {
		tgt := ResolvedTarget{
			Found: true,
			Standalone: &neo4jv1beta1.Neo4jEnterpriseStandalone{
				Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
					Env: []corev1.EnvVar{{Name: "AWS_REGION", Value: "eu-west-1"}},
				},
			},
		}
		require.Empty(t, missingObjectStoreEnv(tgt, serverEnvironment{}, "s3://bucket/chain/"))
	})

	// spec.extraEnvFrom is documented for a credential bundle with the region
	// in it. A check that read only env names failed those clusters with a
	// message telling them to set the region they had set.
	t.Run("the region from an envFrom source is found", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv(),
			serverEnvironment{FromSources: map[string]bool{"AWS_REGION": true}}, "s3://bucket/chain/"))
	})

	t.Run("an envFrom source that could not be read defers to the server", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv(), serverEnvironment{Unresolved: true}, "s3://bucket/chain/"))
	})
}

// readServerEnvironment resolves the names every envFrom source supplies, from
// spec.extraEnvFrom and from the live container, with each source's prefix.
func TestReadServerEnvironment(t *testing.T) {
	const ns = "dr"
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, neo4jv1beta1.AddToScheme(scheme))

	secretRef := func(name, prefix string) corev1.EnvFromSource {
		return corev1.EnvFromSource{Prefix: prefix,
			SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: name}}}
	}
	configMapRef := func(name string) corev1.EnvFromSource {
		return corev1.EnvFromSource{
			ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: name}}}
	}
	cluster := func(sources ...corev1.EnvFromSource) ResolvedTarget {
		return ResolvedTarget{Found: true, Cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "dr-cluster", Namespace: ns},
			Spec:       neo4jv1beta1.Neo4jEnterpriseClusterSpec{ExtraEnvFrom: sources},
		}}
	}
	sts := func(container corev1.Container) *appsv1.StatefulSet {
		container.Name = "neo4j"
		return &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "dr-cluster-server", Namespace: ns},
			Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{container}}}},
		}
	}
	s3 := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dr-s3", Namespace: ns},
		Data: map[string][]byte{"AWS_REGION": []byte("us-east-1"), "AWS_ACCESS_KEY_ID": []byte("k")}}
	region := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "dr-region", Namespace: ns},
		Data: map[string]string{"AWS_DEFAULT_REGION": "us-east-1"}}

	read := func(t *testing.T, c client.Client, target ResolvedTarget) serverEnvironment {
		t.Helper()
		r := &Neo4jReplicaDatabaseReconciler{Client: c, Scheme: scheme}
		env, _ := r.readServerEnvironment(context.Background(), target, ns)
		return env
	}

	t.Run("a Secret in spec.extraEnvFrom supplies the region", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s3).Build()
		env := read(t, c, cluster(secretRef("dr-s3", "")))
		assert.True(t, env.FromSources["AWS_REGION"])
		assert.True(t, env.FromSources["AWS_ACCESS_KEY_ID"])
		assert.False(t, env.Unresolved)
		assert.Empty(t, missingObjectStoreEnv(cluster(secretRef("dr-s3", "")), env, "s3://bucket/chain/"))
	})

	t.Run("a ConfigMap source counts too", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(region).Build()
		assert.True(t, read(t, c, cluster(configMapRef("dr-region"))).FromSources["AWS_DEFAULT_REGION"])
	})

	// A prefix renames every key, so AWS_REGION under prefix S3_ is not the
	// variable the SDK reads.
	t.Run("the source's prefix is applied", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s3).Build()
		target := cluster(secretRef("dr-s3", "S3_"))
		env := read(t, c, target)
		assert.True(t, env.FromSources["S3_AWS_REGION"])
		assert.Equal(t, []string{"AWS_REGION"}, missingObjectStoreEnv(target, env, "s3://bucket/chain/"))
	})

	// The operator's own projections (auto-inherited seed credentials among
	// them) land on the StatefulSet, not in the spec.
	t.Run("envFrom on the live container is read as well", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(s3, sts(corev1.Container{EnvFrom: []corev1.EnvFromSource{secretRef("dr-s3", "")}})).Build()
		assert.True(t, read(t, c, cluster()).FromSources["AWS_REGION"])
	})

	// The pod gets nothing from a source that does not exist, so neither
	// does the check — the region is still missing.
	t.Run("a missing source supplies nothing", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		target := cluster(secretRef("absent", ""))
		env := read(t, c, target)
		assert.Empty(t, env.FromSources)
		assert.False(t, env.Unresolved)
		assert.Equal(t, []string{"AWS_REGION"}, missingObjectStoreEnv(target, env, "s3://bucket/chain/"))
	})

	t.Run("a source that cannot be read leaves the check to the server", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s3).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Secret); ok {
					return apierrors.NewForbidden(corev1.Resource("secrets"), key.Name, errors.New("denied"))
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).Build()
		target := cluster(secretRef("dr-s3", ""))
		env := read(t, c, target)
		assert.True(t, env.Unresolved)
		assert.Empty(t, missingObjectStoreEnv(target, env, "s3://bucket/chain/"))
	})
}

// Cloud workload identity injects credentials — and on some platforms the
// region — into the POD at admission, after everything the operator can read.
// A precondition that fails on what it cannot see would reject a correctly
// configured cluster and never let the server try.
//
// spec.podServiceAccountAnnotations is the field that binds the pods to a
// cloud role, and it already exists for the JVM's other object-store fetches
// (a cluster seedURI, a Neo4jDatabase seedBackupRef). A replica's seed and
// pull are the same fetch by the same JVM.
func TestMissingObjectStoreEnv_WorkloadIdentityDefersToTheServer(t *testing.T) {
	withSA := func(annotations map[string]string) ResolvedTarget {
		return ResolvedTarget{
			Found: true,
			Cluster: &neo4jv1beta1.Neo4jEnterpriseCluster{
				Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
					PodServiceAccountAnnotations: annotations,
				},
			},
		}
	}

	for _, key := range workloadIdentityAnnotations {
		t.Run(key, func(t *testing.T) {
			tgt := withSA(map[string]string{key: "some-identity"})
			require.Empty(t, missingObjectStoreEnv(tgt, serverEnvironment{}, "s3://bucket/chain/"),
				"a cluster bound to a cloud role must be allowed to try")
		})
	}

	t.Run("an unrelated annotation does not count", func(t *testing.T) {
		tgt := withSA(map[string]string{"example.com/team": "dr"})
		require.Equal(t, []string{"AWS_REGION"},
			missingObjectStoreEnv(tgt, serverEnvironment{}, "s3://bucket/chain/"))
	})

	t.Run("no annotations at all keeps the check", func(t *testing.T) {
		require.Equal(t, []string{"AWS_REGION"},
			missingObjectStoreEnv(withSA(nil), serverEnvironment{}, "s3://bucket/chain/"))
	})

	t.Run("a standalone bound to a cloud role", func(t *testing.T) {
		standalone := func(annotations map[string]string) ResolvedTarget {
			return ResolvedTarget{Found: true, Standalone: &neo4jv1beta1.Neo4jEnterpriseStandalone{
				Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{PodServiceAccountAnnotations: annotations},
			}}
		}
		require.Empty(t, missingObjectStoreEnv(standalone(map[string]string{workloadIdentityAnnotations[0]: "role"}), serverEnvironment{}, "s3://bucket/chain/"))
		require.Equal(t, []string{"AWS_REGION"}, missingObjectStoreEnv(standalone(nil), serverEnvironment{}, "s3://bucket/chain/"))
	})
}

// source.credentialsSecretRef now projects. Two properties matter more than the
// happy path: the value must never land in the StatefulSet spec, and two
// replicas naming different Secrets must be refused rather than fought over.
func TestReplicaCredentialProjection(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s3-creds", Namespace: "dr"},
		Data: map[string][]byte{
			"AWS_REGION":            []byte("eu-west-1"),
			"AWS_ACCESS_KEY_ID":     []byte("AKIAEXAMPLE"),
			"AWS_SECRET_ACCESS_KEY": []byte("verysecret"),
			"UNRELATED":             []byte("ignored"),
		},
	}

	t.Run("only known keys, and always by reference", func(t *testing.T) {
		env := desiredCredentialEnv("s3-creds", secret)
		require.Len(t, env, 3, "UNRELATED must not be projected")
		for _, e := range env {
			require.NotNil(t, e.ValueFrom, "%s must be a reference", e.Name)
			require.NotNil(t, e.ValueFrom.SecretKeyRef)
			assert.Equal(t, "s3-creds", e.ValueFrom.SecretKeyRef.Name)
			assert.Empty(t, e.Value,
				"a literal would put the credential in the StatefulSet spec, and in any bundle of it")
		}
	})

	t.Run("a second Secret for the same cluster is refused, not fought over", func(t *testing.T) {
		current := desiredCredentialEnv("other-creds", secret)
		name, other := conflictingCredentialSource(current, "s3-creds")
		assert.NotEmpty(t, name)
		assert.Equal(t, "other-creds", other)
	})

	t.Run("the same Secret is not a conflict", func(t *testing.T) {
		current := desiredCredentialEnv("s3-creds", secret)
		name, _ := conflictingCredentialSource(current, "s3-creds")
		assert.Empty(t, name)
	})

	// Foreign vars belong to the plugin/fleet/Aura controllers; clobbering them
	// is the oscillation mergeEnvVars exists to prevent.
	t.Run("projection preserves foreign env vars", func(t *testing.T) {
		current := []corev1.EnvVar{{Name: "NEO4J_PLUGINS", Value: `["apoc"]`}}
		merged := mergeEnvVars(current, desiredCredentialEnv("s3-creds", secret), map[string]struct{}{})
		var names []string
		for _, e := range merged {
			names = append(names, e.Name)
		}
		assert.Contains(t, names, "NEO4J_PLUGINS")
		assert.Contains(t, names, "AWS_REGION")
	})

	t.Run("envContainsAll detects a changed source", func(t *testing.T) {
		desired := desiredCredentialEnv("s3-creds", secret)
		assert.True(t, envContainsAll(desired, desired))
		assert.False(t, envContainsAll(desiredCredentialEnv("other", secret), desired))
	})
}
