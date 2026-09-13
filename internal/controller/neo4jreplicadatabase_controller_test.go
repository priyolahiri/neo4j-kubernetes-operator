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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	t.Run("s3 without AWS_REGION is reported", func(t *testing.T) {
		missing := missingObjectStoreEnv(withEnv(), "s3://bucket/chain/", "s3://bucket/chain/seed.backup")
		require.Equal(t, []string{"AWS_REGION"}, missing)
	})

	t.Run("s3 with AWS_REGION is satisfied", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv("AWS_REGION"), "s3://bucket/chain/"))
	})

	// IRSA and instance profiles supply credentials without a key pair, so
	// demanding the key and secret would break working deployments. Only the
	// region is genuinely un-inferrable, which is why only it is required.
	t.Run("a key pair is NOT required", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv("AWS_REGION"), "s3://bucket/chain/"))
	})

	// Network mode reads over the wire; there is no bucket to authenticate to.
	t.Run("non-s3 sources need nothing", func(t *testing.T) {
		require.Empty(t, missingObjectStoreEnv(withEnv(), ""))
		require.Empty(t, missingObjectStoreEnv(withEnv(), "file:///backups/chain/"))
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
		require.Empty(t, missingObjectStoreEnv(tgt, "s3://bucket/chain/"))
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
