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

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

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
