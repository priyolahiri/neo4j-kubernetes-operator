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
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// A clusterRef that names no cluster yet is a wait (operations.md id 145):
// Pending, one ClusterNotFound, and the database is created once the cluster
// is there. TestShardedReconcile_AMissingClusterIsAWaitThatEnds pins that with
// no validator; the operator runs one, and its validator refused the missing
// cluster first ("Validation failed: failed to get cluster …"), so the wait
// was never reached and the database sat in Failed (#532).
func TestShardedReconcile_AMissingClusterIsAWaitWithTheOperatorsValidator(t *testing.T) {
	sd := productsShardedDB()
	server := &shardedServer{name: "products"}
	r, rec := shardedTestReconciler(t, server, sd)
	r.ShardedDatabaseValidator = validation.NewShardedDatabaseValidator(r.Client)
	key := client.ObjectKeyFromObject(sd)

	reconcileTimes(t, r, key, repeatPasses)
	events := drainEvents(rec)
	assert.Equal(t, "Pending", shardedPhase(t, r, key))
	assert.Len(t, eventsWithReason(events, EventReasonClusterNotFound), 1)
	assert.Empty(t, eventsWithReason(events, EventReasonValidationFailed))

	cluster := shardingCluster()
	cluster.Spec.Topology.Servers = 3
	require.NoError(t, r.Create(context.Background(), cluster))
	reconcileTimes(t, r, key, 1)
	assert.Equal(t, "Ready", shardedPhase(t, r, key))
	assert.Equal(t, 1, server.count("CREATE DATABASE"))
}

// With the validator wired in, a spec it refuses is still refused while the
// cluster is missing, with every error at once.
func TestShardedReconcile_ASpecErrorIsReportedWhileTheClusterIsMissing(t *testing.T) {
	sd := productsShardedDB()
	sd.Spec.Name = "system"
	sd.Spec.DefaultCypherLanguage = "5"
	r, rec := shardedTestReconciler(t, &shardedServer{name: "products"}, sd)
	r.ShardedDatabaseValidator = validation.NewShardedDatabaseValidator(r.Client)
	key := client.ObjectKeyFromObject(sd)

	reconcileTimes(t, r, key, 1)
	got := &neo4jv1beta1.Neo4jShardedDatabase{}
	require.NoError(t, r.Get(context.Background(), key, got))
	assert.Equal(t, "Failed", got.Status.Phase)
	assert.Contains(t, got.Status.Message, "spec.name")
	assert.Contains(t, got.Status.Message, "spec.defaultCypherLanguage")
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonValidationFailed), 1)
}
