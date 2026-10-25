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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// A deletion's client dials the {name}-client ClusterIP Service, which
// resolves while the deployment exists, but under the routing scheme the
// statement goes to the server the routing table names: on a cluster, a pod's
// FQDN under the headless Service. That record is absent between the pod's
// deletion and its replacement, so a DROP during a restart can fail with "no
// such host" while the cluster CR is there. classifyFinalizerCleanup read that
// as a deployment that was gone and released the Neo4jDatabase's finalizer at
// once, leaving the database in Neo4j. It is the same bounded wait as any
// other unreachable target; only a target CR that is gone releases at once
// (knowledge rule 178).

// errPodNameUnresolved is the routing table's leader not resolving: the pod
// was deleted and its replacement has no DNS record yet.
var errPodNameUnresolved = fmt.Errorf("failed to drop database orders: %w",
	errors.New("ConnectivityError: dial tcp: lookup ec-server-1.ec-headless.default.svc.cluster.local on 10.96.0.10:53: no such host"))

func TestDatabaseDeletion_ANameThatDoesNotResolveDuringARestartWaits(t *testing.T) {
	ctx := context.Background()
	newReconciler := func(c client.Client, rec record.EventRecorder, server databaseNeo4jClient) *Neo4jDatabaseReconciler {
		return &Neo4jDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, RequeueAfter: 30 * time.Second,
			newNeo4jClient: func(*neo4jv1beta1.Neo4jEnterpriseCluster, *neo4jv1beta1.Neo4jEnterpriseStandalone) (databaseNeo4jClient, error) {
				return server, nil
			}}
	}
	terminating := func(since time.Time) *neo4jv1beta1.Neo4jDatabase {
		db := ordersDatabase("orders")
		ts := metav1.NewTime(since)
		db.DeletionTimestamp = &ts
		return db
	}

	t.Run("within the grace period the deletion waits", func(t *testing.T) {
		db := terminating(time.Now())
		c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(readyDatabaseHost(), db).
			WithStatusSubresource(&neo4jv1beta1.Neo4jDatabase{}).Build()
		rec := record.NewFakeRecorder(50)
		server := &failingDropServer{err: errPodNameUnresolved}
		r := newReconciler(c, rec, server)
		key := client.ObjectKeyFromObject(db)

		for _, err := range reconcileTimes(t, r, key, repeatPasses) {
			require.NoError(t, err)
		}
		live := &neo4jv1beta1.Neo4jDatabase{}
		require.NoError(t, c.Get(ctx, key, live))
		assert.Contains(t, live.Finalizers, DatabaseFinalizer,
			"the cluster CR exists: a name that does not resolve is a restart, and the database is still in Neo4j")
		assert.Equal(t, repeatPasses, server.drops, "the drop is retried")
		failed := eventsWithReason(drainEvents(rec), EventReasonDeletionFailed)
		require.Len(t, failed, 1, "announced once: %q", failed)
		assert.Contains(t, failed[0], "no such host")
	})

	t.Run("after the grace period it releases and names the database", func(t *testing.T) {
		db := terminating(time.Now().Add(-finalizerDeletionGracePeriod - time.Minute))
		c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(readyDatabaseHost(), db).
			WithStatusSubresource(&neo4jv1beta1.Neo4jDatabase{}).Build()
		rec := record.NewFakeRecorder(50)
		r := newReconciler(c, rec, &failingDropServer{err: errPodNameUnresolved})

		for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(db), 1) {
			require.NoError(t, err)
		}
		assert.True(t, finalizerReleased(t, c, db, DatabaseFinalizer), "a deletion does not wedge")
		failed := eventsWithReason(drainEvents(rec), EventReasonDeletionFailed)
		require.Len(t, failed, 1)
		assert.True(t, strings.HasPrefix(failed[0], "Warning "))
		assert.Contains(t, failed[0], "DROP DATABASE `orders`")
	})

	t.Run("a cluster CR that is gone releases at once", func(t *testing.T) {
		db := terminating(time.Now())
		c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(db).
			WithStatusSubresource(&neo4jv1beta1.Neo4jDatabase{}).Build()
		rec := record.NewFakeRecorder(50)
		server := &failingDropServer{err: errPodNameUnresolved}
		r := newReconciler(c, rec, server)

		for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(db), 1) {
			require.NoError(t, err)
		}
		assert.True(t, finalizerReleased(t, c, db, DatabaseFinalizer))
		assert.Zero(t, server.drops)
		assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonDeletionFailed))
	})
}

// The kinds that go through deletionCleanup read a name that does not
// resolve as their target restarting, whether the connection meets it (through
// Reconcile here) or the statement (connectFailedCause and statementFailedCause
// below).
func TestDeletion_ANameThatDoesNotResolveIsTheTargetRestarting(t *testing.T) {
	for _, k := range deletionKinds() {
		if !k.checksReady {
			continue // the database, above
		}
		t.Run(k.name, func(t *testing.T) {
			obj := k.obj(time.Now())
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, unreachableTarget()).
				WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
			rec := record.NewFakeRecorder(50)
			connErr := errPodNameUnresolved
			r, sent := k.setup(c, rec, &connErr)

			for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), repeatPasses) {
				require.NoError(t, err)
			}
			assert.False(t, finalizerReleased(t, c, obj, k.finalizer))
			assert.Zero(t, sent())
			warned := eventsWithReason(drainEvents(rec), k.reason)
			require.Len(t, warned, 1)
			assert.Contains(t, warned[0], "is not Ready or does not accept connections yet")
		})
	}
}

func TestDeletionCauses_ANameThatDoesNotResolveIsTheTargetDown(t *testing.T) {
	want := targetDownCause("ec")
	assert.Equal(t, want, connectFailedCause("ec", errPodNameUnresolved))
	assert.Equal(t, want, statementFailedCause("ec", "DROP USER", errPodNameUnresolved))
	assert.NotEqual(t, want, statementFailedCause("ec", "DROP USER", errors.New("Neo.ClientError.Security.Forbidden: no")),
		"a refusal is the statement's own cause")
}
