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

// A spec the operator refuses is a status the user has to fix, not a
// controller error. The cluster and the standalone logged an ERROR on every
// pass while it stayed refused — every 6–8 seconds, 28 lines in about four
// minutes on the #562 walk. It is logged at info when the refusal is
// announced, at V(1) on the passes in between (knowledge rule 136).

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The Neo4jDatabase and Neo4jShardedDatabase validation failures were still an
// ERROR on every requeue (60 in ten minutes on the round-7 walk) while their
// event fired once (knowledge id 197).
func TestReconcile_ARefusedSpecIsLoggedOnceNotAsAnError(t *testing.T) {
	for _, tc := range validationFailedCases() {
		switch tc.name {
		case "Neo4jEnterpriseCluster", "Neo4jEnterpriseStandalone", "Neo4jDatabase", "Neo4jShardedDatabase":
		default:
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			sink := newLevelCaptureSink()
			ctx := log.IntoContext(context.Background(), logr.New(sink))
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(tc.obj).
				WithStatusSubresource(tc.obj).Build()
			r := tc.newReconciler(c, record.NewFakeRecorder(100))
			for range 3 {
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tc.obj)})
				require.NoError(t, err)
			}
			assert.Empty(t, sink.errors(), "a refusal is not a controller error")
			assert.Len(t, sink.mentioning(0, "validation failed"), 1, "logged when it is announced")
			assert.Len(t, sink.mentioning(1, "validation failed"), 2, "and at V(1) on the passes in between")
		})
	}
}

// The other refusals of the database kinds that a requeue meets again: a
// database waiting for its host's seed credentials, a sharded database whose
// cluster has no property sharding, and one whose seedBackupRef names no
// backup. Each was an ERROR on every pass while its event fired once.
func TestReconcile_ADatabaseKindsLastingRefusalIsLoggedOnceNotAsAnError(t *testing.T) {
	cases := []struct {
		name      string
		mentions  string
		reconcile func(ctx context.Context) error
	}{
		{
			name: "Neo4jDatabase seed credentials missing", mentions: "seed credentials",
			reconcile: func() func(context.Context) error {
				db := ordersDatabase("orders")
				db.Spec.SeedURI = "s3://backups/orders.backup"
				db.Spec.SeedCredentials = &neo4jv1beta1.SeedCredentials{SecretRef: "s3-creds"}
				r, _ := databaseTestReconciler(t, &databaseServer{}, readyDatabaseHost(), db)
				return func(ctx context.Context) error {
					_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(db)})
					return err
				}
			}(),
		},
		{
			name: "Neo4jShardedDatabase cluster without property sharding", mentions: "does not support property sharding",
			reconcile: func() func(context.Context) error {
				cluster := shardingCluster()
				cluster.Spec.PropertySharding = nil
				sd := productsShardedDB()
				r, _ := shardedTestReconciler(t, &shardedServer{name: "products"}, cluster, sd)
				return func(ctx context.Context) error {
					_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(sd)})
					return err
				}
			}(),
		},
		{
			name: "Neo4jShardedDatabase seedBackupRef names no backup", mentions: "seedBackupRef",
			reconcile: func() func(context.Context) error {
				sd := productsShardedDB()
				sd.Spec.SeedBackupRef = "no-such-backup"
				r, _ := shardedTestReconciler(t, &shardedServer{name: "products"}, shardingCluster(), sd)
				return func(ctx context.Context) error {
					_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(sd)})
					return err
				}
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := newLevelCaptureSink()
			ctx := log.IntoContext(context.Background(), logr.New(sink))
			for range 3 {
				require.NoError(t, tc.reconcile(ctx))
			}
			assert.Empty(t, sink.errors(), "a refusal or a wait is not a controller error")
			assert.Len(t, sink.mentioning(0, tc.mentions), 1, "logged when it is announced")
			assert.Len(t, sink.mentioning(1, tc.mentions), 2, "and at V(1) on the passes in between")
		})
	}
}
