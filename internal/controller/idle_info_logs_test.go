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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// What an idle Neo4jDatabase and an idle cluster logged at info on every pass
// (#626, the v1.24.1 walk): three lines a database, about every 12 s, and four
// a cluster, about every 10 s, although nothing changed (operations.md id 235).
var (
	idleDatabaseLines = []string{
		"Starting database creation/verification",
		"Database already exists",
		"Database creation/verification completed successfully",
	}
	idleClusterLines = []string{
		"Starting CreateOrUpdate operation",
		"No topology placement configuration specified",
		"No topology constraints configured",
	}
)

// An idle Neo4jDatabase logs the database's existence check at V(1) only;
// the pass that creates it still says so at info.
func TestDatabaseReconcile_AnIdlePassLogsTheCheckAtV1(t *testing.T) {
	server := &databaseServer{}
	db := ordersDatabase("orders")
	r, _ := databaseTestReconciler(t, server, readyDatabaseHost(), db)
	key := client.ObjectKeyFromObject(db)

	first := newLevelCaptureSink()
	require.NoError(t, loggedPass(first, r, key))
	assert.NotEmpty(t, first.mentioning(0, "Database created successfully"), "the creating pass says so at info")

	for i := range 3 {
		sink := newLevelCaptureSink()
		require.NoError(t, loggedPass(sink, r, key))
		for _, line := range idleDatabaseLines {
			assert.Empty(t, sink.mentioning(0, line), "idle pass %d: %q at info", i, line)
			assert.NotEmpty(t, sink.mentioning(1, line), "idle pass %d: %q still at V(1)", i, line)
		}
	}
}

// An idle cluster logs its StatefulSet's create-or-update and its topology
// placement at V(1) only.
func TestClusterReconcile_AnIdlePassLogsItsPlacementAtV1(t *testing.T) {
	cluster := minimalCluster("idle", "idlens")
	cluster.Finalizers = []string{ClusterFinalizer}
	cluster.Spec.Image.Tag = "5.26.32-enterprise"
	c, _ := defaultingClient(t, false, cluster, standardStorageClass())
	r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(1000),
		RequeueAfter: time.Minute, Validator: validation.NewClusterValidator(c), TopologyScheduler: NewTopologyScheduler(c)}
	key := client.ObjectKeyFromObject(cluster)
	pass := func(sink levelCaptureSink) {
		t.Helper()
		cm := NewConfigMapManager(c)
		cm.LiveConfig = nil
		r.ConfigMapManager = cm
		require.NoError(t, loggedPass(sink, r, key))
	}
	pass(newLevelCaptureSink())
	kubeControllerManagerWrites(t, c, client.ObjectKey{Namespace: "idlens", Name: "idle-server"}, false)
	pass(newLevelCaptureSink())

	for i := range 3 {
		sink := newLevelCaptureSink()
		pass(sink)
		for _, line := range idleClusterLines {
			assert.Empty(t, sink.mentioning(0, line), "idle pass %d: %q at info", i, line)
			assert.NotEmpty(t, sink.mentioning(1, line), "idle pass %d: %q still at V(1)", i, line)
		}
	}
}
