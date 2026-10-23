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
	"fmt"
	"sort"
	"strings"
	"time"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// The database gate of a rolling upgrade (#488).
//
// A server that has restarted is Enabled and Available in SHOW SERVERS as soon
// as it has rejoined, while its databases can still be starting or catching
// up; the readiness probe checks less still. Restarting the next server in
// that window leaves a database with three primaries one working copy, and its
// writes stop. Measured on Kind with 30 databases: the operator moved on while
// the server it had just restarted had 29 of 30 copies replicating.
//
// So before the next server restarts, the one that just did must have every
// database copy caught up, read on that server:
//
//   - dbms.cluster.statusCheck (5.24+ and CalVer) replicates a dummy
//     transaction per clustered database; the server's own row must report
//     replicationSuccessful and APPLYING — "can replicate and is actively
//     applying" — not REPLICATING (cannot apply) or UNAVAILABLE;
//   - SHOW DATABASES covers what statusCheck cannot judge, single-primary
//     databases and secondaries: every database on the server must be at its
//     requested status.

const (
	// upgradeStatusCheckTimeout bounds each database's dummy transaction.
	upgradeStatusCheckTimeout = 2 * time.Second
	// upgradeGateCallTimeout bounds the whole check, so a server that does
	// not answer cannot hold the cluster controller's only worker.
	upgradeGateCallTimeout = 20 * time.Second
)

// upgradeGates replaces the network in rolling-upgrade tests; nil in
// production.
type upgradeGates struct {
	// serverAvailable answers isServerAvailable: the server is Enabled and
	// Available, and the version it reports.
	serverAvailable   func(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, podName string) (bool, string, error)
	databasesCaughtUp func(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, podName string) (bool, []string, error)
	// diagnostics replaces CollectDiagnostics during an upgrade.
	diagnostics func(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) error
}

// addressIsPod reports whether a Neo4j server address ("host:port") belongs
// to podName. The host is the pod's FQDN, so it must be the pod name or start
// with "<pod>." — a substring test would let x-server-1 match x-server-10.
func addressIsPod(address, podName string) bool {
	host := address
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	return host == podName || strings.HasPrefix(host, podName+".")
}

// evaluateDatabaseGate decides whether every database copy on podName is
// caught up, from statusCheck rows read on that server and SHOW DATABASES. It
// returns the databases that are not, sorted, for the status message.
func evaluateDatabaseGate(podName string, checks []neo4jclient.ClusterStatusCheckRow, allocs []neo4jclient.DatabaseAllocation) (bool, []string) {
	notReady := map[string]bool{}
	for _, a := range allocs {
		if addressIsPod(a.Address, podName) && a.CurrentStatus != a.RequestedStatus {
			notReady[a.Name] = true
		}
	}
	for _, c := range checks {
		if !c.Requester {
			continue
		}
		if !c.ReplicationSuccessful || c.MemberStatus != "APPLYING" || c.Error != "" {
			notReady[c.Database] = true
		}
	}
	names := make([]string, 0, len(notReady))
	for n := range notReady {
		names = append(names, n)
	}
	sort.Strings(names)
	return len(names) == 0, names
}

// lastRolledDatabasesCaughtUp reads the database gate on podName.
func (r *Neo4jEnterpriseClusterReconciler) lastRolledDatabasesCaughtUp(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, podName string) (bool, []string, error) {
	if r.UpgradeGates != nil && r.UpgradeGates.databasesCaughtUp != nil {
		return r.UpgradeGates.databasesCaughtUp(ctx, cluster, podName)
	}
	ctx, cancel := context.WithTimeout(ctx, upgradeGateCallTimeout)
	defer cancel()
	c, err := (&SplitBrainDetector{Client: r.Client}).createPodSpecificNeo4jClient(ctx, cluster, podName)
	if err != nil {
		return false, nil, err
	}
	defer func() { _ = c.Close() }()
	checks, err := c.ClusterStatusCheck(ctx, upgradeStatusCheckTimeout)
	if err != nil {
		return false, nil, err
	}
	allocs, err := c.DatabaseAllocations(ctx)
	if err != nil {
		return false, nil, err
	}
	ok, notReady := evaluateDatabaseGate(podName, checks, allocs)
	return ok, notReady, nil
}

// databaseGateMessage is the status message while the gate holds the walk.
func databaseGateMessage(ordinal int32, notReady []string) string {
	return fmt.Sprintf("Rolling server ordinal %d: waiting for its databases to catch up (%s)", ordinal, databaseNameList(notReady))
}

// databaseNameList names the first five databases and counts the rest, so a
// status message stays short on a server with hundreds of databases.
func databaseNameList(names []string) string {
	const shown = 5
	if len(names) <= shown {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:shown], ", "), len(names)-shown)
}
