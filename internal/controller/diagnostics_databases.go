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
	"sort"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// toDatabaseDiagnostics maps the SHOW DATABASES rows returned by
// Client.GetDatabases into the CR status representation.
//
// Shared by the cluster and standalone controllers, which previously each
// carried an identical copy of this mapping — meaning a field added to
// DatabaseInfo could silently reach one controller's status and not the
// other's. One function, one place to extend.
//
// Type/Access/Writer are the columns that distinguish a cross-cluster
// replica from an ordinary database; see
// docs/design/cross-cluster-replication.md §5.4 for why the operator needs
// to observe them rather than infer database kind from its own CR spec.
func toDatabaseDiagnostics(databases []neo4jclient.DatabaseInfo) []neo4jv1beta1.DatabaseDiagnosticInfo {
	if len(databases) == 0 {
		return nil
	}

	out := make([]neo4jv1beta1.DatabaseDiagnosticInfo, 0, len(databases))
	for _, d := range databases {
		out = append(out, neo4jv1beta1.DatabaseDiagnosticInfo{
			Name:             d.Name,
			Status:           d.Status,
			RequestedStatus:  d.RequestedStatus,
			Role:             d.Role,
			Default:          d.Default,
			Type:             d.Type,
			Access:           d.Access,
			Writer:           d.Writer,
			LastCommittedTxn: d.LastCommittedTxn,
			ReplicationLag:   d.ReplicationLag,
		})
	}
	sortDatabaseDiagnostics(out)
	return out
}

// sortDatabaseDiagnostics orders the rows deterministically. SHOW DATABASES
// returns them in no stable order (one row per database per server, with no
// address to tell copies apart), so unsorted the same databases read as a
// change on every pass and the status was rewritten each time (#475).
func sortDatabaseDiagnostics(dbs []neo4jv1beta1.DatabaseDiagnosticInfo) {
	sort.SliceStable(dbs, func(i, j int) bool {
		a, b := dbs[i], dbs[j]
		switch {
		case a.Name != b.Name:
			return a.Name < b.Name
		case a.Role != b.Role:
			return a.Role < b.Role
		case a.Writer != b.Writer:
			return a.Writer
		case a.Status != b.Status:
			return a.Status < b.Status
		case a.RequestedStatus != b.RequestedStatus:
			return a.RequestedStatus < b.RequestedStatus
		case a.Type != b.Type:
			return a.Type < b.Type
		case a.Access != b.Access:
			return a.Access < b.Access
		default:
			return a.LastCommittedTxn < b.LastCommittedTxn
		}
	})
}

// withoutTransactionCounters returns a copy of the rows without their
// LastCommittedTxn and ReplicationLag, re-sorted. Those move with every write
// to the database; status shows them as a snapshot, refreshed whenever
// anything else changes and at least every diagnosticsRefreshInterval, so they
// do not on their own make a diagnostics pass a change (#475).
func withoutTransactionCounters(dbs []neo4jv1beta1.DatabaseDiagnosticInfo) []neo4jv1beta1.DatabaseDiagnosticInfo {
	if dbs == nil {
		return nil
	}
	out := make([]neo4jv1beta1.DatabaseDiagnosticInfo, len(dbs))
	copy(out, dbs)
	for i := range out {
		out[i].LastCommittedTxn, out[i].ReplicationLag = 0, 0
	}
	sortDatabaseDiagnostics(out)
	return out
}
