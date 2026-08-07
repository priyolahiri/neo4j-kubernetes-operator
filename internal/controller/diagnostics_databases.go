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
	return out
}
