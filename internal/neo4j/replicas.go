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

package neo4j

import (
	"context"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// DatabaseTypeReplica is the value the SHOW DATABASES `type` column carries
// for a cross-cluster replica (Neo4j 2026.08+).
//
// The operator treats "type is not replica" — rather than its own CR status —
// as the authoritative signal that a promotion has happened, because status
// can be lost to an etcd restore while the live type cannot.
const DatabaseTypeReplica = "replica"

// ReplicaBackupSource describes a backup-based replication source.
type ReplicaBackupSource struct {
	// UpstreamDatabase is the database name on the upstream cluster
	// (replicaConfig.remote).
	UpstreamDatabase string
	// PullURI is the object-storage directory holding the differential chain.
	PullURI string
	// SeedURI is the full backup the replica is initially seeded from.
	SeedURI string
	// Primaries / Secondaries set the replica's topology. Zero means "let
	// Neo4j choose" and the TOPOLOGY clause is omitted.
	Primaries   int32
	Secondaries int32
}

// CreateReplicaDatabaseFromBackup issues CREATE REPLICA DATABASE for the
// backup-pull replication mode.
//
// Shape (Neo4j 2026.08+):
//
//	CREATE REPLICA DATABASE `foo-replica`
//	TOPOLOGY 3 PRIMARIES 1 SECONDARY
//	OPTIONS {seedURI: $seedURI, replicaConfig: {remote: $remote, pullURI: $pullURI}}
//	WAIT
//
// The URIs and the upstream name go through query parameters rather than
// string interpolation; only the database name and the TOPOLOGY counts are
// interpolated, because Neo4j's admin DDL will not accept parameters there.
func (c *Client) CreateReplicaDatabaseFromBackup(ctx context.Context, databaseName string, src ReplicaBackupSource) error {
	if src.UpstreamDatabase == "" {
		return fmt.Errorf("upstream database is required to create replica %q", databaseName)
	}
	if src.PullURI == "" {
		return fmt.Errorf("pullURI is required to create backup-based replica %q", databaseName)
	}

	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer c.closeSession(ctx, session)

	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE REPLICA DATABASE `%s`", escapeBackticks(databaseName))
	if clause := topologyClause(src.Primaries, src.Secondaries); clause != "" {
		sb.WriteString(" " + clause)
	}

	params := map[string]any{
		"remote":  src.UpstreamDatabase,
		"pullURI": src.PullURI,
	}
	if src.SeedURI != "" {
		sb.WriteString(" OPTIONS {seedURI: $seedURI, replicaConfig: {remote: $remote, pullURI: $pullURI}}")
		params["seedURI"] = src.SeedURI
	} else {
		sb.WriteString(" OPTIONS {replicaConfig: {remote: $remote, pullURI: $pullURI}}")
	}
	sb.WriteString(" WAIT")

	if _, err := session.Run(ctx, sb.String(), params); err != nil {
		return fmt.Errorf("failed to create replica database %s: %w", databaseName, err)
	}
	return nil
}

// PromoteReplicaDatabase converts a replica into an ordinary read-write
// database via dbms.promoteReplicaDatabase.
//
// IRREVERSIBLE. A promoted database cannot be re-attached to its upstream, and
// any replication lag outstanding at this moment becomes permanent data loss.
//
// When primaries is zero the options map is left empty and the database keeps
// its current topology.
func (c *Client) PromoteReplicaDatabase(ctx context.Context, databaseName string, primaries, secondaries int32) error {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer c.closeSession(ctx, session)

	options := map[string]any{}
	if primaries > 0 {
		options["primaries"] = primaries
		options["secondaries"] = secondaries
	}

	params := map[string]any{
		"name":    databaseName,
		"options": options,
	}
	if _, err := session.Run(ctx, "CALL dbms.promoteReplicaDatabase($name, $options)", params); err != nil {
		return fmt.Errorf("failed to promote replica database %s: %w", databaseName, err)
	}
	return nil
}

// GetDatabaseInfo returns the SHOW DATABASES row for one database, or nil when
// it does not exist.
//
// This is the observe step every mutating replica path runs first: it is how
// the operator learns that a database it believes is a replica has in fact
// been promoted, whether by a Neo4jReplicaPromotion or by someone at a
// cypher-shell.
func (c *Client) GetDatabaseInfo(ctx context.Context, databaseName string) (*DatabaseInfo, error) {
	databases, err := c.GetDatabases(ctx)
	if err != nil {
		return nil, err
	}
	for i := range databases {
		if databases[i].Name == databaseName {
			return &databases[i], nil
		}
	}
	return nil, nil
}

// topologyClause renders the TOPOLOGY clause, or "" when no topology is
// requested. Singular/plural keywords matter to Neo4j's parser.
func topologyClause(primaries, secondaries int32) string {
	if primaries <= 0 {
		return ""
	}
	clause := fmt.Sprintf("TOPOLOGY %d %s", primaries, pluralise(primaries, "PRIMARY", "PRIMARIES"))
	if secondaries > 0 {
		clause += fmt.Sprintf(" %d %s", secondaries, pluralise(secondaries, "SECONDARY", "SECONDARIES"))
	}
	return clause
}

func pluralise(n int32, singular, plural string) string {
	if n == 1 {
		return singular
	}
	return plural
}
