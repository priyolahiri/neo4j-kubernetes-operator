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
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listingDriver is a Bolt server that answers SHOW DATABASES and SHOW ALIASES
// with fixed rows, so the real Client methods — query, row parsing, matching —
// run without a server. Every other method is the nil embedded interface.
type listingDriver struct {
	neo4j.DriverWithContext
	databases []*neo4j.Record
	aliases   []*neo4j.Record
}

func (d *listingDriver) NewSession(context.Context, neo4j.SessionConfig) neo4j.SessionWithContext {
	return &listingSession{d: d}
}

type listingSession struct {
	neo4j.SessionWithContext
	d *listingDriver
}

func (s *listingSession) Run(_ context.Context, cypher string, _ map[string]any, _ ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
	switch {
	case strings.Contains(cypher, "SHOW DATABASES"):
		return &listingResult{records: s.d.databases}, nil
	case strings.Contains(cypher, "SHOW ALIASES"):
		return &listingResult{records: s.d.aliases}, nil
	}
	return &listingResult{}, nil
}

func (s *listingSession) Close(context.Context) error { return nil }

type listingResult struct {
	neo4j.ResultWithContext
	records []*neo4j.Record
	current *neo4j.Record
}

func (r *listingResult) Next(context.Context) bool {
	if len(r.records) == 0 {
		return false
	}
	r.current, r.records = r.records[0], r.records[1:]
	return true
}

func (r *listingResult) Record() *neo4j.Record { return r.current }
func (r *listingResult) Err() error            { return nil }

// databaseRow is one SHOW DATABASES row as GetDatabases yields it.
func databaseRow(name, dbType string) *neo4j.Record {
	return &neo4j.Record{
		Keys: []string{"name", "currentStatus", "default", "home", "role", "requestedStatus", "type", "access",
			"writer", "lastCommittedTxn", "replicationLag", "address"},
		Values: []any{name, "online", false, false, "primary", "online", dbType, "read-write",
			true, int64(7), int64(0), "server-0:7687"},
	}
}

// aliasRow is one SHOW ALIASES row as ShowAliases yields it.
func aliasRow(name, composite, database string) *neo4j.Record {
	var comp any
	if composite != "" {
		comp = composite
	}
	return &neo4j.Record{
		Keys:   []string{"name", "composite", "database", "location", "url", "user"},
		Values: []any{name, comp, database, "local", nil, nil},
	}
}

func listingClient(d *listingDriver) *Client {
	return &Client{driver: d, circuitBreaker: newCircuitBreaker(), poolMetrics: newConnectionPoolMetrics()}
}

// Neo4j database names "are case-insensitive and normalized to lowercase"
// (Operations Manual 5 and /current/, "Naming rules for databases"): the
// database created as `Orders` is listed as `orders`. DatabaseExists matched
// the listing exactly, so it called that database absent — and the database
// controller sent CREATE again on every pass, the restore controllers took the
// create path over an existing database.
func TestDatabaseExists_IgnoresCase(t *testing.T) {
	c := listingClient(&listingDriver{databases: []*neo4j.Record{
		databaseRow("system", "system"), databaseRow("orders", "standard"),
	}})
	ctx := context.Background()

	for _, name := range []string{"orders", "Orders", "ORDERS"} {
		exists, err := c.DatabaseExists(ctx, name)
		require.NoError(t, err)
		assert.True(t, exists, "%q names the listed database `orders`", name)
	}
	for _, name := range []string{"order", "orders2", "invoices"} {
		exists, err := c.DatabaseExists(ctx, name)
		require.NoError(t, err)
		assert.False(t, exists, "%q is another name", name)
	}
}

// GetDatabaseInfo is the observe step of the replica, promotion, alias and
// composite controllers; an exact match there made a replica named `Ledger`
// look absent on every pass.
func TestGetDatabaseInfo_IgnoresCase(t *testing.T) {
	c := listingClient(&listingDriver{databases: []*neo4j.Record{
		databaseRow("system", "system"), databaseRow("ledger", "replica"),
	}})

	info, err := c.GetDatabaseInfo(context.Background(), "Ledger")
	require.NoError(t, err)
	require.NotNil(t, info, "`Ledger` is the listed `ledger`")
	assert.Equal(t, "ledger", info.Name, "the row as Neo4j lists it")
	assert.Equal(t, DatabaseTypeReplica, info.Type)

	info, err = c.GetDatabaseInfo(context.Background(), "Ledgers")
	require.NoError(t, err)
	assert.Nil(t, info)
}

// Alias names are normalised like database names (DatabaseReference.alias()
// is a NormalizedDatabaseName), so the alias created as `Front` is listed as
// `front`. ShowAlias matched exactly, and the alias controller re-sent CREATE
// ALIAS with an AliasCreated event on every pass.
func TestShowAlias_IgnoresCase(t *testing.T) {
	c := listingClient(&listingDriver{aliases: []*neo4j.Record{
		aliasRow("front", "", "orders"), aliasRow("cineasts.latest", "cineasts", "movies"),
	}})

	got, err := c.ShowAlias(context.Background(), "Front")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "front", got.Name)
	assert.Equal(t, "orders", got.Database)

	got, err = c.ShowAlias(context.Background(), "Cineasts.Latest")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "cineasts", got.Composite)

	got, err = c.ShowAlias(context.Background(), "fronts")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestFindDatabase(t *testing.T) {
	listing := []DatabaseInfo{
		{Name: "system"},
		{Name: "orders", Address: "server-1:7687"},
		{Name: "orders", Address: "server-0:7687"},
	}
	got := FindDatabase(listing, "Orders")
	require.NotNil(t, got)
	assert.Same(t, &listing[1], got, "the first row of the database, not a copy")
	assert.Nil(t, FindDatabase(listing, "order"))
	assert.Nil(t, FindDatabase(nil, "orders"))
}
