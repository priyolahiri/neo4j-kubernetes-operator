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

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// AliasInfo is one row of SHOW ALIASES FOR DATABASE.
type AliasInfo struct {
	// Name is the alias name.
	Name string
	// Database is the database the alias resolves to.
	Database string
	// Location is "local" or "remote".
	Location string
}

// ShowAlias returns the alias with the given name, or nil when no such alias
// exists.
func (c *Client) ShowAlias(ctx context.Context, aliasName string) (*AliasInfo, error) {
	rows, err := c.ShowAliases(ctx)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].Name == aliasName {
			return &rows[i], nil
		}
	}
	return nil, nil
}

// ShowAliases lists every database alias in the DBMS.
func (c *Client) ShowAliases(ctx context.Context) ([]AliasInfo, error) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer c.closeSession(ctx, session)

	result, err := session.Run(ctx,
		"SHOW ALIASES FOR DATABASE YIELD name, database, location RETURN name, database, location", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to show aliases: %w", err)
	}

	var out []AliasInfo
	for result.Next(ctx) {
		rec := result.Record()
		name, _ := rec.Get("name")
		database, _ := rec.Get("database")
		location, _ := rec.Get("location")
		out = append(out, AliasInfo{
			Name:     columnString(name),
			Database: columnString(database),
			Location: columnString(location),
		})
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("error reading aliases: %w", err)
	}
	return out, nil
}

// CreateAlias creates a local alias pointing at targetDatabase, as a no-op if
// an alias of that name already exists.
//
// The target may be a cross-cluster replica: aliases can be created against a
// database whose type is still "replica", which is what lets a failover alias
// be pre-staged at replica-creation time rather than during the outage window.
func (c *Client) CreateAlias(ctx context.Context, aliasName, targetDatabase string) error {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer c.closeSession(ctx, session)

	query := fmt.Sprintf("CREATE ALIAS `%s` IF NOT EXISTS FOR DATABASE `%s`",
		escapeBackticks(aliasName), escapeBackticks(targetDatabase))
	if _, err := session.Run(ctx, query, nil); err != nil {
		return fmt.Errorf("failed to create alias %s for database %s: %w", aliasName, targetDatabase, err)
	}
	return nil
}

// AlterAliasTarget re-points an existing alias at a different database.
func (c *Client) AlterAliasTarget(ctx context.Context, aliasName, targetDatabase string) error {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer c.closeSession(ctx, session)

	query := fmt.Sprintf("ALTER ALIAS `%s` SET DATABASE TARGET `%s`",
		escapeBackticks(aliasName), escapeBackticks(targetDatabase))
	if _, err := session.Run(ctx, query, nil); err != nil {
		return fmt.Errorf("failed to alter alias %s to target %s: %w", aliasName, targetDatabase, err)
	}
	return nil
}

// DropAliasIfExists removes an alias. Dropping an alias never affects the
// database it pointed at.
func (c *Client) DropAliasIfExists(ctx context.Context, aliasName string) error {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer c.closeSession(ctx, session)

	query := fmt.Sprintf("DROP ALIAS `%s` IF EXISTS FOR DATABASE", escapeBackticks(aliasName))
	if _, err := session.Run(ctx, query, nil); err != nil {
		return fmt.Errorf("failed to drop alias %s: %w", aliasName, err)
	}
	return nil
}
