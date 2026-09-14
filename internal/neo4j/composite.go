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

// CompositeDatabaseType is the value of the `type` column in SHOW DATABASES
// for a composite database. Verified on 5.26.30 and 2026.08.1.
const CompositeDatabaseType = "composite"

// CreateCompositeDatabase creates a composite database.
//
// Three things this deliberately does NOT do, each verified against a live
// server rather than taken from the docs:
//
//   - It never emits an OPTIONS clause. The parser accepts `OPTIONS {}` but
//     any actual key is refused — "Invalid input: 'OPTIONS' is not supported
//     in 'CREATE COMPOSITE DATABASE'" on 2026.08.1, "composite databases have
//     no valid options values" on 5.26. A composite has no options at all.
//   - It never emits a topology. A composite stores nothing, so it has none.
//   - It emits DEFAULT LANGUAGE only when asked, because the clause does not
//     parse on 5.26 — see cypherLanguageClause and the validator's version
//     gate.
//
// WAIT is supported on both lines and returns the usual
// (address, state, message, success) row.
func (c *Client) CreateCompositeDatabase(ctx context.Context, name string, wait bool, ifNotExists bool, cypherVersion string) error {
	query := fmt.Sprintf("CREATE COMPOSITE DATABASE `%s`", escapeBackticks(name))
	if ifNotExists {
		query += " IF NOT EXISTS"
	}
	query += cypherLanguageClause(cypherVersion)
	if wait {
		query += " WAIT"
	}
	return c.runSystemWrite(ctx, query, "create composite database")
}

// AlterCompositeDatabaseLanguage sets the composite's default Cypher version.
//
// This is the ONLY property of a composite that can be altered. CalVer only:
// 5.26's ALTER DATABASE accepts just OPTION, ACCESS READ and TOPOLOGY.
func (c *Client) AlterCompositeDatabaseLanguage(ctx context.Context, name, cypherVersion string) error {
	if cypherVersion == "" {
		return nil
	}
	query := fmt.Sprintf("ALTER DATABASE `%s` SET DEFAULT LANGUAGE CYPHER %s",
		escapeBackticks(name), cypherVersion)
	return c.runSystemWrite(ctx, query, "alter composite database language")
}

// DropCompositeDatabase drops a composite and its constituent ALIASES.
//
// CASCADE ALIASES is not optional when constituents exist — a plain drop is
// refused:
//
//	42N82: The database identified by `x` has one or more aliases.
//	       Drop the aliases `x.one` and `x.two` before dropping the database.
//
// It removes only the aliases. The databases they target are untouched, which
// is verified: after a CASCADE drop the targets remain in SHOW DATABASES as
// standard databases.
//
// IF EXISTS works here even though the Neo4j documentation page for deleting
// composite databases does not show it — confirmed on both supported lines,
// including against a database that does not exist.
func (c *Client) DropCompositeDatabase(ctx context.Context, name string, cascadeAliases bool) error {
	query := fmt.Sprintf("DROP COMPOSITE DATABASE `%s` IF EXISTS", escapeBackticks(name))
	if cascadeAliases {
		query += " CASCADE ALIASES"
	}
	return c.runSystemWrite(ctx, query, "drop composite database")
}

// CreateCompositeConstituent creates one constituent alias inside a
// composite's namespace.
//
// ORDER MATTERS, and getting it wrong is unrecoverable without manual
// cleanup. Creating `<composite>.<name>` BEFORE the composite exists SUCCEEDS
// — it silently becomes an ordinary local alias whose name merely contains a
// dot, listed by SHOW ALIASES and even queryable. But the composite can then
// never be created:
//
//	42N87: The database or alias name `x` conflicts with the name `x.y`
//	       of an existing database or alias.
//
// Callers must therefore create the composite first. The controller owns both,
// which is what makes that guaranteed rather than hoped for.
func (c *Client) CreateCompositeConstituent(ctx context.Context, composite, constituent, targetDatabase string) error {
	query := fmt.Sprintf("CREATE ALIAS `%s`.`%s` IF NOT EXISTS FOR DATABASE `%s`",
		escapeBackticks(composite), escapeBackticks(constituent), escapeBackticks(targetDatabase))
	return c.runSystemWrite(ctx, query, "create composite constituent")
}

// AlterCompositeConstituent re-points an existing constituent at a different
// target database.
func (c *Client) AlterCompositeConstituent(ctx context.Context, composite, constituent, targetDatabase string) error {
	query := fmt.Sprintf("ALTER ALIAS `%s`.`%s` SET DATABASE TARGET `%s`",
		escapeBackticks(composite), escapeBackticks(constituent), escapeBackticks(targetDatabase))
	return c.runSystemWrite(ctx, query, "alter composite constituent")
}

// DropCompositeConstituent removes one constituent alias, leaving its target
// database alone.
func (c *Client) DropCompositeConstituent(ctx context.Context, composite, constituent string) error {
	query := fmt.Sprintf("DROP ALIAS `%s`.`%s` IF EXISTS FOR DATABASE",
		escapeBackticks(composite), escapeBackticks(constituent))
	return c.runSystemWrite(ctx, query, "drop composite constituent")
}

// CompositeInfo is what the server reports about a composite database.
type CompositeInfo struct {
	Name string
	// Constituents are fully qualified (`<composite>.<name>`), exactly as
	// SHOW DATABASE returns them.
	Constituents    []string
	CurrentStatus   string
	RequestedStatus string
}

// ShowCompositeDatabase reads a composite's live state, or nil when no
// database of that name exists.
//
// Returns an error when the name exists but is NOT a composite: the caller is
// about to treat it as one, and a standard database answering to the same name
// means someone's spec disagrees with reality in a way no reconcile should
// paper over.
func (c *Client) ShowCompositeDatabase(ctx context.Context, name string) (*CompositeInfo, error) {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeRead,
		DatabaseName: "system",
	})
	defer c.closeSession(ctx, session)

	result, err := session.Run(ctx,
		"SHOW DATABASE $name YIELD name, type, constituents, currentStatus, requestedStatus "+
			"RETURN name, type, constituents, currentStatus, requestedStatus",
		map[string]any{"name": name})
	if err != nil {
		return nil, fmt.Errorf("failed to show composite database %q: %w", name, err)
	}

	// SHOW DATABASE returns one row PER SERVER hosting the database. Every row
	// describes the same database — same type, same constituents — so the
	// first is sufficient and the rest are the same answer again.
	if result.Next(ctx) {
		rec := result.Record()
		gotType, _ := rec.Get("type")
		typeStr, _ := gotType.(string)
		if typeStr != CompositeDatabaseType {
			return nil, fmt.Errorf(
				"database %q exists but its type is %q, not %q — refusing to manage it as a composite",
				name, typeStr, CompositeDatabaseType)
		}
		info := &CompositeInfo{Name: name}
		if v, ok := rec.Get("currentStatus"); ok {
			info.CurrentStatus, _ = v.(string)
		}
		if v, ok := rec.Get("requestedStatus"); ok {
			info.RequestedStatus, _ = v.(string)
		}
		if v, ok := rec.Get("constituents"); ok {
			if list, ok := v.([]any); ok {
				for _, item := range list {
					if s, ok := item.(string); ok {
						info.Constituents = append(info.Constituents, s)
					}
				}
			}
		}
		return info, nil
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("failed to read composite database %q: %w", name, err)
	}
	return nil, nil
}

// QualifyConstituent is how a constituent is addressed everywhere — in SHOW
// DATABASE output, in a USE clause, and in a privilege.
func QualifyConstituent(composite, constituent string) string {
	return composite + "." + constituent
}

// UnqualifyConstituent is the inverse: it strips the composite prefix that the
// server returns, so an observed list can be compared with spec.
func UnqualifyConstituent(composite, qualified string) string {
	return strings.TrimPrefix(qualified, composite+".")
}

// runSystemWrite executes one admin statement against the system database.
func (c *Client) runSystemWrite(ctx context.Context, query, label string) error {
	session := c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: "system",
	})
	defer c.closeSession(ctx, session)

	if _, err := session.Run(ctx, query, nil); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}
