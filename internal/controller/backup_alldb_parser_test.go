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

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// TestParseAllDatabaseArtifactsFromLog verifies the per-database artifact map
// the all-databases backup records for cluster-wide restore (#222): one entry
// per user database, shard physical databases excluded, last-occurrence wins.
func TestParseAllDatabaseArtifactsFromLog(t *testing.T) {
	log := `
Backup of database 'neo4j' completed, written to /backups/neo4j-2026-06-08T01-18-06.backup
Backup of database 'customers' completed, written to /backups/customers-2026-06-08T01-19-12.backup
Backup of database 'products-g000' completed, written to /backups/products-g000-2026-06-08T01-20-00.backup
Backup of database 'products-p000' completed, written to /backups/products-p000-2026-06-08T01-20-30.backup
re-run: customers written to /backups/customers-2026-06-08T02-00-00.backup
`
	got := parseAllDatabaseArtifactsFromLog(log)

	want := map[string]string{
		"neo4j":     "neo4j-2026-06-08T01-18-06.backup",
		"customers": "customers-2026-06-08T02-00-00.backup", // last occurrence wins
	}
	if len(got) != len(want) {
		t.Fatalf("got %d artifacts %+v, want %d (%v)", len(got), got, len(want), want)
	}
	for _, a := range got {
		w, ok := want[a.Database]
		if !ok {
			t.Errorf("unexpected database %q (shard databases must be excluded)", a.Database)
			continue
		}
		if a.Filename != w {
			t.Errorf("database %q: filename = %q, want %q", a.Database, a.Filename, w)
		}
	}
}

// TestParseAllDatabaseArtifactsFromLog_Empty ensures a garbled/empty log is
// non-fatal and yields no artifacts.
func TestParseAllDatabaseArtifactsFromLog_Empty(t *testing.T) {
	if got := parseAllDatabaseArtifactsFromLog(""); len(got) != 0 {
		t.Fatalf("expected no artifacts for empty log, got %+v", got)
	}
	if got := parseAllDatabaseArtifactsFromLog("no backup files here"); len(got) != 0 {
		t.Fatalf("expected no artifacts for non-matching log, got %+v", got)
	}
}

// The type lines are neo4j-admin's own (BackupOutputMonitor), as a 5.26
// Job logged them: a full, a differential, an AUTO differential that fell
// back to a full, and a differential with nothing new to fetch.
func TestParseArtifactTypesFromLog(t *testing.T) {
	log := `
INFO  [c.n.b.b.BackupOutputMonitor] Start full backup of database 'other'.
INFO  [c.n.b.b.BackupOutputMonitor] Finished full backup of database 'other'. Downloaded from tx -1 to tx 5.
INFO  [c.n.b.b.BackupOutputMonitor] Start differential backup of database 'neo4j'.
INFO  [c.n.b.b.BackupOutputMonitor] Finished differential backup of database 'neo4j'.
INFO  [c.n.b.b.BackupOutputMonitor] Start differential backup of database 'fellback'.
INFO  [c.n.b.b.BackupOutputMonitor] Differential backup of database 'fellback' failed. Reason: Differential backups require that a full backup of the same database exists in the folder defined in --to-path.
INFO  [c.n.b.b.BackupOutputMonitor] Falling back to full backup of database 'fellback'.
INFO  [c.n.b.b.BackupOutputMonitor] Start full backup of database 'fellback'.
INFO  [c.n.b.b.BackupOutputMonitor] Start differential backup of database 'idle'.
INFO  [c.n.b.b.BackupOutputMonitor] The remote server (s-0:6362) has not any recent data for database 'DatabaseId{c16ce93f[idle]}'.
INFO  [c.n.b.b.BackupOutputMonitor] Finished artifact creation 'idle-2026-10-06T07-36-03.backup' for database 'idle', took 38ms.
`
	want := map[string]string{"other": "FULL", "neo4j": "DIFF", "fellback": "FULL", "idle": "DIFF"}
	got := parseArtifactTypesFromLog(log)
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for db, w := range want {
		if got[db] != w {
			t.Errorf("database %q: type = %q, want %q", db, got[db], w)
		}
	}
}

func TestRecordArtifactTypes(t *testing.T) {
	log := "Start full backup of database 'neo4j'.\nStart differential backup of database 'other'.\n"
	run := neo4jv1beta1.BackupRun{
		ArtifactFilename: "neo4j-1.backup",
		DatabaseArtifacts: []neo4jv1beta1.DatabaseArtifact{
			{Database: "neo4j", Filename: "neo4j-1.backup"},
			{Database: "other", Filename: "other-1.backup"},
			{Database: "silent", Filename: "silent-1.backup"},
		},
	}
	recordArtifactTypes(&run, log, "neo4j")
	if run.ArtifactType != "FULL" {
		t.Errorf("ArtifactType = %q, want FULL", run.ArtifactType)
	}
	for db, w := range map[string]string{"neo4j": "FULL", "other": "DIFF", "silent": ""} {
		for _, a := range run.DatabaseArtifacts {
			if a.Database == db && a.Type != w {
				t.Errorf("database %q: Type = %q, want %q", db, a.Type, w)
			}
		}
	}
	// No artifact recorded, no type: a type alone would name nothing.
	bare := neo4jv1beta1.BackupRun{}
	recordArtifactTypes(&bare, log, "neo4j")
	if bare.ArtifactType != "" {
		t.Errorf("ArtifactType = %q without an ArtifactFilename, want empty", bare.ArtifactType)
	}
}

// TestParseShardedFamiliesExcludedFromLog verifies the sibling parser surfaces
// the distinct logical sharded databases (e.g. "products") whose shard physical
// databases (…-g000/…-pNNN) appear in an all-databases backup log — the
// families an all-databases restore cannot recreate. Graph + property shards
// collapse to one logical family.
func TestParseShardedFamiliesExcludedFromLog(t *testing.T) {
	log := `
Backup of database 'neo4j' completed, written to /backups/neo4j-2026-06-08T01-18-06.backup
Backup of database 'customers' completed, written to /backups/customers-2026-06-08T01-19-12.backup
Backup of database 'products-g000' completed, written to /backups/products-g000-2026-06-08T01-20-00.backup
Backup of database 'products-p000' completed, written to /backups/products-p000-2026-06-08T01-20-30.backup
Backup of database 'products-p001' completed, written to /backups/products-p001-2026-06-08T01-20-45.backup
`
	got := parseShardedFamiliesExcludedFromLog(log)
	if len(got) != 1 || got[0] != "products" {
		t.Fatalf("got %v, want [products] (graph + property shards collapse to one logical family)", got)
	}

	// Standard-only log → no excluded families.
	if g := parseShardedFamiliesExcludedFromLog("Backup of database 'neo4j' completed, written to /backups/neo4j-2026-06-08T01-18-06.backup"); len(g) != 0 {
		t.Fatalf("expected no excluded families for standard-only log, got %v", g)
	}
}

// TestGroupShardedFamiliesFromLog verifies an all-databases backup log is
// grouped into per-family shard-artifact sets (BackupRun.ShardedFamilies), so
// each family is restorable from the single backup. Families and shards are
// returned sorted, with per-shard filenames captured.
func TestGroupShardedFamiliesFromLog(t *testing.T) {
	log := `
Backup of database 'neo4j' completed, written to /backups/neo4j-2026-06-08T01-18-06.backup
Backup of database 'products-g000' completed, written to /backups/products-g000-2026-06-08T01-20-00.backup
Backup of database 'products-p000' completed, written to /backups/products-p000-2026-06-08T01-20-30.backup
Backup of database 'products-p001' completed, written to /backups/products-p001-2026-06-08T01-20-45.backup
Backup of database 'orders-g000' completed, written to /backups/orders-g000-2026-06-08T01-21-00.backup
Backup of database 'orders-p000' completed, written to /backups/orders-p000-2026-06-08T01-21-30.backup
`
	got := groupShardedFamiliesFromLog(log)
	if len(got) != 2 {
		t.Fatalf("got %d families %+v, want 2", len(got), got)
	}
	// Sorted: orders before products.
	if got[0].Family != "orders" || got[1].Family != "products" {
		t.Fatalf("families not sorted: got %q,%q want orders,products", got[0].Family, got[1].Family)
	}
	if len(got[0].ShardArtifacts) != 2 {
		t.Errorf("orders: got %d shards, want 2", len(got[0].ShardArtifacts))
	}
	if len(got[1].ShardArtifacts) != 3 {
		t.Errorf("products: got %d shards, want 3", len(got[1].ShardArtifacts))
	}
	// Shards sorted within family, filenames captured.
	if got[1].ShardArtifacts[0].ShardName != "products-g000" ||
		got[1].ShardArtifacts[0].Filename != "products-g000-2026-06-08T01-20-00.backup" {
		t.Errorf("products first shard wrong: %+v", got[1].ShardArtifacts[0])
	}

	// Standard-only / empty → nil.
	if g := groupShardedFamiliesFromLog("Backup of database 'neo4j' completed, written to /backups/neo4j-2026-06-08T01-18-06.backup"); len(g) != 0 {
		t.Fatalf("expected no families for standard-only log, got %v", g)
	}
	if g := groupShardedFamiliesFromLog(""); len(g) != 0 {
		t.Fatalf("expected no families for empty log, got %v", g)
	}
}
