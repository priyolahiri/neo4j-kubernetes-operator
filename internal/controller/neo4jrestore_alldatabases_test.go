/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"testing"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

func TestUserDatabasesFromArtifacts_ExcludesSystem(t *testing.T) {
	arts := []neo4jv1beta1.DatabaseArtifact{
		{Database: "neo4j", Filename: "neo4j-t.backup"},
		{Database: "system", Filename: "system-t.backup"},
		{Database: "customers", Filename: "customers-t.backup"},
	}
	got := userDatabasesFromArtifacts(arts)
	want := []string{"neo4j", "customers"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestFilenameForDB(t *testing.T) {
	arts := []neo4jv1beta1.DatabaseArtifact{
		{Database: "neo4j", Filename: "neo4j-t.backup"},
		{Database: "customers", Filename: "customers-t.backup"},
	}
	if got := filenameForDB(arts, "customers"); got != "customers-t.backup" {
		t.Errorf("filenameForDB(customers) = %q, want customers-t.backup", got)
	}
	if got := filenameForDB(arts, "missing"); got != "" {
		t.Errorf("filenameForDB(missing) = %q, want empty", got)
	}
}

func TestEnsureDatabaseResults_SeedsAndIsIdempotent(t *testing.T) {
	r := &Neo4jRestoreReconciler{}
	restore := &neo4jv1beta1.Neo4jRestore{}

	r.ensureDatabaseResults(restore, []string{"neo4j", "customers"})
	if len(restore.Status.DatabaseResults) != 2 {
		t.Fatalf("expected 2 results, got %d", len(restore.Status.DatabaseResults))
	}
	for i := range restore.Status.DatabaseResults {
		if restore.Status.DatabaseResults[i].Phase != StatusPending {
			t.Errorf("result %d phase = %q, want Pending", i, restore.Status.DatabaseResults[i].Phase)
		}
	}

	// Mark one done, then re-run: existing results must be preserved (no reset,
	// no duplicates) and a newly-discovered DB appended.
	restore.Status.DatabaseResults[0].Phase = StatusCompleted
	r.ensureDatabaseResults(restore, []string{"neo4j", "customers", "orders"})
	if len(restore.Status.DatabaseResults) != 3 {
		t.Fatalf("expected 3 results after re-run, got %d", len(restore.Status.DatabaseResults))
	}
	if restore.Status.DatabaseResults[0].Phase != StatusCompleted {
		t.Errorf("existing Completed result was reset to %q", restore.Status.DatabaseResults[0].Phase)
	}
}
