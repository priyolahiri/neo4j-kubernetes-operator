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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// TestClusterDiagnosticsWriteNeeded pins #475: a diagnostics pass that found
// nothing new writes nothing — a fresh lastCollected alone would re-enqueue
// the cluster at once — while any real change, or a lastCollected older than
// the refresh interval, is written.
func TestClusterDiagnosticsWriteNeeded(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	status := func(collected time.Time, health string, cond metav1.ConditionStatus) *neo4jv1beta1.Neo4jEnterpriseClusterStatus {
		at := metav1.NewTime(collected)
		return &neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			Diagnostics: &neo4jv1beta1.ClusterDiagnosticsStatus{
				Servers:       []neo4jv1beta1.ServerDiagnosticInfo{{Name: "s0", State: "Enabled", Health: health}},
				LastCollected: &at,
			},
			Conditions: []metav1.Condition{{Type: ConditionTypeServersHealthy, Status: cond, Reason: "r"}},
		}
	}
	stored := status(now.Add(-time.Minute), "Available", metav1.ConditionTrue)

	if clusterDiagnosticsWriteNeeded(stored, status(now, "Available", metav1.ConditionTrue), now) {
		t.Error("nothing changed but lastCollected: no write")
	}
	if !clusterDiagnosticsWriteNeeded(stored, status(now, "Unavailable", metav1.ConditionTrue), now) {
		t.Error("a server's health changed: write")
	}
	if !clusterDiagnosticsWriteNeeded(stored, status(now, "Available", metav1.ConditionFalse), now) {
		t.Error("a condition changed: write")
	}
	old := status(now.Add(-diagnosticsRefreshInterval), "Available", metav1.ConditionTrue)
	if !clusterDiagnosticsWriteNeeded(old, status(now, "Available", metav1.ConditionTrue), now) {
		t.Error("lastCollected is stale: write to refresh it")
	}
	if !clusterDiagnosticsWriteNeeded(&neo4jv1beta1.Neo4jEnterpriseClusterStatus{}, status(now, "Available", metav1.ConditionTrue), now) {
		t.Error("first collection: write")
	}
}

func TestStandaloneDiagnosticsWriteNeeded(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	diag := func(collected time.Time, status string) *neo4jv1beta1.StandaloneDiagnosticsStatus {
		at := metav1.NewTime(collected)
		return &neo4jv1beta1.StandaloneDiagnosticsStatus{
			Databases:     []neo4jv1beta1.DatabaseDiagnosticInfo{{Name: "neo4j", Status: status}},
			LastCollected: &at,
		}
	}
	stored := diag(now.Add(-time.Minute), "online")
	if standaloneDiagnosticsWriteNeeded(stored, diag(now, "online"), now) {
		t.Error("nothing changed but lastCollected: no write")
	}
	if !standaloneDiagnosticsWriteNeeded(stored, diag(now, "offline"), now) {
		t.Error("a database changed: write")
	}
	if !standaloneDiagnosticsWriteNeeded(diag(now.Add(-diagnosticsRefreshInterval), "online"), diag(now, "online"), now) {
		t.Error("stale: write")
	}
	if !standaloneDiagnosticsWriteNeeded(nil, diag(now, "online"), now) {
		t.Error("first collection: write")
	}
}

// TestDiagnosticsWriteNeeded_IgnoresRowOrderAndTxnCounters: SHOW DATABASES rows
// come back in no stable order and their transaction counters move with every
// write; neither alone is a change to write (#475, found live: the writer flag
// moved between rows on every pass).
func TestDiagnosticsWriteNeeded_IgnoresRowOrderAndTxnCounters(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	rows := func(order []int, txn int64) []neo4jv1beta1.DatabaseDiagnosticInfo {
		all := []neo4jv1beta1.DatabaseDiagnosticInfo{
			{Name: "system", Role: "primary", Status: "online", Writer: true, LastCommittedTxn: txn},
			{Name: "system", Role: "primary", Status: "online", LastCommittedTxn: txn},
			{Name: "neo4j", Role: "primary", Status: "online", Writer: true, LastCommittedTxn: txn},
		}
		var out []neo4jv1beta1.DatabaseDiagnosticInfo
		for _, i := range order {
			out = append(out, all[i])
		}
		return out
	}
	at := metav1.NewTime(now.Add(-time.Minute))
	stored := &neo4jv1beta1.Neo4jEnterpriseClusterStatus{Diagnostics: &neo4jv1beta1.ClusterDiagnosticsStatus{
		Databases: rows([]int{0, 1, 2}, 10), LastCollected: &at}}
	fresh := metav1.NewTime(now)
	shuffled := &neo4jv1beta1.Neo4jEnterpriseClusterStatus{Diagnostics: &neo4jv1beta1.ClusterDiagnosticsStatus{
		Databases: rows([]int{2, 1, 0}, 99), LastCollected: &fresh}}
	if clusterDiagnosticsWriteNeeded(stored, shuffled, now) {
		t.Error("the same rows in another order with moved counters must not be written")
	}
	sa := &neo4jv1beta1.StandaloneDiagnosticsStatus{Databases: stored.Diagnostics.Databases, LastCollected: &at}
	sb := &neo4jv1beta1.StandaloneDiagnosticsStatus{Databases: shuffled.Diagnostics.Databases, LastCollected: &fresh}
	if standaloneDiagnosticsWriteNeeded(sa, sb, now) {
		t.Error("standalone: the same rows in another order with moved counters must not be written")
	}

	sorted := toDatabaseDiagnostics(nil)
	if sorted != nil {
		t.Error("no rows map to nil")
	}
	a, b := rows([]int{2, 0, 1}, 1), rows([]int{1, 2, 0}, 1)
	sortDatabaseDiagnostics(a)
	sortDatabaseDiagnostics(b)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("sorting must give one order whatever the input order: %v vs %v", a, b)
		}
	}
}
