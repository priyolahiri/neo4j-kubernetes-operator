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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// runRetentionScript executes the generated retention script against dir in
// place of /backup/<chain>. The script targets busybox (stat -c); on macOS a
// PATH shim translates that to BSD stat so the test runs on a laptop too —
// find -exec resolves stat through PATH, so a shell function would not do.
func runRetentionScript(t *testing.T, policy *neo4jv1beta1.RetentionPolicy, dir string) {
	t.Helper()
	script := buildRetentionScript(policy, "chain")
	script = strings.Replace(script, "BACKUP_DIR='/backup/chain'", "BACKUP_DIR='"+dir+"'", 1)
	require.Contains(t, script, "BACKUP_DIR='"+dir+"'")

	cmd := exec.CommandContext(t.Context(), "sh", "-c", script)
	cmd.Env = os.Environ()
	if runtime.GOOS == "darwin" {
		shim := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(shim, "stat"), []byte(`#!/bin/sh
if [ "$1" = "-c" ]; then shift 2; exec /usr/bin/stat -f '%m %N' "$@"; fi
exec /usr/bin/stat "$@"
`), 0o755))
		cmd.Env = append(cmd.Env, "PATH="+shim+":"+os.Getenv("PATH"))
	}
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "retention script failed:\n%s", out)
}

// writeArtifact creates one artifact, ageDays old. A split archive
// (parts > 0) is name.backup plus name.backup.1 … name.backup.<parts>.
func writeArtifact(t *testing.T, dir, name string, parts int, ageDays int) {
	t.Helper()
	files := []string{name + ".backup"}
	for i := 1; i <= parts; i++ {
		files = append(files, name+".backup."+strconv.Itoa(i))
	}
	mtime := time.Now().Add(-time.Duration(ageDays) * 24 * time.Hour)
	for _, f := range files {
		p := filepath.Join(dir, f)
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
		require.NoError(t, os.Chtimes(p, mtime, mtime))
	}
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// A split archive is x.backup (metadata) plus x.backup.1…N (data). Retention
// selected `*.backup` and deleted only that, stranding every part on the PVC.
func TestRetentionScript_MaxCountDeletesSplitParts(t *testing.T) {
	dir := t.TempDir()
	writeArtifact(t, dir, "neo4j-2026-10-01T00-00-00", 2, 4) // oldest, split
	writeArtifact(t, dir, "neo4j-2026-10-02T00-00-00", 0, 3) // unsplit
	writeArtifact(t, dir, "neo4j-2026-10-03T00-00-00", 3, 2) // split
	writeArtifact(t, dir, "neo4j-2026-10-04T00-00-00", 1, 1) // newest, split
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("keep"), 0o600))

	runRetentionScript(t, &neo4jv1beta1.RetentionPolicy{MaxCount: 2}, dir)

	require.Equal(t, []string{
		"neo4j-2026-10-03T00-00-00.backup",
		"neo4j-2026-10-03T00-00-00.backup.1",
		"neo4j-2026-10-03T00-00-00.backup.2",
		"neo4j-2026-10-03T00-00-00.backup.3",
		"neo4j-2026-10-04T00-00-00.backup",
		"neo4j-2026-10-04T00-00-00.backup.1",
		"notes.txt",
	}, listDir(t, dir), "the two oldest artifacts go with all their parts; parts count as part of one artifact, not as artifacts")
}

func TestRetentionScript_MaxAgeDeletesSplitPartsAndKeepsNewest(t *testing.T) {
	dir := t.TempDir()
	writeArtifact(t, dir, "neo4j-2026-09-01T00-00-00", 2, 30)
	writeArtifact(t, dir, "neo4j-2026-09-10T00-00-00", 2, 20)
	// Every artifact is older than maxAge: the newest must still survive,
	// with its parts — an artifact without its parts cannot be restored.
	runRetentionScript(t, &neo4jv1beta1.RetentionPolicy{MaxAge: "7d"}, dir)

	require.Equal(t, []string{
		"neo4j-2026-09-10T00-00-00.backup",
		"neo4j-2026-09-10T00-00-00.backup.1",
		"neo4j-2026-09-10T00-00-00.backup.2",
	}, listDir(t, dir))
}
