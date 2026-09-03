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

package main

import (
	"flag"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceFlag_BothFormsBindTheSameVariable(t *testing.T) {
	for _, arg := range []string{"-n", "--namespace", "-namespace"} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		ns := namespaceFlag(fs, "usage")
		require.NoError(t, fs.Parse([]string{arg, "neo4j"}), arg)
		assert.Equal(t, "neo4j", *ns, "%s must set the namespace", arg)
	}
}

// Regression guard. Every command's usage text has always read "-n <namespace>",
// but the short form was never registered, so the documented invocation was
// answered with "flag provided but not defined: -n" and a help dump. The check
// is per command because each builds its own FlagSet.
func TestEveryCommandAcceptsShortNamespaceFlag(t *testing.T) {
	commands := map[string]func([]string, *os.File, *os.File) int{
		"status":         runStatus,
		"diagnose":       runDiagnose,
		"preflight":      runPreflight,
		"explain":        runExplain,
		"connect":        runConnect,
		"cypher":         runCypher,
		"support-bundle": runSupportBundle,
		"validate":       runValidate,
	}

	for name, run := range commands {
		t.Run(name, func(t *testing.T) {
			stderr := captureStderr(t, func(f *os.File) {
				// The command will fail on something else — no cluster, no
				// input file — and that is fine. All this asserts is that it
				// got PAST flag parsing.
				run([]string{"-n", "neo4j"}, f, f)
			})
			assert.NotContains(t, stderr, "flag provided but not defined",
				"%s does not accept -n", name)
		})
	}
}

func TestExportAcceptsShortNamespaceFlag(t *testing.T) {
	stderr := captureStderr(t, func(f *os.File) {
		runExport([]string{"replica-database", "dr", "-n", "neo4j"}, f, f)
	})
	assert.NotContains(t, stderr, "flag provided but not defined")
}

func captureStderr(t *testing.T, fn func(*os.File)) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "err")
	require.NoError(t, err)
	fn(f)
	require.NoError(t, f.Close())
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	return strings.ToLower(string(b))
}

// kubectl accepts flags in any position and so must this CLI: the standard
// library's flag package stops at the first non-flag argument, which meant
// `diagnose Kind/name -n prod` silently searched the DEFAULT namespace and
// then reported "not found" — indistinguishable from the resource being
// absent. Every documented example in docs/user_guide/cli/ puts -n last.
func TestFlagsParseAfterPositionalArguments(t *testing.T) {
	newFS := func() (*flag.FlagSet, *string, *bool, *string) {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		ns := namespaceFlag(fs, "namespace")
		quiet := fs.Bool("quiet", false, "quiet")
		from := fs.String("from-backup", "", "from")
		return fs, ns, quiet, from
	}

	t.Run("short flag after a positional", func(t *testing.T) {
		fs, ns, _, _ := newFS()
		require.NoError(t, parseFlags(fs, []string{"Neo4jEnterpriseCluster/prod", "-n", "prod-ns"}))
		assert.Equal(t, "prod-ns", *ns)
		assert.Equal(t, []string{"Neo4jEnterpriseCluster/prod"}, fs.Args())
	})

	t.Run("long flag and bool interleaved with positionals", func(t *testing.T) {
		fs, ns, quiet, _ := newFS()
		require.NoError(t, parseFlags(fs, []string{"a", "--namespace", "x", "b", "--quiet"}))
		assert.Equal(t, "x", *ns)
		assert.True(t, *quiet)
		assert.Equal(t, []string{"a", "b"}, fs.Args())
	})

	t.Run("export's documented form", func(t *testing.T) {
		// docs/user_guide/cli/export.md shows every invocation this way; before
		// the fix it failed with "one of --from-backup or --from-cluster is
		// required" because the flags were never parsed at all.
		fs, ns, _, from := newFS()
		require.NoError(t, parseFlags(fs, []string{"replica-database", "dr-copy", "--from-backup", "nightly", "-n", "neo4j"}))
		assert.Equal(t, "nightly", *from)
		assert.Equal(t, "neo4j", *ns)
		assert.Equal(t, []string{"replica-database", "dr-copy"}, fs.Args())
	})

	t.Run("flag=value form needs no lookahead", func(t *testing.T) {
		fs, ns, _, _ := newFS()
		require.NoError(t, parseFlags(fs, []string{"pos", "--namespace=inline"}))
		assert.Equal(t, "inline", *ns)
		assert.Equal(t, []string{"pos"}, fs.Args())
	})

	t.Run("everything after -- stays positional", func(t *testing.T) {
		fs, ns, _, _ := newFS()
		require.NoError(t, parseFlags(fs, []string{"-n", "ns", "--", "-n", "not-a-flag"}))
		assert.Equal(t, "ns", *ns)
		assert.Equal(t, []string{"-n", "not-a-flag"}, fs.Args())
	})

	t.Run("an unknown flag is still an error, not a silent drop", func(t *testing.T) {
		fs, _, _, _ := newFS()
		assert.Error(t, parseFlags(fs, []string{"pos", "--nope", "x"}))
	})
}
