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
