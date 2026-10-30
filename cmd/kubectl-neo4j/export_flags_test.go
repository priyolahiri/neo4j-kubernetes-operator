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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runExportCaptured runs `kubectl neo4j export` with args and returns what it
// wrote to stdout and stderr (lower-cased), and its exit code.
func runExportCaptured(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	out, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	stderr = captureStderr(t, func(f *os.File) {
		code = runExport(args, out, f)
	})
	require.NoError(t, out.Close())
	b, err := os.ReadFile(out.Name())
	require.NoError(t, err)
	return string(b), stderr, code
}

// #619 S6: `kubectl neo4j export --context X replica-database …` exited 2 with
// `unknown export target "--context"`, while validate and preflight take their
// flags in any position. The flags before the target are now the target's, as
// they are after it.
func TestExport_FlagsBeforeTheTarget(t *testing.T) {
	// A kubeconfig with one context, "real": a --context that names another
	// fails in the client's construction, naming it — which proves the value
	// reached the replica-database parse.
	kubeconfig := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: real
  cluster: {server: "https://127.0.0.1:1"}
users:
- name: real
  user: {token: "x"}
contexts:
- name: real
  context: {cluster: real, user: real}
current-context: real
`), 0o600))
	required := []string{"--from-backup", "nightly", "--cluster-ref", "dr", "--upstream-database", "neo4j"}

	t.Run("--context and --kubeconfig before the target are the target's", func(t *testing.T) {
		args := append([]string{"--context", "other", "--kubeconfig", kubeconfig, "replica-database", "dr-copy"}, required...)
		_, stderr, code := runExportCaptured(t, args...)
		assert.NotContains(t, stderr, "unknown export target")
		assert.Contains(t, stderr, "could not connect to the cluster")
		assert.Contains(t, stderr, "other", "the --context given before the target is the one the client was built with")
		assert.Equal(t, exitUsage, code)
	})

	t.Run("-n, a value flag and a boolean flag before the target", func(t *testing.T) {
		_, stderr, code := runExportCaptured(t, "-n", "prod", "--seed-from-latest", "--cluster-ref", "dr", "replica-database", "dr-copy")
		assert.NotContains(t, stderr, "unknown export target")
		assert.Contains(t, stderr, "one of --from-backup or --from-cluster is required",
			"the target's own checks ran: -n and --cluster-ref took their values, --seed-from-latest took none")
		assert.Equal(t, exitUsage, code)
	})

	t.Run("a flag's value is never the target", func(t *testing.T) {
		_, stderr, code := runExportCaptured(t, "--context", "replica-database")
		assert.NotContains(t, stderr, "unknown export target")
		assert.Contains(t, stderr, "usage:", "no target was given: the export usage")
		assert.Equal(t, exitUsage, code)
	})

	t.Run("an unknown target after flags is named", func(t *testing.T) {
		_, stderr, code := runExportCaptured(t, "--context", "dr", "replica-databse", "x")
		assert.Contains(t, stderr, `unknown export target "replica-databse"`)
		assert.Equal(t, exitUsage, code)
	})

	t.Run("help before a target", func(t *testing.T) {
		stdout, _, code := runExportCaptured(t, "--context", "dr", "-h")
		assert.Contains(t, stdout, "kubectl neo4j export replica-database")
		assert.Equal(t, exitOK, code)
	})

	t.Run("the target first still works", func(t *testing.T) {
		args := append([]string{"replica-database", "dr-copy", "--context", "other", "--kubeconfig", kubeconfig}, required...)
		_, stderr, code := runExportCaptured(t, args...)
		assert.Contains(t, stderr, "could not connect to the cluster")
		assert.Contains(t, stderr, "other")
		assert.Equal(t, exitUsage, code)
	})
}
