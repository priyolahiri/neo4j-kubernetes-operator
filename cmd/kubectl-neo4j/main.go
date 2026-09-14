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

// Command kubectl-neo4j is a kubectl plugin for the Neo4j Kubernetes Operator.
//
// Installed on PATH as `kubectl-neo4j`, it is invoked as `kubectl neo4j <cmd>`;
// it also runs standalone. See docs/design/cli-tool.md for the design and the
// rule that governs what may be added here:
//
//	The CLI may create and read Custom Resources and read Kubernetes state.
//	It must NEVER execute Cypher or neo4j-admin for a mutating operation.
//
// The point of this binary is that it reuses the operator's OWN packages —
// internal/validation, internal/resources, api/v1beta1 — rather than restating
// their rules. Anything that would require copying a rule out of the operator
// is a signal to export it there, not to duplicate it here.
package main

import (
	"fmt"
	"os"
	"runtime/debug"
)

// version is what this binary reports, and the single value every command
// reads — `--version`, the support bundle's metadata, explain's "carries %s
// rules" footer, and the version-skew check in validate.
//
// It is deliberately reported in validate's output: offline validation is only
// ever authoritative for the operator release it was built from, and output
// that does not say which release it speaks for invites silent version skew.
//
// `make build-cli` and the release build stamp it with
// -ldflags "-X main.version=vX.Y.Z". `go install <module>@vX.Y.Z` cannot —
// there is no way for a module to request ldflags — so for that path it is
// resolved from the build info Go embeds instead. See resolveVersion.
var version = "dev"

func init() {
	version = resolveVersion(version, debug.ReadBuildInfo)
}

// resolveVersion decides what this binary reports as its version.
//
// The ldflags value wins whenever it is present. Otherwise the module version
// Go embeds is used, which is what makes `go install <module>@vX.Y.Z` report a
// real version — the documented install path, which until now produced a
// binary reporting "dev".
//
// That was not cosmetic: warnOnVersionSkew returns early on "dev", so every
// user who installed the documented way had skew detection silently disabled —
// precisely the case it exists for.
//
// What a LOCAL build reports is worth knowing, because it is not "dev". With
// VCS info available Go derives Main.Version from the repository, so a build
// from this tree reports e.g. "v1.15.0+dirty" (tag plus the vcs.modified
// flag), and one from a commit past a tag reports a pseudo-version. Those
// flow through to the skew check, and they should: a modified or mid-branch
// tree genuinely does not carry a release's rules, so warning is the correct
// answer. The previous blanket "dev" suppressed that warning for every local
// build, including ones that had really diverged.
//
// "(devel)" — what Go reports when VCS info is unavailable, e.g. under
// -buildvcs=false or when building outside a repository — still maps to "dev".
// There is nothing to compare in that case.
func resolveVersion(stamped string, readBuildInfo func() (*debug.BuildInfo, bool)) string {
	if stamped != "dev" {
		return stamped
	}
	info, ok := readBuildInfo()
	if !ok || info == nil {
		return "dev"
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	return "dev"
}

const usage = `kubectl-neo4j — CLI for the Neo4j Kubernetes Operator

Usage:
  kubectl neo4j <command> [flags]

Commands:
  validate    Validate Neo4j CR manifests against the operator's own validators
  preflight   Check the cluster-side preconditions a manifest depends on
  status      Show the state of every Neo4j resource in a namespace
  diagnose    Explain WHY a resource is unhealthy, at the Kubernetes level
  connect     Print how to reach a deployment (address, port-forward, scheme)
  cypher      Open a cypher-shell session against a deployment
  support-bundle  Collect a redacted diagnostic archive
  explain     Explain a status condition or phase, and what to do about it
  export      Author a downstream manifest from an upstream resource's status
  version     Print the version

Run "kubectl neo4j <command> -h" for command flags.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main's testable core: argv in, exit code out, no os.Exit.
func run(args []string, stdout, stderr *os.File) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}

	switch args[0] {
	case "validate":
		return runValidate(args[1:], stdout, stderr)
	case "preflight":
		return runPreflight(args[1:], stdout, stderr)
	case "status":
		return runStatus(args[1:], stdout, stderr)
	case "diagnose":
		return runDiagnose(args[1:], stdout, stderr)
	case "connect":
		return runConnect(args[1:], stdout, stderr)
	case "cypher":
		return runCypher(args[1:], stdout, stderr)
	case "support-bundle":
		return runSupportBundle(args[1:], stdout, stderr)
	case "explain":
		return runExplain(args[1:], stdout, stderr)
	case "export":
		return runExport(args[1:], stdout, stderr)
	// --version is muscle memory, and this CLI's whole version-skew story rests
	// on users being able to read their own version, so accept both spellings.
	case "version", "--version", "-v":
		fmt.Fprintln(stdout, version)
		return exitOK
	case "-h", "--help", "help":
		fmt.Fprint(stdout, usage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return exitUsage
	}
}
