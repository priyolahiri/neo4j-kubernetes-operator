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
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Exit codes are part of this command's contract with CI, so they are named
// rather than written as bare integers at the return sites.
const (
	exitOK      = 0 // no errors (warnings alone do not fail unless --strict)
	exitInvalid = 1 // at least one validation error, or a warning under --strict
	exitUsage   = 2 // bad flags, unreadable input, undecodable YAML
)

// validators maps a Kind to an offline validation function.
//
// Membership here is a claim that the kind's validator is SAFE WITH A NIL
// KUBERNETES CLIENT, established per kind rather than assumed:
//
//   - Neo4jEnterpriseStandalone, Neo4jBackup, Neo4jPlugin — their constructors
//     take no client at all.
//   - Neo4jDatabaseAlias, Neo4jReplicaDatabase — their constructors accept a
//     client and store it, but no code path dereferences it.
//   - Neo4jEnterpriseCluster — its single client use is
//     ValidateAdminSecretPassword, which returns early when the client is nil.
//
// TestOfflineValidatorsAreNilClientSafe pins every one of those claims, so a
// future client call added to any of these validators fails a test here rather
// than panicking in a user's terminal.
//
// Kinds NOT listed resolve cross-references (clusterRef, Secrets, roles)
// through the API server. Running them with no cluster would report "not
// found" as a validation failure — a false error, which is worse than not
// checking — so they are reported as skipped until a --context path exists.
var validators = map[string]func([]byte) (field.ErrorList, []string, error){
	"Neo4jEnterpriseCluster": func(doc []byte) (field.ErrorList, []string, error) {
		var obj neo4jv1beta1.Neo4jEnterpriseCluster
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, err
		}
		out := validation.NewClusterValidator(nil).ValidateCreateWithWarnings(context.Background(), &obj)
		return out.Errors, out.Warnings, nil
	},
	"Neo4jEnterpriseStandalone": func(doc []byte) (field.ErrorList, []string, error) {
		var obj neo4jv1beta1.Neo4jEnterpriseStandalone
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, err
		}
		return validation.NewStandaloneValidator().ValidateCreate(&obj), nil, nil
	},
	"Neo4jBackup": func(doc []byte) (field.ErrorList, []string, error) {
		var obj neo4jv1beta1.Neo4jBackup
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, err
		}
		return validation.NewBackupValidator().Validate(&obj), nil, nil
	},
	"Neo4jPlugin": func(doc []byte) (field.ErrorList, []string, error) {
		var obj neo4jv1beta1.Neo4jPlugin
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, err
		}
		out := validation.NewPluginValidator().Validate(&obj)
		return out.Errors, out.Warnings, nil
	},
	"Neo4jDatabaseAlias": func(doc []byte) (field.ErrorList, []string, error) {
		var obj neo4jv1beta1.Neo4jDatabaseAlias
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, err
		}
		out := validation.NewAliasValidator(nil).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, nil
	},
	"Neo4jReplicaDatabase": func(doc []byte) (field.ErrorList, []string, error) {
		var obj neo4jv1beta1.Neo4jReplicaDatabase
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, err
		}
		out := validation.NewReplicaValidator(nil).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, nil
	},
}

// finding is one rendered line of output, decoupled from field.Error so that
// errors and warnings can be sorted and printed uniformly.
type finding struct {
	severity string // "error" | "warning"
	path     string
	detail   string
}

// docResult is the outcome for a single YAML document.
type docResult struct {
	source   string
	kind     string
	name     string
	skipped  string // non-empty = reason this document was not validated
	findings []finding
}

func (d docResult) errorCount() int {
	n := 0
	for _, f := range d.findings {
		if f.severity == "error" {
			n++
		}
	}
	return n
}

func (d docResult) warningCount() int { return len(d.findings) - d.errorCount() }

func runValidate(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var files multiFlag
	fs.Var(&files, "f", "Manifest file, directory, or - for stdin. Repeatable.")
	strict := fs.Bool("strict", false, "Treat warnings as errors (exit non-zero on warnings)")
	quiet := fs.Bool("quiet", false, "Print findings only; suppress the summary and banner")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Validate Neo4j CR manifests against the operator's own validators.

Usage:
  kubectl neo4j validate -f <file|dir|-> [-f ...] [--strict] [--quiet]

Runs offline. No cluster connection is made, so checks that resolve
cross-references (clusterRef, Secrets) are reported as skipped.

Note: the operator stops validating a resource after certain critical errors
(an invalid image, for example), so fixing the reported errors and re-running
may surface further ones. A clean run means nothing further is reachable, not
that nothing was ever wrong.

Exit codes: 0 clean, 1 validation errors (or warnings with --strict), 2 usage.

Flags:
`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if len(files) == 0 {
		fs.Usage()
		return exitUsage
	}

	inputs, err := expandInputs(files)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}

	var results []docResult
	for _, in := range inputs {
		rs, err := validateSource(in)
		if err != nil {
			fmt.Fprintf(stderr, "error: %s: %v\n", in, err)
			return exitUsage
		}
		results = append(results, rs...)
	}

	return report(results, stdout, *strict, *quiet)
}

// validateSource reads one input (a path, or "-" for stdin) and validates every
// YAML document it contains.
func validateSource(source string) ([]docResult, error) {
	var r io.Reader
	if source == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(source)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}

	var results []docResult
	// NewYAMLReader splits on "---" so a single file may hold many objects,
	// which is how people actually write manifests.
	reader := k8syaml.NewYAMLReader(bufio.NewReader(r))
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if isEmptyDoc(doc) {
			continue
		}
		res, err := validateDoc(doc, source)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}

// isEmptyDoc reports whether a YAML document carries no content at all.
//
// A whitespace check is not sufficient: `---\n# a comment\n---` yields a
// document that is non-empty as bytes but decodes to nothing. Treating that as
// an object produces a spurious "no kind field" line for every commented
// separator in a hand-written manifest, which is exactly the kind of noise that
// makes people stop reading a linter's output.
func isEmptyDoc(doc []byte) bool {
	if len(strings.TrimSpace(string(doc))) == 0 {
		return true
	}
	var probe map[string]interface{}
	if err := yaml.Unmarshal(doc, &probe); err != nil {
		// Leave malformed YAML to validateDoc, which reports it properly.
		return false
	}
	return len(probe) == 0
}

// validateDoc decodes one document far enough to learn its Kind, then hands it
// to the operator's validator for that Kind.
func validateDoc(doc []byte, source string) (docResult, error) {
	var meta struct {
		metav1.TypeMeta   `json:",inline"`
		metav1.ObjectMeta `json:"metadata,omitempty"`
	}
	if err := yaml.Unmarshal(doc, &meta); err != nil {
		return docResult{}, fmt.Errorf("cannot parse YAML: %w", err)
	}

	res := docResult{source: source, kind: meta.Kind, name: meta.Name}

	switch {
	case meta.Kind == "":
		res.skipped = "no kind field — not a Kubernetes object"
		return res, nil
	case !strings.HasPrefix(meta.APIVersion, "neo4j.neo4j.com/"):
		res.skipped = fmt.Sprintf("not a Neo4j operator resource (apiVersion %q)", meta.APIVersion)
		return res, nil
	}

	fn, ok := validators[meta.Kind]
	if !ok {
		res.skipped = fmt.Sprintf("%s validation resolves cross-references and needs a cluster connection; not yet supported offline", meta.Kind)
		return res, nil
	}

	errs, warns, err := fn(doc)
	if err != nil {
		return docResult{}, fmt.Errorf("cannot decode %s: %w", meta.Kind, err)
	}
	for _, e := range errs {
		res.findings = append(res.findings, finding{"error", e.Field, e.ErrorBody()})
	}
	for _, w := range warns {
		res.findings = append(res.findings, finding{"warning", "", w})
	}

	sort.SliceStable(res.findings, func(i, j int) bool {
		if res.findings[i].severity != res.findings[j].severity {
			return res.findings[i].severity == "error"
		}
		return res.findings[i].path < res.findings[j].path
	})
	return res, nil
}

func report(results []docResult, stdout *os.File, strict, quiet bool) int {
	totalErr, totalWarn, validated, skipped := 0, 0, 0, 0

	for _, r := range results {
		if r.skipped != "" {
			skipped++
			if !quiet {
				fmt.Fprintf(stdout, "- %s: skipped — %s\n", describe(r), r.skipped)
			}
			continue
		}
		validated++
		totalErr += r.errorCount()
		totalWarn += r.warningCount()

		if len(r.findings) == 0 {
			if !quiet {
				fmt.Fprintf(stdout, "✓ %s: ok\n", describe(r))
			}
			continue
		}
		fmt.Fprintf(stdout, "%s:\n", describe(r))
		for _, f := range r.findings {
			mark := "✗"
			if f.severity == "warning" {
				mark = "⚠"
			}
			if f.path != "" {
				fmt.Fprintf(stdout, "  %s %s: %s\n", mark, f.path, f.detail)
			} else {
				fmt.Fprintf(stdout, "  %s %s\n", mark, f.detail)
			}
		}
	}

	if !quiet {
		fmt.Fprintf(stdout, "\n%d validated, %d skipped — %d error(s), %d warning(s)\n",
			validated, skipped, totalErr, totalWarn)
		// Offline output is only authoritative for the release it was built
		// from. Saying so is the cheap half of the version-skew answer.
		fmt.Fprintf(stdout, "validated against operator rules %s\n", version)
	}

	if totalErr > 0 || (strict && totalWarn > 0) {
		return exitInvalid
	}
	return exitOK
}

func describe(r docResult) string {
	kind := r.kind
	if kind == "" {
		kind = "document"
	}
	if r.name == "" {
		return fmt.Sprintf("%s (%s)", kind, r.source)
	}
	return fmt.Sprintf("%s/%s (%s)", kind, r.name, r.source)
}

// expandInputs turns the -f values into a concrete file list, walking any
// directory one level for the YAML extensions people actually use.
func expandInputs(files []string) ([]string, error) {
	var out []string
	for _, f := range files {
		if f == "-" {
			out = append(out, f)
			continue
		}
		info, err := os.Stat(f)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			out = append(out, f)
			continue
		}
		entries, err := os.ReadDir(f)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			switch strings.ToLower(filepath.Ext(e.Name())) {
			case ".yaml", ".yml", ".json":
				out = append(out, filepath.Join(f, e.Name()))
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no manifest files found")
	}
	return out, nil
}

// multiFlag collects a repeatable string flag.
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}
