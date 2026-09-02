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

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// validatorFn runs one kind's validator. The client is nil unless the user
// asked to connect (--connect / --context / --kubeconfig).
// validatorFn runs one kind's validator. The client is nil unless the user
// asked to connect. The third return is PENDING: transient dependency gaps
// (a password Secret not applied yet) that the operator routes to phase
// Pending rather than failing on. They are neither errors nor warnings, and
// dropping them would hide the difference between "wrong" and "not yet".
type validatorFn func(doc []byte, c client.Client) (errs field.ErrorList, warnings, pending []string, err error)

// kindValidator pairs a validator with whether it can run without a cluster.
type kindValidator struct {
	fn validatorFn
	// needsClient marks validators that resolve cross-references (a clusterRef,
	// a Secret, a role) through the API server. Run offline they would report
	// "not found" for things that merely are not reachable — a false error,
	// which is worse than not checking — so they are skipped unless connected.
	needsClient bool
}

// validators is the full set of kinds this operator has a Go validator for.
//
// It is NOT every CRD. Of the operator's 26 kinds only these 12 have
// operator-side validation at all; the rest (the Aura suite, Neo4jRestore,
// Neo4jReplicaPromotion) are governed by their CRD schema alone, which
// `kubectl apply --dry-run=server` already enforces. Saying so precisely
// matters: an earlier version of this command told users those kinds needed a
// cluster connection, which implied --context would validate them. It will not.
//
// needsClient:false is a claim that the validator tolerates a nil client,
// established per kind and pinned by TestOfflineValidatorsAreNilClientSafe:
//   - Standalone, Backup, Plugin — constructor takes no client.
//   - DatabaseAlias, ReplicaDatabase — accept a client and never dereference it.
//   - EnterpriseCluster — its one client call returns early on nil.
var validators = map[string]kindValidator{
	"Neo4jEnterpriseCluster": {fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jEnterpriseCluster
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewClusterValidator(c).ValidateCreateWithWarnings(context.Background(), &obj)
		return out.Errors, out.Warnings, nil, nil
	}},
	"Neo4jEnterpriseStandalone": {fn: func(doc []byte, _ client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jEnterpriseStandalone
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		return validation.NewStandaloneValidator().ValidateCreate(&obj), nil, nil, nil
	}},
	"Neo4jBackup": {fn: func(doc []byte, _ client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jBackup
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		return validation.NewBackupValidator().Validate(&obj), nil, nil, nil
	}},
	"Neo4jPlugin": {fn: func(doc []byte, _ client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jPlugin
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewPluginValidator().Validate(&obj)
		return out.Errors, out.Warnings, nil, nil
	}},
	"Neo4jDatabaseAlias": {fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jDatabaseAlias
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewAliasValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, nil, nil
	}},
	"Neo4jReplicaDatabase": {fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jReplicaDatabase
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewReplicaValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, nil, nil
	}},

	// --- require a cluster connection ---------------------------------
	"Neo4jDatabase": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jDatabase
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewDatabaseValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, nil, nil
	}},
	"Neo4jUser": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jUser
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewUserValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, out.Pending, nil
	}},
	"Neo4jRole": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jRole
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewRoleValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, nil, nil
	}},
	"Neo4jRoleBinding": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jRoleBinding
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewRoleBindingValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, nil, nil
	}},
	"Neo4jAuthRule": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jAuthRule
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		out := validation.NewAuthRuleValidator(c).Validate(context.Background(), &obj)
		return out.Errors, out.Warnings, nil, nil
	}},
	// ShardedDatabase is the odd one out: its entrypoint returns a plain error
	// rather than an errors-plus-warnings result, so it is adapted here rather
	// than changing the operator's signature for the CLI's convenience.
	"Neo4jShardedDatabase": {needsClient: true, fn: func(doc []byte, c client.Client) (field.ErrorList, []string, []string, error) {
		var obj neo4jv1beta1.Neo4jShardedDatabase
		if err := yaml.Unmarshal(doc, &obj); err != nil {
			return nil, nil, nil, err
		}
		if vErr := validation.NewShardedDatabaseValidator(c).ValidateShardedDatabase(context.Background(), &obj); vErr != nil {
			return field.ErrorList{field.Invalid(field.NewPath("spec"), obj.Spec.Name, vErr.Error())}, nil, nil, nil
		}
		return nil, nil, nil, nil
	}},
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

func (d docResult) errorCount() int { return d.countOf("error") }

func (d docResult) warningCount() int { return d.countOf("warning") }

// pendingCount are transient dependency gaps — not yet satisfiable, not wrong.
// They never affect the exit code, including under --strict: failing CI because
// a Secret has not been applied yet would punish correct manifests.
func (d docResult) pendingCount() int { return d.countOf("pending") }

func (d docResult) countOf(sev string) int {
	n := 0
	for _, f := range d.findings {
		if f.severity == sev {
			n++
		}
	}
	return n
}

func runValidate(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var files multiFlag
	fs.Var(&files, "f", "Manifest file, directory, or - for stdin. Repeatable.")
	strict := fs.Bool("strict", false, "Treat warnings as errors (exit non-zero on warnings)")
	quiet := fs.Bool("quiet", false, "Print findings only; suppress the summary and banner")
	connect := fs.Bool("connect", false, "Connect to the cluster in the current kubeconfig context to run cross-reference checks")
	kubeContext := fs.String("context", "", "Kubeconfig context to use (implies --connect)")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file (implies --connect)")
	namespace := fs.String("namespace", "", "Namespace for objects whose manifest omits one")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Validate Neo4j CR manifests against the operator's own validators.

Usage:
  kubectl neo4j validate -f <file|dir|-> [-f ...] [--connect] [--strict] [--quiet]

Runs OFFLINE by default: no cluster connection is made, and kinds whose
validators resolve cross-references (clusterRef, Secrets, roles) are reported
as skipped. Pass --connect (or --context/--kubeconfig) to check those too.

Note: the operator stops validating a resource after certain critical errors,
so fixing the reported errors and re-running may surface further ones. A clean
run means nothing further is reachable, not that nothing was ever wrong.

Not every kind has an operator-side validator. Those that do not are governed
by their CRD schema alone — use kubectl apply --dry-run=server for that.

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

	// Connecting is opt-in. Defaulting to the current context would make a
	// command that is documented as offline fail in CI, where a kubeconfig
	// usually does not exist — so the user has to ask, either directly or by
	// naming a context/kubeconfig.
	var c client.Client
	wantCluster := *connect || *kubeContext != "" || *kubeconfig != ""
	if wantCluster {
		var err error
		c, err = newClusterClient(*kubeconfig, *kubeContext)
		if err != nil {
			fmt.Fprintf(stderr, "error: could not connect to the cluster: %v\n", err)
			return exitUsage
		}
	}

	inputs, err := expandInputs(files)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}

	var results []docResult
	for _, in := range inputs {
		rs, err := validateSource(in, c, *namespace)
		if err != nil {
			fmt.Fprintf(stderr, "error: %s: %v\n", in, err)
			return exitUsage
		}
		results = append(results, rs...)
	}

	if wantCluster && !*quiet {
		warnOnVersionSkew(c, stdout)
	}
	return report(results, stdout, *strict, *quiet, wantCluster)
}

// newClusterClient builds a direct (uncached) client from the user's kubeconfig.
// Uncached is deliberate: a CLI makes a handful of reads and exits, so an
// informer cache would cost more than it saves and would need list/watch
// permissions the user may not have.
func newClusterClient(kubeconfigPath, contextName string) (client.Client, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules.ExplicitPath = kubeconfigPath
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{CurrentContext: contextName},
	).ClientConfig()
	if err != nil {
		return nil, err
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := neo4jv1beta1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}

// warnOnVersionSkew compares this binary's version with the operator running in
// the cluster. Q4 of the design: a CLI carries the validation rules of the
// release it was built from, so a mismatch means rules silently added or
// removed relative to what will actually reconcile the manifest.
//
// Advisory only, and silent when it cannot tell: the operator may be in a
// namespace the user cannot list, which is not an error worth failing over.
func warnOnVersionSkew(c client.Client, stdout *os.File) {
	if c == nil || version == "dev" {
		return
	}
	var deployments appsv1.DeploymentList
	if err := c.List(context.Background(), &deployments,
		client.MatchingLabels{"app.kubernetes.io/name": "neo4j-operator"}); err != nil {
		return
	}
	for _, d := range deployments.Items {
		for _, ctr := range d.Spec.Template.Spec.Containers {
			for _, env := range ctr.Env {
				if env.Name != "OPERATOR_VERSION" || env.Value == "" || env.Value == "latest" {
					continue
				}
				if strings.TrimPrefix(env.Value, "v") != strings.TrimPrefix(version, "v") {
					fmt.Fprintf(stdout,
						"⚠ version skew: this CLI carries %s rules, but the operator in %s/%s is %s.\n"+
							"  Rules added or removed between those releases are checked incorrectly here.\n",
						version, d.Namespace, d.Name, env.Value)
				}
				return
			}
		}
	}
}

// validateSource reads one input (a path, or "-" for stdin) and validates every
// YAML document it contains.
func validateSource(source string, c client.Client, defaultNamespace string) ([]docResult, error) {
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
		res, err := validateDoc(doc, source, c, defaultNamespace)
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
func validateDoc(doc []byte, source string, c client.Client, defaultNamespace string) (docResult, error) {
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

	kv, ok := validators[meta.Kind]
	if !ok {
		// Precisely worded on purpose. These kinds have NO operator-side
		// validator, so no amount of connecting will check them — telling a
		// user to pass --context here would send them after something that
		// does not exist.
		res.skipped = fmt.Sprintf(
			"no operator-side validator for %s; its CRD schema still applies (kubectl apply --dry-run=server)", meta.Kind)
		return res, nil
	}
	if kv.needsClient && c == nil {
		res.skipped = fmt.Sprintf(
			"%s validation resolves cross-references; re-run with --connect to check it", meta.Kind)
		return res, nil
	}

	// A manifest may omit metadata.namespace and rely on the apply-time
	// default. Cross-reference lookups need one, so mirror kubectl: the
	// object's own namespace wins, else --namespace, else "default".
	if defaultNamespace != "" {
		doc = withDefaultNamespace(doc, meta.Namespace, defaultNamespace)
	}

	errs, warns, pending, err := kv.fn(doc, c)
	if err != nil {
		return docResult{}, fmt.Errorf("cannot decode %s: %w", meta.Kind, err)
	}
	for _, e := range errs {
		res.findings = append(res.findings, finding{"error", e.Field, e.ErrorBody()})
	}
	for _, w := range warns {
		res.findings = append(res.findings, finding{"warning", "", w})
	}
	for _, pnd := range pending {
		res.findings = append(res.findings, finding{"pending", "", pnd})
	}

	severityRank := map[string]int{"error": 0, "warning": 1, "pending": 2}
	sort.SliceStable(res.findings, func(i, j int) bool {
		ri, rj := severityRank[res.findings[i].severity], severityRank[res.findings[j].severity]
		if ri != rj {
			return ri < rj
		}
		return res.findings[i].path < res.findings[j].path
	})
	return res, nil
}

func report(results []docResult, stdout *os.File, strict, quiet, connected bool) int {
	totalErr, totalWarn, totalPending, validated, skipped := 0, 0, 0, 0, 0

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
		totalPending += r.pendingCount()

		if len(r.findings) == 0 {
			if !quiet {
				fmt.Fprintf(stdout, "✓ %s: ok\n", describe(r))
			}
			continue
		}
		fmt.Fprintf(stdout, "%s:\n", describe(r))
		for _, f := range r.findings {
			mark := "✗"
			switch f.severity {
			case "warning":
				mark = "⚠"
			case "pending":
				mark = "…"
			}
			if f.path != "" {
				fmt.Fprintf(stdout, "  %s %s: %s\n", mark, f.path, f.detail)
			} else {
				fmt.Fprintf(stdout, "  %s %s\n", mark, f.detail)
			}
		}
	}

	if !quiet {
		summary := fmt.Sprintf("\n%d validated, %d skipped — %d error(s), %d warning(s)",
			validated, skipped, totalErr, totalWarn)
		if totalPending > 0 {
			summary += fmt.Sprintf(", %d pending", totalPending)
		}
		fmt.Fprintln(stdout, summary)
		if !connected && skipped > 0 {
			fmt.Fprintln(stdout, "run with --connect to also check kinds that resolve cross-references")
		}
		// Offline output is only authoritative for the release it was built
		// from. Saying so is the cheap half of the version-skew answer.
		fmt.Fprintf(stdout, "validated against operator rules %s\n", version)
	}

	if totalErr > 0 || (strict && totalWarn > 0) {
		return exitInvalid
	}
	return exitOK
}

// withDefaultNamespace injects metadata.namespace into a document that omits
// one, so cross-reference lookups resolve against the namespace the user would
// actually apply into. Returns the document unchanged if it already has one.
func withDefaultNamespace(doc []byte, existing, fallback string) []byte {
	if existing != "" {
		return doc
	}
	var obj map[string]interface{}
	if err := yaml.Unmarshal(doc, &obj); err != nil {
		return doc
	}
	md, ok := obj["metadata"].(map[string]interface{})
	if !ok {
		return doc
	}
	md["namespace"] = fallback
	patched, err := yaml.Marshal(obj)
	if err != nil {
		return doc
	}
	return patched
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
