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
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// resourceStatus is one row of output, read generically rather than through 26
// typed switches. Every Neo4j CRD's status carries the same three fields
// (phase, ready, message) plus conditions, so unstructured access is both
// shorter and automatically correct for CRDs added later.
type resourceStatus struct {
	kind      string
	namespace string
	name      string
	phase     string
	ready     string
	age       string
	message   string
}

// healthy reports whether this row needs the user's attention. Deliberately
// conservative: an unrecognised phase counts as healthy rather than alarming
// someone about a status vocabulary this binary predates (the same reasoning
// the project's ArgoCD health checks use for the Aura kinds).
func (r resourceStatus) healthy() bool {
	switch strings.ToLower(r.phase) {
	case "failed", "error", "degraded", "unknown":
		return false
	}
	return r.ready != "false"
}

func runStatus(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	namespace := namespaceFlag(fs, "Namespace to inspect (default: the kubeconfig context's namespace)")
	allNamespaces := fs.Bool("all-namespaces", false, "Inspect every namespace")
	kubeContext := fs.String("context", "", "Kubeconfig context to use")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file")
	problemsOnly := fs.Bool("problems", false, "Show only resources that need attention")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Show the state of every Neo4j resource in a namespace.

Usage:
  kubectl neo4j status [-n <namespace>] [--all-namespaces] [--problems]

Reports each resource's phase, readiness and age, and the status message for
anything that is not healthy. Read-only.

Exit codes: 0 on a successful query (even if resources are unhealthy — mirroring
kubectl get), 2 on a usage or connection error.

Flags:
`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	c, err := newClusterClient(*kubeconfig, *kubeContext)
	if err != nil {
		fmt.Fprintf(stderr, "error: could not connect to the cluster: %v\n", err)
		return exitUsage
	}

	ns := *namespace
	if *allNamespaces {
		ns = ""
	} else if ns == "" {
		ns = currentNamespace(*kubeconfig, *kubeContext)
	}

	rows, err := collectStatus(context.Background(), c, ns)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}
	renderStatus(rows, stdout, *allNamespaces, *problemsOnly, ns)
	return exitOK
}

// registeredNeo4jKinds returns every Kind in the operator's API group, taken
// from the client's SCHEME rather than a hardcoded list. A CRD added to
// api/v1beta1 therefore appears here with no change to this command — the
// alternative is a list that silently goes stale, which is exactly the failure
// mode this repo's catalogue checks exist to prevent.
//
// "List" kinds are filtered out: the scheme registers both Foo and FooList, and
// listing FooList would ask the API server for FooListList.
func registeredNeo4jKinds(c client.Client) []schema.GroupVersionKind {
	var kinds []schema.GroupVersionKind
	for gvk := range c.Scheme().AllKnownTypes() {
		if gvk.Group != neo4jv1beta1.GroupVersion.Group {
			continue
		}
		if gvk.Version != neo4jv1beta1.GroupVersion.Version {
			continue
		}
		if strings.HasSuffix(gvk.Kind, "List") {
			continue
		}
		// runtime.Scheme also carries meta kinds (WatchEvent, Status, …) that
		// are not custom resources and cannot be listed.
		switch gvk.Kind {
		case "WatchEvent", "Status", "APIGroup", "APIResourceList",
			"CreateOptions", "UpdateOptions", "PatchOptions", "DeleteOptions",
			"GetOptions", "ListOptions":
			continue
		}
		kinds = append(kinds, gvk)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i].Kind < kinds[j].Kind })
	return kinds
}

// currentNamespace resolves the namespace from the kubeconfig context, the way
// kubectl does when -n is omitted. Falls back to "default" so the command still
// does something sensible with a minimal kubeconfig.
func currentNamespace(kubeconfigPath, contextName string) string {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules.ExplicitPath = kubeconfigPath
	}
	ns, _, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{CurrentContext: contextName},
	).Namespace()
	if err != nil || ns == "" {
		return "default"
	}
	return ns
}

func collectStatus(ctx context.Context, c client.Client, namespace string) ([]resourceStatus, error) {
	var rows []resourceStatus

	for _, kind := range registeredNeo4jKinds(c) {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(kind.GroupVersion().WithKind(kind.Kind + "List"))

		opts := []client.ListOption{}
		if namespace != "" {
			opts = append(opts, client.InNamespace(namespace))
		}
		if err := c.List(ctx, list, opts...); err != nil {
			// A kind the user cannot read, or a CRD not installed, is not a
			// reason to fail the whole command — report what IS visible.
			continue
		}
		for i := range list.Items {
			rows = append(rows, rowFrom(&list.Items[i]))
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].kind != rows[j].kind {
			return rows[i].kind < rows[j].kind
		}
		if rows[i].namespace != rows[j].namespace {
			return rows[i].namespace < rows[j].namespace
		}
		return rows[i].name < rows[j].name
	})
	return rows, nil
}

func rowFrom(obj *unstructured.Unstructured) resourceStatus {
	r := resourceStatus{
		kind:      obj.GetKind(),
		namespace: obj.GetNamespace(),
		name:      obj.GetName(),
		phase:     "-",
		ready:     "-",
		age:       humanAge(obj.GetCreationTimestamp().Time),
	}
	if phase, ok, _ := unstructured.NestedString(obj.Object, "status", "phase"); ok && phase != "" {
		r.phase = phase
	}
	if ready, ok, _ := unstructured.NestedBool(obj.Object, "status", "ready"); ok {
		r.ready = fmt.Sprintf("%t", ready)
	}
	if msg, ok, _ := unstructured.NestedString(obj.Object, "status", "message"); ok {
		r.message = msg
	}
	return r
}

func renderStatus(rows []resourceStatus, stdout *os.File, allNamespaces, problemsOnly bool, ns string) {
	shown := rows
	if problemsOnly {
		shown = nil
		for _, r := range rows {
			if !r.healthy() {
				shown = append(shown, r)
			}
		}
	}

	if len(shown) == 0 {
		switch {
		case problemsOnly && len(rows) > 0:
			fmt.Fprintf(stdout, "all %d Neo4j resource(s) look healthy\n", len(rows))
		case allNamespaces:
			fmt.Fprintln(stdout, "no Neo4j resources found in any namespace")
		case ns == "":
			fmt.Fprintln(stdout, "no Neo4j resources found")
		default:
			fmt.Fprintf(stdout, "no Neo4j resources found in namespace %q\n", ns)
		}
		return
	}

	w := tabwriter.NewWriter(stdout, 0, 0, 3, ' ', 0)
	if allNamespaces {
		fmt.Fprintln(w, "KIND\tNAMESPACE\tNAME\tPHASE\tREADY\tAGE")
	} else {
		fmt.Fprintln(w, "KIND\tNAME\tPHASE\tREADY\tAGE")
	}
	for _, r := range shown {
		if allNamespaces {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", r.kind, r.namespace, r.name, r.phase, r.ready, r.age)
		} else {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", r.kind, r.name, r.phase, r.ready, r.age)
		}
	}
	_ = w.Flush()

	// Messages are the part people actually act on, so they are printed below
	// the table rather than squeezed into a column that would wrap unreadably.
	//
	// Pending is included even though it is not unhealthy: "waiting for
	// password Secret X" is precisely the thing a user needs to read, and
	// hiding it because the resource is not technically broken would withhold
	// the one line that says what to do next. It is marked "…" rather than "✗",
	// matching how `validate` distinguishes "not yet" from "wrong".
	var notes []string
	for _, r := range rows {
		if r.message == "" {
			continue
		}
		switch {
		case !r.healthy():
			notes = append(notes, fmt.Sprintf("✗ %s/%s: %s", r.kind, r.name, r.message))
		case strings.EqualFold(r.phase, "pending"):
			notes = append(notes, fmt.Sprintf("… %s/%s: %s", r.kind, r.name, r.message))
		}
	}
	// Collect first, then print — so a run with nothing to say does not emit a
	// separator followed by nothing.
	if len(notes) > 0 {
		fmt.Fprintln(stdout)
		for _, n := range notes {
			fmt.Fprintln(stdout, n)
		}
	}
}

// humanAge renders a duration the way kubectl does, coarsest useful unit only.
func humanAge(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}
