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
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// target is a resolved Neo4j deployment: which pod to talk to, in which
// container, over which scheme. Working this out is the entire value of these
// commands — the terminal plumbing is not.
type target struct {
	kind      string // Neo4jEnterpriseCluster | Neo4jEnterpriseStandalone
	name      string
	namespace string
	pod       string
	container string
	tls       bool
}

func (t target) scheme() string {
	if t.tls {
		return "bolt+s"
	}
	return "bolt"
}

// cypherShellArgs builds the in-container command.
//
// The password is referenced BY NAME, never by value. DB_USERNAME and
// DB_PASSWORD are already in the container's environment via secretKeyRef
// (internal/resources/cluster.go), so the shell expands them inside the pod and
// the secret never leaves it. Passing `-p <value>` instead would put the
// password in this process's argv, in the pod's argv, and — the one people
// forget — verbatim in the Kubernetes API audit log, which records an exec
// request's command array.
func (t target) cypherShellArgs(query string) string {
	cmd := fmt.Sprintf(`cypher-shell -a %s://localhost:7687 -u "$DB_USERNAME" -p "$DB_PASSWORD"`, t.scheme())
	if query != "" {
		// Single-quote the user's query for the container shell, escaping any
		// embedded single quotes. The CLI never composes Cypher of its own —
		// it passes the user's text through unchanged.
		cmd += fmt.Sprintf(" '%s'", strings.ReplaceAll(query, "'", `'\''`))
	}
	return cmd
}

func runCypher(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("cypher", flag.ContinueOnError)
	fs.SetOutput(stderr)
	namespace := fs.String("namespace", "", "Namespace of the deployment")
	kubeContext := fs.String("context", "", "Kubeconfig context to use")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file")
	query := fs.String("c", "", "Run a single query and exit, instead of opening an interactive session")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Open a cypher-shell session against a Neo4j deployment.

Usage:
  kubectl neo4j cypher [name] [-n <namespace>] [-c "<query>"]

Resolves the deployment, picks a ready pod, chooses bolt:// or bolt+s:// from
its TLS settings, and hands you an interactive session. With no name, the only
deployment in the namespace is used.

The admin password is never read by this command or placed on your command
line: it is already inside the pod, and is referenced there by variable name.

Flags:
`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	if _, err := exec.LookPath("kubectl"); err != nil {
		fmt.Fprintln(stderr, "error: kubectl was not found on your PATH.")
		fmt.Fprintln(stderr, "  This command hands the interactive session to `kubectl exec`, which handles")
		fmt.Fprintln(stderr, "  terminal sizing, signals and cluster authentication. Install kubectl, or use")
		fmt.Fprintln(stderr, "  `kubectl neo4j connect` to print the command to run yourself.")
		return exitUsage
	}

	c, err := newClusterClient(*kubeconfig, *kubeContext)
	if err != nil {
		fmt.Fprintf(stderr, "error: could not connect to the cluster: %v\n", err)
		return exitUsage
	}
	ns := *namespace
	if ns == "" {
		ns = currentNamespace(*kubeconfig, *kubeContext)
	}

	tgt, err := resolveTarget(context.Background(), c, ns, fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}

	kargs := []string{"exec", "-n", tgt.namespace, tgt.pod, "-c", tgt.container}
	if *query == "" {
		kargs = append(kargs, "-it")
	} else {
		kargs = append(kargs, "-i")
	}
	kargs = append(kargs, "--", "sh", "-c", tgt.cypherShellArgs(*query))

	if *kubeContext != "" {
		kargs = append([]string{"--context", *kubeContext}, kargs...)
	}
	if *kubeconfig != "" {
		kargs = append([]string{"--kubeconfig", *kubeconfig}, kargs...)
	}

	// context.Background deliberately: this is an interactive session, and a
	// cancellable context would let us kill the user's shell out from under
	// them. Ctrl-C belongs to cypher-shell, and `kubectl exec -it` forwards it.
	cmd := exec.CommandContext(context.Background(), "kubectl", kargs...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, stdout, stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// cypher-shell's own exit code is the useful one — a failed query
			// should not be reported as a CLI usage error.
			return exitErr.ExitCode()
		}
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}
	return exitOK
}

func runConnect(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("connect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	namespace := fs.String("namespace", "", "Namespace of the deployment")
	kubeContext := fs.String("context", "", "Kubeconfig context to use")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Print how to reach a Neo4j deployment.

Usage:
  kubectl neo4j connect [name] [-n <namespace>]

Shows the in-cluster address, the port-forward command for reaching it from
your machine, where the credentials live, and the correct Bolt scheme for its
TLS settings. Executes nothing.

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
	if ns == "" {
		ns = currentNamespace(*kubeconfig, *kubeContext)
	}

	tgt, err := resolveTarget(context.Background(), c, ns, fs.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}

	svc := fmt.Sprintf("%s-client", tgt.name)
	fmt.Fprintf(stdout, "%s/%s in namespace %s\n\n", tgt.kind, tgt.name, tgt.namespace)
	fmt.Fprintf(stdout, "In-cluster Bolt:\n  %s://%s.%s.svc.cluster.local:7687\n\n", tgt.scheme(), svc, tgt.namespace)
	fmt.Fprintf(stdout, "From your machine:\n  kubectl port-forward -n %s svc/%s 7687:7687 7474:7474\n", tgt.namespace, svc)
	fmt.Fprintf(stdout, "  then connect to %s://localhost:7687\n\n", tgt.scheme())
	fmt.Fprintf(stdout, "Interactive session (no local port-forward needed):\n  kubectl neo4j cypher %s -n %s\n\n", tgt.name, tgt.namespace)
	fmt.Fprintf(stdout, "Credentials live in the admin Secret for this deployment; they are already\n")
	fmt.Fprintf(stdout, "present inside the pod, so `kubectl neo4j cypher` never needs to read them.\n")
	if tgt.tls {
		fmt.Fprintf(stdout, "\nTLS is enabled: plain bolt:// is rejected by this deployment — use %s://.\n", tgt.scheme())
	}
	return exitOK
}

// resolveTarget finds the deployment to talk to and a pod that can serve the
// session. With no name it accepts exactly one deployment in the namespace,
// and otherwise asks rather than guessing.
func resolveTarget(ctx context.Context, c client.Client, namespace, name string) (target, error) {
	var clusters neo4jv1beta1.Neo4jEnterpriseClusterList
	var standalones neo4jv1beta1.Neo4jEnterpriseStandaloneList
	_ = c.List(ctx, &clusters, client.InNamespace(namespace))
	_ = c.List(ctx, &standalones, client.InNamespace(namespace))

	type candidate struct {
		kind, name string
		tls        bool
		selector   client.MatchingLabels
	}
	var found []candidate
	for i := range clusters.Items {
		cl := &clusters.Items[i]
		if name != "" && cl.Name != name {
			continue
		}
		found = append(found, candidate{
			kind: "Neo4jEnterpriseCluster", name: cl.Name,
			tls:      cl.Spec.TLS != nil && cl.Spec.TLS.Mode == "cert-manager",
			selector: client.MatchingLabels{"neo4j.com/cluster": cl.Name},
		})
	}
	for i := range standalones.Items {
		st := &standalones.Items[i]
		if name != "" && st.Name != name {
			continue
		}
		found = append(found, candidate{
			kind: "Neo4jEnterpriseStandalone", name: st.Name,
			tls:      st.Spec.TLS != nil && st.Spec.TLS.Mode == "cert-manager",
			selector: client.MatchingLabels{"app": st.Name},
		})
	}

	switch {
	case len(found) == 0 && name != "":
		return target{}, fmt.Errorf("no Neo4j deployment named %q in namespace %q", name, namespace)
	case len(found) == 0:
		return target{}, fmt.Errorf("no Neo4j deployment found in namespace %q", namespace)
	case len(found) > 1:
		var names []string
		for _, f := range found {
			names = append(names, f.name)
		}
		return target{}, fmt.Errorf(
			"namespace %q has %d Neo4j deployments (%s) — name the one you want",
			namespace, len(found), strings.Join(names, ", "))
	}

	f := found[0]
	pod, err := readyPod(ctx, c, namespace, f.selector)
	if err != nil {
		return target{}, err
	}
	return target{
		kind: f.kind, name: f.name, namespace: namespace,
		pod: pod, container: resources.Neo4jContainer, tls: f.tls,
	}, nil
}

// readyPod prefers a Ready pod and says so plainly when none is — "connection
// refused" from a pod that was never going to answer is a worse error than
// naming the actual problem.
func readyPod(ctx context.Context, c client.Client, namespace string, sel client.MatchingLabels) (string, error) {
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(namespace), sel); err != nil {
		return "", fmt.Errorf("could not list pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pods found for this deployment in namespace %q", namespace)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		for _, cond := range p.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				return p.Name, nil
			}
		}
	}
	return "", fmt.Errorf("no ready pod for this deployment (%d pod(s) exist but none are Ready) — try `kubectl neo4j status`", len(pods.Items))
}
