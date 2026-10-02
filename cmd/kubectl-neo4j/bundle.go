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
	"archive/tar"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// redactedPlaceholder replaces every withheld value. A single, obvious,
// greppable token so a recipient can see WHERE something was removed rather
// than wondering whether a field was empty or censored.
const redactedPlaceholder = "**REDACTED-BY-KUBECTL-NEO4J**"

// sensitiveEnvSubstrings marks env vars whose LITERAL value must never ship.
// Env vars sourced from a Secret via valueFrom are already safe — the manifest
// holds only the reference — but a user who typed a password directly into
// spec.env would otherwise have it collected and mailed to a stranger.
var sensitiveEnvSubstrings = []string{"PASSWORD", "PASSWD", "SECRET", "TOKEN", "APIKEY", "API_KEY", "CREDENTIAL", "PRIVATE_KEY", "AUTH"}

// bundleFile is one entry destined for the archive.
type bundleFile struct {
	name string
	body []byte
}

func runSupportBundle(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("support-bundle", flag.ContinueOnError)
	fs.SetOutput(stderr)
	namespace := namespaceFlag(fs, "Namespace to collect from")
	kubeContext := fs.String("context", "", "Kubeconfig context to use")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file")
	out := fs.String("o", "", "Output file (default: neo4j-support-bundle-<timestamp>.tar.gz)")
	logLines := fs.Int64("log-lines", 2000, "Tail this many lines from each container log")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Collect a diagnostic bundle for a Neo4j deployment.

Usage:
  kubectl neo4j support-bundle [-n <namespace>] [-o bundle.tar.gz]

Gathers Neo4j resources, workloads, events, pod logs and operator logs into one
archive. Read-only.

Secrets are NEVER collected: their values are replaced with a placeholder, and
so are literal values of environment variables whose names look sensitive. The
archive lists every redaction it made in REDACTIONS.txt — read it before
sharing, since only you can judge whether your own spec.config or logs contain
something private.

Collection is best-effort: a read that fails is skipped, not fatal. Every one is
listed in errors.txt (what was being read, and the error), so a section that is
missing from the archive is never mistaken for one that was empty.

Flags:
`)
		fs.PrintDefaults()
	}
	if err := parseFlags(fs, args); err != nil {
		return exitUsage
	}

	cfg, err := newClusterConfig(*kubeconfig, *kubeContext)
	if err != nil {
		fmt.Fprintf(stderr, "error: could not connect to the cluster: %v\n", err)
		return exitUsage
	}
	c, err := newClusterClient(*kubeconfig, *kubeContext)
	if err != nil {
		fmt.Fprintf(stderr, "error: could not connect to the cluster: %v\n", err)
		return exitUsage
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}

	ns := *namespace
	if ns == "" {
		ns = currentNamespace(*kubeconfig, *kubeContext)
	}

	target := *out
	if target == "" {
		target = fmt.Sprintf("neo4j-support-bundle-%s.tar.gz", time.Now().UTC().Format("20060102-150405"))
	}

	files, notes, failures := collectBundle(context.Background(), c, clientset, ns, *logLines)
	files = finishBundle(files, notes, failures)

	if err := writeArchive(target, files); err != nil {
		fmt.Fprintf(stderr, "error: could not write %s: %v\n", target, err)
		return exitUsage
	}

	fmt.Fprintf(stdout, "wrote %s (%d file(s))\n", target, len(files))
	fmt.Fprintf(stdout, "%d redaction(s) applied — see REDACTIONS.txt inside the archive.\n", len(notes))
	fmt.Fprintf(stdout, "%d read(s) failed — see errors.txt inside the archive.\n", len(failures))
	fmt.Fprintln(stdout, "Review the contents before sharing: only you can judge whether your own")
	fmt.Fprintln(stdout, "configuration or log output contains something private.")
	return exitOK
}

// finishBundle adds the two files that describe the collection itself:
// REDACTIONS.txt (what was withheld) and errors.txt (what could not be read).
// Both are always present, so their absence never has to be interpreted — an
// empty errors.txt says every read succeeded, where a missing one could mean
// that or an older kubectl-neo4j.
func finishBundle(files []bundleFile, notes, failures []string) []bundleFile {
	return append(files,
		bundleFile{name: "REDACTIONS.txt", body: []byte(renderRedactions(notes))},
		bundleFile{name: "errors.txt", body: []byte(renderReadErrors(failures))},
	)
}

// maxErrorLen bounds one recorded error. API errors are short; the cap only
// guards against a pathological one bloating the file.
const maxErrorLen = 500

// readFailure formats one failed read for errors.txt: what was being read, and
// the error — and nothing else. Only err.Error() is ever recorded, never a
// response body or any data the read might have returned, so this file cannot
// carry a value the redaction pass exists to withhold. Flattened to one line so
// a multi-line error cannot forge an extra entry.
func readFailure(what string, err error) string {
	msg := strings.Join(strings.Fields(err.Error()), " ")
	if r := []rune(msg); len(r) > maxErrorLen {
		msg = string(r[:maxErrorLen]) + "…"
	}
	return what + ": " + msg
}

func renderReadErrors(failures []string) string {
	var b strings.Builder
	b.WriteString("Reads that failed\n")
	b.WriteString("=================\n\n")
	b.WriteString("Collection is best-effort: a read that fails is skipped and the rest carries on.\n")
	b.WriteString("Everything listed below is therefore MISSING from this archive. Only the error\n")
	b.WriteString("text is recorded here, never any data the read might have returned.\n\n")
	if len(failures) == 0 {
		b.WriteString("(every read succeeded)\n")
		return b.String()
	}
	sorted := append([]string(nil), failures...)
	sort.Strings(sorted)
	for _, f := range sorted {
		b.WriteString("- " + f + "\n")
	}
	return b.String()
}

// collectBundle gathers everything, tolerating per-item failures. A bundle is
// most wanted when a cluster is unhealthy, so one unreadable resource must not
// abort the collection. The third return value lists every read that failed,
// for errors.txt: a silently missing section is indistinguishable from a
// section that was empty, and tells the recipient nothing about what
// permissions the collector had.
func collectBundle(ctx context.Context, c client.Client, cs kubernetes.Interface, ns string, logLines int64) ([]bundleFile, []string, []string) {
	var files []bundleFile
	var notes []string
	var failures []string
	fail := func(what string, err error) { failures = append(failures, readFailure(what, err)) }

	files = append(files, bundleFile{
		name: "meta.txt",
		body: []byte(fmt.Sprintf(
			"collected-by: kubectl-neo4j %s\ncollected-at: %s\nnamespace: %s\n",
			version, time.Now().UTC().Format(time.RFC3339), ns)),
	})

	// Neo4j custom resources, one YAML per object.
	for _, gvk := range registeredNeo4jKinds(c) {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			// Not installed, or not readable by this user.
			fail("list "+gvk.Kind, err)
			continue
		}
		for i := range list.Items {
			item := &list.Items[i]
			cleaned, n := redactUnstructured(item)
			notes = append(notes, n...)
			body, err := yaml.Marshal(cleaned.Object)
			if err != nil {
				fail(fmt.Sprintf("render %s/%s", gvk.Kind, item.GetName()), err)
				continue
			}
			files = append(files, bundleFile{
				name: path.Join("resources", gvk.Kind, item.GetName()+".yaml"),
				body: body,
			})
		}
	}

	// Events explain far more than status does about a stuck reconcile.
	var events corev1.EventList
	if err := c.List(ctx, &events, client.InNamespace(ns)); err == nil {
		files = append(files, bundleFile{name: "events.txt", body: []byte(renderEvents(events.Items))})
	} else {
		fail("list events", err)
	}

	// Pods: status summary plus logs, current and previous.
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(ns)); err == nil {
		for i := range pods.Items {
			p := &pods.Items[i]
			files = append(files, bundleFile{
				name: path.Join("pods", p.Name, "status.txt"),
				body: []byte(renderPodStatus(p)),
			})
			lf, lfail := collectContainerLogs(ctx, cs, p, logLines, path.Join("pods", p.Name))
			files = append(files, lf...)
			failures = append(failures, lfail...)
		}
	} else {
		fail("list pods", err)
	}

	// Secrets: names and keys only, never values. Included at all because
	// "the Secret is missing the password key" is a real and common cause, and
	// the shape is enough to see it.
	var secrets corev1.SecretList
	if err := c.List(ctx, &secrets, client.InNamespace(ns)); err == nil {
		var b strings.Builder
		for i := range secrets.Items {
			s := &secrets.Items[i]
			keys := make([]string, 0, len(s.Data))
			for k := range s.Data {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			fmt.Fprintf(&b, "%s\ttype=%s\tkeys=[%s]\n", s.Name, s.Type, strings.Join(keys, " "))
			notes = append(notes, fmt.Sprintf("Secret %q: values withheld, key names kept", s.Name))
		}
		files = append(files, bundleFile{name: "secrets-keys-only.txt", body: []byte(b.String())})
	} else {
		fail("list secrets (names and key names only)", err)
	}

	// The operator's own log, which lives in ANOTHER namespace.
	//
	// This is the single most useful artifact for an escalation about
	// reconcile behaviour — and the command promised it in its help text and
	// in the docs while collecting nothing, because everything above is scoped
	// to the target namespace and the operator does not run there.
	opFiles, opNotes, opFailures := collectOperatorLogs(ctx, c, cs, logLines)
	files = append(files, opFiles...)
	notes = append(notes, opNotes...)
	failures = append(failures, opFailures...)

	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	return files, notes, failures
}

// collectContainerLogs reads the current log of every container in the pod, and
// the previous one where a previous instance can exist. dir is the archive
// directory the logs are filed under.
//
// A failed read is returned, not dropped. The one exception is a previous log
// for a container that has never restarted: that instance does not exist, so
// the API server's refusal is the correct answer rather than a fault, and
// listing it would put a false "failure" in front of every healthy pod.
func collectContainerLogs(ctx context.Context, cs kubernetes.Interface, p *corev1.Pod, logLines int64, dir string) ([]bundleFile, []string) {
	var files []bundleFile
	var failures []string
	for _, ctr := range p.Spec.Containers {
		for _, prev := range []bool{false, true} {
			body, err := podLogs(ctx, cs, p.Namespace, p.Name, ctr.Name, prev, logLines)
			if err != nil {
				if !prev || containerRestarted(p, ctr.Name) {
					which := "log"
					if prev {
						which = "previous log"
					}
					failures = append(failures, readFailure(
						fmt.Sprintf("%s of container %s in pod %s/%s", which, ctr.Name, p.Namespace, p.Name), err))
				}
				continue
			}
			suffix := ctr.Name + ".log"
			if prev {
				suffix = ctr.Name + ".previous.log"
			}
			files = append(files, bundleFile{name: path.Join(dir, suffix), body: body})
		}
	}
	return files, failures
}

// containerRestarted reports whether a previous instance of the container can
// exist, from the pod's own status.
func containerRestarted(p *corev1.Pod, container string) bool {
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Name == container {
			return cs.RestartCount > 0
		}
	}
	return false
}

// collectOperatorLogs finds the operator by the label it ships with, wherever
// it runs, and takes the log of each of its pods. Best-effort like everything
// else here: a user without cluster-wide read access gets a failure entry
// saying so rather than a failed bundle.
func collectOperatorLogs(ctx context.Context, c client.Client, cs kubernetes.Interface, logLines int64) ([]bundleFile, []string, []string) {
	// The identifying label (app.kubernetes.io/name=neo4j-operator) is on the
	// DEPLOYMENT, not on its pods — the pod template carries only
	// control-plane=controller-manager. So find the Deployment by label and
	// follow its own selector to the pods, which works for both the kustomize
	// and Helm installs rather than depending on either one's pod labels.
	var deployments appsv1.DeploymentList
	if err := c.List(ctx, &deployments,
		client.MatchingLabels{"app.kubernetes.io/name": "neo4j-operator"}); err != nil {
		return nil, nil, []string{readFailure("operator logs (cannot list deployments cluster-wide)", err)}
	}
	if len(deployments.Items) == 0 {
		return nil, nil, []string{
			"operator logs: no Deployment labelled " +
				"app.kubernetes.io/name=neo4j-operator was found in any namespace this user can " +
				"read. If the operator IS running, that absence is itself worth reporting — it is " +
				"what a resource with no status looks like.",
		}
	}

	var failures []string
	var pods corev1.PodList
	for i := range deployments.Items {
		d := &deployments.Items[i]
		if d.Spec.Selector == nil || len(d.Spec.Selector.MatchLabels) == 0 {
			continue
		}
		var found corev1.PodList
		if err := c.List(ctx, &found,
			client.InNamespace(d.Namespace),
			client.MatchingLabels(d.Spec.Selector.MatchLabels)); err != nil {
			failures = append(failures, readFailure(
				fmt.Sprintf("list pods of operator Deployment %s/%s", d.Namespace, d.Name), err))
			continue
		}
		pods.Items = append(pods.Items, found.Items...)
	}
	if len(pods.Items) == 0 {
		return nil, nil, append(failures,
			"operator logs: the operator Deployment was found but none of "+
				"its pods could be listed. A Deployment with no running pod is itself the answer "+
				"to why nothing is reconciling.")
	}

	var files []bundleFile
	var notes []string
	for i := range pods.Items {
		p := &pods.Items[i]
		dir := path.Join("operator", p.Namespace, p.Name)
		files = append(files, bundleFile{
			name: path.Join(dir, "status.txt"),
			body: []byte(renderPodStatus(p)),
		})
		lf, lfail := collectContainerLogs(ctx, cs, p, logLines, dir)
		files = append(files, lf...)
		failures = append(failures, lfail...)
		notes = append(notes, fmt.Sprintf("operator log collected from %s/%s", p.Namespace, p.Name))
	}
	return files, notes, failures
}

func podLogs(ctx context.Context, cs kubernetes.Interface, ns, pod, container string, previous bool, tail int64) ([]byte, error) {
	req := cs.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{
		Container: container,
		Previous:  previous,
		TailLines: &tail,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	return io.ReadAll(stream)
}

// redactUnstructured removes values that must not leave the cluster. It works
// on a DEEP COPY: mutating the listed object would corrupt the caller's cache
// view, and for a read-only command that would be a particularly silly bug.
func redactUnstructured(obj *unstructured.Unstructured) (*unstructured.Unstructured, []string) {
	out := obj.DeepCopy()
	var notes []string
	kindName := fmt.Sprintf("%s/%s", obj.GetKind(), obj.GetName())

	// A Secret reached through the generic path would otherwise ship wholesale.
	if obj.GetKind() == "Secret" {
		if _, ok, _ := unstructured.NestedMap(out.Object, "data"); ok {
			_ = unstructured.SetNestedField(out.Object, redactedPlaceholder, "data")
			notes = append(notes, kindName+": all data withheld")
		}
	}

	// last-applied-configuration is a full copy of a previous manifest, so it
	// re-introduces anything redacted elsewhere in the object.
	annotations := out.GetAnnotations()
	if _, ok := annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		annotations["kubectl.kubernetes.io/last-applied-configuration"] = redactedPlaceholder
		out.SetAnnotations(annotations)
		notes = append(notes, kindName+": last-applied-configuration withheld (it duplicates the spec verbatim)")
	}

	notes = append(notes, redactEnvIn(out.Object, kindName)...)
	return out, notes
}

// redactEnvIn walks arbitrary nested maps looking for Kubernetes-shaped env
// lists, and blanks any LITERAL value whose name looks sensitive. Written as a
// generic walk rather than against fixed paths because env vars appear at
// several depths (pod templates, init containers, sidecars) and a path list
// would silently miss the next one added.
func redactEnvIn(node interface{}, owner string) []string {
	var notes []string
	switch typed := node.(type) {
	case map[string]interface{}:
		for key, child := range typed {
			if key == "env" {
				if list, ok := child.([]interface{}); ok {
					notes = append(notes, redactEnvList(list, owner)...)
					continue
				}
			}
			notes = append(notes, redactEnvIn(child, owner)...)
		}
	case []interface{}:
		for _, child := range typed {
			notes = append(notes, redactEnvIn(child, owner)...)
		}
	}
	return notes
}

func redactEnvList(list []interface{}, owner string) []string {
	var notes []string
	for _, entry := range list {
		envVar, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := envVar["name"].(string)
		if _, hasLiteral := envVar["value"]; !hasLiteral {
			continue // valueFrom: only a reference is stored, nothing to hide
		}
		if !looksSensitive(name) {
			continue
		}
		envVar["value"] = redactedPlaceholder
		notes = append(notes, fmt.Sprintf("%s: env %q had a literal value, withheld", owner, name))
	}
	return notes
}

func looksSensitive(name string) bool {
	upper := strings.ToUpper(name)
	for _, needle := range sensitiveEnvSubstrings {
		if strings.Contains(upper, needle) {
			return true
		}
	}
	return false
}

func renderRedactions(notes []string) string {
	var b strings.Builder
	b.WriteString("Redactions applied by kubectl-neo4j\n")
	b.WriteString("===================================\n\n")
	b.WriteString("Every value listed below was replaced with " + redactedPlaceholder + ".\n\n")
	if len(notes) == 0 {
		b.WriteString("(nothing required redaction)\n")
	} else {
		sort.Strings(notes)
		for _, n := range notes {
			b.WriteString("- " + n + "\n")
		}
	}
	b.WriteString("\nThis list is not a guarantee of safety. Redaction covers Secret values,\n")
	b.WriteString("last-applied-configuration annotations, and literal environment variables\n")
	b.WriteString("with sensitive-looking names. It cannot know whether your own spec.config,\n")
	b.WriteString("connection strings or application log output contain something private.\n")
	b.WriteString("Review the archive before sharing it.\n")
	return b.String()
}

func renderEvents(events []corev1.Event) string {
	sort.Slice(events, func(i, j int) bool {
		return events[i].LastTimestamp.Time.Before(events[j].LastTimestamp.Time)
	})
	var b strings.Builder
	fmt.Fprintf(&b, "%-24s %-9s %-28s %s\n", "LAST SEEN", "TYPE", "OBJECT", "MESSAGE")
	for i := range events {
		e := &events[i]
		fmt.Fprintf(&b, "%-24s %-9s %-28s %s\n",
			e.LastTimestamp.UTC().Format(time.RFC3339), e.Type,
			e.InvolvedObject.Kind+"/"+e.InvolvedObject.Name, e.Message)
	}
	return b.String()
}

func renderPodStatus(p *corev1.Pod) string {
	var b strings.Builder
	fmt.Fprintf(&b, "name: %s\nphase: %s\nnode: %s\n\n", p.Name, p.Status.Phase, p.Spec.NodeName)
	for _, cs := range p.Status.ContainerStatuses {
		fmt.Fprintf(&b, "container %s: ready=%t restarts=%d\n", cs.Name, cs.Ready, cs.RestartCount)
		if cs.LastTerminationState.Terminated != nil {
			t := cs.LastTerminationState.Terminated
			// Exit 137 is OOMKilled, the single most common Neo4j Enterprise
			// failure on an under-provisioned cluster, so the reason and code
			// are surfaced rather than buried in the raw object.
			fmt.Fprintf(&b, "  last termination: reason=%s exit=%d\n", t.Reason, t.ExitCode)
		}
	}
	fmt.Fprintln(&b)
	for _, cond := range p.Status.Conditions {
		fmt.Fprintf(&b, "condition %s=%s %s\n", cond.Type, cond.Status, cond.Message)
	}
	return b.String()
}

// yamlMarshal is a thin alias so tests can render an object exactly as the
// bundle does, rather than approximating it with a different marshaller.
func yamlMarshal(v interface{}) ([]byte, error) { return yaml.Marshal(v) }

func writeArchive(target string, files []bundleFile) error {
	f, err := os.Create(target)
	if err != nil {
		return err
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	root := strings.TrimSuffix(path.Base(target), ".tar.gz")

	for _, bf := range files {
		hdr := &tar.Header{
			Name:    path.Join(root, bf.name),
			Mode:    0o600,
			Size:    int64(len(bf.body)),
			ModTime: time.Now(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := tw.Write(bf.body); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}
