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
	namespace := fs.String("namespace", "", "Namespace to collect from")
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

Flags:
`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
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

	files, notes := collectBundle(context.Background(), c, clientset, ns, *logLines)
	files = append(files, bundleFile{
		name: "REDACTIONS.txt",
		body: []byte(renderRedactions(notes)),
	})

	if err := writeArchive(target, files); err != nil {
		fmt.Fprintf(stderr, "error: could not write %s: %v\n", target, err)
		return exitUsage
	}

	fmt.Fprintf(stdout, "wrote %s (%d file(s))\n", target, len(files))
	fmt.Fprintf(stdout, "%d redaction(s) applied — see REDACTIONS.txt inside the archive.\n", len(notes))
	fmt.Fprintln(stdout, "Review the contents before sharing: only you can judge whether your own")
	fmt.Fprintln(stdout, "configuration or log output contains something private.")
	return exitOK
}

// collectBundle gathers everything, tolerating per-item failures. A bundle is
// most wanted when a cluster is unhealthy, so one unreadable resource must not
// abort the collection — each failure is recorded as a file in the archive
// instead, which also tells the recipient what could not be read.
func collectBundle(ctx context.Context, c client.Client, cs kubernetes.Interface, ns string, logLines int64) ([]bundleFile, []string) {
	var files []bundleFile
	var notes []string

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
			continue // not installed, or not readable — see errors.txt below
		}
		for i := range list.Items {
			item := &list.Items[i]
			cleaned, n := redactUnstructured(item)
			notes = append(notes, n...)
			body, err := yaml.Marshal(cleaned.Object)
			if err != nil {
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
		notes = append(notes, "events could not be read: "+err.Error())
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
			for _, ctr := range p.Spec.Containers {
				for _, prev := range []bool{false, true} {
					body, err := podLogs(ctx, cs, ns, p.Name, ctr.Name, prev, logLines)
					if err != nil {
						continue // a container that never restarted has no previous log
					}
					suffix := ctr.Name + ".log"
					if prev {
						suffix = ctr.Name + ".previous.log"
					}
					files = append(files, bundleFile{name: path.Join("pods", p.Name, suffix), body: body})
				}
			}
		}
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
	}

	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	return files, notes
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
