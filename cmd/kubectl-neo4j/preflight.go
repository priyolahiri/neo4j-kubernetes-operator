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

// preflight is `validate`'s move applied a second time.
//
// `validate` took the operator's spec rules and moved them from after-apply to
// before-apply, which is the whole answer to having no admission webhooks. But
// a large class of first failures is not in the manifest at all — it is in the
// cluster the manifest is about to land in. A StorageClass that does not exist,
// one that cannot expand, a credentials Secret missing a key, no node large
// enough for the pod: every one of those produces a correct-looking manifest
// that never becomes a running database.
//
// Those checks cannot live in `validate`, because 21 of the operator's 29
// validators are deliberately offline and the rest resolve cross-references
// only. They are cluster facts, and this is where they belong.
//
// # The boundary, stated once
//
// preflight checks SHAPE, not REACHABILITY. It reads Kubernetes objects — a
// StorageClass, a Secret's key names, a ServiceAccount's annotations, node
// allocatable capacity. It does NOT contact S3, GCS or Azure, and it never
// runs a probe Pod. A bucket that exists but denies access still fails at run
// time, and the output says so rather than implying a clean run means the
// backup will work.
//
// That boundary is why the command is cheap: no new image, no new RBAC beyond
// reads the user already has, and nothing that mutates the cluster.

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// preflightResult is one resource's worth of checks.
type preflightResult struct {
	kind    string
	name    string
	source  string // the file it was read from, or "cluster"
	checks  []symptom
	skipped string // non-empty when this kind has no cluster-side checks
}

func (r preflightResult) problems() bool {
	for _, c := range r.checks {
		if c.mark == markProblem {
			return true
		}
	}
	return false
}

func runPreflight(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("preflight", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var files multiFlag
	fs.Var(&files, "f", "Manifest to check before applying (repeatable, '-' for stdin)")
	namespace := namespaceFlag(fs, "Namespace the resources live in, or will be applied to")
	kubeContext := fs.String("context", "", "Kubeconfig context to use")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Check the cluster-side preconditions a manifest depends on.

Usage:
  kubectl neo4j preflight -f cluster.yaml [-n <namespace>]   # before you apply
  kubectl neo4j preflight <Kind>/<name> [-n <namespace>]     # for what is deployed
  kubectl neo4j preflight [-n <namespace>]                   # everything there

"validate" checks the manifest. This checks the cluster it is about to land in:
does the StorageClass exist and can it expand, is there a node big enough for
the pod, does the credentials Secret carry the keys the backup Job will mount,
does the ServiceAccount carry a cloud identity. Read-only.

It checks SHAPE, not REACHABILITY: it never contacts S3, GCS or Azure and never
runs a probe pod, so a bucket that exists but denies access still fails later.

Exit codes: 0 when every check passed (warnings alone do not fail), 1 when a
check failed, 2 on a usage or connection error.

Flags:
`)
		fs.PrintDefaults()
	}
	if err := parseFlags(fs, args); err != nil {
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

	ctx := context.Background()
	var results []preflightResult

	switch {
	case len(files) > 0:
		for _, f := range files {
			objs, err := readManifests(f)
			if err != nil {
				fmt.Fprintf(stderr, "error: %s: %v\n", f, err)
				return exitUsage
			}
			for _, obj := range objs {
				results = append(results, preflightObject(ctx, c, ns, f, obj))
			}
		}
	default:
		target := fs.Arg(0)
		if target != "" && !strings.Contains(target, "/") {
			fmt.Fprintf(stderr, "error: expected <Kind>/<name>, got %q\n", target)
			return exitUsage
		}
		live, err := readLiveTargets(ctx, c, ns, target)
		if err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return exitUsage
		}
		if target != "" && len(live) == 0 {
			fmt.Fprintf(stderr, "error: %s not found in namespace %q\n", target, ns)
			return exitUsage
		}
		for _, obj := range live {
			results = append(results, preflightObject(ctx, c, ns, "cluster", obj))
		}
	}

	return renderPreflight(results, stdout, ns)
}

// preflightSubject is the decoded form of whichever kind we are checking. Only
// the kinds with cluster-side preconditions are represented; everything else
// is reported as skipped rather than silently passing, so a clean run never
// implies a check that was not made.
type preflightSubject struct {
	kind       string
	name       string
	cluster    *neo4jv1beta1.Neo4jEnterpriseCluster
	standalone *neo4jv1beta1.Neo4jEnterpriseStandalone
	backup     *neo4jv1beta1.Neo4jBackup
}

func preflightObject(ctx context.Context, c client.Client, ns, source string, raw []byte) preflightResult {
	subject, err := decodeSubject(raw)
	if err != nil {
		return preflightResult{source: source, kind: "?", name: "?", checks: []symptom{{
			mark: markProblem, subject: "manifest", what: "could not be read", detail: err.Error(),
		}}}
	}

	res := preflightResult{kind: subject.kind, name: subject.name, source: source}
	switch {
	case subject.cluster != nil:
		res.checks = preflightInstance(ctx, c, ns,
			subject.cluster.Spec.Storage, subject.cluster.Spec.Image,
			subject.cluster.Spec.Resources, int(subject.cluster.Spec.Topology.Servers))
	case subject.standalone != nil:
		res.checks = preflightInstance(ctx, c, ns,
			subject.standalone.Spec.Storage, subject.standalone.Spec.Image,
			subject.standalone.Spec.Resources, 1)
	case subject.backup != nil:
		res.checks = preflightBackup(ctx, c, ns, subject.backup)
	default:
		res.skipped = "no cluster-side preconditions are defined for this kind"
	}
	return res
}

// preflightInstance covers the two kinds that run Neo4j pods. Both carry the
// same StorageSpec and ImageSpec, so the checks are shared rather than written
// twice and drifting.
func preflightInstance(ctx context.Context, c client.Client, ns string,
	storage neo4jv1beta1.StorageSpec, image neo4jv1beta1.ImageSpec,
	res *corev1.ResourceRequirements, servers int) []symptom {
	var checks []symptom
	checks = append(checks, checkStorageClass(ctx, c, storage.ClassName)...)
	checks = append(checks, checkPullSecrets(ctx, c, ns, image.PullSecrets)...)
	checks = append(checks, checkNodeCapacity(ctx, c, res, servers)...)
	return checks
}

// checkStorageClass covers two distinct failures with one lookup.
//
// A missing class is the more urgent: the operator refuses to build the
// StatefulSet and records a StorageClassNotFound event, so nothing happens at
// all. allowVolumeExpansion is the quieter one — it costs nothing today and
// makes spec.storage.size effectively immutable, which is only discovered
// during the resize that a full disk made urgent.
func checkStorageClass(ctx context.Context, c client.Client, className string) []symptom {
	if className == "" {
		// An empty className means the cluster default, which cannot be
		// resolved by name. Saying "not checked" is more honest than looking
		// up the default-annotated class and asserting it is the one that will
		// be used, which the scheduler decides, not us.
		return []symptom{{
			mark: markWarning, subject: "storageclass", what: "not specified, so the cluster default will be used",
			action: "The default class's expansion support is not checked here. If you may need " +
				"to grow the volume later, name a class with allowVolumeExpansion: true.",
		}}
	}

	var sc storagev1.StorageClass
	if err := c.Get(ctx, types.NamespacedName{Name: className}, &sc); err != nil {
		if apierrors.IsNotFound(err) {
			return []symptom{{
				mark: markProblem, subject: "storageclass " + className, what: "does not exist",
				action: "The operator will refuse to create the StatefulSet and record a " +
					"StorageClassNotFound event. Create the class, name an existing one, or " +
					"leave spec.storage.className empty to use the cluster default.",
			}}
		}
		return []symptom{{
			mark: markWarning, subject: "storageclass " + className, what: "could not be read",
			detail: err.Error(),
		}}
	}

	if sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion {
		return []symptom{{
			mark: markWarning, subject: "storageclass " + className, what: "does not allow volume expansion",
			action: "spec.storage.size will be effectively immutable: the operator rejects an " +
				"expansion on a class with allowVolumeExpansion != true. Fix it now with " +
				"kubectl patch storageclass " + className +
				` -p '{"allowVolumeExpansion": true}'` + " — after the PVCs exist, growing " +
				"them means a data migration.",
		}}
	}
	return nil
}

func checkPullSecrets(ctx context.Context, c client.Client, ns string, names []string) []symptom {
	var checks []symptom
	for _, name := range names {
		var s corev1.Secret
		err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &s)
		if apierrors.IsNotFound(err) {
			checks = append(checks, symptom{
				mark: markProblem, subject: "imagePullSecret " + name, what: "does not exist in " + ns,
				action: "Every pod will stay in ImagePullBackOff. Create the Secret, or remove it " +
					"from spec.image.pullSecrets.",
			})
		}
	}
	return checks
}

// checkNodeCapacity answers "will this pod fit anywhere", which is the top
// pod-startup failure in the troubleshooting guide and is invisible to any
// manifest-level check.
//
// It deliberately compares against a SINGLE node's allocatable memory rather
// than the cluster total: the scheduler places a pod on one node, and a
// three-node cluster with 1Gi each cannot run one 2Gi pod however the total
// reads.
func checkNodeCapacity(ctx context.Context, c client.Client, res *corev1.ResourceRequirements, servers int) []symptom {
	if res == nil || res.Requests == nil {
		return nil
	}
	want, ok := res.Requests[corev1.ResourceMemory]
	if !ok || want.IsZero() {
		return nil
	}

	var nodes corev1.NodeList
	if err := c.List(ctx, &nodes); err != nil {
		// Node reads need cluster scope, which a namespace-scoped user may not
		// have. That is not a failure of the deployment being checked.
		return []symptom{{
			mark: markWarning, subject: "nodes", what: "could not be listed, so capacity was not checked",
			detail: err.Error(),
		}}
	}

	fits := 0
	var largest resource.Quantity
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if !nodeReady(n) {
			continue
		}
		alloc := n.Status.Allocatable[corev1.ResourceMemory]
		if alloc.Cmp(largest) > 0 {
			largest = alloc
		}
		if alloc.Cmp(want) >= 0 {
			fits++
		}
	}

	if fits == 0 {
		return []symptom{{
			mark: markProblem, subject: "node capacity",
			what:   fmt.Sprintf("no Ready node can fit a %s memory request", want.String()),
			detail: fmt.Sprintf("largest Ready node has %s allocatable", largest.String()),
			action: "Every pod will stay Pending as Unschedulable. Add capacity, or lower " +
				"spec.resources.requests.memory — but Neo4j Enterprise will not start below " +
				"1.5Gi, so that has a floor.",
		}}
	}
	if servers > 1 && fits < servers {
		return []symptom{{
			mark: markWarning, subject: "node capacity",
			what: fmt.Sprintf("%d Ready node(s) can fit a %s request, for %d server(s)", fits, want.String(), servers),
			action: "Enough for the pods to schedule if they may share nodes. If you spread " +
				"servers with spec.placement.antiAffinity (or your own required " +
				"podAntiAffinity), the surplus servers will stay Pending.",
		}}
	}
	return nil
}

func nodeReady(n *corev1.Node) bool {
	for _, cond := range n.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// preflightBackup replaces the documented after-the-failure ritual — a
// `kubectl run` probe pod per cloud vendor, reached for only once a backup has
// already failed — with a check that runs before the CR is applied.
//
// What it can establish is the shape: the Secret exists and carries the keys
// the Job will mount, or an ambient identity is bound to the ServiceAccount
// that will run it. What it cannot establish is whether the credentials are
// valid or the bucket is writable, and it says so rather than implying it.
func preflightBackup(ctx context.Context, c client.Client, ns string, backup *neo4jv1beta1.Neo4jBackup) []symptom {
	var checks []symptom

	cloud := backup.Spec.Storage.Cloud
	if backup.Spec.Storage.Type == "pvc" {
		return append(checks, checkBackupPVC(ctx, c, ns, backup)...)
	}
	if cloud == nil || cloud.Provider == "" {
		return append(checks, symptom{
			mark: markWarning, subject: "storage.cloud", what: "no provider is set",
			action: "A cloud storage type without spec.storage.cloud.provider has neither " +
				"credentials nor an identity to run with.",
		})
	}

	switch {
	case cloud.CredentialsSecretRef != "":
		checks = append(checks, checkCredentialsSecret(ctx, c, ns, cloud.Provider, cloud.CredentialsSecretRef)...)
	default:
		checks = append(checks, checkAmbientIdentity(ctx, c, ns, cloud.Provider)...)
	}
	return checks
}

// checkCredentialsSecret verifies the Secret carries every key the Job will
// mount. A missing key does not fail politely: the pod never starts, and the
// kubelet reports CreateContainerConfigError with no mention of backups.
func checkCredentialsSecret(ctx context.Context, c client.Client, ns, provider, name string) []symptom {
	var s corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &s); err != nil {
		if apierrors.IsNotFound(err) {
			return []symptom{{
				mark: markProblem, subject: "secret " + name, what: "does not exist in " + ns,
				action: "spec.storage.cloud.credentialsSecretRef names it, and the backup Job " +
					"mounts its keys. Create it before applying the backup.",
			}}
		}
		return []symptom{{
			mark: markWarning, subject: "secret " + name, what: "could not be read", detail: err.Error(),
		}}
	}

	want := resources.CloudCredentialKeys(provider)
	if len(want) == 0 {
		return nil
	}
	var missing []string
	for _, k := range want {
		if _, ok := s.Data[k]; !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return []symptom{{
			mark: markProblem, subject: "secret " + name,
			what:   fmt.Sprintf("is missing %d key(s) the %s backup Job mounts", len(missing), provider),
			detail: "missing: " + strings.Join(missing, ", "),
			action: "The Job's pod will not start — the kubelet reports " +
				"CreateContainerConfigError, which does not mention backups. Add the keys " +
				"to the Secret.",
		}}
	}
	return []symptom{{
		mark: markWarning, subject: "secret " + name,
		what: "carries every key the Job mounts",
		action: "Shape only. Whether the credentials are valid, and whether they can write to " +
			"the bucket, is not checked here and is only known at run time.",
	}}
}

// checkAmbientIdentity covers the no-Secret path: the Job runs as the
// operator-managed ServiceAccount and relies on IRSA, GKE Workload Identity or
// Azure Workload Identity being bound to it.
func checkAmbientIdentity(ctx context.Context, c client.Client, ns, provider string) []symptom {
	annotation := resources.CloudIdentityAnnotation(provider)
	saName := resources.BackupServiceAccountName

	var sa corev1.ServiceAccount
	err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: saName}, &sa)
	if apierrors.IsNotFound(err) {
		return []symptom{{
			mark: markWaiting, subject: "serviceaccount " + saName, what: "does not exist yet",
			action: "The operator creates it on the first backup, so this is expected before " +
				"one has ever run. Its cloud identity comes from " +
				"spec.storage.cloud.identity.autoCreate.annotations — set " + annotation +
				" there, or the Job will run with no credentials.",
		}}
	}
	if err != nil {
		return []symptom{{
			mark: markWarning, subject: "serviceaccount " + saName, what: "could not be read", detail: err.Error(),
		}}
	}

	if annotation != "" && sa.Annotations[annotation] == "" {
		return []symptom{{
			mark: markProblem, subject: "serviceaccount " + saName,
			what:   "has no " + provider + " identity annotation",
			detail: "expected annotation " + annotation,
			action: "No credentialsSecretRef is set, so the Job relies on an ambient identity — " +
				"and none is bound. Set it via spec.storage.cloud.identity.autoCreate.annotations, " +
				"which the operator stamps onto this ServiceAccount.",
		}}
	}
	return []symptom{{
		mark: markWarning, subject: "serviceaccount " + saName,
		what: "is bound to a " + provider + " identity",
		action: "Shape only. Whether that identity is allowed to write to the bucket is not " +
			"checked here and is only known at run time.",
	}}
}

func checkBackupPVC(ctx context.Context, c client.Client, ns string, backup *neo4jv1beta1.Neo4jBackup) []symptom {
	pvc := backup.Spec.Storage.PVC
	if pvc == nil {
		return nil
	}
	if pvc.Name != "" {
		var existing corev1.PersistentVolumeClaim
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: pvc.Name}, &existing); apierrors.IsNotFound(err) {
			return []symptom{{
				mark: markProblem, subject: "pvc " + pvc.Name, what: "does not exist in " + ns,
				action: "spec.storage.pvc.name references an existing claim. Create it, or drop " +
					"the name so the operator provisions one.",
			}}
		}
		return nil
	}
	return checkStorageClass(ctx, c, pvc.StorageClassName)
}

// decodeSubject turns one manifest document into the typed object its Kind
// calls for. Unknown kinds are not an error — they are reported as skipped.
func decodeSubject(raw []byte) (preflightSubject, error) {
	var probe struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
	}
	if err := yaml.Unmarshal(raw, &probe); err != nil {
		return preflightSubject{}, err
	}
	if probe.Kind == "" {
		return preflightSubject{}, errors.New("document has no kind field")
	}

	s := preflightSubject{kind: probe.Kind, name: probe.Metadata.Name}
	switch probe.Kind {
	case "Neo4jEnterpriseCluster":
		var o neo4jv1beta1.Neo4jEnterpriseCluster
		if err := yaml.Unmarshal(raw, &o); err != nil {
			return s, err
		}
		s.cluster = &o
	case "Neo4jEnterpriseStandalone":
		var o neo4jv1beta1.Neo4jEnterpriseStandalone
		if err := yaml.Unmarshal(raw, &o); err != nil {
			return s, err
		}
		s.standalone = &o
	case "Neo4jBackup":
		var o neo4jv1beta1.Neo4jBackup
		if err := yaml.Unmarshal(raw, &o); err != nil {
			return s, err
		}
		s.backup = &o
	}
	return s, nil
}

// readManifests splits one input into YAML documents, the same way `validate`
// does — a manifest file holding several objects is how people actually write
// them.
func readManifests(source string) ([][]byte, error) {
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

	var docs [][]byte
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
		docs = append(docs, doc)
	}
	return docs, nil
}

// readLiveTargets serialises live resources back to YAML so that the manifest
// path and the live path run through exactly the same decoding and checks —
// two code paths would eventually disagree about what a spec means.
func readLiveTargets(ctx context.Context, c client.Client, ns, target string) ([][]byte, error) {
	wantKind, wantName := "", ""
	if target != "" {
		parts := strings.SplitN(target, "/", 2)
		wantKind, wantName = parts[0], parts[1]
	}

	var out [][]byte
	for _, gvk := range registeredNeo4jKinds(c) {
		if wantKind != "" && !strings.EqualFold(gvk.Kind, wantKind) {
			continue
		}
		// Only the kinds with cluster-side checks are worth fetching; the rest
		// would be decoded, found to have nothing to check, and reported as
		// skipped, which is noise when no target was named.
		switch gvk.Kind {
		case "Neo4jEnterpriseCluster", "Neo4jEnterpriseStandalone", "Neo4jBackup":
		default:
			continue
		}
		list, err := listAsYAML(ctx, c, gvk.Kind, ns, wantName)
		if err != nil {
			continue
		}
		out = append(out, list...)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out, nil
}

func listAsYAML(ctx context.Context, c client.Client, kind, ns, wantName string) ([][]byte, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(neo4jv1beta1.GroupVersion.WithKind(kind + "List"))
	if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
		return nil, err
	}
	var out [][]byte
	for i := range list.Items {
		item := &list.Items[i]
		if wantName != "" && item.GetName() != wantName {
			continue
		}
		body, err := yaml.Marshal(item.Object)
		if err != nil {
			continue
		}
		out = append(out, body)
	}
	return out, nil
}

func renderPreflight(results []preflightResult, stdout *os.File, ns string) int {
	if len(results) == 0 {
		fmt.Fprintf(stdout, "nothing to check in namespace %q\n", ns)
		return exitOK
	}

	problems := 0
	for i, r := range results {
		if i > 0 {
			fmt.Fprintln(stdout)
		}
		if r.problems() {
			problems++
		}

		head := fmt.Sprintf("%s/%s", r.kind, r.name)
		if r.source != "" && r.source != "cluster" {
			head += fmt.Sprintf(" (%s)", r.source)
		}
		fmt.Fprintln(stdout, head)

		if r.skipped != "" {
			fmt.Fprintf(stdout, "  %s %s\n", markWaiting, r.skipped)
			continue
		}
		if len(r.checks) == 0 {
			fmt.Fprintf(stdout, "  %s every cluster-side precondition passed\n", markWarning)
			continue
		}
		for _, ch := range r.checks {
			fmt.Fprintf(stdout, "  %s %s — %s\n", ch.mark, ch.subject, ch.what)
			if ch.detail != "" {
				fmt.Fprintf(stdout, "      %s\n", wrapIndent(ch.detail, 6))
			}
			if ch.action != "" {
				fmt.Fprintf(stdout, "      → %s\n", wrapIndent(ch.action, 8))
			}
		}
	}

	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Shape only: no bucket, registry or endpoint was contacted.")
	if problems > 0 {
		fmt.Fprintf(stdout, "%d of %d resource(s) would fail on a precondition.\n", problems, len(results))
		return exitProblems
	}
	return exitOK
}
