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

// diagnose crosses the boundary `status` stops at.
//
// `status` reads custom resources, and `explain` decodes the operator's own
// condition and phase vocabulary. But the failure taxonomy in
// docs/user_guide/guides/troubleshooting.md is dominated by things one layer
// BELOW the CR — pods that will not schedule, containers OOMKilled at 1Gi,
// image pulls that fail, PVCs that never bind. Today `status` reports
// "Pending / not ready" for every one of those and the user is handed back to
// a 98-line runbook to work out which it was.
//
// This command answers that question. It reads the CR, finds the workload the
// operator built for it, and names the Kubernetes-level cause.
//
// # Why this does not drift the way a prose runbook does
//
// Every rule below is anchored to a fact the Kubernetes API defines, not to a
// string this project made up. Where client-go exports a constant it is used —
// corev1.PodScheduled, corev1.PodReasonUnschedulable, corev1.ClaimBound — so a
// rename upstream fails this build rather than silently matching nothing.
//
// Three reasons have no exported constant because kubelet, not the API, owns
// them: "CrashLoopBackOff", "ImagePullBackOff" and "OOMKilled". Those are
// matched as literals, and the OOM check additionally matches exit code 137 so
// that the single most common Neo4j Enterprise failure is not detected by a
// string alone.
//
// Workloads are located through the operator's OWN exported selectors
// (resources.ServerPodSelector and friends) rather than by rebuilding the
// label scheme here — the same discipline that makes `validate` share the
// operator's validators.

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// exitProblems is diagnose's "I found something" code. It shares the value of
// exitInvalid deliberately: to a CI consumer both mean "the command ran fine
// and the answer is bad", which is the distinction the exit code exists to
// carry. 2 stays reserved for "the command could not run at all".
const exitProblems = exitInvalid

// Severity markers reuse validate's and status's vocabulary so that the three
// commands do not each teach the user a different alphabet:
//
//	✗  something is wrong and will not fix itself
//	…  waiting on something that may still resolve on its own
//	⚠  worth knowing, not itself a failure
const (
	markProblem = "✗"
	markWaiting = "…"
	markWarning = "⚠"
)

// noStatusGrace is how long a resource may carry no phase at all before that
// silence is itself reported. A CR the operator has never written status to is
// the visible symptom of the operator not running, not having RBAC for the
// kind, or not watching this namespace — a failure mode with no other signal,
// since nothing is broken, there is simply nothing there.
const noStatusGrace = 2 * time.Minute

type symptom struct {
	mark    string
	subject string // "pod prod-server-2"
	what    string // short observation
	detail  string // the cluster's own words, verbatim
	action  string // what the user should do
}

type diagnosis struct {
	kind     string
	name     string
	phase    string
	ready    string
	message  string
	summary  string // "2/3 servers ready"
	symptoms []symptom
}

// problems reports whether this diagnosis contains anything that will not fix
// itself. Waiting and warning symptoms deliberately do not count: exiting
// non-zero because a pod is 20 seconds into its readiness probe would make the
// exit code useless in the CI loop it exists for.
func (d diagnosis) problems() bool {
	for _, f := range d.symptoms {
		if f.mark == markProblem {
			return true
		}
	}
	return false
}

func runDiagnose(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("diagnose", flag.ContinueOnError)
	fs.SetOutput(stderr)
	namespace := namespaceFlag(fs, "Namespace to inspect (default: the kubeconfig context's namespace)")
	kubeContext := fs.String("context", "", "Kubeconfig context to use")
	kubeconfig := fs.String("kubeconfig", "", "Path to the kubeconfig file")
	quiet := fs.Bool("quiet", false, "Print only resources that have something to report")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Explain why a Neo4j resource is not healthy, at the Kubernetes level.

Usage:
  kubectl neo4j diagnose [<Kind>/<name>] [-n <namespace>]

Where "status" reports that a resource is Pending, this reports WHY: the pod
that will not schedule, the container that was OOMKilled, the image that will
not pull, the PVC that never bound. Read-only.

Examples:
  kubectl neo4j diagnose
  kubectl neo4j diagnose Neo4jEnterpriseCluster/prod -n neo4j

Exit codes: 0 when nothing is wrong, 1 when a problem was found, 2 on a usage
or connection error. Things that may still resolve on their own (a readiness
probe that has not passed yet) are reported but do not change the exit code.

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

	target := fs.Arg(0)
	if target != "" && !strings.Contains(target, "/") {
		fmt.Fprintf(stderr, "error: expected <Kind>/<name>, got %q\n", target)
		return exitUsage
	}

	results, err := diagnoseNamespace(context.Background(), c, ns, target)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitUsage
	}
	if target != "" && len(results) == 0 {
		fmt.Fprintf(stderr, "error: %s not found in namespace %q\n", target, ns)
		return exitUsage
	}

	return renderDiagnosis(results, stdout, ns, *quiet)
}

// diagnoseNamespace builds a diagnosis for every Neo4j resource in a namespace,
// or for the single <Kind>/<name> given in target.
func diagnoseNamespace(ctx context.Context, c client.Client, ns, target string) ([]diagnosis, error) {
	wantKind, wantName := "", ""
	if target != "" {
		parts := strings.SplitN(target, "/", 2)
		wantKind, wantName = parts[0], parts[1]
	}

	// Events, pods, PVCs and workloads are each listed ONCE for the namespace
	// rather than per resource. A namespace with a dozen CRs would otherwise
	// issue dozens of identical LISTs, and a diagnostic command is most often
	// run against a cluster that is already struggling.
	env := loadNamespaceEnv(ctx, c, ns)

	var out []diagnosis
	for _, gvk := range registeredNeo4jKinds(c) {
		if wantKind != "" && !strings.EqualFold(gvk.Kind, wantKind) {
			continue
		}
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
		if err := c.List(ctx, list, client.InNamespace(ns)); err != nil {
			// Not installed, or not readable by this user. Reporting what IS
			// visible beats failing the whole command, exactly as in `status`.
			continue
		}
		for i := range list.Items {
			item := &list.Items[i]
			if wantName != "" && item.GetName() != wantName {
				continue
			}
			out = append(out, diagnoseResource(item, env))
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].kind != out[j].kind {
			return out[i].kind < out[j].kind
		}
		return out[i].name < out[j].name
	})
	return out, nil
}

// namespaceEnv is everything below the CR layer, read once.
type namespaceEnv struct {
	pods         []corev1.Pod
	pvcs         []corev1.PersistentVolumeClaim
	statefulSets []appsv1.StatefulSet
	jobs         []batchv1.Job
	events       []corev1.Event
	operator     operatorLocation
}

// operatorLocation is where the operator Deployment actually runs.
//
// The guidance used to hard-code `-n neo4j-operator`, so on any install whose
// namespace differs — `make dev-up` uses neo4j-operator-dev, and a
// namespace-scoped install can use anything — the command handed the user a
// copy-paste line that fails. That is a bad thing to get wrong precisely here,
// since these symptoms fire when the operator is suspected of not running.
type operatorLocation struct {
	namespace string
	name      string
}

// findOperator locates the operator Deployment by the label it ships with. It
// is best-effort: a user who cannot list Deployments cluster-wide still gets
// the guidance, just with the conventional default filled in.
func findOperator(ctx context.Context, c client.Client) operatorLocation {
	fallback := operatorLocation{namespace: "neo4j-operator", name: "neo4j-operator-controller-manager"}
	var deployments appsv1.DeploymentList
	if err := c.List(ctx, &deployments,
		client.MatchingLabels{"app.kubernetes.io/name": "neo4j-operator"}); err != nil {
		return fallback
	}
	for i := range deployments.Items {
		d := &deployments.Items[i]
		return operatorLocation{namespace: d.Namespace, name: d.Name}
	}
	return fallback
}

// operatorLogsCommand renders the command that actually works on this cluster.
func operatorLogsCommand(op operatorLocation) string {
	if op.namespace == "" || op.name == "" {
		op = operatorLocation{namespace: "neo4j-operator", name: "neo4j-operator-controller-manager"}
	}
	return fmt.Sprintf("kubectl logs -n %s deployment/%s", op.namespace, op.name)
}

func loadNamespaceEnv(ctx context.Context, c client.Client, ns string) namespaceEnv {
	var env namespaceEnv
	env.operator = findOperator(ctx, c)
	// Each list is independently optional: a user with partial RBAC (able to
	// read CRs but not Pods, say) should still get everything else rather than
	// an error, so failures are dropped rather than propagated.
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(ns)); err == nil {
		env.pods = pods.Items
	}
	var pvcs corev1.PersistentVolumeClaimList
	if err := c.List(ctx, &pvcs, client.InNamespace(ns)); err == nil {
		env.pvcs = pvcs.Items
	}
	var sts appsv1.StatefulSetList
	if err := c.List(ctx, &sts, client.InNamespace(ns)); err == nil {
		env.statefulSets = sts.Items
	}
	var jobs batchv1.JobList
	if err := c.List(ctx, &jobs, client.InNamespace(ns)); err == nil {
		env.jobs = jobs.Items
	}
	var events corev1.EventList
	if err := c.List(ctx, &events, client.InNamespace(ns)); err == nil {
		env.events = events.Items
	}
	return env
}

func diagnoseResource(obj *unstructured.Unstructured, env namespaceEnv) diagnosis {
	row := rowFrom(obj) // same generic status read as `status`
	d := diagnosis{
		kind:    row.kind,
		name:    row.name,
		phase:   row.phase,
		ready:   row.ready,
		message: row.message,
	}

	switch obj.GetKind() {
	case "Neo4jEnterpriseCluster":
		d.summary, d.symptoms = diagnoseInstance(obj.GetName(),
			resources.ServerPodSelector(obj.GetName()), env)
	case "Neo4jEnterpriseStandalone":
		d.summary, d.symptoms = diagnoseInstance(obj.GetName(),
			resources.StandalonePodSelector(obj.GetName()), env)
	case "Neo4jBackup":
		d.summary, d.symptoms = diagnoseBackup(obj.GetName(), env)
	}

	d.symptoms = append(d.symptoms, warningEvents(obj, env)...)
	d.symptoms = append(d.symptoms, missingStatusSymptom(obj, row, env.operator)...)
	return d
}

// diagnoseInstance covers the two kinds that own a StatefulSet of Neo4j pods.
func diagnoseInstance(name string, selector map[string]string, env namespaceEnv) (string, []symptom) {
	pods := selectPods(env.pods, selector)
	pvcs := selectPVCs(env.pvcs, resources.PVCSelectorByInstance(name))

	var symptoms []symptom
	unschedulable := false
	for i := range pods {
		podSymptoms := diagnosePod(&pods[i])
		if podIsUnschedulable(&pods[i]) {
			unschedulable = true
		}
		symptoms = append(symptoms, podSymptoms...)
	}
	symptoms = append(symptoms, diagnosePVCs(pvcs, unschedulable)...)

	summary := ""
	if sts := findStatefulSetFor(env.statefulSets, selector); sts != nil {
		desired := int32(1)
		if sts.Spec.Replicas != nil {
			desired = *sts.Spec.Replicas
		}
		summary = fmt.Sprintf("%d/%d pods ready", sts.Status.ReadyReplicas, desired)
		// Zero pods for a StatefulSet that wants some is a distinct failure
		// from pods that exist and are unhealthy: nothing was ever scheduled,
		// so there is no pod-level evidence to read and the user would
		// otherwise see an empty diagnosis under a "not ready" heading.
		if len(pods) == 0 && desired > 0 {
			symptoms = append(symptoms, symptom{
				mark:    markProblem,
				subject: "statefulset " + sts.Name,
				what:    fmt.Sprintf("wants %d pod(s), none exist", desired),
				action: "No pod was created at all. Check the StatefulSet's own events " +
					"(kubectl describe statefulset " + sts.Name + ") — a rejected pod " +
					"template, a missing ServiceAccount or a quota denial stops creation " +
					"before any pod appears.",
			})
		}
	} else if len(pods) == 0 {
		symptoms = append(symptoms, symptom{
			mark:    markProblem,
			subject: name,
			what:    "no StatefulSet and no pods",
			action: "The operator has not built the workload yet. Check that it is running " +
				"and watching this namespace: " + operatorLogsCommand(env.operator),
		})
	}
	return summary, symptoms
}

// diagnosePod is the heart of the command: one pod's Kubernetes-level state,
// turned into a cause and an action.
func diagnosePod(p *corev1.Pod) []symptom {
	var symptoms []symptom

	// 1. Scheduling. PodScheduled=False with reason Unschedulable is the
	// "Pods Stuck in Pending State" heading of the troubleshooting guide, and
	// the scheduler's own message names the exact shortfall.
	for _, cond := range p.Status.Conditions {
		if cond.Type != corev1.PodScheduled || cond.Status != corev1.ConditionFalse {
			continue
		}
		if cond.Reason == corev1.PodReasonUnschedulable {
			symptoms = append(symptoms, symptom{
				mark:    markProblem,
				subject: "pod " + p.Name,
				what:    "cannot be scheduled",
				detail:  cond.Message,
				action: "No node can satisfy the pod. Compare spec.resources.requests with " +
					"node capacity (kubectl describe nodes), or check that the PVC's " +
					"StorageClass can provision in the zones the nodes are in. Neo4j " +
					"Enterprise will not start below 1.5Gi of memory, so lowering the " +
					"request has a floor.",
			})
		}
	}

	// 2. Container state, current and previous.
	for _, cs := range p.Status.ContainerStatuses {
		if w := cs.State.Waiting; w != nil {
			switch w.Reason {
			case "ImagePullBackOff", "ErrImagePull", "InvalidImageName":
				symptoms = append(symptoms, symptom{
					mark:    markProblem,
					subject: "container " + cs.Name + " in " + p.Name,
					what:    "image cannot be pulled (" + w.Reason + ")",
					detail:  w.Message,
					action: "Check spec.image.repo and spec.image.tag. Neo4j Enterprise images " +
						"are not public: the namespace needs an imagePullSecret unless the " +
						"registry allows anonymous pulls.",
				})
			case "CrashLoopBackOff":
				symptoms = append(symptoms, symptom{
					mark:    markProblem,
					subject: "container " + cs.Name + " in " + p.Name,
					what:    fmt.Sprintf("crash-looping after %d restart(s)", cs.RestartCount),
					detail:  w.Message,
					action: "The reason is in the log of the run that died, not the current one: " +
						"kubectl logs " + p.Name + " -c " + cs.Name + " --previous",
				})
			case "CreateContainerConfigError":
				symptoms = append(symptoms, symptom{
					mark:    markProblem,
					subject: "container " + cs.Name + " in " + p.Name,
					what:    "container config is not resolvable",
					detail:  w.Message,
					action: "Usually a Secret or ConfigMap referenced by env valueFrom does not " +
						"exist yet, or lacks the key named. The message above says which.",
				})
			}
		}

		// OOMKilled is matched on the kubelet's reason string OR exit code 137
		// because it is the single most common Neo4j Enterprise failure on an
		// under-provisioned cluster, and neither signal is an API constant we
		// can lean on alone.
		if t := cs.LastTerminationState.Terminated; t != nil {
			if t.Reason == "OOMKilled" || t.ExitCode == 137 {
				symptoms = append(symptoms, symptom{
					mark:    markProblem,
					subject: "container " + cs.Name + " in " + p.Name,
					what:    fmt.Sprintf("was OOMKilled (exit %d)", t.ExitCode),
					action: "Raise spec.resources.limits.memory. Neo4j Enterprise needs at least " +
						"1.5Gi to start at all, and the JVM heap plus page cache must fit " +
						"inside the limit — see docs/user_guide/guides/resource_sizing.md.",
				})
			} else if t.ExitCode != 0 {
				symptoms = append(symptoms, symptom{
					mark:    markWarning,
					subject: "container " + cs.Name + " in " + p.Name,
					what:    fmt.Sprintf("previously exited %d (%s)", t.ExitCode, t.Reason),
					detail:  strings.TrimSpace(t.Message),
					action:  "kubectl logs " + p.Name + " -c " + cs.Name + " --previous",
				})
			}
		}

		// A container that is running but not ready is usually Neo4j still
		// starting — reported so the user knows the pod is not stuck, but NOT
		// as a problem, because it commonly resolves without intervention.
		if cs.State.Running != nil && !cs.Ready {
			symptoms = append(symptoms, symptom{
				mark:    markWaiting,
				subject: "container " + cs.Name + " in " + p.Name,
				what:    "running but not ready",
				action: "Its readiness probe has not passed yet. Neo4j Enterprise takes tens of " +
					"seconds to open Bolt on first start; if this persists, read the log.",
			})
		}
	}
	return symptoms
}

// podIsUnschedulable reports the scheduler's own verdict, which decides whether
// an unbound PVC is a cause or merely a consequence.
func podIsUnschedulable(p *corev1.Pod) bool {
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodScheduled &&
			cond.Status == corev1.ConditionFalse &&
			cond.Reason == corev1.PodReasonUnschedulable {
			return true
		}
	}
	return false
}

// diagnosePVCs reports claims that never bound.
//
// unschedulable changes what an unbound claim MEANS. The common
// WaitForFirstConsumer binding mode holds a claim Pending on purpose until a
// pod is scheduled, so when the pod cannot be scheduled the claim is a
// downstream effect, not a second fault. Reporting it as its own problem sent
// users to check storage when the actual cause was memory — so it is still
// shown (it is real, and hiding it would be worse) but marked as pending and
// pointed back at the scheduling failure.
func diagnosePVCs(pvcs []corev1.PersistentVolumeClaim, unschedulable bool) []symptom {
	var symptoms []symptom
	for i := range pvcs {
		pvc := &pvcs[i]
		if pvc.Status.Phase == corev1.ClaimBound {
			continue
		}
		if unschedulable {
			symptoms = append(symptoms, symptom{
				mark:    markWaiting,
				subject: "pvc " + pvc.Name,
				what:    fmt.Sprintf("is %s, not Bound", pvc.Status.Phase),
				action: "Expected while the pod cannot be scheduled: a WaitForFirstConsumer " +
					"StorageClass binds only once a pod is placed. Fix the scheduling problem " +
					"above and this should bind on its own.",
			})
			continue
		}
		sc := "(cluster default)"
		if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
			sc = *pvc.Spec.StorageClassName
		}
		symptoms = append(symptoms, symptom{
			mark:    markProblem,
			subject: "pvc " + pvc.Name,
			what:    fmt.Sprintf("is %s, not Bound", pvc.Status.Phase),
			action: "The pod cannot start until this binds. StorageClass " + sc +
				" must exist and be able to provision — kubectl get storageclass, then " +
				"kubectl describe pvc " + pvc.Name + " for the provisioner's own reason.",
		})
	}
	return symptoms
}

// diagnoseBackup covers Neo4jBackup, whose workload is a Job rather than a
// StatefulSet. Backup failures are their own chapter of the troubleshooting
// guide, and the useful evidence is on the Job and its pod.
func diagnoseBackup(name string, env namespaceEnv) (string, []symptom) {
	jobs := selectJobs(env.jobs, resources.BackupJobSelector(name))
	if len(jobs) == 0 {
		return "", nil
	}

	var symptoms []symptom
	var succeeded, failed int
	for i := range jobs {
		j := &jobs[i]
		switch {
		case j.Status.Failed > 0:
			failed++
			f := symptom{
				mark:    markProblem,
				subject: "job " + j.Name,
				what:    fmt.Sprintf("failed (%d pod failure(s))", j.Status.Failed),
				action: "Read the Job's pod log for neo4j-admin's own error: " +
					"kubectl logs job/" + j.Name + ". A permission error names the " +
					"ServiceAccount; a path error names the bucket.",
			}
			for _, cond := range j.Status.Conditions {
				if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
					f.detail = strings.TrimSpace(cond.Reason + ": " + cond.Message)
				}
			}
			symptoms = append(symptoms, f)
		case j.Status.Succeeded > 0:
			succeeded++
		}
	}

	// Pods of a backup Job get the same container-level treatment as server
	// pods: an OOMKilled or unschedulable backup pod fails for exactly the
	// reasons a server pod does, and the user needs the same answer.
	for i := range env.pods {
		p := &env.pods[i]
		if matchesLabels(p.Labels, resources.BackupJobSelector(name)) {
			symptoms = append(symptoms, diagnosePod(p)...)
		}
	}

	return fmt.Sprintf("%d job(s): %d succeeded, %d failed", len(jobs), succeeded, failed), symptoms
}

// warningEvents surfaces Warning events the API server recorded against this
// exact object. Events explain far more about a stuck reconcile than status
// does, and the operator emits structured ones (internal/controller/events.go)
// precisely so they can be read this way.
func warningEvents(obj *unstructured.Unstructured, env namespaceEnv) []symptom {
	var recent []corev1.Event
	for i := range env.events {
		e := env.events[i]
		if e.Type != corev1.EventTypeWarning {
			continue
		}
		if e.InvolvedObject.Kind != obj.GetKind() || e.InvolvedObject.Name != obj.GetName() {
			continue
		}
		recent = append(recent, e)
	}
	sort.Slice(recent, func(i, j int) bool {
		return recent[i].LastTimestamp.After(recent[j].LastTimestamp.Time)
	})
	// Only the newest few: a resource that has been failing for a day can
	// carry hundreds of near-identical events, and printing them all buries
	// the pod-level symptoms that are the point of this command.
	if len(recent) > 3 {
		recent = recent[:3]
	}
	var symptoms []symptom
	for _, e := range recent {
		symptoms = append(symptoms, symptom{
			mark:    markWarning,
			subject: "event " + e.Reason,
			what:    fmt.Sprintf("%s ago", humanAge(e.LastTimestamp.Time)),
			detail:  e.Message,
		})
	}
	return symptoms
}

// missingStatusSymptom reports a resource the operator has never written status
// to. There is no other signal for this: nothing is broken, the CR simply sits
// there — which is what a user sees when the operator is not running, cannot
// read the kind, or is not watching this namespace.
func missingStatusSymptom(obj *unstructured.Unstructured, row resourceStatus, op operatorLocation) []symptom {
	if row.phase != "-" || row.ready != "-" {
		return nil
	}
	created := obj.GetCreationTimestamp().Time
	if created.IsZero() || time.Since(created) < noStatusGrace {
		return nil
	}
	return []symptom{{
		mark:    markProblem,
		subject: obj.GetKind() + "/" + obj.GetName(),
		what:    "has no status after " + humanAge(created),
		action: "Nothing has reconciled it. Check the operator is running, has RBAC for " +
			"this kind, and is watching this namespace (a namespace-scoped install " +
			"ignores resources outside WATCH_NAMESPACE without reporting anything): " +
			operatorLogsCommand(op),
	}}
}

func selectPods(pods []corev1.Pod, selector map[string]string) []corev1.Pod {
	var out []corev1.Pod
	for i := range pods {
		if matchesLabels(pods[i].Labels, selector) {
			out = append(out, pods[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func selectPVCs(pvcs []corev1.PersistentVolumeClaim, selector map[string]string) []corev1.PersistentVolumeClaim {
	var out []corev1.PersistentVolumeClaim
	for i := range pvcs {
		if matchesLabels(pvcs[i].Labels, selector) {
			out = append(out, pvcs[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func selectJobs(jobs []batchv1.Job, selector map[string]string) []batchv1.Job {
	var out []batchv1.Job
	for i := range jobs {
		if matchesLabels(jobs[i].Labels, selector) {
			out = append(out, jobs[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func findStatefulSetFor(sets []appsv1.StatefulSet, selector map[string]string) *appsv1.StatefulSet {
	for i := range sets {
		if matchesLabels(sets[i].Spec.Template.Labels, selector) {
			return &sets[i]
		}
	}
	return nil
}

// matchesLabels reports whether labels contains every key/value in selector —
// the same subset semantics client.MatchingLabels uses server-side, kept here
// because the lists are fetched once and filtered in memory.
func matchesLabels(labels, selector map[string]string) bool {
	if len(selector) == 0 {
		return false
	}
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func renderDiagnosis(results []diagnosis, stdout *os.File, ns string, quiet bool) int {
	shown := 0
	problems := 0
	for _, d := range results {
		if d.problems() {
			problems++
		}
		if quiet && len(d.symptoms) == 0 {
			continue
		}
		if shown > 0 {
			fmt.Fprintln(stdout)
		}
		shown++
		renderOne(d, stdout)
	}

	if shown == 0 {
		if len(results) == 0 {
			fmt.Fprintf(stdout, "no Neo4j resources found in namespace %q\n", ns)
		} else {
			fmt.Fprintf(stdout, "%d Neo4j resource(s) in %q, nothing to report\n", len(results), ns)
		}
		return exitOK
	}

	if problems > 0 {
		fmt.Fprintf(stdout, "\n%d of %d resource(s) have problems.\n", problems, len(results))
		fmt.Fprintln(stdout, "For an archive to attach to an issue: kubectl neo4j support-bundle")
		return exitProblems
	}
	return exitOK
}

func renderOne(d diagnosis, stdout *os.File) {
	head := fmt.Sprintf("%s/%s — %s", d.kind, d.name, d.phase)
	if d.summary != "" {
		head += " · " + d.summary
	}
	fmt.Fprintln(stdout, head)
	if d.message != "" {
		fmt.Fprintf(stdout, "  %s\n", d.message)
	}
	if len(d.symptoms) == 0 {
		fmt.Fprintln(stdout, "  nothing to report below the resource level")
		return
	}
	for _, f := range d.symptoms {
		fmt.Fprintf(stdout, "  %s %s — %s\n", f.mark, f.subject, f.what)
		if f.detail != "" {
			fmt.Fprintf(stdout, "      %s\n", wrapIndent(f.detail, 6))
		}
		if f.action != "" {
			fmt.Fprintf(stdout, "      → %s\n", wrapIndent(f.action, 8))
		}
	}
}

// wrapIndent soft-wraps long text at 78 columns so a scheduler message or an
// action does not run off the terminal. Continuation lines are indented to line
// up under the first, which is what makes a multi-line action readable at all.
func wrapIndent(s string, indent int) string {
	s = strings.Join(strings.Fields(s), " ")
	const width = 78
	if len(s)+indent <= width {
		return s
	}
	pad := strings.Repeat(" ", indent)
	var b strings.Builder
	line := 0
	for i, word := range strings.Fields(s) {
		switch {
		case i == 0:
			b.WriteString(word)
			line = indent + len(word)
		case line+1+len(word) > width:
			b.WriteString("\n" + pad + word)
			line = indent + len(word)
		default:
			b.WriteString(" " + word)
			line += 1 + len(word)
		}
	}
	return b.String()
}
