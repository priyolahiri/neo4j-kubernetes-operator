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

package controller

// When a Neo4jPlugin may write to its target's StatefulSet (operations.md ids
// 141, 152 and 155). A write restarts every server, so it waits until the
// target is Ready with every server available and has rolled out its latest
// spec; and the plugin is Ready only once the roll its own write caused is
// done. The one write that does not wait is the one that changes what every
// server that is down keeps failing in: the plugin's own init container, or
// the settings the plugin gives the neo4j container.

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// targetRolloutPending says what keeps the target from running its latest
// spec: "" when nothing does. Its controller must have recorded the spec
// (status.observedGeneration), and its StatefulSet must have no rollout in
// flight (statefulSetRolloutPending).
//
// Phase Ready alone is not enough. Between a spec change and the status write
// of the pass that acts on it, the target still reads Ready from the pass
// before: the cluster writes its StatefulSet first and turns Forming only once
// it has checked the cluster's formation, and the plugin, woken by that write,
// read the Ready in between (v1.22.0 journey: the plugin wrote its CA update in
// the second the trusted CAs changed, and the servers rolled twice at once). A
// standalone reads Ready while its one pod does, also when that pod is about
// to be replaced.
func (r *Neo4jPluginReconciler) targetRolloutPending(ctx context.Context, deployment *DeploymentInfo, generation, observedGeneration int64) string {
	if observedGeneration != generation {
		return fmt.Sprintf("%s has not acted on its latest spec yet", targetName(deployment))
	}
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts); err != nil {
		return fmt.Sprintf("cannot read StatefulSet %s: %v", r.getStatefulSetName(deployment), err)
	}
	return statefulSetRolloutPending(sts)
}

// statefulSetRolloutPending says what keeps sts from running its pod template
// on every replica, ready: "" when nothing does. Its controller must have seen
// the template (status.observedGeneration: the API server moves the
// generation on every template write, before the controller has looked), every
// replica must be on it (currentRevision becomes updateRevision once the last
// pod is replaced), and every replica ready.
//
// The servers a Paused or Failed upgrade holds below its partition count as a
// rollout in flight here, unlike for the cluster's own phase (operations.md id
// 149): a plugin written then would reach only the servers above the partition.
func statefulSetRolloutPending(sts *appsv1.StatefulSet) string {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	switch {
	case sts.Status.ObservedGeneration < sts.Generation:
		return fmt.Sprintf("StatefulSet %s has not rolled out its latest pod template yet", sts.Name)
	case sts.Status.CurrentRevision != sts.Status.UpdateRevision || sts.Status.UpdatedReplicas < replicas:
		return fmt.Sprintf("StatefulSet %s is rolling out its pod template (%d of %d pods updated)", sts.Name, sts.Status.UpdatedReplicas, replicas)
	case sts.Status.ReadyReplicas < replicas:
		return fmt.Sprintf("StatefulSet %s has %d of %d pods ready", sts.Name, sts.Status.ReadyReplicas, replicas)
	}
	return ""
}

// rolloutPending says what keeps the plugin's own write from having reached
// every server: the target's StatefulSet still rolling it out ("" when the roll
// is done), and the pods that cannot start (failing): those the plugin's init
// container keeps from starting, and those whose neo4j container keeps
// exiting. The old pods report Ready until they stop, so pod readiness alone
// let a plugin read Ready in the second it wrote (v1.22.0 journey: Ready at
// 09:04:09 CEST, the roll done at 09:07:43).
//
// written is the StatefulSet generation this pass's own write created (0: it
// wrote nothing). The read below comes from the informer cache, which can
// still hold the StatefulSet from before that write — fully rolled out — so a
// cached generation below it is a roll not even started (seen live on Kind:
// a plugin Ready in the second it wrote, while the server it broke
// crash-looped).
func (r *Neo4jPluginReconciler) rolloutPending(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo, written int64) (pending, failing string) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts); err != nil {
		return fmt.Sprintf("cannot read StatefulSet %s: %v", r.getStatefulSetName(deployment), err), ""
	}
	pending = statefulSetRolloutPending(sts)
	if sts.Generation < written {
		pending = fmt.Sprintf("StatefulSet %s has not rolled out its latest pod template yet", sts.Name)
	}
	if pending == "" {
		return pending, ""
	}
	name := resources.PluginInitContainerName(plugin.Spec.Name)
	verified := isVerifiedDownloadInstallMode(plugin)
	failure := func(pod *corev1.Pod) (string, bool) {
		if code, ok := initContainerFailure(pod, name); ok && verified {
			return failingPod(pod.Name, "init container "+name, code), true
		}
		if code, ok := containerFailure(pod, "neo4j"); ok && !isPodReady(pod) {
			return failingPod(pod.Name, "container neo4j", code), true
		}
		return "", false
	}
	var described []string
	for _, pod := range r.failingPods(ctx, sts, deployment, failure) {
		d, _ := failure(pod)
		described = append(described, d)
	}
	return pending, strings.Join(described, ", ")
}

// failingPods lists the pods of sts that failure says are failing, in ordinal
// order.
func (r *Neo4jPluginReconciler) failingPods(ctx context.Context, sts *appsv1.StatefulSet, deployment *DeploymentInfo, failure func(*corev1.Pod) (string, bool)) []*corev1.Pod {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(deployment.Namespace), client.MatchingLabels(r.getPodLabels(deployment))); err != nil {
		return nil
	}
	byOrdinal := map[int]*corev1.Pod{}
	var ordinals []int
	for i := range pods.Items {
		ordinal, ok := statefulSetOrdinal(sts.Name, pods.Items[i].Name)
		if !ok {
			continue
		}
		if _, failing := failure(&pods.Items[i]); failing {
			byOrdinal[ordinal] = &pods.Items[i]
			ordinals = append(ordinals, ordinal)
		}
	}
	sort.Ints(ordinals)
	out := make([]*corev1.Pod, 0, len(ordinals))
	for _, o := range ordinals {
		out = append(out, byOrdinal[o])
	}
	return out
}

// replaceStuckPods deletes, on a standalone, the pod the plugin's init
// container keeps from starting once the StatefulSet has a newer template for
// it, so the StatefulSet creates it again from that template. A standalone's
// StatefulSet uses OrderedReady pod management, which waits for a pod that is
// not Ready before it rolls a new template out — Kubernetes' documented
// "forced rollback" (StatefulSets, Rolling Updates): reverting the template is
// not enough, the broken pod must be deleted too. So the plugin's write that
// repairs the init container never reached the pod (checked on Kind v1.34: the
// pod stayed on the broken revision). A cluster's StatefulSet uses Parallel
// pod management and replaces such a pod itself, highest ordinal first.
//
// Deleted only while the StatefulSet's controller has seen its latest template
// (status.observedGeneration) and the pod is on an older revision than it: a
// pod deleted before that would come back on the broken template. The pod is
// not Ready, so nothing it served is lost; its volume claims stay.
//
// A pod whose neo4j container keeps exiting is stuck the same way once the
// plugin has written a change of its settings (operations.md id 155). It is
// deleted on the same conditions, while the plugin is not settled (a Ready
// plugin of this generation has written nothing to wait for) and the template
// changes the pod's env in variables the standalone does not render itself —
// what the plugin's write changed.
func (r *Neo4jPluginReconciler) replaceStuckPods(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) {
	if deployment.Type != "standalone" {
		return
	}
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts); err != nil {
		return
	}
	if sts.Spec.PodManagementPolicy == appsv1.ParallelPodManagement ||
		sts.Status.ObservedGeneration < sts.Generation || sts.Status.UpdateRevision == "" {
		return
	}
	name := resources.PluginInitContainerName(plugin.Spec.Name)
	verified := isVerifiedDownloadInstallMode(plugin)
	settled := plugin.Status.Phase == neo4jv1beta1.PhaseReady && plugin.Status.ObservedGeneration == plugin.Generation
	targetOwned := targetOwnedEnvNames(sts)
	stuck := func(pod *corev1.Pod) (string, bool) {
		if code, ok := initContainerFailure(pod, name); ok && verified {
			return fmt.Sprintf("init container %s exited with code %d", name, code), true
		}
		if settled {
			return "", false
		}
		code, ok := containerCrashLooping(pod, "neo4j")
		if !ok {
			return "", false
		}
		// What the template changes in the pod's env, other than what the
		// standalone renders itself.
		for _, n := range changedEnvNames(neo4jContainerEnv(pod.Spec), neo4jContainerEnv(sts.Spec.Template.Spec)) {
			if _, own := targetOwned[n]; !own {
				return fmt.Sprintf("container neo4j exited with code %d", code), true
			}
		}
		return "", false
	}
	for _, pod := range r.failingPods(ctx, sts, deployment, stuck) {
		if isPodReady(pod) || pod.DeletionTimestamp != nil || pod.Labels[appsv1.ControllerRevisionHashLabelKey] == sts.Status.UpdateRevision {
			continue
		}
		why, _ := stuck(pod)
		reason := EventReasonPluginInitContainerFailing
		if !strings.HasPrefix(why, "init container ") {
			reason = EventReasonPluginSettingsFailing
		}
		if err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil {
			log.FromContext(ctx).Error(err, "Cannot delete the pod that cannot start", "pod", pod.Name)
			continue
		}
		r.Recorder.Eventf(plugin, corev1.EventTypeNormal, reason,
			"Deleted pod %s of %s (%s) so that StatefulSet %s creates it again from revision %s: "+
				"an OrderedReady StatefulSet does not replace a pod that is not Ready",
			pod.Name, targetName(deployment), why, sts.Name, sts.Status.UpdateRevision)
	}
}

// selfRepair decides whether a write of the plugin may go to a target that is
// not Ready with every server available, and returns why: the pods it repairs,
// described ("" when it must wait), and whether they fail with the plugin's
// settings rather than in its init container. It may when the write changes
// what keeps the target so, and can cost no server that is still up:
//
//   - the write changes what the down pods fail in: a VerifiedDownload
//     plugin's init container or a volume that container mounts
//     (initContainerWriteChanges: a new checksum, URL, auth Secret or CA list),
//     or the env the plugin gives the neo4j container (a setting removed or
//     changed, operations.md id 155);
//   - every server pod that is not Ready is failing in it: in that init
//     container (initContainerFailure: it ran and exited non-zero, and has not
//     succeeded since), or, in its neo4j container, exited non-zero and not
//     running again (containerCrashLooping) while running a variable this
//     write changes (podRunsOtherSettings) — not a server still starting, nor
//     one down for a reason the write leaves as it is. No pod is missing or
//     being deleted;
//   - those pods are the highest ordinals, at or above the partition: the
//     StatefulSet controller replaces the highest-ordinal pod not on the new
//     template first, and the next only once that one is ready, so it replaces
//     the failing pods before it touches a server that is up;
//   - on a cluster, Neo4j's last view (status.diagnostics.servers) reports no
//     other server unavailable: a pod can read Ready while its server is out
//     of the cluster, and the roll after the repair goes by pod readiness.
//
// Without it, a plugin whose own init container kept a server from starting
// waited for that server for ever: the target stays Forming while the roll is
// stuck, and the gate waited for Ready (v1.22.0 journey: trusted CAs removed,
// the plugin Waiting with its CA volume still naming them, server-2 in
// Init:Error until the CA Secret was deleted by hand). A setting Neo4j refuses
// at startup did the same (v1.22.0 journey: the GenAI example's
// genai.vector.enabled kept server-2 crash-looping on 2026.08.1, and only the
// plugin's write takes it away).
func (r *Neo4jPluginReconciler) selfRepair(ctx context.Context, plugin *neo4jv1beta1.Neo4jPlugin, deployment *DeploymentInfo) (string, bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: r.getStatefulSetName(deployment), Namespace: deployment.Namespace}, sts); err != nil {
		return "", false, client.IgnoreNotFound(err)
	}
	name := resources.PluginInitContainerName(plugin.Spec.Name)
	desired := sts.DeepCopy()
	if _, err := r.applyPlugin(ctx, desired, plugin, deployment); err != nil {
		return "", false, err
	}
	initChanges := isVerifiedDownloadInstallMode(plugin) &&
		initContainerWriteChanges(sts.Spec.Template.Spec, desired.Spec.Template.Spec, name)
	want := neo4jContainerEnv(desired.Spec.Template.Spec)
	settings := changedEnvNames(neo4jContainerEnv(sts.Spec.Template.Spec), want)
	if !initChanges && len(settings) == 0 {
		return "", false, nil
	}
	wantByName := envByName(want)
	inSettings := false
	failure := func(pod *corev1.Pod) (string, bool) {
		if code, ok := initContainerFailure(pod, name); ok && initChanges {
			return failingPod(pod.Name, "init container "+name, code), true
		}
		if code, ok := containerCrashLooping(pod, "neo4j"); ok && podRunsOtherSettings(pod, wantByName, settings) {
			inSettings = true
			return failingPod(pod.Name, "container neo4j", code), true
		}
		return "", false
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(deployment.Namespace), client.MatchingLabels(r.getPodLabels(deployment))); err != nil {
		return "", false, err
	}
	failing, down := podsDownOnlyIn(sts, pods.Items, failure)
	if len(down) == 0 {
		return "", false, nil
	}
	if cluster, ok := deployment.Object.(*neo4jv1beta1.Neo4jEnterpriseCluster); ok {
		if other := unavailableServerOutside(cluster, down); other != "" {
			log.FromContext(ctx).Info("Not repairing what keeps the servers down: another server is unavailable", "server", other)
			return "", false, nil
		}
	}
	return strings.Join(failing, ", "), inSettings, nil
}

// podsDownOnlyIn returns the pods of sts that are down — described, and by
// name — when failure says every one of them fails in what the write changes
// and they are the highest ordinals at or above the partition; nothing
// otherwise.
func podsDownOnlyIn(sts *appsv1.StatefulSet, pods []corev1.Pod, failure func(*corev1.Pod) (string, bool)) ([]string, map[string]bool) {
	replicas := 1
	if sts.Spec.Replicas != nil {
		replicas = int(*sts.Spec.Replicas)
	}
	partition := 0
	if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil {
		partition = int(*ru.Partition)
	}
	byOrdinal := make(map[int]*corev1.Pod, len(pods))
	for i := range pods {
		if ordinal, ok := statefulSetOrdinal(sts.Name, pods[i].Name); ok && ordinal < replicas {
			byOrdinal[ordinal] = &pods[i]
		}
	}
	var failing []string
	down := map[string]bool{}
	upAbove := false
	for ordinal := replicas - 1; ordinal >= 0; ordinal-- {
		pod := byOrdinal[ordinal]
		if pod == nil || pod.DeletionTimestamp != nil {
			return nil, nil
		}
		if isPodReady(pod) {
			upAbove = true
			continue
		}
		described, ok := failure(pod)
		if !ok || upAbove || ordinal < partition {
			return nil, nil
		}
		failing = append(failing, described)
		down[pod.Name] = true
	}
	return failing, down
}

// unavailableServerOutside names a server the cluster's last diagnostics
// report as not Available whose pod is not among down; "" when there is none,
// or no diagnostics to read (spec.monitoring.enabled: false). A server whose
// address names no pod counts as outside.
func unavailableServerOutside(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, down map[string]bool) string {
	if cluster.Status.Diagnostics == nil {
		return ""
	}
	for _, s := range cluster.Status.Diagnostics.Servers {
		if s.Health == "Available" {
			continue
		}
		host := s.Address
		if h, _, err := net.SplitHostPort(s.Address); err == nil {
			host = h
		}
		if pod := strings.SplitN(host, ".", 2)[0]; pod == "" || !down[pod] {
			return fmt.Sprintf("%s (%s, %s)", s.Name, s.Address, s.Health)
		}
	}
	return ""
}

// initContainerFailure reports whether the pod's init container name is
// failing, with the exit code of its last run: it ran and exited non-zero, and
// has not succeeded since — Init:Error, or Init:CrashLoopBackOff waiting to
// run again (or running again) after a failed run.
func initContainerFailure(pod *corev1.Pod, name string) (int32, bool) {
	for _, s := range pod.Status.InitContainerStatuses {
		if s.Name != name {
			continue
		}
		if t := s.State.Terminated; t != nil {
			return t.ExitCode, t.ExitCode != 0
		}
		if t := s.LastTerminationState.Terminated; t != nil && t.ExitCode != 0 {
			return t.ExitCode, true
		}
		return 0, false
	}
	return 0, false
}

// containerFailure reports whether the pod's container name, not ready, has
// failed and not succeeded since, with the exit code of its last failed run:
// it exited non-zero and waits to run again (CrashLoopBackOff), or runs again
// after a failed run without being ready yet. What a status message names.
func containerFailure(pod *corev1.Pod, name string) (int32, bool) {
	for _, s := range pod.Status.ContainerStatuses {
		if s.Name != name || s.Ready {
			continue
		}
		if t := s.State.Terminated; t != nil {
			return t.ExitCode, t.ExitCode != 0
		}
		if t := s.LastTerminationState.Terminated; t != nil && t.ExitCode != 0 {
			return t.ExitCode, true
		}
		return 0, false
	}
	return 0, false
}

// containerCrashLooping is containerFailure for a container that is not
// running now: it exited non-zero and has not started again
// (Terminated, or Waiting — CrashLoopBackOff — after a failed run). A
// container running again may be starting fine, and one that failed long ago
// and is not ready for another reason must not count.
func containerCrashLooping(pod *corev1.Pod, name string) (int32, bool) {
	for _, s := range pod.Status.ContainerStatuses {
		if s.Name != name || s.State.Running != nil {
			continue
		}
		return containerFailure(&corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{s}}}, name)
	}
	return 0, false
}

// podRunsOtherSettings reports whether the pod's neo4j container runs, for one
// of names, another variable (or none) than want, the neo4j container's env
// after the plugin's edits: whether the write changes what that pod runs with.
func podRunsOtherSettings(pod *corev1.Pod, want map[string]corev1.EnvVar, names []string) bool {
	have := envByName(neo4jContainerEnv(pod.Spec))
	for _, name := range names {
		h, inPod := have[name]
		w, inWant := want[name]
		if inPod != inWant || (inPod && !equality.Semantic.DeepEqual(h, w)) {
			return true
		}
	}
	return false
}

// initContainerWriteChanges reports whether desired, the live pod spec after
// the plugin's edits, differs from live in the init container name or in a
// volume that container mounts in either.
func initContainerWriteChanges(live, desired corev1.PodSpec, name string) bool {
	have, inLive := podInitContainer(live, name)
	want, inDesired := podInitContainer(desired, name)
	if inLive != inDesired || !equality.Semantic.DeepEqual(have, want) {
		return true
	}
	for _, c := range []corev1.Container{have, want} {
		for _, m := range c.VolumeMounts {
			i, j := volumeIndex(&live, m.Name), volumeIndex(&desired, m.Name)
			if (i < 0) != (j < 0) || (i >= 0 && !equality.Semantic.DeepEqual(live.Volumes[i], desired.Volumes[j])) {
				return true
			}
		}
	}
	return false
}

// podInitContainer returns the pod's init container name.
func podInitContainer(pod corev1.PodSpec, name string) (corev1.Container, bool) {
	for _, c := range pod.InitContainers {
		if c.Name == name {
			return c, true
		}
	}
	return corev1.Container{}, false
}

// statefulSetOrdinal returns the ordinal of a pod of the StatefulSet sts
// ({sts}-{ordinal}).
func statefulSetOrdinal(sts, pod string) (int, bool) {
	suffix, ok := strings.CutPrefix(pod, sts+"-")
	if !ok {
		return 0, false
	}
	ordinal, err := strconv.Atoi(suffix)
	if err != nil || ordinal < 0 || strconv.Itoa(ordinal) != suffix {
		return 0, false
	}
	return ordinal, true
}

// failingPod describes a pod that cannot start because a container fails;
// container says which ("init container plugin-init-apoc", "container neo4j").
func failingPod(pod, container string, code int32) string {
	return fmt.Sprintf("%s (%s exited with code %d)", pod, container, code)
}
