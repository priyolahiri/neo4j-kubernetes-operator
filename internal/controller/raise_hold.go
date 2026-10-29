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

// A raise of spec.topology.servers waits for what is left of the servers a
// scale-down removed (operations.md id 229).
//
// A drain ends with every removed server dropped; the pass that finds that
// releases the replica hold and lowers the replicas. The StatefulSet
// controller then deletes the removed pods, and the garbage collector their
// volumes (persistentVolumeClaimRetentionPolicy.whenScaled: Delete makes each
// volume the condemned pod's). That takes seconds — a pod's shutdown, then the
// collector. A raise in those seconds put the replicas back up while they
// were there: the StatefulSet controller took the volumes back for the new
// pods (a volume whose pod is in range again is retained), the servers
// started with the identities Neo4j had dropped, which cannot rejoin, and the
// cluster stayed Forming (v1.24.0 journey, and v1.23.0; #619 S1).
//
// So the pass keeps the in-memory spec.topology.servers below the first
// ordinal above the StatefulSet's replicas that still has its pod, or a
// volume being reclaimed — one being deleted, or one a pod owns — and says so
// in the TopologyChangeDeferred condition (reason WaitsForRemovedServers) and
// one ScaleUpDeferred event. The rest of the pass works on the servers there
// are, as for a held upgrade (id 163): the ConfigMap, the drain and the
// formation check. It looks again every raiseHoldPoll; nothing Kubernetes
// reports wakes it when a volume goes. A volume nothing reclaims — no pod
// owns it, it is not being deleted — is not held for: it is not one a
// scale-down left (Kubernetes 1.32+ always hands a removed pod's volume to
// the collector), it may be one made ready for a new server. One that is
// reclaimed but outlasts removedServersGrace — a pod terminating on a node
// that is gone, a volume a foreign finalizer keeps — is still held for, and
// the message and one ScaleUpBlocked Warning name it and how to remove it:
// adding the server on it would start the dropped identity. The operator
// deletes nothing itself.
//
// A cluster already stuck — raised before this hold, or on a volume no
// pod owned — is reported: droppedServersInKeptPods.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// raiseHoldPoll is how soon a pass that holds a raise looks again.
const raiseHoldPoll = 5 * time.Second

// removedServersGrace is how long a raise waits for what is left of the
// removed servers before it names it as stuck. A pod's shutdown and the
// collector take seconds.
const removedServersGrace = 2 * time.Minute

// removedServerLeftover is what is left at one ordinal above the replicas: its
// pod, its volume being reclaimed, or what could not be read.
type removedServerLeftover struct {
	ordinal int32
	pod     *corev1.Pod
	volume  *corev1.PersistentVolumeClaim
	readErr error
}

// removedServerLeftovers reads, from the API server, the pod and the data
// volume of each ordinal from replicas up to requested-1, and returns those
// with something left, lowest ordinal first.
func removedServerLeftovers(ctx context.Context, reader client.Reader, cluster *neo4jv1beta1.Neo4jEnterpriseCluster, replicas, requested int32) []removedServerLeftover {
	var left []removedServerLeftover
	for n := replicas; n < requested; n++ {
		l := removedServerLeftover{ordinal: n}
		podName := fmt.Sprintf("%s-server-%d", cluster.Name, n)
		pod := &corev1.Pod{}
		if err := reader.Get(ctx, types.NamespacedName{Name: podName, Namespace: cluster.Namespace}, pod); err == nil {
			l.pod = pod
		} else if !apierrors.IsNotFound(err) {
			l.readErr = err
		}
		pvc := &corev1.PersistentVolumeClaim{}
		if err := reader.Get(ctx, types.NamespacedName{Name: dataVolumeName(cluster.Name, n), Namespace: cluster.Namespace}, pvc); err == nil {
			if pvc.DeletionTimestamp != nil || ownedByPod(pvc, podName) {
				l.volume = pvc
			}
		} else if !apierrors.IsNotFound(err) && l.readErr == nil {
			l.readErr = err
		}
		if l.pod != nil || l.volume != nil || l.readErr != nil {
			left = append(left, l)
		}
	}
	return left
}

// dataVolumeName is the data PersistentVolumeClaim of a server pod
// (volumeClaimTemplates "data" of StatefulSet {cluster}-server).
func dataVolumeName(clusterName string, ordinal int32) string {
	return fmt.Sprintf("%s-%s-server-%d", resources.DataVolume, clusterName, ordinal)
}

// ownedByPod reports whether the StatefulSet controller handed the volume to
// a condemned pod (whenScaled: Delete), so the collector deletes it once the
// pod is gone.
func ownedByPod(pvc *corev1.PersistentVolumeClaim, pod string) bool {
	for _, o := range pvc.OwnerReferences {
		if o.Kind == "Pod" && o.Name == pod {
			return true
		}
	}
	return false
}

// holdRaiseForRemovedServers keeps the in-memory spec.topology.servers below
// the first ordinal above the server StatefulSet's replicas that still has a
// removed server's pod or volume, and reports whether it held. The replicas
// are read from the API server, on every pass: a cache that has not seen the
// drain lower them reads no raise, and a hold below the replicas there are
// would lower them with no drain.
func (r *Neo4jEnterpriseClusterReconciler) holdRaiseForRemovedServers(ctx context.Context, cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	requested := cluster.Spec.Topology.Servers
	key := types.NamespacedName{Name: cluster.Name + "-server", Namespace: cluster.Namespace}
	logger := log.FromContext(ctx)
	live := &appsv1.StatefulSet{}
	if err := r.apiReader().Get(ctx, key, live); err != nil || live.Spec.Replicas == nil {
		if err != nil && !apierrors.IsNotFound(err) {
			logger.Info("Scale-up: the server StatefulSet could not be read from the API server; not holding a raise", "error", err.Error())
		}
		return false
	}
	replicas := *live.Spec.Replicas
	if replicas >= requested {
		return false
	}
	left := removedServerLeftovers(ctx, r.apiReader(), cluster, replicas, requested)
	if len(left) == 0 {
		return false
	}
	keep := left[0].ordinal

	now := r.now()
	since := now
	if c := findCondition(cluster.Status.Conditions, ConditionTypeTopologyChangeDeferred); c != nil &&
		c.Status == metav1.ConditionTrue && c.Reason == ConditionReasonWaitsForRemovedServers {
		since = c.LastTransitionTime.Time
	}
	stuck := now.Sub(since) >= removedServersGrace
	want := metav1.Condition{
		Type:    ConditionTypeTopologyChangeDeferred,
		Status:  metav1.ConditionTrue,
		Reason:  ConditionReasonWaitsForRemovedServers,
		Message: raiseHoldMessage(cluster, requested, keep, left, stuck),
	}
	before, wrote := r.writeTopologyDeferral(ctx, cluster, want)
	// At info when what it says changes; the passes in between, every
	// raiseHoldPoll, at V(1).
	logLevel := logger.V(1)
	if wrote {
		logLevel = logger
	}
	logLevel.Info("Scale-up waits for the pods and volumes of removed servers to be gone",
		"requested", requested, "replicas", replicas, "keeping", keep, "leftovers", describeLeftovers(cluster, left), "stuck", stuck)
	if wrote {
		began := before == nil || before.Status != metav1.ConditionTrue || before.Reason != ConditionReasonWaitsForRemovedServers
		switch {
		case began:
			r.Recorder.Eventf(cluster, corev1.EventTypeNormal, EventReasonScaleUpDeferred,
				"Adding %s waits until the pods and volumes of the servers a scale-down removed are gone: %s",
				addedServersLabel(keep, requested), leftoverNames(cluster, left))
		case stuck && !strings.Contains(before.Message, raiseHoldStuckMarker):
			r.Recorder.Event(cluster, corev1.EventTypeWarning, EventReasonScaleUpBlocked, want.Message)
		}
	}
	cluster.Spec.Topology.Servers = keep
	r.wakes.note(clusterKey(cluster), raiseHoldPoll)
	return true
}

// raiseHoldStuckMarker is in the message of a raise whose wait has outlasted
// removedServersGrace, and in no other.
const raiseHoldStuckMarker = "has not gone in"

// raiseHoldMessage is the TopologyChangeDeferred message of a held raise: the
// counts and the servers it waits for; past the grace, what is left of each
// and how to remove it.
func raiseHoldMessage(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, requested, keep int32, left []removedServerLeftover, stuck bool) string {
	head := fmt.Sprintf("spec.topology.servers is %d; the server StatefulSet keeps %d servers", requested, keep)
	why := "a server added on a removed server's volume would start as the server Neo4j dropped, and could not rejoin"
	if !stuck {
		return fmt.Sprintf("%s until the pods and volumes of the servers a scale-down removed are gone (%s): %s",
			head, leftoverNames(cluster, left), why)
	}
	return fmt.Sprintf("%s: what is left of the servers a scale-down removed %s %s: %s. Remove it, and the servers are added with new identities: %s. "+
		"Or set spec.topology.servers back to %d", head, raiseHoldStuckMarker, removedServersGrace, describeLeftovers(cluster, left),
		strings.Join(leftoverFixes(cluster, left), "; "), keep)
}

// leftoverNames names the pods of the ordinals a raise waits for.
func leftoverNames(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, left []removedServerLeftover) string {
	names := make([]string, 0, len(left))
	for _, l := range left {
		names = append(names, fmt.Sprintf("%s-server-%d", cluster.Name, l.ordinal))
	}
	return strings.Join(names, ", ")
}

// describeLeftovers says what is left at each ordinal.
func describeLeftovers(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, left []removedServerLeftover) string {
	var parts []string
	for _, l := range left {
		if l.pod != nil {
			state := "is still running"
			if l.pod.DeletionTimestamp != nil {
				state = "has been terminating since " + l.pod.DeletionTimestamp.UTC().Format(time.RFC3339)
			}
			parts = append(parts, fmt.Sprintf("pod %s %s", l.pod.Name, state))
		}
		if v := l.volume; v != nil {
			state := "is owned by its removed pod and waits for the garbage collector"
			if v.DeletionTimestamp != nil {
				state = "is being deleted"
				if f := foreignFinalizers(v); len(f) > 0 {
					state += ", held by " + strings.Join(f, ", ")
				}
			}
			parts = append(parts, fmt.Sprintf("volume %s %s", v.Name, state))
		}
		if l.readErr != nil {
			parts = append(parts, fmt.Sprintf("%s-server-%d could not be read (%v)", cluster.Name, l.ordinal, l.readErr))
		}
	}
	return strings.Join(parts, "; ")
}

// foreignFinalizers are a volume's finalizers other than Kubernetes' own
// pvc-protection, which lets go once no pod uses it.
func foreignFinalizers(pvc *corev1.PersistentVolumeClaim) []string {
	var out []string
	for _, f := range pvc.Finalizers {
		if f != "kubernetes.io/pvc-protection" {
			out = append(out, f)
		}
	}
	sort.Strings(out)
	return out
}

// leftoverFixes is how to remove what is left: a pod whose node is gone is
// only removed by force, a volume by deleting it, or by whoever owns the
// finalizer that keeps it.
func leftoverFixes(cluster *neo4jv1beta1.Neo4jEnterpriseCluster, left []removedServerLeftover) []string {
	var fixes []string
	for _, l := range left {
		if l.pod != nil {
			fixes = append(fixes, fmt.Sprintf("kubectl delete pod -n %s %s --grace-period=0 --force, if its node is gone", cluster.Namespace, l.pod.Name))
		}
		if v := l.volume; v != nil {
			if v.DeletionTimestamp != nil && len(foreignFinalizers(v)) > 0 {
				fixes = append(fixes, fmt.Sprintf("release %s from %s", v.Name, strings.Join(foreignFinalizers(v), ", ")))
			} else if v.DeletionTimestamp == nil {
				fixes = append(fixes, fmt.Sprintf("kubectl delete pvc -n %s %s", cluster.Namespace, v.Name))
			}
		}
	}
	if len(fixes) == 0 {
		fixes = append(fixes, "kubectl get pod,pvc -n "+cluster.Namespace+" shows what is left")
	}
	return fixes
}

// addedServersLabel is "server 3" or "servers 3-4": the ordinals from keep up
// to requested-1 that the raise adds once it is no longer held.
func addedServersLabel(keep, requested int32) string {
	if requested-keep == 1 {
		return fmt.Sprintf("server %d", keep)
	}
	return fmt.Sprintf("servers %d-%d", keep, requested-1)
}

// droppedServerInPod is a server pod the spec keeps that runs a server Neo4j
// has dropped.
type droppedServerInPod struct {
	pod string
	id  string
}

// droppedServersInKeptPods finds the server pods below expected that run a
// server SHOW SERVERS lists Dropped: a raise that started them on the volumes
// of servers a scale-down removed (before holdRaiseForRemovedServers, or on a
// volume no pod owned). A dropped server "cannot rejoin a cluster"
// (Operations Manual, "Managing servers in a cluster"). servers has the
// addresses filled in that rows no longer report (withKnownAddresses). A
// Dropped row is matched to its pod by its address, when no member is that
// pod's server and the pod is ready. With no address known for any Dropped
// row (an operator restarted since), the pods it names are those below
// expected whose pod is ready and whose server is no member, when there are
// any such rows:
// SHOW SERVERS cannot say which is which. Dropped rows of pods at or above
// expected — a scale-down's, while they terminate — are not these.
func droppedServersInKeptPods(servers []neo4jclient.ServerInfo, notReadyPods []string, clusterName string, expected int) []droppedServerInPod {
	members := map[int]bool{}
	for _, s := range servers {
		if !serverIsMember(s) {
			continue
		}
		if ord, ok := parseServerOrdinal(s.Address, clusterName); ok {
			members[ord] = true
		}
	}
	// A pod that runs a dropped server is up, and ready: the journey's were.
	// One that is not ready may be a new server still starting while the
	// row of the one it replaces lingers.
	notReady := map[string]bool{}
	for _, p := range notReadyPods {
		notReady[strings.SplitN(p, " ", 2)[0]] = true
	}
	var found []droppedServerInPod
	var unnamed []string
	for _, s := range servers {
		if !strings.EqualFold(s.State, "dropped") {
			continue
		}
		ord, ok := serverOrdinal(s, clusterName)
		if !ok {
			unnamed = append(unnamed, serverIdentifier(s))
			continue
		}
		pod := fmt.Sprintf("%s-server-%d", clusterName, ord)
		if ord < expected && !members[ord] && !notReady[pod] {
			found = append(found, droppedServerInPod{pod: pod, id: serverIdentifier(s)})
		}
	}
	if len(found) > 0 || len(unnamed) == 0 {
		sort.Slice(found, func(i, j int) bool { return found[i].pod < found[j].pod })
		return found
	}
	sort.Strings(unnamed)
	for ord := 0; ord < expected; ord++ {
		pod := fmt.Sprintf("%s-server-%d", clusterName, ord)
		if !members[ord] && !notReady[pod] {
			found = append(found, droppedServerInPod{pod: pod})
		}
	}
	if len(found) == 0 {
		return nil
	}
	found[0].id = strings.Join(unnamed, ", ")
	return found
}

// droppedServersMessage is the Forming message of a cluster whose kept pods
// run dropped servers, with the way out.
func droppedServersMessage(clusterName string, found []droppedServerInPod) string {
	pods := make([]string, 0, len(found))
	var ids []string
	lowest := -1
	for _, f := range found {
		pods = append(pods, f.pod)
		if f.id != "" {
			ids = append(ids, f.id)
		}
		if ord, ok := parseServerOrdinal(f.pod, clusterName); ok && (lowest < 0 || ord < lowest) {
			lowest = ord
		}
	}
	return fmt.Sprintf("%s run servers Neo4j has dropped (%s): they started on the volumes of servers a scale-down removed, "+
		"before those were deleted, and a dropped server cannot rejoin. Set spec.topology.servers to %d; "+
		"once ServersPendingDrain reads \"Scale-down drain complete\", raise it again: the operator adds the servers "+
		"once the removed ones' pods and volumes are gone, with new identities",
		strings.Join(pods, ", "), strings.Join(ids, ", "), lowest)
}
