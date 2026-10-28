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

// A StatefulSet a storage expansion deleted is created again as it was
// (operations.md id 171).
//
// A volumeClaimTemplate cannot change, so an expansion patches the claims,
// deletes the StatefulSet leaving its pods running (orphan), and the next pass
// creates it again with the new claim templates — Kubernetes' documented way
// to resize a StatefulSet's volumes, which keeps the pod template as it was.
// The operator created it again from the deployment's own template, which
// holds only what the operator renders: the plugins' NEO4J_PLUGINS entries and
// variables, their procedure lists, their init containers and volumes (rules
// 141, 155) were gone, so the template changed and the StatefulSet controller
// replaced the running pods — ungated, one held down already, which lost the
// cluster its majority — and the plugins then restored their writes in three
// more rolls (#562 walk: APOC and GDS gone for about 17 minutes).
//
// So the deleted StatefulSet is kept in memory, and the one created again
// takes its pod template, replica count, update strategy (a held upgrade's
// partition) and annotations (the ownership records of rules 141 and 155),
// with the new claim templates: the same template is the same revision, and
// no pod restarts. The next pass then applies the deployment's spec as it
// would to any StatefulSet. An operator restart between the delete and the
// create loses the memory; the template is then read from the
// ControllerRevision the orphaned pods run (their controller-revision-hash
// label names it, and the deleted StatefulSet's revisions are orphaned with
// them): the exact template, where a pod's spec is not — admission adds
// volumes, tolerations and defaults to a pod, and a template rebuilt from it
// would be a new revision that rolls every pod.
//
// A StatefulSet has two revisions: the update revision (its template), which
// pods at or above the partition are created from, and the current revision,
// which pods below it are created from. Kubernetes sets a new StatefulSet's
// current revision to its update revision. Created again on a held upgrade's
// template, every server below the partition that restarted came back on the
// upgrade's target, ungated — an image that cannot be pulled took a second
// server down — and the walk that resumed it re-created the server being
// replaced from the held target again (#562 re-walk). So when the pods below
// the partition run another revision than the template, it is created in two
// steps: first with their template, frozen at its replica count, so the
// StatefulSet controller records it as the current revision; once it has
// (status.currentRevision, at the generation written), the held template and
// partition are written back (restoreHeldTemplate). Every server keeps the
// revision it runs, and comes back on it if it restarts, as before the
// expansion. The annotations below carry the second step across an operator
// restart.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

//+kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get

const (
	// recreateRestorePartitionAnnotation marks a server StatefulSet created
	// again with the template the pods below a held partition run, frozen at
	// its replica count; its value is the partition the second step writes
	// back with the held template.
	recreateRestorePartitionAnnotation = "neo4j.com/recreate-restore-partition"
	// recreateRestoreRevisionAnnotation names the ControllerRevision whose
	// template the second step writes back when the deleted StatefulSet is not
	// in memory (an operator restart).
	recreateRestoreRevisionAnnotation = "neo4j.com/recreate-restore-revision"
)

// recreateSource is what a StatefulSet created again after a storage
// expansion takes from the one deleted.
type recreateSource struct {
	template corev1.PodTemplateSpec
	replicas *int32
	// updateStrategy and annotations are known only from memory.
	updateStrategy *appsv1.StatefulSetUpdateStrategy
	annotations    map[string]string
	// updateRevision is the revision template is: the deleted StatefulSet's
	// status.updateRevision, or the one the highest-ordinal pod runs.
	updateRevision string
	// currentRevision is the revision the pods below the partition run: the
	// deleted StatefulSet's status.currentRevision, or the lowest-ordinal
	// pod's.
	currentRevision string
	// from says where it came from, for the log.
	from string
}

// deletedStatefulSets remembers, per StatefulSet (key ns/name), the
// StatefulSet a storage expansion deleted until the pass that creates it
// again, or, for one created in two steps, until the second step. Safe for
// the cluster controller's parallel workers (rule 129).
type deletedStatefulSets struct {
	m sync.Map
}

func statefulSetKey(namespace, name string) string { return namespace + "/" + name }

// remember keeps a copy of a StatefulSet about to be orphan-deleted.
func (d *deletedStatefulSets) remember(sts *appsv1.StatefulSet) {
	d.m.Store(statefulSetKey(sts.Namespace, sts.Name), sts.DeepCopy())
}

// forget drops what is kept for a StatefulSet once it exists again.
func (d *deletedStatefulSets) forget(namespace, name string) {
	d.m.Delete(statefulSetKey(namespace, name))
}

// remembered returns the deleted StatefulSet kept for namespace/name, when
// ownerUID (if set) owns it: a deployment deleted and created again under the
// same name is not the one that was.
func (d *deletedStatefulSets) remembered(namespace, name string, ownerUID types.UID) *appsv1.StatefulSet {
	v, ok := d.m.Load(statefulSetKey(namespace, name))
	if !ok {
		return nil
	}
	prev, ok := v.(*appsv1.StatefulSet)
	if !ok || (ownerUID != "" && !ownedBy(prev, ownerUID)) {
		return nil
	}
	return prev
}

// statefulSetGonePoll and statefulSetGoneWait bound how long the pass that
// orphan-deleted a StatefulSet waits for it to be gone.
var (
	statefulSetGonePoll = 200 * time.Millisecond
	statefulSetGoneWait = 15 * time.Second
)

// waitStatefulSetGone waits, a bounded time, until the StatefulSet key is gone
// from the API server (uncached) and from the cache (cached), and reports
// whether it is. An orphan delete is not immediate: the garbage collector
// releases the pods and revisions before it removes the object, and until then
// the cache still holds it. The pass that deleted it then creates it again at
// once; it used to return, and the next pass, started by the delete's watch
// event, still read the StatefulSet, applied the spec to it and waited for the
// cluster's formation checks: it was created again only about 10s later (#562
// re-walk), and the plugins found no StatefulSet meanwhile.
func waitStatefulSetGone(ctx context.Context, cached, uncached client.Reader, key types.NamespacedName) bool {
	gone := func(reader client.Reader) bool {
		err := reader.Get(ctx, key, &appsv1.StatefulSet{})
		return apierrors.IsNotFound(err)
	}
	err := wait.PollUntilContextTimeout(ctx, statefulSetGonePoll, statefulSetGoneWait, true, func(context.Context) (bool, error) {
		return gone(uncached) && gone(cached), nil
	})
	return err == nil
}

// orphanRevision is the ordinal of a pod and the revision it runs.
type orphanRevision struct {
	ordinal  int
	revision string
}

// orphanedRevisions returns the lowest- and highest-ordinal pods of the
// StatefulSet name (selected by podLabels) that carry a revision label and
// are not being deleted; an empty revision when there is none.
func orphanedRevisions(ctx context.Context, c client.Client, namespace, name string, podLabels map[string]string) (lowest, highest orphanRevision) {
	lowest, highest = orphanRevision{ordinal: -1}, orphanRevision{ordinal: -1}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels(podLabels)); err != nil {
		return lowest, highest
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		ordinal, ok := statefulSetOrdinal(name, pod.Name)
		hash := pod.Labels[appsv1.ControllerRevisionHashLabelKey]
		if !ok || pod.DeletionTimestamp != nil || hash == "" {
			continue
		}
		if ordinal > highest.ordinal {
			highest = orphanRevision{ordinal, hash}
		}
		if lowest.ordinal < 0 || ordinal < lowest.ordinal {
			lowest = orphanRevision{ordinal, hash}
		}
	}
	return lowest, highest
}

// source returns what to create the StatefulSet namespace/name again from:
// the deleted one kept in memory, else the template of the ControllerRevision
// its highest-ordinal orphaned pod (podLabels) runs, read through reader; nil
// when neither says anything — a StatefulSet created for the first time, or
// one whose pods are gone too. ownerUID, when set, must own the remembered
// StatefulSet.
func (d *deletedStatefulSets) source(ctx context.Context, c client.Client, reader client.Reader, namespace, name string, ownerUID types.UID, podLabels map[string]string) *recreateSource {
	lowest, highest := orphanedRevisions(ctx, c, namespace, name, podLabels)
	if prev := d.remembered(namespace, name, ownerUID); prev != nil {
		src := &recreateSource{
			template: *prev.Spec.Template.DeepCopy(), replicas: prev.Spec.Replicas,
			updateStrategy: prev.Spec.UpdateStrategy.DeepCopy(), annotations: prev.Annotations,
			updateRevision: prev.Status.UpdateRevision, currentRevision: prev.Status.CurrentRevision,
			from: "the deleted StatefulSet",
		}
		if src.updateRevision == "" {
			src.updateRevision = highest.revision
		}
		if src.currentRevision == "" {
			src.currentRevision = lowest.revision
		}
		return src
	}
	if highest.revision == "" {
		return nil
	}
	template, err := readRevisionTemplate(ctx, reader, namespace, highest.revision)
	if err != nil {
		log.FromContext(ctx).Info("Could not read the revision the orphaned pods run; the StatefulSet is created from the spec",
			"statefulSet", name, "revision", highest.revision, "error", err.Error())
		return nil
	}
	replicas := int32(highest.ordinal + 1)
	return &recreateSource{
		template: template, replicas: &replicas, from: "ControllerRevision " + highest.revision,
		updateRevision: highest.revision, currentRevision: lowest.revision,
	}
}

// readRevisionTemplate reads the pod template of the ControllerRevision name.
func readRevisionTemplate(ctx context.Context, reader client.Reader, namespace, name string) (corev1.PodTemplateSpec, error) {
	rev := &appsv1.ControllerRevision{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, rev); err != nil {
		return corev1.PodTemplateSpec{}, err
	}
	return templateFromControllerRevision(rev)
}

// templateFromControllerRevision reads the pod template a StatefulSet's
// ControllerRevision records: the StatefulSet controller stores
// {"spec":{"template":{…,"$patch":"replace"}}}.
func templateFromControllerRevision(rev *appsv1.ControllerRevision) (corev1.PodTemplateSpec, error) {
	var data struct {
		Spec struct {
			Template *corev1.PodTemplateSpec `json:"template"`
		} `json:"spec"`
	}
	raw := rev.Data.Raw
	if len(raw) == 0 {
		return corev1.PodTemplateSpec{}, fmt.Errorf("revision %s has no data", rev.Name)
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return corev1.PodTemplateSpec{}, fmt.Errorf("revision %s: %w", rev.Name, err)
	}
	if data.Spec.Template == nil || len(data.Spec.Template.Spec.Containers) == 0 {
		return corev1.PodTemplateSpec{}, fmt.Errorf("revision %s holds no pod template", rev.Name)
	}
	return *data.Spec.Template, nil
}

// ownedBy reports whether obj has an owner reference to uid.
func ownedBy(obj *appsv1.StatefulSet, uid types.UID) bool {
	for _, ref := range obj.OwnerReferences {
		if ref.UID == uid {
			return true
		}
	}
	return false
}

// applyTo makes sts, about to be created with spec, the StatefulSet as it was:
// its template, replica count and, from memory, update strategy and
// annotations. The rest of spec — the claim templates above all — is the
// new one's.
func (s *recreateSource) applyTo(sts *appsv1.StatefulSet) {
	sts.Spec.Template = *s.template.DeepCopy()
	if s.replicas != nil {
		r := *s.replicas
		sts.Spec.Replicas = &r
	}
	if s.updateStrategy != nil {
		sts.Spec.UpdateStrategy = *s.updateStrategy.DeepCopy()
	}
	if len(s.annotations) > 0 {
		if sts.Annotations == nil {
			sts.Annotations = map[string]string{}
		}
		for k, v := range s.annotations {
			sts.Annotations[k] = v
		}
	}
}

// statefulSetPartition returns sts's RollingUpdate partition, or nil.
func statefulSetPartition(sts *appsv1.StatefulSet) *int32 {
	if ru := sts.Spec.UpdateStrategy.RollingUpdate; ru != nil {
		return ru.Partition
	}
	return nil
}

// holdBelowPartition makes sts — the StatefulSet as it was, about to be
// created again — the first step of a two-step recreate, when its partition
// keeps pods below it that run another revision than its template: created
// with their template, frozen at the replica count, it gets their revision as
// its current one. The partition and the revision to write back are recorded
// in its annotations. It reports whether it did.
func (s *recreateSource) holdBelowPartition(ctx context.Context, reader client.Reader, sts *appsv1.StatefulSet) bool {
	partition := statefulSetPartition(sts)
	if partition == nil || *partition <= 0 || sts.Spec.Replicas == nil ||
		s.currentRevision == "" || s.currentRevision == s.updateRevision {
		return false
	}
	below, err := readRevisionTemplate(ctx, reader, sts.Namespace, s.currentRevision)
	if err != nil {
		log.FromContext(ctx).Info("Could not read the revision the servers below the partition run; the StatefulSet is created on the held template",
			"statefulSet", sts.Name, "revision", s.currentRevision, "error", err.Error())
		return false
	}
	restore := *partition
	frozen := *sts.Spec.Replicas
	sts.Spec.Template = below
	sts.Spec.UpdateStrategy.RollingUpdate.Partition = &frozen
	if sts.Annotations == nil {
		sts.Annotations = map[string]string{}
	}
	sts.Annotations[recreateRestorePartitionAnnotation] = strconv.Itoa(int(restore))
	sts.Annotations[recreateRestoreRevisionAnnotation] = s.updateRevision
	return true
}

// restorePending reports whether sts waits for the second step of a two-step
// recreate, and the partition it writes back (nil when the annotation does
// not hold one: the second step then drops it and keeps every server frozen).
func restorePending(sts *appsv1.StatefulSet) (*int32, bool) {
	raw, ok := sts.Annotations[recreateRestorePartitionAnnotation]
	if !ok {
		return nil, false
	}
	// A partition is an int32; a value out of that range is no partition, as
	// one that does not parse is not (CodeQL go/incorrect-integer-conversion).
	p, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || p < 0 {
		return nil, true
	}
	v := int32(p)
	return &v, true
}

// restoreHeldTemplate is the second step of a two-step recreate, run on the
// live StatefulSet (sts) of cluster: once the StatefulSet controller has
// recorded its current revision at the generation written, it writes back the
// held template — from memory, else the revision the first step recorded —
// and partition, drops the annotations, and reports true. It reports false,
// changing nothing, while the controller has not; the status write that
// records it starts the next pass. With neither the memory nor the revision,
// it drops the annotations and keeps the template the servers below the
// partition run: the upgrade writes its target again when it goes on.
func (r *Neo4jEnterpriseClusterReconciler) restoreHeldTemplate(ctx context.Context, sts *appsv1.StatefulSet,
	cluster *neo4jv1beta1.Neo4jEnterpriseCluster, desired corev1.PodTemplateSpec) bool {
	logger := log.FromContext(ctx)
	partition, _ := restorePending(sts)
	if sts.Status.ObservedGeneration < sts.Generation || sts.Status.CurrentRevision == "" {
		logger.V(1).Info("Waiting for the StatefulSet controller to record the current revision before the held template is written back",
			"statefulSet", sts.Name)
		return false
	}
	revision := sts.Annotations[recreateRestoreRevisionAnnotation]
	delete(sts.Annotations, recreateRestorePartitionAnnotation)
	delete(sts.Annotations, recreateRestoreRevisionAnnotation)
	if partition == nil {
		logger.Info("The StatefulSet created again records no partition to write back; every server stays frozen until the upgrade goes on",
			"statefulSet", sts.Name)
		return true
	}

	var template corev1.PodTemplateSpec
	var from string
	prev := r.deletedServerStatefulSets.remembered(sts.Namespace, sts.Name, cluster.UID)
	switch {
	case prev != nil:
		template, from = *prev.Spec.Template.DeepCopy(), "the deleted StatefulSet"
		for k, v := range prev.Annotations {
			if _, ok := sts.Annotations[k]; !ok {
				sts.Annotations[k] = v
			}
		}
	case revision != "":
		t, err := readRevisionTemplate(ctx, r.apiReader(), sts.Namespace, revision)
		if err != nil {
			logger.Info("Could not read the held template to write back; the StatefulSet keeps the template the servers below the partition run, and the upgrade writes its target when it goes on",
				"statefulSet", sts.Name, "revision", revision, "error", err.Error())
			return true
		}
		template, from = t, "ControllerRevision "+revision
	default:
		return true
	}
	sts.Spec.Template = template
	if sts.Spec.UpdateStrategy.RollingUpdate == nil {
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
	}
	p := *partition
	sts.Spec.UpdateStrategy.RollingUpdate.Partition = &p
	if prev == nil {
		// Read from a revision: the records and the stamp the memory would
		// have brought, as on any write of the template (#562 re-walk).
		writeOwnedTemplateItems(sts, desired)
		r.stampTemplateHashIfApplied(ctx, sts, desired)
	}
	logger.Info("Wrote the held template back to the StatefulSet created again; every server keeps the revision it runs",
		"statefulSet", sts.Name, "partition", p, "from", from, "currentRevision", sts.Status.CurrentRevision)
	return true
}

// stampTemplateHashIfApplied stamps the cluster-template hash of desired on
// sts when its template is what the operator's apply of desired would leave —
// the update branch's merge (foreign env vars, init containers, volumes and
// pod annotations kept, the operator's own items desired's) changes nothing —
// as for a template the operator wrote (#562 re-walk: a StatefulSet created
// again from a revision had no stamp, and while a server stayed down, a
// cluster that is not fully ready applies only what the stamp shows the
// operator now renders differently, see isTemplateChangeSignificant). A
// template the apply would change keeps no stamp, and the next pass writes the
// difference: a stamp then would say the operator had applied what it had not.
func (r *Neo4jEnterpriseClusterReconciler) stampTemplateHashIfApplied(ctx context.Context, sts *appsv1.StatefulSet, desired corev1.PodTemplateSpec) {
	if !r.templateApplied(ctx, sts, desired) {
		return
	}
	setClusterTemplateHash(sts, desired)
}

// templateApplied reports whether the update branch of
// createOrUpdateResourceInternal, applying desired to sts, would leave its
// template as the API server stores it, the order of the env vars, init
// containers and volumes aside (the merge appends what it keeps; the apply
// compares them by name).
//
// As the API server stores it: a template read from a ControllerRevision is
// the stored one — defaulted (probe thresholds, termination message path and
// policy, restart and DNS policy, grace period, scheduler name, a volume with
// no source made an emptyDir) and converted (deprecatedServiceAccount) — and
// what the operator renders carries none of that. Compared as they were, the
// two never matched on a real cluster, and the stamp never landed (the
// re-walk of 2a50fa56: the hash stayed absent until Staging wrote the
// template).
// So both templates go through the write the operator is about to make — the
// create of sts, or the update of the live StatefulSet — as a dry run
// (storedTemplate): the API server's own defaulting, conversion and mutating
// admission, whatever its version, and no list of defaults to keep in step
// with it. A comparison by the fields the operator sets would hide a value
// someone else set where it sets none; the field-by-field check of
// isTemplateChangeSignificant compares too little (the stamp is what catches
// a nodeSelector or a probe). A dry run that is refused stamps nothing: the
// operator's next write of the template stamps it, as before.
func (r *Neo4jEnterpriseClusterReconciler) templateApplied(ctx context.Context, sts *appsv1.StatefulSet, desired corev1.PodTemplateSpec) bool {
	applied, err := r.storedTemplate(ctx, sts, appliedTemplate(sts, desired))
	if err == nil {
		var current corev1.PodTemplateSpec
		if current, err = r.storedTemplate(ctx, sts, sts.Spec.Template); err == nil {
			sortTemplateItems(&applied)
			sortTemplateItems(&current)
			return equality.Semantic.DeepEqual(applied, current)
		}
	}
	log.FromContext(ctx).Info("Could not dry-run the StatefulSet's template; its template hash is stamped by the next write of it",
		"statefulSet", sts.Name, "error", err.Error())
	return false
}

// storedTemplate returns template as the API server stores it in sts: the
// write the operator is about to make — the create of sts, or the update of
// the live one (it has a UID) — sent as a dry run, with template in place of
// sts's own. Nothing is stored.
func (r *Neo4jEnterpriseClusterReconciler) storedTemplate(ctx context.Context, sts *appsv1.StatefulSet, template corev1.PodTemplateSpec) (corev1.PodTemplateSpec, error) {
	probe := sts.DeepCopy()
	probe.Spec.Template = *template.DeepCopy()
	var err error
	if probe.UID == "" {
		err = r.Create(ctx, probe, client.DryRunAll)
	} else {
		err = r.Update(ctx, probe, client.DryRunAll)
	}
	if err != nil {
		return corev1.PodTemplateSpec{}, err
	}
	return probe.Spec.Template, nil
}

// appliedTemplate is the template the update branch of
// createOrUpdateResourceInternal writes when it applies desired to sts: desired
// with the selector's labels, its env vars, init containers and volumes merged
// with what is not the cluster's, and the pod annotations of both.
func appliedTemplate(sts *appsv1.StatefulSet, desired corev1.PodTemplateSpec) corev1.PodTemplateSpec {
	current := *sts.Spec.Template.DeepCopy()
	applied := *desired.DeepCopy()
	if sts.Spec.Selector != nil {
		if applied.Labels == nil {
			applied.Labels = map[string]string{}
		}
		for k, v := range sts.Spec.Selector.MatchLabels {
			applied.Labels[k] = v
		}
	}
	owned := readOwnedTemplateItems(sts, current)
	if len(applied.Spec.Containers) > 0 && len(current.Spec.Containers) > 0 {
		applied.Spec.Containers[0].Env = mergeEnvVars(current.Spec.Containers[0].Env, desired.Spec.Containers[0].Env, owned.env)
	}
	applied.Spec.InitContainers = mergeOwnedByName(current.Spec.InitContainers, desired.Spec.InitContainers, owned.initContainers, containerNameOf)
	applied.Spec.Volumes = mergeOwnedByName(current.Spec.Volumes, desired.Spec.Volumes, owned.volumes, volumeNameOf)
	applied.Annotations = mergePodTemplateAnnotations(current.Annotations, desired.Annotations)
	return applied
}

// sortTemplateItems orders t's server container env vars, init containers and
// volumes by name.
func sortTemplateItems(t *corev1.PodTemplateSpec) {
	if len(t.Spec.Containers) > 0 {
		env := t.Spec.Containers[0].Env
		sort.SliceStable(env, func(i, j int) bool { return env[i].Name < env[j].Name })
	}
	init := t.Spec.InitContainers
	sort.SliceStable(init, func(i, j int) bool { return init[i].Name < init[j].Name })
	vols := t.Spec.Volumes
	sort.SliceStable(vols, func(i, j int) bool { return vols[i].Name < vols[j].Name })
}
