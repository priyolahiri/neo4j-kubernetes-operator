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
// would be a new revision that rolls every pod. Without the memory the
// annotations are gone, and the next pass's apply reads the template as
// changed and converges, as after an operator upgrade.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

//+kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get

// recreateSource is what a StatefulSet created again after a storage
// expansion takes from the one deleted.
type recreateSource struct {
	template corev1.PodTemplateSpec
	replicas *int32
	// updateStrategy and annotations are known only from memory.
	updateStrategy *appsv1.StatefulSetUpdateStrategy
	annotations    map[string]string
	// from says where it came from, for the log.
	from string
}

// deletedStatefulSets remembers, per StatefulSet (key ns/name), the
// StatefulSet a storage expansion deleted until the pass that creates it
// again. Safe for the cluster controller's parallel workers (rule 129).
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

// source returns what to create the StatefulSet namespace/name again from:
// the deleted one kept in memory, else the template of the ControllerRevision
// its orphaned pods (podLabels) run, read through reader; nil when neither
// says anything — a StatefulSet created for the first time, or one whose pods
// are gone too. ownerUID, when set, must own the remembered StatefulSet: a
// deployment deleted and created again under the same name is not the one
// that was.
func (d *deletedStatefulSets) source(ctx context.Context, c client.Client, reader client.Reader, namespace, name string, ownerUID types.UID, podLabels map[string]string) *recreateSource {
	if v, ok := d.m.Load(statefulSetKey(namespace, name)); ok {
		if prev, ok := v.(*appsv1.StatefulSet); ok && (ownerUID == "" || ownedBy(prev, ownerUID)) {
			strategy := prev.Spec.UpdateStrategy.DeepCopy()
			return &recreateSource{
				template: *prev.Spec.Template.DeepCopy(), replicas: prev.Spec.Replicas,
				updateStrategy: strategy, annotations: prev.Annotations, from: "the deleted StatefulSet",
			}
		}
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels(podLabels)); err != nil {
		return nil
	}
	highest, revision := -1, ""
	for i := range pods.Items {
		pod := &pods.Items[i]
		ordinal, ok := statefulSetOrdinal(name, pod.Name)
		if !ok || pod.DeletionTimestamp != nil || ordinal < highest {
			continue
		}
		if hash := pod.Labels[appsv1.ControllerRevisionHashLabelKey]; hash != "" {
			highest, revision = ordinal, hash
		}
	}
	if revision == "" {
		return nil
	}
	rev := &appsv1.ControllerRevision{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: revision}, rev); err != nil {
		log.FromContext(ctx).Info("Could not read the revision the orphaned pods run; the StatefulSet is created from the spec",
			"statefulSet", name, "revision", revision, "error", err.Error())
		return nil
	}
	template, err := templateFromControllerRevision(rev)
	if err != nil {
		log.FromContext(ctx).Info("Could not read the template of the revision the orphaned pods run; the StatefulSet is created from the spec",
			"statefulSet", name, "revision", revision, "error", err.Error())
		return nil
	}
	replicas := int32(highest + 1)
	return &recreateSource{template: template, replicas: &replicas, from: "ControllerRevision " + revision}
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
