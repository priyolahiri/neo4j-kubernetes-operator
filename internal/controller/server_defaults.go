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

// A write of an object that is already as the operator wants it is no write
// (operations.md id 235).
//
// The operator renders a StatefulSet's update strategy and a Service's spec
// leaving out what the API server fills in: a StatefulSet's partition (0) and,
// with MaxUnavailableStatefulSet off, no maxUnavailable at all; a Service's
// type (ClusterIP), session affinity (None), and on a NodePort or
// LoadBalancer Service its external traffic policy (Cluster). Assigned over
// the stored object on every pass, the rendered value differed from it, so
// controllerutil.CreateOrUpdate and the cluster's Service apply sent an Update
// on every pass that the API server then defaulted back to what it had
// stored. On a StatefulSet whose status the StatefulSet controller had just
// written, that Update carried the pass's older status, and the API server
// persisted it with a new resourceVersion although nothing but the
// timestamp of the operator's managedFields entry moved: the v1.24.0 journey
// and the #619 walk saw 5 such writes during one roll, and 4 after one server
// pod was replaced (#619 S8). Each Service took three Updates a pass, each
// logged "Updating Service", the headless one with the API server's warning
// "spec.SessionAffinity is ignored for headless services".
//
// The comparisons here take the API server's defaults into account, so a
// rendering that only leaves a default out keeps what is stored, and an
// object that is as wanted is not written.

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// defaultedUpdateStrategy is s as the API server and the StatefulSet
// controller read it: type RollingUpdate when unset, and under RollingUpdate
// partition 0 and maxUnavailable 1 when unset (the StatefulSet controller's
// value while MaxUnavailableStatefulSet is off, when the API server drops the
// field; the API server's default while it is on).
func defaultedUpdateStrategy(s appsv1.StatefulSetUpdateStrategy) appsv1.StatefulSetUpdateStrategy {
	out := *s.DeepCopy()
	if out.Type == "" {
		out.Type = appsv1.RollingUpdateStatefulSetStrategyType
	}
	if out.Type != appsv1.RollingUpdateStatefulSetStrategyType {
		out.RollingUpdate = nil
		return out
	}
	if out.RollingUpdate == nil {
		out.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
	}
	if out.RollingUpdate.Partition == nil {
		zero := int32(0)
		out.RollingUpdate.Partition = &zero
	}
	if out.RollingUpdate.MaxUnavailable == nil {
		one := intstr.FromInt32(1)
		out.RollingUpdate.MaxUnavailable = &one
	}
	return out
}

// sameUpdateStrategy reports whether a and b are one strategy once defaulted.
func sameUpdateStrategy(a, b appsv1.StatefulSetUpdateStrategy) bool {
	return equality.Semantic.DeepEqual(defaultedUpdateStrategy(a), defaultedUpdateStrategy(b))
}

// defaultedPVCRetention is p as the API server stores it: Retain for each
// policy left unset.
func defaultedPVCRetention(p *appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy) appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy {
	out := appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{}
	if p != nil {
		out = *p
	}
	if out.WhenDeleted == "" {
		out.WhenDeleted = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}
	if out.WhenScaled == "" {
		out.WhenScaled = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}
	return out
}

// keepStoredStatefulSetFields puts back into spec, the StatefulSet spec a
// pass is about to write, the stored value of each field it set that is the
// same once defaulted (the update strategy and the volume claim retention
// policy), so CreateOrUpdate finds nothing changed when nothing did.
func keepStoredStatefulSetFields(stored appsv1.StatefulSetSpec, spec *appsv1.StatefulSetSpec) {
	if sameUpdateStrategy(stored.UpdateStrategy, spec.UpdateStrategy) {
		spec.UpdateStrategy = *stored.UpdateStrategy.DeepCopy()
	}
	if equality.Semantic.DeepEqual(defaultedPVCRetention(stored.PersistentVolumeClaimRetentionPolicy),
		defaultedPVCRetention(spec.PersistentVolumeClaimRetentionPolicy)) {
		spec.PersistentVolumeClaimRetentionPolicy = stored.PersistentVolumeClaimRetentionPolicy.DeepCopy()
	}
}

// withServiceDefaults returns spec with what the API server fills in on a
// Service the operator leaves out: type ClusterIP, session affinity None, on
// a NodePort or LoadBalancer Service external traffic policy Cluster, and on
// each port protocol TCP and a target port equal to the port. The operator's
// rendering, compared with or written over a stored Service, then differs
// from it only where the operator wants a change.
func withServiceDefaults(spec corev1.ServiceSpec) corev1.ServiceSpec {
	out := *spec.DeepCopy()
	if out.Type == "" {
		out.Type = corev1.ServiceTypeClusterIP
	}
	if out.SessionAffinity == "" {
		out.SessionAffinity = corev1.ServiceAffinityNone
	}
	if out.ExternalTrafficPolicy == "" &&
		(out.Type == corev1.ServiceTypeNodePort || out.Type == corev1.ServiceTypeLoadBalancer) {
		out.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyCluster
	}
	for i := range out.Ports {
		if out.Ports[i].Protocol == "" {
			out.Ports[i].Protocol = corev1.ProtocolTCP
		}
		if out.Ports[i].TargetPort.Type == intstr.Int && out.Ports[i].TargetPort.IntVal == 0 {
			out.Ports[i].TargetPort = intstr.FromInt32(out.Ports[i].Port)
		}
	}
	return out
}
