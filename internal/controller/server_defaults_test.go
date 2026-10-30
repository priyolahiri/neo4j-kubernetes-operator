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

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// storedAsTheAPIServerDoes fills in what the API server does on a create or
// update of a StatefulSet or Service and the fake client does not. With
// maxUnavailable false the API server drops a StatefulSet's maxUnavailable,
// as with MaxUnavailableStatefulSet off (Kubernetes 1.34); with it true it
// defaults it to 1 (the gate on).
func storedAsTheAPIServerDoes(obj client.Object, maxUnavailable bool) {
	switch o := obj.(type) {
	case *appsv1.StatefulSet:
		apiServerDefaults(o)
		if o.UID == "" {
			o.UID = types.UID(o.Name + "-uid")
		}
		ru := o.Spec.UpdateStrategy.RollingUpdate
		if o.Spec.UpdateStrategy.Type == "" {
			o.Spec.UpdateStrategy.Type = appsv1.RollingUpdateStatefulSetStrategyType
		}
		if ru != nil {
			if ru.Partition == nil {
				ru.Partition = ptr.To[int32](0)
			}
			switch {
			case !maxUnavailable:
				ru.MaxUnavailable = nil
			case ru.MaxUnavailable == nil:
				one := intstr.FromInt32(1)
				ru.MaxUnavailable = &one
			}
		}
		if o.Spec.PersistentVolumeClaimRetentionPolicy == nil {
			o.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
				WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			}
		}
	case *corev1.Service:
		if o.Spec.Type == "" {
			o.Spec.Type = corev1.ServiceTypeClusterIP
		}
		if o.Spec.SessionAffinity == "" {
			o.Spec.SessionAffinity = corev1.ServiceAffinityNone
		}
		if o.Spec.ExternalTrafficPolicy == "" && (o.Spec.Type == corev1.ServiceTypeNodePort || o.Spec.Type == corev1.ServiceTypeLoadBalancer) {
			o.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyCluster
		}
		for i := range o.Spec.Ports {
			if o.Spec.Ports[i].Protocol == "" {
				o.Spec.Ports[i].Protocol = corev1.ProtocolTCP
			}
			if o.Spec.Ports[i].TargetPort.Type == intstr.Int && o.Spec.Ports[i].TargetPort.IntVal == 0 {
				o.Spec.Ports[i].TargetPort = intstr.FromInt32(o.Spec.Ports[i].Port)
			}
		}
	}
}

// updatesByKind counts the Updates sent to a fake API server, by kind.
type updatesByKind struct {
	mu sync.Mutex
	n  map[string]int
}

func (u *updatesByKind) add(obj client.Object) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.n == nil {
		u.n = map[string]int{}
	}
	u.n[fmt.Sprintf("%T", obj)]++
}

func (u *updatesByKind) reset() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.n = map[string]int{}
}

// of returns the count for the kind of obj.
func (u *updatesByKind) of(obj client.Object) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.n[fmt.Sprintf("%T", obj)]
}

func (u *updatesByKind) String() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	var kinds []string
	for k, v := range u.n {
		kinds = append(kinds, fmt.Sprintf("%s=%d", k, v))
	}
	sort.Strings(kinds)
	return fmt.Sprint(kinds)
}

// defaultingClient is a fake API server that stores StatefulSets and Services
// as the API server does, and counts every Update sent.
func defaultingClient(t *testing.T, maxUnavailable bool, objs ...client.Object) (client.Client, *updatesByKind) {
	t.Helper()
	updates := &updatesByKind{}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(objs...).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseCluster{}, &neo4jv1beta1.Neo4jEnterpriseStandalone{}, &appsv1.StatefulSet{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				storedAsTheAPIServerDoes(obj, maxUnavailable)
				return c.Create(ctx, obj, opts...)
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updates.add(obj)
				storedAsTheAPIServerDoes(obj, maxUnavailable)
				return c.Update(ctx, obj, opts...)
			},
		}).Build()
	return c, updates
}

// kubeControllerManagerWrites the StatefulSet's status as its controller does
// during a roll: a pod replaced and not ready yet, or every pod updated and
// ready again.
func kubeControllerManagerWrites(t *testing.T, c client.Client, key client.ObjectKey, rolling bool) {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(), key, sts))
	n := *sts.Spec.Replicas
	sts.Status = rolledOutStatus(n, sts.Generation)
	if rolling {
		sts.Status.ReadyReplicas, sts.Status.AvailableReplicas, sts.Status.UpdatedReplicas = n-1, n-1, 1
		sts.Status.CurrentRevision, sts.Status.UpdateRevision = "old", "new"
	}
	require.NoError(t, c.Status().Update(context.Background(), sts))
}

// #619 S8, found by the #619 walk: the cluster controller wrote its server
// StatefulSet on the first pass after each status write of the StatefulSet
// controller — 5 times during one roll, 4 after a server pod was replaced —
// each stored object the same as before but for the timestamp of the
// operator's managedFields entry. The pass assigned the update strategy it
// renders (partition unset, maxUnavailable 1) over the stored one (partition
// 0; no maxUnavailable while the gate is off), so CreateOrUpdate found a
// change on every pass. Its Services the same: type, session affinity. A pass
// that changes nothing sends nothing now.
func TestClusterReconcile_WritesNothingWhenNothingChanged(t *testing.T) {
	for name, maxUnavailable := range map[string]bool{"MaxUnavailableStatefulSet off": false, "MaxUnavailableStatefulSet on": true} {
		t.Run(name, func(t *testing.T) {
			cluster := minimalCluster("loop", "loopns")
			cluster.Finalizers = []string{ClusterFinalizer}
			cluster.Spec.Image.Tag = "5.26.32-enterprise"
			c, updates := defaultingClient(t, maxUnavailable, cluster, standardStorageClass())
			r := &Neo4jEnterpriseClusterReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(1000),
				RequeueAfter: time.Minute, Validator: validation.NewClusterValidator(c)}
			pass := func() {
				t.Helper()
				cm := NewConfigMapManager(c)
				cm.LiveConfig = nil
				r.ConfigMapManager = cm
				_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
				require.NoError(t, err)
			}
			key := client.ObjectKey{Namespace: "loopns", Name: "loop-server"}
			pass()
			kubeControllerManagerWrites(t, c, key, false)
			pass()
			updates.reset()

			for _, rolling := range []bool{true, true, false, true, false} {
				kubeControllerManagerWrites(t, c, key, rolling)
				pass()
			}
			assert.Zero(t, updates.of(&appsv1.StatefulSet{}), "no Update of the StatefulSet: %s", updates)
			assert.Zero(t, updates.of(&corev1.Service{}), "no Update of a Service: %s", updates)
		})
	}
}

// The standalone's StatefulSet and Services the same: its strategy is
// RollingUpdate with nothing under it, stored with partition 0, and its
// client Service had no session affinity.
func TestStandaloneReconcile_WritesNothingWhenNothingChanged(t *testing.T) {
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "fms", Namespace: "u4", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.32-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
		},
	}
	c, updates := defaultingClient(t, false, standalone, standardStorageClass())
	r := &Neo4jEnterpriseStandaloneReconciler{Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(1000),
		RequeueAfter: time.Minute, Validator: validation.NewStandaloneValidator()}
	pass := func() {
		t.Helper()
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(standalone)})
		require.NoError(t, err)
	}
	key := client.ObjectKeyFromObject(standalone)
	pass()
	kubeControllerManagerWrites(t, c, key, false)
	pass()
	updates.reset()

	for range 4 {
		kubeControllerManagerWrites(t, c, key, false)
		pass()
	}
	assert.Zero(t, updates.of(&appsv1.StatefulSet{}), "no Update of the StatefulSet: %s", updates)
	assert.Zero(t, updates.of(&corev1.Service{}), "no Update of a Service: %s", updates)
}

// A rendering that only leaves out what the API server fills in is the stored
// value; one that asks for something else is not.
func TestSameUpdateStrategy(t *testing.T) {
	one, two := intstr.FromInt32(1), intstr.FromInt32(2)
	rendered := appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{MaxUnavailable: &one}}
	stored := func(partition int32, maxUnavailable *intstr.IntOrString) appsv1.StatefulSetUpdateStrategy {
		return appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{Partition: &partition, MaxUnavailable: maxUnavailable}}
	}
	assert.True(t, sameUpdateStrategy(stored(0, nil), rendered), "gate off: maxUnavailable dropped")
	assert.True(t, sameUpdateStrategy(stored(0, &one), rendered), "gate on")
	assert.True(t, sameUpdateStrategy(stored(0, nil), appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType}),
		"the standalone's: nothing under RollingUpdate")
	assert.False(t, sameUpdateStrategy(stored(2, nil), rendered), "a partition left over is not the rendering")
	assert.False(t, sameUpdateStrategy(stored(0, &two), rendered))
	assert.False(t, sameUpdateStrategy(appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}, rendered))
}
