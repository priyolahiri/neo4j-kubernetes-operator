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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// A dependent of a target that is being deleted counted it as present: during
// a namespace teardown it waited, with one Warning, until the target CR was
// gone. And a target CR that was gone released its dependents at once, as if
// its system database had gone with it — which it does not when its volumes
// are kept (storage.retentionPolicy: Retain): a deployment created again from
// them brings the user, the grant, the database back. The fate of what a
// deletion removes turns on the target's volumes (knowledge rule 178):
//   - being deleted with its volumes (Delete, the default): released at once;
//   - being deleted, its volumes kept: the cleanup runs while it may still
//     serve, and waits as for any target;
//   - gone, its volumes kept: waits, bounded, for a deployment created again
//     from them, then releases naming what they hold;
//   - gone with its volumes: released at once.

// terminatingTarget is the standalone "sa" being deleted, with the retention
// policy given, Ready or not.
func terminatingTarget(retention string, ready bool) *neo4jv1beta1.Neo4jEnterpriseStandalone {
	sa := unreachableTarget()
	sa.Spec.Storage.RetentionPolicy = retention
	sa.Status.Ready = ready
	if !ready {
		sa.Status.Phase = "Pending"
	}
	now := metav1.Now()
	sa.DeletionTimestamp = &now
	sa.Finalizers = []string{StandaloneFinalizer}
	return sa
}

// keptVolume is a data volume of "sa" its deletion left; deleting marks it as
// being deleted.
func keptVolume(deleting bool) *corev1.PersistentVolumeClaim {
	labels := resources.PVCSelectorByInstance("sa")
	labels["app.kubernetes.io/managed-by"] = "neo4j-operator"
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "data-sa-0", Namespace: repeatedWarningsNS, Labels: labels}}
	if deleting {
		now := metav1.Now()
		pvc.DeletionTimestamp = &now
		pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
	}
	return pvc
}

func TestDeletion_ATargetBeingDeletedOrGone(t *testing.T) {
	type outcome int
	const (
		releasedAtOnce outcome = iota
		dropped
		waits
	)
	for _, sc := range []struct {
		name       string
		objects    func() []client.Object
		want       outcome
		cause      string
		readyCheck bool // only for the kinds that check readiness
	}{
		{name: "a target being deleted with its volumes, not Ready",
			objects: func() []client.Object { return []client.Object{terminatingTarget("", false)} }, want: releasedAtOnce},
		{name: "a target being deleted with its volumes, Ready",
			objects: func() []client.Object { return []client.Object{terminatingTarget("Delete", true)} }, want: releasedAtOnce},
		{name: "a target being deleted that keeps its volumes, Ready",
			objects: func() []client.Object { return []client.Object{terminatingTarget("Retain", true)} }, want: dropped},
		{name: "a target being deleted that keeps its volumes, not Ready",
			objects: func() []client.Object { return []client.Object{terminatingTarget("Retain", false)} }, want: waits,
			cause: "is not Ready or does not accept connections yet", readyCheck: true},
		{name: "a target gone, its volumes kept",
			objects: func() []client.Object { return []client.Object{keptVolume(false)} }, want: waits,
			cause: `clusterRef "sa" was deleted and its volumes were kept`},
		{name: "a target gone, its volume being deleted",
			objects: func() []client.Object { return []client.Object{keptVolume(true)} }, want: releasedAtOnce},
		{name: "a target gone with no volume",
			objects: func() []client.Object { return nil }, want: releasedAtOnce},
	} {
		for _, k := range deletionKinds() {
			if sc.readyCheck && !k.checksReady {
				continue
			}
			t.Run(sc.name+"/"+k.name, func(t *testing.T) {
				obj := k.obj(time.Now())
				c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(append(sc.objects(), obj)...).
					WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
				rec := record.NewFakeRecorder(50)
				var connErr error
				r, sent := k.setup(c, rec, &connErr)

				for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), 1) {
					require.NoError(t, err)
				}
				events := drainEvents(rec)
				switch sc.want {
				case releasedAtOnce:
					assert.True(t, finalizerReleased(t, c, obj, k.finalizer), "nothing can be left")
					assert.Zero(t, sent(), "no statement for what went with the volumes")
					assert.Empty(t, eventsWithReason(events, k.reason), "%q", events)
				case dropped:
					assert.True(t, finalizerReleased(t, c, obj, k.finalizer))
					assert.Equal(t, 1, sent(), "a target being deleted that keeps its volumes still takes the drop")
				case waits:
					assert.False(t, finalizerReleased(t, c, obj, k.finalizer), "the volumes still hold it")
					assert.Zero(t, sent())
					warned := eventsWithReason(events, k.reason)
					require.Len(t, warned, 1, "%q", events)
					assert.Contains(t, warned[0], sc.cause)
				}
			})
		}
	}
}

// Once the grace period is over, a deletion that waited on kept volumes
// releases and says what they still hold.
func TestDeletion_KeptVolumesAreNamedWhenTheWaitEnds(t *testing.T) {
	for _, k := range deletionKinds() {
		t.Run(k.name, func(t *testing.T) {
			obj := k.obj(time.Now().Add(-finalizerDeletionGracePeriod - time.Minute))
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, keptVolume(false)).
				WithStatusSubresource(obj).Build()
			rec := record.NewFakeRecorder(50)
			var connErr error
			r, _ := k.setup(c, rec, &connErr)

			for _, err := range reconcileTimes(t, r, client.ObjectKeyFromObject(obj), 1) {
				require.NoError(t, err)
			}
			assert.True(t, finalizerReleased(t, c, obj, k.finalizer))
			released := eventsWithReason(drainEvents(rec), k.reason)
			require.Len(t, released, 1)
			assert.True(t, strings.HasPrefix(released[0], "Warning "))
			assert.Contains(t, released[0], "its volumes were kept")
			assert.Contains(t, released[0], k.byHand)
		})
	}
}
