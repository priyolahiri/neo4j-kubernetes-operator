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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// Advisory warnings used to be emitted on every reconcile pass: 35 identical
// events in three minutes on one standalone. Each spent the object's event
// budget, which client-go's spam filter caps per object, so the flood could
// crowd out the events that report real changes. A warning is now announced
// when it starts to apply, and again every advisoryWarningRefresh while it
// still does, so it never expires out of kubectl describe while still true.
func TestAdvisoryWarnings(t *testing.T) {
	clock := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	newWarnings := func() *advisoryWarnings {
		return &advisoryWarnings{now: func() time.Time { return clock }}
	}
	obj := func(uid string) client.Object {
		return &neo4jv1beta1.Neo4jEnterpriseStandalone{
			ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default", UID: types.UID(uid)}}
	}

	t.Run("announced once, not on every pass", func(t *testing.T) {
		a, rec := newWarnings(), record.NewFakeRecorder(10)
		for range 5 {
			a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1", "w2"})
		}
		assert.Equal(t, []string{
			"Warning " + EventReasonValidationWarning + " w1",
			"Warning " + EventReasonValidationWarning + " w2",
		}, drainEvents(rec))
	})

	t.Run("announced again once the refresh interval has passed", func(t *testing.T) {
		a, rec := newWarnings(), record.NewFakeRecorder(10)
		a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1"})
		clock = clock.Add(advisoryWarningRefresh - time.Second)
		a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1"})
		require.Len(t, drainEvents(rec), 1, "not yet")
		clock = clock.Add(time.Second)
		a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1"})
		assert.Len(t, drainEvents(rec), 1, "the refresh keeps the event from expiring while it is still true")
	})

	// A field removed and then set again is a new statement by the user; it
	// must not wait out the refresh interval of the first one.
	t.Run("a warning that stops applying is announced again when it returns", func(t *testing.T) {
		a, rec := newWarnings(), record.NewFakeRecorder(10)
		a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1"})
		a.record(rec, obj("u1"), EventReasonValidationWarning, nil)
		a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1"})
		assert.Len(t, drainEvents(rec), 2)
	})

	t.Run("a new warning is announced while an old one is held back", func(t *testing.T) {
		a, rec := newWarnings(), record.NewFakeRecorder(10)
		a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1"})
		a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1", "w2"})
		assert.Equal(t, []string{
			"Warning " + EventReasonValidationWarning + " w1",
			"Warning " + EventReasonValidationWarning + " w2",
		}, drainEvents(rec))
	})

	t.Run("objects, recreated objects and reasons are tracked apart", func(t *testing.T) {
		a, rec := newWarnings(), record.NewFakeRecorder(10)
		a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1"})
		a.record(rec, obj("u2"), EventReasonValidationWarning, []string{"w1"}) // same name, recreated
		a.record(rec, obj("u1"), EventReasonTopologyWarning, []string{"w1"})
		assert.Len(t, drainEvents(rec), 3)
	})

	// Entries for deleted objects must not pile up for the operator's
	// lifetime. Dropping one older than the refresh interval changes nothing:
	// it would be announced again on its next pass anyway.
	t.Run("stale entries are swept", func(t *testing.T) {
		a, rec := newWarnings(), record.NewFakeRecorder(10)
		a.record(rec, obj("gone"), EventReasonValidationWarning, []string{"w1"})
		clock = clock.Add(advisoryWarningRefresh)
		a.record(rec, obj("u1"), EventReasonValidationWarning, []string{"w1"})
		assert.Len(t, a.announced, 1, "the deleted object's entry is dropped")
	})

	t.Run("a nil recorder is a no-op", func(t *testing.T) {
		newWarnings().record(nil, obj("u1"), EventReasonValidationWarning, []string{"w1"})
	})
}

// The bug as the v1.21.0 journey found it, end to end: a standalone with
// spec.upgradeStrategy.maxUnavailableDuringUpgrade: 2, reconciled repeatedly,
// gets the warning once.
func TestStandaloneReconcile_WarnsOnceNotEveryPass(t *testing.T) {
	scheme := noEffectTestScheme(t)
	standalone := &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: "default", Finalizers: []string{StandaloneFinalizer}},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			AcceptLicenseAgreement: "eval",
			Image:                  neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "5.26.0-enterprise"},
			Storage:                neo4jv1beta1.StorageSpec{ClassName: "standard", Size: "1Gi"},
			UpgradeStrategy:        &neo4jv1beta1.UpgradeStrategySpec{MaxUnavailableDuringUpgrade: ptr.To[int32](2)},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(standalone).
		WithStatusSubresource(&neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	r := &Neo4jEnterpriseStandaloneReconciler{
		Client: c, Scheme: scheme, Recorder: rec, Validator: validation.NewStandaloneValidator(),
	}

	for range 3 {
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(standalone)})
		require.NoError(t, err)
	}

	var warned int
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, "spec.upgradeStrategy.maxUnavailableDuringUpgrade") {
			warned++
		}
	}
	assert.Equal(t, 1, warned)
}
