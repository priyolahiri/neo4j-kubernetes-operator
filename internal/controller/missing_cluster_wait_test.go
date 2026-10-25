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

// Applying a database, a user or a role together with its cluster is the
// common case: one kubectl apply -f dir/, one GitOps sync. Every reconciler
// that resolves a clusterRef waits for a deployment that does not exist yet —
// phase Pending, one ClusterNotFound Warning — and carries on once it is
// there (knowledge operations.md ids 145 and 182). The database, user, role,
// binding and auth rule validators refused the missing deployment first, so
// with the operator's validators wired in the wait was unreachable: the
// object went ValidationFailed (or Failed), with a ValidationFailed Warning,
// until a later pass found the cluster. The composite, alias and replica
// reached their wait but announced nothing. These run every kind with the
// validator the operator wires in.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// errNoBolt stands in for a target the test has no Neo4j behind.
var errNoBolt = errors.New("connection refused")

type missingClusterCase struct {
	name string
	// obj is a valid object whose clusterRef ("sa") names nothing yet;
	// invalid breaks one rule of its spec on top.
	obj, invalid func() client.Object
	// invalidField is the field the broken rule names.
	invalidField string
	reconciler   func(c client.Client, rec record.EventRecorder) reconcile.Reconciler
	// state reads the phase and the Ready condition's reason and message.
	state func(t *testing.T, c client.Client, key client.ObjectKey) (phase, reason, message string)
}

// readyState is a state reader for kinds whose status carries a phase and a
// Ready condition.
func readyState[T client.Object](newObj func() T, phase func(T) string, conds func(T) []metav1.Condition) func(t *testing.T, c client.Client, key client.ObjectKey) (string, string, string) {
	return func(t *testing.T, c client.Client, key client.ObjectKey) (string, string, string) {
		t.Helper()
		o := newObj()
		require.NoError(t, c.Get(context.Background(), key, o))
		cond := meta.FindStatusCondition(conds(o), ConditionTypeReady)
		if cond == nil {
			return phase(o), "", ""
		}
		return phase(o), cond.Reason, cond.Message
	}
}

func missingClusterCases() []missingClusterCase {
	meta := repeatedWarningsMeta
	return []missingClusterCase{
		{
			name: "Neo4jDatabase",
			obj: func() client.Object {
				return &neo4jv1beta1.Neo4jDatabase{ObjectMeta: meta("orders", DatabaseFinalizer),
					Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "sa", Name: "orders", Wait: true, IfNotExists: true}}
			},
			invalid: func() client.Object {
				return &neo4jv1beta1.Neo4jDatabase{ObjectMeta: meta("orders", DatabaseFinalizer),
					Spec: neo4jv1beta1.Neo4jDatabaseSpec{ClusterRef: "sa", Name: "system", Wait: true, IfNotExists: true}}
			},
			invalidField: "spec.name",
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					DatabaseValidator: validation.NewDatabaseValidator(c),
					newNeo4jClient: func(*neo4jv1beta1.Neo4jEnterpriseCluster, *neo4jv1beta1.Neo4jEnterpriseStandalone) (databaseNeo4jClient, error) {
						return &databaseServer{}, nil
					}}
			},
			state: readyState(func() *neo4jv1beta1.Neo4jDatabase { return &neo4jv1beta1.Neo4jDatabase{} },
				func(o *neo4jv1beta1.Neo4jDatabase) string { return o.Status.Phase },
				func(o *neo4jv1beta1.Neo4jDatabase) []metav1.Condition { return o.Status.Conditions }),
		},
		{
			name: "Neo4jUser",
			obj: func() client.Object {
				return &neo4jv1beta1.Neo4jUser{ObjectMeta: meta("alice", Neo4jUserFinalizer),
					Spec: neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice", Roles: []string{"reader"},
						PasswordSecretRef: &neo4jv1beta1.SecretKeyRef{Name: "alice-password"}}}
			},
			invalid: func() client.Object {
				return &neo4jv1beta1.Neo4jUser{ObjectMeta: meta("alice", Neo4jUserFinalizer),
					Spec: neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "1alice", Roles: []string{"reader"},
						PasswordSecretRef: &neo4jv1beta1.SecretKeyRef{Name: "alice-password"}}}
			},
			invalidField: "spec.username",
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				server := newFakeAuthServer()
				return &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, Validator: validation.NewUserValidator(c),
					newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}
			},
			state: readyState(func() *neo4jv1beta1.Neo4jUser { return &neo4jv1beta1.Neo4jUser{} },
				func(o *neo4jv1beta1.Neo4jUser) string { return o.Status.Phase },
				func(o *neo4jv1beta1.Neo4jUser) []metav1.Condition { return o.Status.Conditions }),
		},
		{
			name: "Neo4jRole",
			obj: func() client.Object {
				return &neo4jv1beta1.Neo4jRole{ObjectMeta: meta("analyst", Neo4jRoleFinalizer),
					Spec: neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "analyst"}}
			},
			invalid: func() client.Object {
				return &neo4jv1beta1.Neo4jRole{ObjectMeta: meta("analyst", Neo4jRoleFinalizer),
					Spec: neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "1analyst"}}
			},
			invalidField: "spec.name",
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jRoleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, Validator: validation.NewRoleValidator(c),
					connectTarget: func(ResolvedTarget) (*neo4jclient.Client, error) { return nil, errNoBolt }}
			},
			state: readyState(func() *neo4jv1beta1.Neo4jRole { return &neo4jv1beta1.Neo4jRole{} },
				func(o *neo4jv1beta1.Neo4jRole) string { return o.Status.Phase },
				func(o *neo4jv1beta1.Neo4jRole) []metav1.Condition { return o.Status.Conditions }),
		},
		{
			name: "Neo4jRoleBinding",
			obj: func() client.Object {
				return &neo4jv1beta1.Neo4jRoleBinding{ObjectMeta: meta("bob", Neo4jRoleBindingFinalizer),
					Spec: neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "sa", Username: "bob", Roles: []string{"reader"}}}
			},
			invalid: func() client.Object {
				return &neo4jv1beta1.Neo4jRoleBinding{ObjectMeta: meta("bob", Neo4jRoleBindingFinalizer),
					Spec: neo4jv1beta1.Neo4jRoleBindingSpec{ClusterRef: "sa", Username: "1bob", Roles: []string{"reader"}}}
			},
			invalidField: "spec.username",
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				server := newFakeAuthServer()
				server.users["bob"] = nil
				return &Neo4jRoleBindingReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, Validator: validation.NewRoleBindingValidator(c),
					newNeo4jClient: func(ResolvedTarget) (roleBindingNeo4jClient, error) { return server, nil }}
			},
			state: readyState(func() *neo4jv1beta1.Neo4jRoleBinding { return &neo4jv1beta1.Neo4jRoleBinding{} },
				func(o *neo4jv1beta1.Neo4jRoleBinding) string { return o.Status.Phase },
				func(o *neo4jv1beta1.Neo4jRoleBinding) []metav1.Condition { return o.Status.Conditions }),
		},
		{
			name: "Neo4jAuthRule",
			obj: func() client.Object {
				return &neo4jv1beta1.Neo4jAuthRule{ObjectMeta: meta("rule", Neo4jAuthRuleFinalizer),
					Spec: neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "sa", Name: "rule",
						Condition: "abac.oidc.user_attribute('dept') = 'sales'", GrantedRoles: []string{"reader"}}}
			},
			invalid: func() client.Object {
				return &neo4jv1beta1.Neo4jAuthRule{ObjectMeta: meta("rule", Neo4jAuthRuleFinalizer),
					Spec: neo4jv1beta1.Neo4jAuthRuleSpec{ClusterRef: "sa", Name: "1rule",
						Condition: "abac.oidc.user_attribute('dept') = 'sales'", GrantedRoles: []string{"reader"}}}
			},
			invalidField: "spec.name",
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jAuthRuleReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, Validator: validation.NewAuthRuleValidator(c),
					connectTarget: func(ResolvedTarget) (*neo4jclient.Client, error) { return nil, errNoBolt }}
			},
			state: readyState(func() *neo4jv1beta1.Neo4jAuthRule { return &neo4jv1beta1.Neo4jAuthRule{} },
				func(o *neo4jv1beta1.Neo4jAuthRule) string { return o.Status.Phase },
				func(o *neo4jv1beta1.Neo4jAuthRule) []metav1.Condition { return o.Status.Conditions }),
		},
		{
			name: "Neo4jCompositeDatabase",
			obj: func() client.Object {
				return &neo4jv1beta1.Neo4jCompositeDatabase{ObjectMeta: meta("sales", Neo4jCompositeDatabaseFinalizer),
					Spec: neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "sa", Name: "sales"}}
			},
			invalid: func() client.Object {
				return &neo4jv1beta1.Neo4jCompositeDatabase{ObjectMeta: meta("sales", Neo4jCompositeDatabaseFinalizer),
					Spec: neo4jv1beta1.Neo4jCompositeDatabaseSpec{ClusterRef: "sa", Name: "system"}}
			},
			invalidField: "spec.name",
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jCompositeDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewCompositeDatabaseValidator(c)}
			},
			state: readyState(func() *neo4jv1beta1.Neo4jCompositeDatabase { return &neo4jv1beta1.Neo4jCompositeDatabase{} },
				func(o *neo4jv1beta1.Neo4jCompositeDatabase) string { return o.Status.Phase },
				func(o *neo4jv1beta1.Neo4jCompositeDatabase) []metav1.Condition { return o.Status.Conditions }),
		},
		{
			name: "Neo4jDatabaseAlias",
			obj: func() client.Object {
				return &neo4jv1beta1.Neo4jDatabaseAlias{ObjectMeta: meta("latest", Neo4jDatabaseAliasFinalizer),
					Spec: neo4jv1beta1.Neo4jDatabaseAliasSpec{ClusterRef: "sa", Name: "latest", TargetDatabase: "orders"}}
			},
			invalid: func() client.Object {
				return &neo4jv1beta1.Neo4jDatabaseAlias{ObjectMeta: meta("latest", Neo4jDatabaseAliasFinalizer),
					Spec: neo4jv1beta1.Neo4jDatabaseAliasSpec{ClusterRef: "sa", Name: "latest", TargetDatabase: "system"}}
			},
			invalidField: "spec.targetDatabase",
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jDatabaseAliasReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator: validation.NewAliasValidator(c)}
			},
			state: readyState(func() *neo4jv1beta1.Neo4jDatabaseAlias { return &neo4jv1beta1.Neo4jDatabaseAlias{} },
				func(o *neo4jv1beta1.Neo4jDatabaseAlias) string { return o.Status.Phase },
				func(o *neo4jv1beta1.Neo4jDatabaseAlias) []metav1.Condition { return o.Status.Conditions }),
		},
		{
			name: "Neo4jReplicaDatabase",
			obj: func() client.Object {
				return &neo4jv1beta1.Neo4jReplicaDatabase{ObjectMeta: meta("foo-replica", Neo4jReplicaDatabaseFinalizer),
					Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{ClusterRef: "sa", UpstreamDatabase: "foo",
						Source: neo4jv1beta1.ReplicaSourceSpec{Mode: neo4jv1beta1.ReplicaSourceModeNetwork,
							UpstreamClusterRef: &neo4jv1beta1.UpstreamClusterRef{Name: "prod"}}}}
			},
			invalid: func() client.Object {
				return &neo4jv1beta1.Neo4jReplicaDatabase{ObjectMeta: meta("foo-replica", Neo4jReplicaDatabaseFinalizer),
					Spec: neo4jv1beta1.Neo4jReplicaDatabaseSpec{ClusterRef: "sa", UpstreamDatabase: "foo",
						Source: neo4jv1beta1.ReplicaSourceSpec{Mode: neo4jv1beta1.ReplicaSourceModeNetwork}}}
			},
			invalidField: "spec.source",
			reconciler: func(c client.Client, rec record.EventRecorder) reconcile.Reconciler {
				return &Neo4jReplicaDatabaseReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
					Validator:      validation.NewReplicaValidator(c),
					newNeo4jClient: func(ResolvedTarget, client.Client) (replicaNeo4jClient, error) { return nil, errNoBolt }}
			},
			state: readyState(func() *neo4jv1beta1.Neo4jReplicaDatabase { return &neo4jv1beta1.Neo4jReplicaDatabase{} },
				func(o *neo4jv1beta1.Neo4jReplicaDatabase) string { return o.Status.Phase },
				func(o *neo4jv1beta1.Neo4jReplicaDatabase) []metav1.Condition { return o.Status.Conditions }),
		},
	}
}

// A user applied with neither its password Secret nor its cluster waits for
// the cluster first, as every kind does (rule 182): it said SecretPending, and
// its ClusterNotFound wait began only once the Secret was there. With the
// cluster created, it waits for the Secret; with both, it goes on.
func TestUserReconcile_AMissingClusterIsReportedBeforeAMissingSecret(t *testing.T) {
	ctx := context.Background()
	user := &neo4jv1beta1.Neo4jUser{ObjectMeta: repeatedWarningsMeta("alice", Neo4jUserFinalizer),
		Spec: neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice", Roles: []string{"reader"},
			PasswordSecretRef: &neo4jv1beta1.SecretKeyRef{Name: "alice-password"}}}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(user).
		WithStatusSubresource(user, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	server := newFakeAuthServer()
	r := &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec, Validator: validation.NewUserValidator(c),
		newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}
	key := client.ObjectKeyFromObject(user)
	state := readyState(func() *neo4jv1beta1.Neo4jUser { return &neo4jv1beta1.Neo4jUser{} },
		func(o *neo4jv1beta1.Neo4jUser) string { return o.Status.Phase },
		func(o *neo4jv1beta1.Neo4jUser) []metav1.Condition { return o.Status.Conditions })

	reconcileTimes(t, r, key, repeatPasses)
	phase, reason, message := state(t, c, key)
	assert.Equal(t, neo4jv1beta1.PhasePending, phase)
	assert.Equal(t, EventReasonClusterNotFound, reason, message)
	assert.Len(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound), 1)

	require.NoError(t, c.Create(ctx, authTargetStandalone()))
	reconcileTimes(t, r, key, 1)
	_, reason, message = state(t, c, key)
	assert.Equal(t, ConditionReasonSecretPending, reason, message)
	assert.Contains(t, message, "alice-password")

	require.NoError(t, c.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "alice-password", Namespace: repeatedWarningsNS},
		Data:       map[string][]byte{"password": []byte("a-long-password")},
	}))
	reconcileTimes(t, r, key, 1)
	phase, reason, message = state(t, c, key)
	assert.Equal(t, "Ready", phase, "%s: %s", reason, message)
}

// missingClusterClient holds obj, and the user's password Secret, and no
// deployment.
func missingClusterClient(t *testing.T, obj client.Object) client.Client {
	t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "alice-password", Namespace: repeatedWarningsNS},
		Data:       map[string][]byte{"password": []byte("a-long-password")},
	}
	return fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(obj, secret).
		WithStatusSubresource(obj, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
}

// A clusterRef that names nothing yet: Pending, one ClusterNotFound however
// many passes meet it, no ValidationFailed; and once the deployment exists
// the object goes on by itself.
func TestReconcile_AMissingClusterIsAWaitThatEnds(t *testing.T) {
	for _, tc := range missingClusterCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			obj := tc.obj()
			c := missingClusterClient(t, obj)
			rec := record.NewFakeRecorder(100)
			key := client.ObjectKeyFromObject(obj)

			reconcileTimes(t, tc.reconciler(c, rec), key, repeatPasses)
			events := drainEvents(rec)
			phase, reason, message := tc.state(t, c, key)
			assert.Equal(t, neo4jv1beta1.PhasePending, phase, "%s: %s", reason, message)
			assert.Equal(t, EventReasonClusterNotFound, reason, message)
			assert.Len(t, eventsWithReason(events, EventReasonClusterNotFound), 1, "%q", events)
			assert.Empty(t, eventsWithReason(events, EventReasonValidationFailed), "a missing cluster is not a refused spec")

			// A restarted operator reads the wait from status.
			reconcileTimes(t, tc.reconciler(c, rec), key, 1)
			assert.Empty(t, eventsWithReason(drainEvents(rec), EventReasonClusterNotFound))

			// The deployment arrives; the next pass goes past the wait.
			require.NoError(t, c.Create(ctx, authTargetStandalone()))
			reconcileTimes(t, tc.reconciler(c, rec), key, 1)
			phase, reason, message = tc.state(t, c, key)
			assert.NotEqual(t, EventReasonClusterNotFound, reason, "%s: %s", phase, message)
		})
	}
}

// A spec that breaks a rule is refused whether or not its cluster exists,
// with the rule it breaks and nothing about the cluster: the missing cluster
// was reported as an error of its own, beside the spec's.
func TestReconcile_ASpecErrorIsReportedWhileTheClusterIsMissing(t *testing.T) {
	for _, tc := range missingClusterCases() {
		t.Run(tc.name, func(t *testing.T) {
			obj := tc.invalid()
			c := missingClusterClient(t, obj)
			rec := record.NewFakeRecorder(100)
			key := client.ObjectKeyFromObject(obj)

			reconcileTimes(t, tc.reconciler(c, rec), key, repeatPasses)
			events := drainEvents(rec)
			_, reason, message := tc.state(t, c, key)
			assert.Equal(t, EventReasonValidationFailed, reason, message)
			assert.Contains(t, message, tc.invalidField)
			assert.NotContains(t, message, "clusterRef", "the missing cluster is a wait, not one of the spec's errors")
			assert.Len(t, eventsWithReason(events, EventReasonValidationFailed), 1, "%q", events)
		})
	}
}
