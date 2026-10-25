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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// The password a Neo4jUser's Secret holds is recorded as applied — its change
// token in status.passwordSecretHash — by a status write right after the
// statement that applied it, and PasswordRotated and the "already matches"
// UserUpdated are announced from that write (knowledge rule 15). Recorded only
// at the end of a successful pass, the token was missing after every pass a
// failing grant stopped: the next sent the same ALTER again and announced
// "password already matches spec" (18 in two minutes, v1.22.0 combined-build
// walk), and a pass on a copy from before the end write announced
// PasswordRotated twice for a new user.

func passwordSecret(value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "alice-pw", Namespace: repeatedWarningsNS},
		Data:       map[string][]byte{"password": []byte(value)},
	}
}

func passwordUser(roles ...string) *neo4jv1beta1.Neo4jUser {
	return &neo4jv1beta1.Neo4jUser{
		ObjectMeta: repeatedWarningsMeta("alice", Neo4jUserFinalizer),
		Spec: neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "alice", Roles: roles,
			PasswordSecretRef: &neo4jv1beta1.SecretKeyRef{Name: "alice-pw"}},
	}
}

func TestUserReconcile_AnAlreadyMatchingPasswordIsAnnouncedOnceWhileAGrantFails(t *testing.T) {
	ctx := context.Background()
	server := newFakeAuthServer()
	// The user exists and Neo4j already holds the Secret's password, but status
	// has no token (lost, or an adopted user).
	server.users["alice"] = nil
	server.alterErr = errPasswordUnchanged
	// A Neo4jRole whose role Neo4j does not have yet: the grant fails.
	auditor := &neo4jv1beta1.Neo4jRole{
		ObjectMeta: repeatedWarningsMeta("auditor", Neo4jRoleFinalizer),
		Spec:       neo4jv1beta1.Neo4jRoleSpec{ClusterRef: "sa", Name: "auditor"},
	}
	user := passwordUser("auditor")
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).
		WithObjects(user, auditor, passwordSecret("s3cret-pass"), authTargetStandalone()).
		WithStatusSubresource(user, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	r := &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
		newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}

	reconcileTimes(t, r, client.ObjectKeyFromObject(user), repeatPasses)

	events := drainEvents(rec)
	updated := eventsWithReason(events, EventReasonUserUpdated)
	require.Len(t, updated, 1, "announced once, not on every retry of the grant: %q", updated)
	assert.Contains(t, updated[0], "password already matches spec")
	assert.Empty(t, eventsWithReason(events, EventReasonPasswordRotated),
		"nothing was rotated: the already-matching password is one event, not two")
	assert.Len(t, eventsWithReason(events, EventReasonUserSyncFailed), 1, "the grant's failure, once")
	assert.Equal(t, 1, server.alters, "the same ALTER is not sent again on every retry")
	live := &neo4jv1beta1.Neo4jUser{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(user), live))
	assert.NotEmpty(t, live.Status.PasswordSecretHash, "recorded although the pass failed")
	assert.Equal(t, "Failed", live.Status.Phase)
}

func TestUserReconcile_ANewUsersPasswordIsAnnouncedOnce(t *testing.T) {
	ctx := context.Background()
	server := newFakeAuthServer()
	server.alterErr = errPasswordUnchanged // an ALTER to the password it was created with
	user := passwordUser("reader")
	secret := passwordSecret("s3cret-pass")
	base := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).
		WithObjects(user.DeepCopy(), secret, authTargetStandalone()).
		WithStatusSubresource(user, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	c := &staleFirstRead{Client: base}
	rec := record.NewFakeRecorder(100)
	newReconciler := func() *Neo4jUserReconciler {
		return &Neo4jUserReconciler{Client: c, Scheme: base.Scheme(), Recorder: rec,
			newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}
	}
	key := client.ObjectKeyFromObject(user)

	reconcileTimes(t, newReconciler(), key, 1)
	// A pass whose cache still holds the user from before the first pass's
	// writes, then a restarted operator.
	c.stale = user.DeepCopy()
	reconcileTimes(t, newReconciler(), key, 1)
	reconcileTimes(t, newReconciler(), key, 1)

	events := drainEvents(rec)
	assert.Len(t, eventsWithReason(events, EventReasonUserCreated), 1)
	assert.Len(t, eventsWithReason(events, EventReasonPasswordRotated), 1, "once at creation: %q", events)
	assert.Empty(t, eventsWithReason(events, EventReasonUserUpdated), "nothing was updated: %q", events)
	assert.Len(t, eventsWithReason(events, EventReasonUserReady), 1)

	// A real rotation: the Secret is written, the ALTER goes through.
	server.alterErr = nil
	live := &corev1.Secret{}
	require.NoError(t, base.Get(ctx, client.ObjectKeyFromObject(secret), live))
	live.Data["password"] = []byte("n3w-s3cret-pass")
	require.NoError(t, base.Update(ctx, live))
	reconcileTimes(t, newReconciler(), key, repeatPasses)
	events = drainEvents(rec)
	assert.Len(t, eventsWithReason(events, EventReasonUserUpdated), 1)
	assert.Len(t, eventsWithReason(events, EventReasonPasswordRotated), 1)
	assert.Empty(t, eventsWithReason(events, EventReasonUserReady), "a rotation leaves the user Ready")
}

// The v1.22.x walk: a Neo4jUser for neo4j, its Secret holding the admin
// password, announced PasswordRotated and "password already matches spec" at
// its creation. Neo4j refused the ALTER as unchanged: nothing was rotated, and
// one event says so.
func TestUserReconcile_TheAdminUsersPasswordIsAnnouncedOnce(t *testing.T) {
	server := newFakeAuthServer()
	server.users["neo4j"] = []string{"admin"}
	server.alterErr = errPasswordUnchanged
	user := &neo4jv1beta1.Neo4jUser{
		ObjectMeta: repeatedWarningsMeta("neo4j", Neo4jUserFinalizer),
		Spec: neo4jv1beta1.Neo4jUserSpec{ClusterRef: "sa", Username: "neo4j", Roles: []string{"admin"},
			PasswordSecretRef: &neo4jv1beta1.SecretKeyRef{Name: "alice-pw"}},
	}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).
		WithObjects(user, passwordSecret("the-admin-password"), authTargetStandalone()).
		WithStatusSubresource(user, &neo4jv1beta1.Neo4jEnterpriseStandalone{}).Build()
	rec := record.NewFakeRecorder(100)
	r := &Neo4jUserReconciler{Client: c, Scheme: c.Scheme(), Recorder: rec,
		newNeo4jClient: func(ResolvedTarget) (userNeo4jClient, error) { return server, nil }}

	reconcileTimes(t, r, client.ObjectKeyFromObject(user), repeatPasses)

	events := drainEvents(rec)
	rotated := eventsWithReason(events, EventReasonPasswordRotated)
	updated := eventsWithReason(events, EventReasonUserUpdated)
	assert.Len(t, append(rotated, updated...), 1, "one password event: %q", events)
	assert.Len(t, updated, 1)
	assert.Contains(t, updated[0], "password already matches spec")
	assert.Len(t, eventsWithReason(events, EventReasonUserReady), 1)
}
