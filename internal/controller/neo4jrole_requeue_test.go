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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
)

// A Neo4jRole failure that lasts until someone acts is retried on the role's
// fixed interval (#526): a built-in role the target does not have was reported
// with a nil error, which went back to controller-runtime as "done", so nothing
// requeued the role; learn mode's state that the API server will not keep went
// to error backoff, which grows to over a quarter of an hour.
func TestRoleFail_LastingFailuresRequeueOnTheFixedInterval(t *testing.T) {
	const requeue = 30 * time.Second
	ctx := context.Background()
	gr := schema.GroupResource{Group: "neo4j.neo4j.com", Resource: "neo4jroles"}
	for _, tc := range []struct {
		name string
		err  error
		want ctrl.Result
		errd bool
	}{
		{name: "a built-in role the target lacks (no error)", err: nil, want: ctrl.Result{RequeueAfter: requeue}},
		{name: "a CRD that prunes learn mode's status", err: errNeo4jRoleCRDOutdated, want: ctrl.Result{RequeueAfter: requeue}},
		{name: "a status the API server refuses", err: apierrors.NewInvalid(schema.GroupKind{Group: gr.Group, Kind: "Neo4jRole"}, "r", nil),
			want: ctrl.Result{RequeueAfter: requeue}},
		{name: "a status too large to store", err: apierrors.NewRequestEntityTooLargeError("status too large"),
			want: ctrl.Result{RequeueAfter: requeue}},
		{name: "a refusal Neo4j repeats", err: errors.New("Neo.ClientError.Statement.SyntaxError: Invalid input"),
			want: ctrl.Result{RequeueAfter: requeue}},
		{name: "a conflict keeps error backoff", err: apierrors.NewConflict(gr, "r", errors.New("modified")), errd: true},
		{name: "a connectivity blip keeps error backoff", err: errors.New("ConnectivityError: EOF"), errd: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			role := &neo4jv1beta1.Neo4jRole{ObjectMeta: repeatedWarningsMeta("role", Neo4jRoleFinalizer)}
			c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(role).WithStatusSubresource(role).Build()
			r := &Neo4jRoleReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}

			res, err := r.fail(ctx, role, "label", tc.err, requeue)
			if tc.errd {
				require.Error(t, err)
				assert.Equal(t, ctrl.Result{}, res)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, res, "retried on the fixed interval")
		})
	}
}

// The chain that reaches fail(): an API server that prunes the learn-mode
// fields (CRDs older than the operator) makes setLearnState report
// errNeo4jRoleCRDOutdated, which fail() retries on the fixed interval.
func TestSetLearnState_APruningAPIServerIsALastingFailure(t *testing.T) {
	ctx := context.Background()
	role := &neo4jv1beta1.Neo4jRole{ObjectMeta: repeatedWarningsMeta("role", Neo4jRoleFinalizer)}
	c := fake.NewClientBuilder().WithScheme(noEffectTestScheme(t)).WithObjects(role).WithStatusSubresource(role).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				// What an old CRD's structural schema does to fields it lacks.
				if r, ok := obj.(*neo4jv1beta1.Neo4jRole); ok {
					r.Status.PrivilegeRenderingScope = ""
					r.Status.PrivilegeRenderings = nil
					r.Status.UnattributedPrivileges = nil
				}
				return c.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	r := &Neo4jRoleReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}

	err := r.setLearnState(ctx, role, newLearnState("scope"), nil)
	require.ErrorIs(t, err, errNeo4jRoleCRDOutdated)
	res, ferr := r.fail(ctx, role, "persisting learned privilege renderings failed", err, 30*time.Second)
	require.NoError(t, ferr)
	assert.Equal(t, ctrl.Result{RequeueAfter: 30 * time.Second}, res)
}

// One database is one name to Neo4j in any case, so a role's warnings list it
// once (#529): `TOPODB` next to `topodb` was listed twice.
func TestPrivilegeTargets_DeduplicateIgnoringCase(t *testing.T) {
	stmts := []string{
		"GRANT ACCESS ON DATABASE TOPODB TO r",
		"GRANT ACCESS ON DATABASES topodb, other TO r",
		"GRANT MATCH {*} ON GRAPH Travel NODES * TO r",
		"GRANT READ {name} ON GRAPH travel NODES Person TO r",
	}
	assert.Equal(t, []string{"TOPODB", "other", "Travel"}, privilegeDatabaseNames(stmts),
		"each database once, as the first statement spells it")
	assert.Equal(t, []string{"Travel"}, distinctPrivilegeTargets(stmts, neo4jclient.PrivilegeGraphTargets),
		"the composite GRAPH check reads the same way")
}
