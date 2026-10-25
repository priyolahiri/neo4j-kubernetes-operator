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
	"fmt"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	neo4jclient "github.com/priyolahiri/neo4j-kubernetes-operator/internal/neo4j"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/validation"
)

// fakeAuthServer is the system database of one Neo4j, as the Neo4jUser,
// Neo4jRoleBinding, Neo4jRole, Neo4jAuthRule and Neo4jCompositeDatabase
// reconcilers see it through their client seams: users and the roles granted
// to them, the roles that exist, and a drop that can be made to fail.
type fakeAuthServer struct {
	users map[string][]string // username -> granted roles (PUBLIC implicit)
	roles map[string]bool     // non-built-in roles that exist

	listRoles int      // SHOW ROLES calls
	granted   []string // "role->user", in order
	dropErr   error    // returned by every drop and revoke while set
	drops     int      // drop and revoke attempts
	alterErr  error    // returned by every ALTER USER while set
	alters    int      // ALTER USER attempts
	showErr   error    // returned by SHOW USERS while set
}

// errPasswordUnchanged is Neo4j refusing an ALTER USER … SET PASSWORD to the
// password the user already has.
var errPasswordUnchanged = errors.New("Neo.ClientError.General.InvalidArguments: ArgumentError: " +
	"Failed to alter the specified user 'alice': Old password and new password cannot be the same")

// errBoltRefused is what connecting to a server whose Bolt port is not open yet
// returns.
var errBoltRefused = errors.New("failed to verify Neo4j connectivity: ConnectivityError: " +
	"dial tcp 10.96.12.7:7687: connect: connection refused")

func newFakeAuthServer() *fakeAuthServer {
	return &fakeAuthServer{users: map[string][]string{}, roles: map[string]bool{}}
}

func (s *fakeAuthServer) ShowUser(_ context.Context, username string) (*neo4jclient.UserInfo, error) {
	if s.showErr != nil {
		return nil, s.showErr
	}
	roles, ok := s.users[username]
	if !ok {
		return nil, nil
	}
	return &neo4jclient.UserInfo{User: username, Roles: slices.Clone(roles)}, nil
}

func (s *fakeAuthServer) CreateUserAdvanced(_ context.Context, username string, _ *neo4jclient.AlterUserOptions, _ bool) error {
	if _, ok := s.users[username]; !ok {
		s.users[username] = nil
	}
	return nil
}

func (s *fakeAuthServer) AlterUser(context.Context, string, *neo4jclient.AlterUserOptions) error {
	s.alters++
	return s.alterErr
}

func (s *fakeAuthServer) ListUserRoles(_ context.Context, username string) ([]string, error) {
	return slices.Clone(s.users[username]), nil
}

func (s *fakeAuthServer) ListRoles(context.Context) ([]neo4jclient.RoleInfo, error) {
	s.listRoles++
	out := []neo4jclient.RoleInfo{{Role: "PUBLIC"}, {Role: "reader"}, {Role: "editor"},
		{Role: "publisher"}, {Role: "architect"}, {Role: "admin"}}
	names := make([]string, 0, len(s.roles))
	for name := range s.roles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		out = append(out, neo4jclient.RoleInfo{Role: name})
	}
	return out, nil
}

// GrantRoleToUser refuses a role that does not exist, as Neo4j does.
func (s *fakeAuthServer) GrantRoleToUser(_ context.Context, role, username string) error {
	if !validation.IsBuiltInRole(role) && !s.roles[role] {
		return fmt.Errorf("Neo.ClientError.Statement.ArgumentError: Failed to grant role '%s' to user '%s': Role does not exist", role, username)
	}
	if !slices.Contains(s.users[username], role) {
		s.users[username] = append(s.users[username], role)
	}
	s.granted = append(s.granted, role+"->"+username)
	return nil
}

func (s *fakeAuthServer) RevokeRoleFromUser(_ context.Context, role, username string) error {
	s.drops++
	if s.dropErr != nil {
		return s.dropErr
	}
	s.users[username] = slices.DeleteFunc(s.users[username], func(r string) bool { return r == role })
	return nil
}

func (s *fakeAuthServer) drop() error {
	s.drops++
	return s.dropErr
}

func (s *fakeAuthServer) DropUserIfExists(context.Context, string) error     { return s.drop() }
func (s *fakeAuthServer) DropRoleIfExists(context.Context, string) error     { return s.drop() }
func (s *fakeAuthServer) DropAuthRuleIfExists(context.Context, string) error { return s.drop() }
func (s *fakeAuthServer) DropAliasIfExists(context.Context, string) error    { return s.drop() }
func (s *fakeAuthServer) DropCompositeDatabase(context.Context, string, bool) error {
	return s.drop()
}
func (s *fakeAuthServer) Close() error { return nil }

// authTargetStandalone is a Ready standalone named "sa", the target the tests'
// objects reference.
func authTargetStandalone() *neo4jv1beta1.Neo4jEnterpriseStandalone {
	return &neo4jv1beta1.Neo4jEnterpriseStandalone{
		ObjectMeta: metav1.ObjectMeta{Name: "sa", Namespace: repeatedWarningsNS},
		Spec: neo4jv1beta1.Neo4jEnterpriseStandaloneSpec{
			Image: neo4jv1beta1.ImageSpec{Repo: "neo4j", Tag: "2026.08.1-enterprise"},
		},
		Status: neo4jv1beta1.Neo4jEnterpriseStandaloneStatus{Phase: "Ready", Ready: true},
	}
}

// setStandaloneReady flips the standalone's readiness as its own reconciler
// does while its pod restarts.
func setStandaloneReady(t *testing.T, c client.Client, ready bool) {
	t.Helper()
	sa := &neo4jv1beta1.Neo4jEnterpriseStandalone{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: repeatedWarningsNS, Name: "sa"}, sa))
	sa.Status.Ready = ready
	sa.Status.Phase = "Ready"
	if !ready {
		sa.Status.Phase = "Pending"
	}
	require.NoError(t, c.Status().Update(context.Background(), sa))
}
