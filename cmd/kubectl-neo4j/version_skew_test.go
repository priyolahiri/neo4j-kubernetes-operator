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

package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func operatorDeployment(ns, tag string, available int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "neo4j-operator-controller-manager", Namespace: ns,
			Labels: map[string]string{"app.kubernetes.io/name": "neo4j-operator"}},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "manager", Image: "ghcr.io/x/neo4j-operator:" + tag}},
		}}},
		Status: appsv1.DeploymentStatus{AvailableReplicas: available},
	}
}

func leaderLease(ns, holder string, renewed time.Time) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "neo4j-operator-leader-election", Namespace: ns},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: ptr.To(holder), LeaseDurationSeconds: ptr.To(int32(15)),
			RenewTime: &metav1.MicroTime{Time: renewed}},
	}
}

func skewOutput(t *testing.T, cliVersion string, objs ...client.Object) string {
	t.Helper()
	old := version
	version = cliVersion
	t.Cleanup(func() { version = old })
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	warnOnVersionSkew(testClient(t, objs...), f)
	require.NoError(t, f.Close())
	b, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	return string(b)
}

// The v1.22.0 re-walk: the skew warning compared the CLI with a v1.22.0
// operator Deployment scaled to 0 in another namespace — the first the list
// returned — not with the operator that was running (#573).
func TestWarnOnVersionSkew_ComparesWithTheRunningOperator(t *testing.T) {
	scaledDown := operatorDeployment("a-old", "v1.22.0", 0)
	running := operatorDeployment("neo4j-operator", "v1.23.0", 1)

	assert.Empty(t, skewOutput(t, "v1.23.0", scaledDown, running), "the running operator matches the CLI")
	assert.Contains(t, skewOutput(t, "v1.24.0", scaledDown, running), "neo4j-operator/neo4j-operator-controller-manager is v1.23.0")
}

func TestRunningOperator(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	deployments := func(ds ...*appsv1.Deployment) []appsv1.Deployment {
		out := make([]appsv1.Deployment, 0, len(ds))
		for _, d := range ds {
			out = append(out, *d)
		}
		return out
	}

	t.Run("none available: it says so", func(t *testing.T) {
		d, note := runningOperator(ctx, testClient(t), deployments(operatorDeployment("a", "v1.22.0", 0)), now)
		assert.Nil(t, d)
		assert.Contains(t, note, "none of the 1 operator Deployment(s)")
		assert.Contains(t, note, "a/neo4j-operator-controller-manager v1.22.0")
	})

	t.Run("several running on one version: no choice to make", func(t *testing.T) {
		d, note := runningOperator(ctx, testClient(t),
			deployments(operatorDeployment("a", "v1.23.0", 1), operatorDeployment("b", "v1.23.0", 1)), now)
		require.NotNil(t, d)
		assert.Empty(t, note)
	})

	t.Run("several running: the one holding its leader lease", func(t *testing.T) {
		c := testClient(t,
			leaderLease("a", "neo4j-operator-controller-manager-5d9c-x7k2p_0f3e", now.Add(-time.Hour)), // expired
			leaderLease("b", "neo4j-operator-controller-manager-6f7d-q2w3e_9a1b", now.Add(-5*time.Second)))
		d, note := runningOperator(ctx, c,
			deployments(operatorDeployment("a", "v1.22.0", 1), operatorDeployment("b", "v1.23.0", 1)), now)
		require.NotNil(t, d, note)
		assert.Equal(t, "b", d.Namespace)
	})

	t.Run("several running and no single leader: it says so", func(t *testing.T) {
		d, note := runningOperator(ctx, testClient(t),
			deployments(operatorDeployment("a", "v1.22.0", 1), operatorDeployment("b", "v1.23.0", 1)), now)
		assert.Nil(t, d)
		assert.Contains(t, note, "2 operators are running")
	})

	t.Run("no Deployment: silent", func(t *testing.T) {
		d, note := runningOperator(ctx, testClient(t), nil, now)
		assert.Nil(t, d)
		assert.Empty(t, note)
	})
}
