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

package controller_test

import (
	"context"
	"time"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

// standInLabel marks a cluster that stands in for a Ready cluster in specs of
// the controllers that wait on one (backup, restore). The suite's cluster
// controller leaves such clusters alone (its Ignore seam): envtest runs no
// pods, so it would rightly take them out of Ready, and with several workers
// it does so before the spec's controller looks. Set it before Create.
const standInLabel = "test.neo4j.com/stand-in"

// standInLabels is the label set a stand-in cluster is created with.
func standInLabels() map[string]string { return map[string]string{standInLabel: "true"} }

func isStandInCluster(c *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	return c.Labels[standInLabel] == "true"
}

// markClusterReady patches a stand-in cluster to Ready and waits until the
// API shows it.
func markClusterReady(ctx context.Context, key types.NamespacedName) {
	c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	Expect(k8sClient.Get(ctx, key, c)).To(Succeed())
	Expect(isStandInCluster(c)).To(BeTrue(),
		"create %s with standInLabels(), or the cluster controller will take it out of Ready", key)
	patch := client.MergeFrom(c.DeepCopy())
	c.Status.Phase = neo4jv1beta1.PhaseReady
	c.Status.Conditions = []metav1.Condition{{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "TestReady",
		LastTransitionTime: metav1.Now(),
	}}
	Expect(k8sClient.Status().Patch(ctx, c, patch)).To(Succeed())
	Eventually(func() string {
		latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		_ = k8sClient.Get(ctx, key, latest)
		return latest.Status.Phase
	}, 30*time.Second, 200*time.Millisecond).Should(Equal(neo4jv1beta1.PhaseReady))
}
