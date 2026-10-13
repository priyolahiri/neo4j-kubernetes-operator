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

// markClusterReady makes an envtest cluster Ready for the controllers that
// wait on one (backup, restore, database). envtest runs no pods, so only a
// patch can do it — and the cluster controller reconciles a new cluster at
// once: a pass that began before the patch can write its own phase over it
// afterwards. So the patch is re-applied until Ready has held for two seconds;
// once a pass has observed Ready it keeps it. With one cluster worker that
// pass usually ran late, behind other specs' clusters, which is why a single
// patch used to be enough.
func markClusterReady(ctx context.Context, key types.NamespacedName) {
	var heldSince time.Time
	Eventually(func() bool {
		c := &neo4jv1beta1.Neo4jEnterpriseCluster{}
		if err := k8sClient.Get(ctx, key, c); err != nil {
			heldSince = time.Time{}
			return false
		}
		if c.Status.Phase == neo4jv1beta1.PhaseReady && readyConditionTrue(c.Status.Conditions) {
			if heldSince.IsZero() {
				heldSince = time.Now()
			}
			return time.Since(heldSince) >= 2*time.Second
		}
		heldSince = time.Time{}
		patch := client.MergeFrom(c.DeepCopy())
		c.Status.Phase = neo4jv1beta1.PhaseReady
		c.Status.Conditions = []metav1.Condition{{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "TestReady",
			LastTransitionTime: metav1.Now(),
		}}
		_ = k8sClient.Status().Patch(ctx, c, patch)
		return false
	}, 60*time.Second, 200*time.Millisecond).Should(BeTrue(), "cluster %s did not stay Ready", key)
}

func readyConditionTrue(conds []metav1.Condition) bool {
	for _, c := range conds {
		if c.Type == "Ready" {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}
