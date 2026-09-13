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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
	"github.com/priyolahiri/neo4j-kubernetes-operator/internal/resources"
)

// The CCDR proxy is HAProxy in `mode tcp`. It terminates nothing and
// authenticates nothing, so Neo4j's cluster SSL policy is the ONLY access
// control in front of the tx-shipping port the proxy publishes through a load
// balancer. Enable the proxy with no `spec.tls` and that port is reachable
// from the load balancer's address with neither authentication nor
// encryption — anyone who gets there can stream the database.
//
// The operator does not refuse the configuration (a user may terminate TLS
// elsewhere, or run deliberately on a private network, and refusing would
// break clusters already running this way). It must say so, durably: an event
// ages out in an hour and is lost across an operator restart, which is exactly
// when someone auditing a cluster would come looking.
func TestCCDRProxy_ReportsWhetherAnythingAuthenticatesTheExposedPort(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	if err := neo4jv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	cluster := func(tls *neo4jv1beta1.TLSSpec) *neo4jv1beta1.Neo4jEnterpriseCluster {
		return &neo4jv1beta1.Neo4jEnterpriseCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "default"},
			Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
				Topology: neo4jv1beta1.TopologyConfiguration{Servers: 2},
				TLS:      tls,
				CrossClusterReplication: &neo4jv1beta1.CrossClusterReplicationSpec{
					Enabled: true,
				},
			},
		}
	}

	// The proxy Service pre-created WITH an ingress address: the condition is
	// about a port that is actually reachable, so an unassigned load balancer
	// exposes nothing yet.
	assignedService := func(c *neo4jv1beta1.Neo4jEnterpriseCluster) *corev1.Service {
		svc := resources.BuildCCDRProxyService(c)
		svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "10.0.0.9"}}
		return svc
	}

	cases := []struct {
		name       string
		tls        *neo4jv1beta1.TLSSpec
		wantStatus metav1.ConditionStatus
		wantReason string
		wantEvent  bool
	}{
		{
			name:       "no spec.tls — nothing authenticates the exposed port",
			tls:        nil,
			wantStatus: metav1.ConditionFalse,
			wantReason: "NoClusterTLS",
			wantEvent:  true,
		},
		{
			name: "strictPeerValidation off — trust_all, so still nothing",
			tls: &neo4jv1beta1.TLSSpec{
				Mode:                 resources.CertManagerMode,
				StrictPeerValidation: func() *bool { b := false; return &b }(),
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: "NoClusterTLS",
			wantEvent:  true,
		},
		{
			name:       "cert-manager with the default strict posture — a peer certificate is required",
			tls:        &neo4jv1beta1.TLSSpec{Mode: resources.CertManagerMode},
			wantStatus: metav1.ConditionTrue,
			wantReason: "MutualTLSRequired",
			wantEvent:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := cluster(tc.tls)
			recorder := record.NewFakeRecorder(10)
			r := &Neo4jEnterpriseClusterReconciler{
				Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(c, assignedService(c)).WithStatusSubresource(c).Build(),
				Scheme:   scheme,
				Recorder: recorder,
			}

			if _, err := r.reconcileCrossClusterReplicationProxy(context.Background(), c); err != nil {
				t.Fatalf("reconcile: %v", err)
			}

			latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(c), latest); err != nil {
				t.Fatalf("get: %v", err)
			}
			cond := meta.FindStatusCondition(latest.Status.Conditions, ConditionTypeCrossClusterProxySecure)
			if cond == nil {
				t.Fatal("no CrossClusterProxySecure condition — an exposed port with no report on what guards it")
			}
			if cond.Status != tc.wantStatus || cond.Reason != tc.wantReason {
				t.Fatalf("condition = %s/%s, want %s/%s", cond.Status, cond.Reason, tc.wantStatus, tc.wantReason)
			}

			var warned bool
			for {
				select {
				case ev := <-recorder.Events:
					if strings.Contains(ev, EventReasonCCDRProxyUnauthenticated) {
						warned = true
					}
					continue
				default:
				}
				break
			}
			if warned != tc.wantEvent {
				t.Fatalf("warning event emitted = %v, want %v", warned, tc.wantEvent)
			}
		})
	}
}

// A cluster that no longer exposes anything must not keep a False security
// condition: a permanent false alarm trains people to ignore the real one.
func TestCCDRProxy_ConditionClearedWhenDisabled(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	if err := neo4jv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}

	c := &neo4jv1beta1.Neo4jEnterpriseCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "default"},
		Spec: neo4jv1beta1.Neo4jEnterpriseClusterSpec{
			Topology: neo4jv1beta1.TopologyConfiguration{Servers: 2},
		},
		Status: neo4jv1beta1.Neo4jEnterpriseClusterStatus{
			CrossClusterReplication: &neo4jv1beta1.CrossClusterReplicationStatus{Ready: true},
			Conditions: []metav1.Condition{{
				Type:               ConditionTypeCrossClusterProxySecure,
				Status:             metav1.ConditionFalse,
				Reason:             "NoClusterTLS",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}

	r := &Neo4jEnterpriseClusterReconciler{
		Client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(c).WithStatusSubresource(c).Build(),
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	if _, err := r.reconcileCrossClusterReplicationProxy(context.Background(), c); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	latest := &neo4jv1beta1.Neo4jEnterpriseCluster{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(c), latest); err != nil {
		t.Fatalf("get: %v", err)
	}
	if meta.FindStatusCondition(latest.Status.Conditions, ConditionTypeCrossClusterProxySecure) != nil {
		t.Fatal("CrossClusterProxySecure survived the proxy being disabled")
	}
}
