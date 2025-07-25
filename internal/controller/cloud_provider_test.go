/*
Copyright 2025.

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDetectCloudProvider(t *testing.T) {
	tests := []struct {
		name     string
		nodes    []corev1.Node
		expected CloudProvider
	}{
		{
			name: "AWS cloud provider",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"kubernetes.io/hostname":           "ip-10-0-0-1",
							"node.kubernetes.io/instance-type": "m5.large",
						},
					},
					Spec: corev1.NodeSpec{
						ProviderID: "aws:///us-west-2a/i-1234567890abcdef0",
					},
				},
			},
			expected: CloudProviderAWS,
		},
		{
			name: "GCP cloud provider",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"kubernetes.io/hostname": "gke-cluster-1-default-pool-1234abcd-5678",
						},
					},
					Spec: corev1.NodeSpec{
						ProviderID: "gce://my-project/us-central1-a/gke-cluster-1-default-pool-1234abcd-5678",
					},
				},
			},
			expected: CloudProviderGCP,
		},
		{
			name: "Azure cloud provider",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"kubernetes.io/hostname": "aks-nodepool1-12345678-vmss000000",
						},
					},
					Spec: corev1.NodeSpec{
						ProviderID: "azure:///subscriptions/12345678-1234-1234-1234-123456789012/resourceGroups/MC_rg_cluster_eastus/providers/Microsoft.Compute/virtualMachineScaleSets/aks-nodepool1-12345678-vmss/virtualMachines/0",
					},
				},
			},
			expected: CloudProviderAzure,
		},
		{
			name: "Unknown cloud provider",
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node1",
						Labels: map[string]string{
							"kubernetes.io/hostname": "node1",
						},
					},
					Spec: corev1.NodeSpec{
						ProviderID: "kind://docker/kind/kind-control-plane",
					},
				},
			},
			expected: CloudProviderUnknown,
		},
		{
			name:     "No nodes",
			nodes:    []corev1.Node{},
			expected: CloudProviderUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client with nodes
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)

			objs := []client.Object{}
			for i := range tt.nodes {
				objs = append(objs, &tt.nodes[i])
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			result := DetectCloudProvider(context.Background(), fakeClient)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetDefaultServiceAnnotations(t *testing.T) {
	tests := []struct {
		name         string
		provider     CloudProvider
		wantNonEmpty bool
	}{
		{
			name:         "AWS provider",
			provider:     CloudProviderAWS,
			wantNonEmpty: true,
		},
		{
			name:         "GCP provider",
			provider:     CloudProviderGCP,
			wantNonEmpty: true,
		},
		{
			name:         "Azure provider",
			provider:     CloudProviderAzure,
			wantNonEmpty: true,
		},
		{
			name:         "Unknown provider",
			provider:     CloudProviderUnknown,
			wantNonEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			annotations := GetDefaultServiceAnnotations(tt.provider)
			if tt.wantNonEmpty {
				assert.NotEmpty(t, annotations)
			} else {
				assert.Empty(t, annotations)
			}
		})
	}
}
