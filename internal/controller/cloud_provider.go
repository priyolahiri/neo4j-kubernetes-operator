package controller

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// CloudProvider represents a cloud provider type
type CloudProvider string

const (
	CloudProviderUnknown CloudProvider = "unknown"
	CloudProviderAWS     CloudProvider = "aws"
	CloudProviderGCP     CloudProvider = "gcp"
	CloudProviderAzure   CloudProvider = "azure"
	CloudProviderOnPrem  CloudProvider = "onprem"
)

// DetectCloudProvider attempts to detect the cloud provider based on node labels
func DetectCloudProvider(ctx context.Context, k8sClient client.Client) CloudProvider {
	logger := log.FromContext(ctx)

	// List nodes to check their labels
	nodeList := &corev1.NodeList{}
	if err := k8sClient.List(ctx, nodeList); err != nil {
		logger.Error(err, "Failed to list nodes for cloud provider detection")
		return CloudProviderUnknown
	}

	if len(nodeList.Items) == 0 {
		return CloudProviderUnknown
	}

	// Check the first node's labels for cloud provider hints
	node := nodeList.Items[0]

	// AWS detection
	if _, ok := node.Labels["eks.amazonaws.com/nodegroup"]; ok {
		return CloudProviderAWS
	}
	if region, ok := node.Labels["topology.kubernetes.io/region"]; ok && strings.Contains(region, "us-") {
		// Additional AWS region check
		if _, hasZone := node.Labels["topology.kubernetes.io/zone"]; hasZone {
			zone := node.Labels["topology.kubernetes.io/zone"]
			if strings.Contains(zone, region) && len(zone) > len(region) {
				// AWS zones are typically region + letter (us-east-1a)
				return CloudProviderAWS
			}
		}
	}

	// GCP detection
	if _, ok := node.Labels["cloud.google.com/gke-nodepool"]; ok {
		return CloudProviderGCP
	}
	if hostname := node.Labels["kubernetes.io/hostname"]; strings.HasPrefix(hostname, "gke-") {
		return CloudProviderGCP
	}

	// Azure detection
	if _, ok := node.Labels["kubernetes.azure.com/cluster"]; ok {
		return CloudProviderAzure
	}
	if agentpool, ok := node.Labels["kubernetes.azure.com/agentpool"]; ok && agentpool != "" {
		return CloudProviderAzure
	}
	if hostname := node.Labels["kubernetes.io/hostname"]; strings.HasPrefix(hostname, "aks-") {
		return CloudProviderAzure
	}

	// Check provider ID in node spec
	if node.Spec.ProviderID != "" {
		providerID := strings.ToLower(node.Spec.ProviderID)
		if strings.HasPrefix(providerID, "aws://") {
			return CloudProviderAWS
		}
		if strings.HasPrefix(providerID, "gce://") {
			return CloudProviderGCP
		}
		if strings.HasPrefix(providerID, "azure://") {
			return CloudProviderAzure
		}
	}

	// If we have zones but can't detect cloud provider, assume on-prem
	if _, hasZone := node.Labels["topology.kubernetes.io/zone"]; hasZone {
		return CloudProviderOnPrem
	}

	return CloudProviderUnknown
}

// GetDefaultServiceAnnotations returns cloud-specific service annotations
func GetDefaultServiceAnnotations(provider CloudProvider) map[string]string {
	switch provider {
	case CloudProviderAWS:
		return map[string]string{
			"service.beta.kubernetes.io/aws-load-balancer-type":                              "nlb",
			"service.beta.kubernetes.io/aws-load-balancer-backend-protocol":                  "tcp",
			"service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled": "true",
		}
	case CloudProviderGCP:
		return map[string]string{
			"cloud.google.com/neg":            `{"ingress": true}`,
			"cloud.google.com/backend-config": `{"default": "neo4j-backend-config"}`,
		}
	case CloudProviderAzure:
		return map[string]string{
			"service.beta.kubernetes.io/azure-load-balancer-internal":                  "false",
			"service.beta.kubernetes.io/azure-load-balancer-health-probe-request-path": "/",
		}
	default:
		return map[string]string{}
	}
}

// ShouldDefaultToLoadBalancer returns true if the service should default to LoadBalancer type
func ShouldDefaultToLoadBalancer(provider CloudProvider) bool {
	switch provider {
	case CloudProviderAWS, CloudProviderGCP, CloudProviderAzure:
		return true
	default:
		return false
	}
}
