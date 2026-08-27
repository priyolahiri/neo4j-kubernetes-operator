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

package resources

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	neo4jv1beta1 "github.com/priyolahiri/neo4j-kubernetes-operator/api/v1beta1"
)

const (
	// CCDRProxyBasePort is the first external port the proxy listens on,
	// forwarding to server ordinal 0's cluster port (6000). Ordinal i is
	// exposed on CCDRProxyBasePort+i.
	CCDRProxyBasePort = 16000

	// ccdrProxyContainerName matches the haproxy image's container name in
	// the proxy Deployment template.
	ccdrProxyContainerName = "haproxy"

	// ccdrProxyConfigMountPath is where the official haproxy image's default
	// command (`haproxy -f /usr/local/etc/haproxy/haproxy.cfg`) reads its
	// config from.
	ccdrProxyConfigMountPath = "/usr/local/etc/haproxy"
)

// CCDRProxyName returns the canonical resource name for the network-mode
// CCDR exposure proxy's Deployment/Service/ConfigMap/NetworkPolicy. Short
// suffix (5 chars) — cluster names are capped at 56 chars specifically so
// generated-resource suffixes up to 7 chars stay within the 63-char DNS
// label limit (see maxClusterNameLength in internal/validation).
func CCDRProxyName(clusterName string) string {
	return clusterName + "-ccdr"
}

// ccdrProxyLabels returns the label set shared by the proxy's Deployment
// pod template and Service selector.
func ccdrProxyLabels(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "ccdr-proxy",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/component":  "ccdr-proxy",
		"app.kubernetes.io/managed-by": "neo4j-operator",
	}
}

// BuildCCDRProxyConfigMap renders the haproxy.cfg for the network-mode CCDR
// exposure proxy: one frontend/backend pair per server ordinal, `mode tcp`
// throughout, pure L4 passthrough — the proxy never terminates or inspects
// TLS, so it needs no certificate of its own and end-to-end mutual TLS
// between the two Neo4j clusters is unaffected by its presence.
//
// Returns nil when spec.crossClusterReplication is unset or disabled.
func BuildCCDRProxyConfigMap(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *corev1.ConfigMap {
	if !ccdrReplicationEnabled(cluster) {
		return nil
	}

	var cfg strings.Builder
	cfg.WriteString("global\n    log stdout format raw local0\n    maxconn 4096\n\n")
	cfg.WriteString("defaults\n    mode tcp\n    timeout connect 5s\n    timeout client 1h\n    timeout server 1h\n    log global\n\n")

	servers := cluster.Spec.Topology.Servers
	for i := int32(0); i < servers; i++ {
		port := CCDRProxyBasePort + i
		backendAddr := fmt.Sprintf("%s-server-%d.%s-headless.%s.svc.cluster.local:%d",
			cluster.Name, i, cluster.Name, cluster.Namespace, DiscoveryPort)
		fmt.Fprintf(&cfg, "frontend f%d\n    bind *:%d\n    default_backend b%d\n\n", i, port, i)
		fmt.Fprintf(&cfg, "backend b%d\n    server s%d %s\n\n", i, i, backendAddr)
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CCDRProxyName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    ccdrProxyLabels(cluster),
		},
		Data: map[string]string{
			"haproxy.cfg": cfg.String(),
		},
	}
}

// BuildCCDRProxyDeployment renders the Deployment running the network-mode
// CCDR exposure proxy. haproxy over a first-party binary: a pinned public
// image needs no new build/release pipeline (no second container image to
// build, multi-arch-publish, and maintain through every release) — same
// supply-chain rationale as the pinned busybox seed-proxy image.
//
// Returns nil when spec.crossClusterReplication is unset or disabled.
func BuildCCDRProxyDeployment(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *appsv1.Deployment {
	if !ccdrReplicationEnabled(cluster) {
		return nil
	}

	replicas := int32(2) // stateless L4 forwarding; 2 for availability, no shared state.
	labels := ccdrProxyLabels(cluster)
	readOnlyRoot := true
	allowPrivilegeEscalation := false
	runAsNonRoot := true
	// UID/GID 99 matches the official haproxy:3.0-alpine image's built-in
	// "haproxy" user (confirmed via `docker run ... id haproxy` against the
	// pinned digest below) — the image's own default, not an operator choice.
	runAsUser := int64(99)

	var ports []corev1.ContainerPort
	for i := int32(0); i < cluster.Spec.Topology.Servers; i++ {
		ports = append(ports, corev1.ContainerPort{
			Name:          fmt.Sprintf("ccdr-%d", i),
			ContainerPort: CCDRProxyBasePort + i,
			Protocol:      corev1.ProtocolTCP,
		})
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CCDRProxyName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name: ccdrProxyContainerName,
						// Digest-pinned (multi-arch manifest-list digest,
						// resolved via `docker buildx imagetools inspect
						// haproxy:3.0-alpine`), version 3.0.26 — same
						// supply-chain rationale as the pinned busybox
						// seed-proxy image.
						Image: "haproxy:3.0-alpine@sha256:7c8dac975b9def049d6585b7efe865486acaa7b6ec5e74eec45f08fde8bb2ad4",
						Ports: ports,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "config",
							MountPath: ccdrProxyConfigMountPath,
							ReadOnly:  true,
						}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("20m"),
								corev1.ResourceMemory: resource.MustParse("32Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("200m"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
						},
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             &runAsNonRoot,
							RunAsUser:                &runAsUser,
							ReadOnlyRootFilesystem:   &readOnlyRoot,
							AllowPrivilegeEscalation: &allowPrivilegeEscalation,
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{"ALL"},
							},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.FromInt32(CCDRProxyBasePort),
								},
							},
							InitialDelaySeconds: 2,
							PeriodSeconds:       5,
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: CCDRProxyName(cluster.Name),
								},
							},
						},
					}},
				},
			},
		},
	}
}

// BuildCCDRProxyService renders the LoadBalancer Service fronting the
// network-mode CCDR exposure proxy, one port per server ordinal.
//
// Returns nil when spec.crossClusterReplication is unset or disabled.
func BuildCCDRProxyService(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) *corev1.Service {
	if !ccdrReplicationEnabled(cluster) {
		return nil
	}
	ccdr := cluster.Spec.CrossClusterReplication

	var ports []corev1.ServicePort
	for i := int32(0); i < cluster.Spec.Topology.Servers; i++ {
		port := CCDRProxyBasePort + i
		ports = append(ports, corev1.ServicePort{
			Name:       fmt.Sprintf("ccdr-%d", i),
			Port:       port,
			TargetPort: intstr.FromInt32(port),
			Protocol:   corev1.ProtocolTCP,
		})
	}

	annotations := map[string]string{}
	for k, v := range ccdr.Annotations {
		annotations[k] = v
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        CCDRProxyName(cluster.Name),
			Namespace:   cluster.Namespace,
			Labels:      ccdrProxyLabels(cluster),
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeLoadBalancer,
			Selector: ccdrProxyLabels(cluster),
			Ports:    ports,
		},
	}
}

// ccdrReplicationEnabled is the single guard every builder in this file
// checks first, so "disabled" and "unset" always behave identically.
func ccdrReplicationEnabled(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	return cluster.Spec.CrossClusterReplication != nil && cluster.Spec.CrossClusterReplication.Enabled
}

// CCDRProxyLoadBalancerInternalEffective returns the effective value of
// spec.crossClusterReplication.loadBalancerInternal, defaulting to true.
func CCDRProxyLoadBalancerInternalEffective(cluster *neo4jv1beta1.Neo4jEnterpriseCluster) bool {
	if !ccdrReplicationEnabled(cluster) {
		return true
	}
	if v := cluster.Spec.CrossClusterReplication.LoadBalancerInternal; v != nil {
		return *v
	}
	return true
}
