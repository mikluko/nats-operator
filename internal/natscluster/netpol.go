package natscluster

import (
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// inClusterNamespaceFile holds the namespace of a pod that mounts its
// ServiceAccount token.
const inClusterNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// inClusterNamespace is the namespace the running pod is in, or "" outside
// a pod.
func inClusterNamespace() string {
	b, err := os.ReadFile(inClusterNamespaceFile)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func networkPolicyName(nc *clusterv1beta1.NatsCluster) string { return nc.Name }

// networkPolicyEnabled reports whether nc renders its NetworkPolicy: on
// unless monitor.networkPolicy is false.
func networkPolicyEnabled(spec *clusterv1beta1.NatsClusterSpec) bool {
	return spec.Monitor == nil || spec.Monitor.NetworkPolicy == nil || *spec.Monitor.NetworkPolicy
}

// networkPolicy is the NetworkPolicy over nc's pods, or nil when
// monitor.networkPolicy is false. It admits from anywhere every port the
// servers listen on but the monitoring port, and the monitoring port only
// from monitorNamespace; with monitorNamespace empty, from nowhere. The
// exporter sidecar reaches the monitoring port over the pod's loopback,
// which no NetworkPolicy governs.
func networkPolicy(nc *clusterv1beta1.NatsCluster, monitorNamespace string) *networkingv1.NetworkPolicy {
	if !networkPolicyEnabled(&nc.Spec) {
		return nil
	}
	open := []int32{PortClient, PortRoute}
	if exporterEnabled(&nc.Spec) {
		open = append(open, PortMetrics)
	}
	if nc.Spec.Gateway != nil {
		open = append(open, PortGateway)
	}
	if nc.Spec.Leafnodes != nil {
		open = append(open, PortLeafnodes)
	}
	var ports []networkingv1.NetworkPolicyPort
	for _, p := range open {
		ports = append(ports, policyPort(p))
	}
	rules := []networkingv1.NetworkPolicyIngressRule{{Ports: ports}}
	if monitorNamespace != "" {
		rules = append(rules, networkingv1.NetworkPolicyIngressRule{
			Ports: []networkingv1.NetworkPolicyPort{policyPort(PortMonitor)},
			From: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{corev1.LabelMetadataName: monitorNamespace},
			}}},
		})
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: networkPolicyName(nc), Namespace: nc.Namespace, Labels: labels(nc)},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: clusterSelector(nc)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress:     rules,
		},
	}
}

func policyPort(port int32) networkingv1.NetworkPolicyPort {
	return networkingv1.NetworkPolicyPort{Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(intstr.FromInt32(port))}
}
