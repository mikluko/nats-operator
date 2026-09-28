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
// monitor.networkPolicy is false. It admits the route port only from nc's
// pods, the monitoring port only from monitorNamespace, the metrics port
// from monitorNamespace and exporter.from, and every other port the servers
// listen on from anywhere; with neither monitorNamespace nor exporter.from,
// the monitoring and metrics ports are admitted from nowhere.
func networkPolicy(nc *clusterv1beta1.NatsCluster, monitorNamespace string) *networkingv1.NetworkPolicy {
	if !networkPolicyEnabled(&nc.Spec) {
		return nil
	}
	open := []int32{PortClient}
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
	rules := []networkingv1.NetworkPolicyIngressRule{
		{Ports: ports},
		{
			Ports: []networkingv1.NetworkPolicyPort{policyPort(PortRoute)},
			From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: clusterSelector(nc)}}},
		},
	}
	var monitors []networkingv1.NetworkPolicyPeer
	if monitorNamespace != "" {
		monitors = append(monitors, networkingv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{corev1.LabelMetadataName: monitorNamespace},
		}})
		rules = append(rules, networkingv1.NetworkPolicyIngressRule{
			Ports: []networkingv1.NetworkPolicyPort{policyPort(PortMonitor)},
			From:  monitors,
		})
	}
	if exporterEnabled(&nc.Spec) {
		scrapers := append([]networkingv1.NetworkPolicyPeer(nil), monitors...)
		if nc.Spec.Exporter != nil {
			scrapers = append(scrapers, nc.Spec.Exporter.From...)
		}
		if len(scrapers) > 0 {
			rules = append(rules, networkingv1.NetworkPolicyIngressRule{
				Ports: []networkingv1.NetworkPolicyPort{policyPort(PortMetrics)},
				From:  scrapers,
			})
		}
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
