package natscluster

import (
	"testing"

	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// TestNetworkPolicy pins what reaches a server's pod: the route port only
// from the NATS cluster's own pods, the monitoring port only from the
// cluster controller's namespace, the metrics port from there and
// exporter.from, every other rendered listener from anywhere.
func TestNetworkPolicy(t *testing.T) {
	type rule struct {
		ports []int32
		from  []networkingv1.NetworkPolicyPeer
	}
	rules := func(np *networkingv1.NetworkPolicy) []rule {
		var out []rule
		for _, in := range np.Spec.Ingress {
			var r rule
			for _, p := range in.Ports {
				r.ports = append(r.ports, p.Port.IntVal)
			}
			r.from = in.From
			out = append(out, r)
		}
		return out
	}
	namespace := func(name string) networkingv1.NetworkPolicyPeer {
		return networkingv1.NetworkPolicyPeer{NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"kubernetes.io/metadata.name": name},
		}}
	}
	peers := func(nc *clusterv1beta1.NatsCluster) []networkingv1.NetworkPolicyPeer {
		return []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: clusterSelector(nc)}}}
	}
	prometheus := networkingv1.NetworkPolicyPeer{
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "observability"}},
		PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "prometheus"}},
	}

	tests := []struct {
		name      string
		nc        func(t *testing.T) *clusterv1beta1.NatsCluster
		namespace string
		want      func(nc *clusterv1beta1.NatsCluster) []rule
	}{
		{
			name:      "story 1",
			nc:        storyCluster,
			namespace: "nats-operator",
			want: func(nc *clusterv1beta1.NatsCluster) []rule {
				return []rule{
					{ports: []int32{PortClient}},
					{ports: []int32{PortRoute}, from: peers(nc)},
					{ports: []int32{PortMonitor}, from: []networkingv1.NetworkPolicyPeer{namespace("nats-operator")}},
					{ports: []int32{PortMetrics}, from: []networkingv1.NetworkPolicyPeer{namespace("nats-operator")}},
				}
			},
		},
		{
			name: "exporter.from admits the metrics port and not the monitoring port",
			nc: func(t *testing.T) *clusterv1beta1.NatsCluster {
				nc := storyCluster(t)
				nc.Spec.Exporter = &clusterv1beta1.Exporter{From: []networkingv1.NetworkPolicyPeer{prometheus}}
				return nc
			},
			namespace: "nats-operator",
			want: func(nc *clusterv1beta1.NatsCluster) []rule {
				return []rule{
					{ports: []int32{PortClient}},
					{ports: []int32{PortRoute}, from: peers(nc)},
					{ports: []int32{PortMonitor}, from: []networkingv1.NetworkPolicyPeer{namespace("nats-operator")}},
					{ports: []int32{PortMetrics}, from: []networkingv1.NetworkPolicyPeer{namespace("nats-operator"), prometheus}},
				}
			},
		},
		{
			name: "gateway and leafnodes from anywhere, no exporter",
			nc: func(t *testing.T) *clusterv1beta1.NatsCluster {
				nc := storySupercluster(t, "west")
				nc.Spec.Leafnodes = &clusterv1beta1.Leafnodes{}
				nc.Spec.Exporter = &clusterv1beta1.Exporter{Enabled: ptr.To(false)}
				return nc
			},
			namespace: "ops",
			want: func(nc *clusterv1beta1.NatsCluster) []rule {
				return []rule{
					{ports: []int32{PortClient, PortGateway, PortLeafnodes}},
					{ports: []int32{PortRoute}, from: peers(nc)},
					{ports: []int32{PortMonitor}, from: []networkingv1.NetworkPolicyPeer{namespace("ops")}},
				}
			},
		},
		{
			name: "an unknown controller namespace admits the monitoring and metrics ports from nowhere",
			nc:   storyCluster,
			want: func(nc *clusterv1beta1.NatsCluster) []rule {
				return []rule{
					{ports: []int32{PortClient}},
					{ports: []int32{PortRoute}, from: peers(nc)},
				}
			},
		},
		{
			name: "an unknown controller namespace admits the metrics port from exporter.from alone",
			nc: func(t *testing.T) *clusterv1beta1.NatsCluster {
				nc := storyCluster(t)
				nc.Spec.Exporter = &clusterv1beta1.Exporter{From: []networkingv1.NetworkPolicyPeer{prometheus}}
				return nc
			},
			want: func(nc *clusterv1beta1.NatsCluster) []rule {
				return []rule{
					{ports: []int32{PortClient}},
					{ports: []int32{PortRoute}, from: peers(nc)},
					{ports: []int32{PortMetrics}, from: []networkingv1.NetworkPolicyPeer{prometheus}},
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := tt.nc(t)
			np := networkPolicy(nc, tt.namespace)
			require.Equal(t, clusterSelector(nc), np.Spec.PodSelector.MatchLabels)
			require.Equal(t, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}, np.Spec.PolicyTypes)
			require.Equal(t, tt.want(nc), rules(np))
		})
	}
	t.Run("monitor.networkPolicy false renders none", func(t *testing.T) {
		nc := storyCluster(t)
		nc.Spec.Monitor = &clusterv1beta1.Monitor{NetworkPolicy: ptr.To(false)}
		require.Nil(t, networkPolicy(nc, "nats-operator"))
	})
}
