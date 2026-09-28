package natscluster

import (
	"testing"

	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/utils/ptr"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// TestNetworkPolicy pins what reaches a server's pod: every rendered
// listener from anywhere, the monitoring port only from the cluster
// controller's namespace.
func TestNetworkPolicy(t *testing.T) {
	type rule struct {
		ports []int32
		from  string
	}
	rules := func(np *networkingv1.NetworkPolicy) []rule {
		var out []rule
		for _, in := range np.Spec.Ingress {
			var r rule
			for _, p := range in.Ports {
				r.ports = append(r.ports, p.Port.IntVal)
			}
			for _, f := range in.From {
				r.from = f.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"]
			}
			out = append(out, r)
		}
		return out
	}

	t.Run("story 1", func(t *testing.T) {
		nc := storyCluster(t)
		np := networkPolicy(nc, "nats-operator")
		require.Equal(t, clusterSelector(nc), np.Spec.PodSelector.MatchLabels)
		require.Equal(t, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}, np.Spec.PolicyTypes)
		require.Equal(t, []rule{
			{ports: []int32{PortClient, PortRoute, PortMetrics}},
			{ports: []int32{PortMonitor}, from: "nats-operator"},
		}, rules(np))
	})
	t.Run("gateway and leafnodes, no exporter", func(t *testing.T) {
		nc := storySupercluster(t, "west")
		nc.Spec.Leafnodes = &clusterv1beta1.Leafnodes{}
		nc.Spec.Exporter = &clusterv1beta1.Exporter{Enabled: ptr.To(false)}
		require.Equal(t, []int32{PortClient, PortRoute, PortGateway, PortLeafnodes}, rules(networkPolicy(nc, "ops"))[0].ports)
	})
	t.Run("an unknown controller namespace admits the monitoring port from nowhere", func(t *testing.T) {
		require.Equal(t, []rule{{ports: []int32{PortClient, PortRoute, PortMetrics}}}, rules(networkPolicy(storyCluster(t), "")))
	})
	t.Run("monitor.networkPolicy false renders none", func(t *testing.T) {
		nc := storyCluster(t)
		nc.Spec.Monitor = &clusterv1beta1.Monitor{NetworkPolicy: ptr.To(false)}
		require.Nil(t, networkPolicy(nc, "nats-operator"))
	})
}
