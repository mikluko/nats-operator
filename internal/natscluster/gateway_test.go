package natscluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/e2e"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// storySupercluster reads story 6's NatsCluster manifest for member, east
// or west.
func storySupercluster(t *testing.T, member string) *clusterv1beta1.NatsCluster {
	t.Helper()
	b, err := os.ReadFile("../../docs/content/docs/stories/06-supercluster/01-" + member + ".yaml")
	require.NoError(t, err)
	nc := &clusterv1beta1.NatsCluster{}
	require.NoError(t, yaml.UnmarshalStrict(b, nc))
	return nc
}

func gatewayJSON(t *testing.T, nc *clusterv1beta1.NatsCluster, in Inputs) string {
	t.Helper()
	b, err := json.Marshal(serverConfig(nc, in, "west-0", podLayout(nc), "").Gateway)
	require.NoError(t, err)
	return string(b)
}

func TestServerConfig_Gateway(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*clusterv1beta1.NatsCluster)
		want   string
	}{
		{"story 6 west, Explicit", func(*clusterv1beta1.NatsCluster) {}, `{
			"name": "west",
			"listen": "0.0.0.0:7222",
			"advertise": "nats-west.example.net:7222",
			"reject_unknown": true,
			"tls": {
				"cert_file": "/etc/nats-gateway-tls/tls.crt",
				"key_file": "/etc/nats-gateway-tls/tls.key",
				"ca_file": "/etc/nats-gateway-tls/ca.crt",
				"verify": true
			},
			"gateways": [{"name": "east", "urls": ["tls://nats-east.example.net:7222"]}]
		}`},
		{"Gossip", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway.Discovery = clusterv1beta1.GatewayDiscoveryGossip
		}, `{
			"name": "west",
			"listen": "0.0.0.0:7222",
			"advertise": "nats-west.example.net:7222",
			"reject_unknown": false,
			"tls": {
				"cert_file": "/etc/nats-gateway-tls/tls.crt",
				"key_file": "/etc/nats-gateway-tls/tls.key",
				"ca_file": "/etc/nats-gateway-tls/ca.crt",
				"verify": true
			},
			"gateways": [{"name": "east", "urls": ["tls://nats-east.example.net:7222"]}]
		}`},
		{"in the clear, only its own entry, no advertise", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway.TLS = nil
			nc.Spec.Gateway.Advertise = ""
			nc.Spec.Gateway.Remotes = []clusterv1beta1.GatewayRemote{{Name: "west", URL: "nats://w:7222"}}
		}, `{"name": "west", "listen": "0.0.0.0:7222", "reject_unknown": true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := storySupercluster(t, "west")
			tt.mutate(nc)
			require.JSONEq(t, tt.want, gatewayJSON(t, nc, Inputs{}))
		})
	}

	t.Run("no gateway", func(t *testing.T) {
		require.Nil(t, serverConfig(storyCluster(t), Inputs{}, "demo-0", podLayout(storyCluster(t)), "").Gateway)
	})
}

// TestRestartReason_Gateway pins that every gateway change a spec can make
// is restart-only.
func TestRestartReason_Gateway(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*clusterv1beta1.NatsCluster)
		reason string
	}{
		{"remote added", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway.Remotes = append(nc.Spec.Gateway.Remotes, clusterv1beta1.GatewayRemote{Name: "south", URL: "tls://s:7222"})
		}, "gateway.gateways is restart-only"},
		{"remote URL", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway.Remotes[0].URL = "tls://east.example.org:7222"
		}, "gateway.gateways is restart-only"},
		{"discovery", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway.Discovery = clusterv1beta1.GatewayDiscoveryGossip
		}, "gateway.reject_unknown is restart-only"},
		{"advertise", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway.Advertise = "nats-west.example.org:7222"
		}, "gateway.advertise is restart-only"},
		{"TLS off", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway.TLS = nil
		}, "gateway.tls is restart-only"},
		{"gateway removed", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway = nil
		}, "gateway is restart-only"},
		{"own entry only", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.Gateway.Remotes[1].URL = "tls://elsewhere:7222"
		}, ""},
	}
	render := func(nc *clusterv1beta1.NatsCluster) []byte {
		b, err := serverConfig(nc, Inputs{}, "west-0", podLayout(nc), "r1").Render()
		require.NoError(t, err)
		return b
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := storySupercluster(t, "west")
			from := render(nc)
			tt.mutate(nc)
			require.Equal(t, tt.reason, restartReason("2.15.0", from, render(nc)))
		})
	}
}

func TestGatewayHosts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*clusterv1beta1.Gateway)
		want   []string
	}{
		{"story 6: own entry and advertise agree", func(*clusterv1beta1.Gateway) {}, []string{"nats-west.example.net"}},
		{"both, differing", func(g *clusterv1beta1.Gateway) { g.Advertise = "10.0.0.7:7222" }, []string{"nats-west.example.net", "10.0.0.7"}},
		{"no own entry", func(g *clusterv1beta1.Gateway) { g.Remotes = g.Remotes[:1] }, []string{"nats-west.example.net"}},
		{"neither", func(g *clusterv1beta1.Gateway) { g.Remotes, g.Advertise = g.Remotes[:1], "" }, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := storySupercluster(t, "west")
			tt.mutate(nc.Spec.Gateway)
			require.Equal(t, tt.want, gatewayHosts(nc))
		})
	}
}

func TestGatewayCertificate(t *testing.T) {
	nc := storySupercluster(t, "west")
	require.Equal(t, "west-gateway-tls", gatewaySecret(nc))
	issuer := gatewayIssuer(nc)
	require.NotNil(t, issuer)
	got := gatewayCertificate(nc, issuer, []string{"nats-west.example.net", "10.0.0.7"})
	b, err := json.Marshal(got.Object)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"apiVersion": "cert-manager.io/v1",
		"kind": "Certificate",
		"metadata": {"name": "west-gateway", "namespace": "nats-system", "labels": {
			"app.kubernetes.io/instance": "west", "app.kubernetes.io/managed-by": "cluster-controller",
			"app.kubernetes.io/name": "nats", "cluster.nats.mikluko.io/cluster": "west"}},
		"spec": {
			"secretName": "west-gateway-tls",
			"dnsNames": ["nats-west.example.net"],
			"ipAddresses": ["10.0.0.7"],
			"usages": ["server auth", "client auth"],
			"privateKey": {"algorithm": "ECDSA", "size": 256},
			"issuerRef": {"name": "nats-gateway-ca", "kind": "ClusterIssuer", "group": "cert-manager.io"}
		}
	}`, string(b))

	nc.Spec.Gateway.TLS = &clusterv1beta1.ListenerTLS{CertificateSource: clusterv1beta1.CertificateSource{SecretRef: &natsv1beta1.SecretReference{Name: "mine"}}}
	require.Equal(t, "mine", gatewaySecret(nc))
	require.Nil(t, gatewayIssuer(nc))
	nc.Spec.Gateway.TLS = nil
	require.Empty(t, gatewaySecret(nc))
}

func TestRender_Gateway(t *testing.T) {
	nc := storySupercluster(t, "west")
	p, err := Render(nc, Inputs{})
	require.NoError(t, err)

	svc := p.GatewayService
	require.NotNil(t, svc)
	require.Equal(t, "west-gateway", svc.Name)
	require.Equal(t, corev1.ServiceTypeLoadBalancer, svc.Spec.Type)
	require.Equal(t, nc.Spec.Gateway.Service.Annotations, svc.Annotations)
	require.True(t, svc.Spec.PublishNotReadyAddresses, "servers reach each other's gateways before any is Ready")
	require.Equal(t, map[string]string{LabelCluster: "west"}, svc.Spec.Selector)
	require.Equal(t, []corev1.ServicePort{servicePort("gateway", PortGateway)}, svc.Spec.Ports)
	require.Contains(t, p.HeadlessService.Spec.Ports, servicePort("gateway", PortGateway))

	for _, s := range p.Servers {
		nats := container(t, s.StatefulSet, "nats")
		require.Contains(t, nats.Ports, corev1.ContainerPort{Name: "gateway", ContainerPort: PortGateway})
		require.Contains(t, nats.VolumeMounts, corev1.VolumeMount{Name: "gateway-tls", MountPath: gatewayTLSDir, ReadOnly: true})
		require.Contains(t, s.StatefulSet.Spec.Template.Spec.Volumes, corev1.Volume{Name: "gateway-tls", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "west-gateway-tls"},
		}})
	}

	nc.Spec.Gateway.Service = nil
	nc.Spec.Gateway.TLS = nil
	p, err = Render(nc, Inputs{})
	require.NoError(t, err)
	require.Nil(t, p.GatewayService)
	for _, v := range p.Servers[0].StatefulSet.Spec.Template.Spec.Volumes {
		require.NotEqual(t, "gateway-tls", v.Name)
	}

	p, err = Render(storyCluster(t), Inputs{})
	require.NoError(t, err)
	require.Nil(t, p.GatewayService)
	require.NotContains(t, p.HeadlessService.Spec.Ports, servicePort("gateway", PortGateway))
}

func servers(gws ...*sysobs.Gateways) *sysobs.Snapshot {
	snap := &sysobs.Snapshot{}
	for i, g := range gws {
		snap.Servers = append(snap.Servers, sysobs.Server{Name: fmt.Sprintf("west-%d", i), Gateways: g})
	}
	return snap
}

func TestGatewayStatus(t *testing.T) {
	east := func(inbound int) *sysobs.Gateways {
		return &sysobs.Gateways{Outbound: []string{"east"}, Inbound: map[string]int{"east": inbound}}
	}
	tests := []struct {
		name    string
		mutate  func(*clusterv1beta1.Gateway)
		snap    *sysobs.Snapshot
		want    []clusterv1beta1.GatewayStatus
		status  string
		reason  string
		message string
	}{
		{"meshed", nil, servers(east(1), east(1), east(1)),
			[]clusterv1beta1.GatewayStatus{{Name: "east", Connected: true, Inbound: 3, Outbound: 3}},
			"True", ReasonAllMembersReachable, "1 of 1 remote members connected"},
		{"one server not dialing", nil, servers(east(2), east(1), &sysobs.Gateways{Inbound: map[string]int{}}),
			[]clusterv1beta1.GatewayStatus{{Name: "east", Inbound: 3, Outbound: 2}},
			"False", ReasonMembersUnreachable, "0 of 1 remote members connected: east unreachable"},
		{"a server not answering is not counted", nil, servers(east(2), east(1), nil),
			[]clusterv1beta1.GatewayStatus{{Name: "east", Connected: true, Inbound: 3, Outbound: 2}},
			"True", ReasonAllMembersReachable, "1 of 1 remote members connected"},
		{"gossip discovers a member", func(g *clusterv1beta1.Gateway) { g.Discovery = clusterv1beta1.GatewayDiscoveryGossip },
			servers(&sysobs.Gateways{Outbound: []string{"east", "south"}, Inbound: map[string]int{"south": 1}}),
			[]clusterv1beta1.GatewayStatus{{Name: "east", Connected: true, Outbound: 1}, {Name: "south", Connected: true, Inbound: 1, Outbound: 1}},
			"True", ReasonAllMembersReachable, "2 of 2 remote members connected"},
		{"nothing answers", nil, servers(nil, nil),
			[]clusterv1beta1.GatewayStatus{{Name: "east"}},
			"False", ReasonMembersUnreachable, "0 of 1 remote members connected: east unreachable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := storySupercluster(t, "west")
			if tt.mutate != nil {
				tt.mutate(nc.Spec.Gateway)
			}
			got := gatewayStatus(nc, tt.snap)
			require.Equal(t, tt.want, got)
			c := gatewaysCondition(got, Observed{Snapshot: tt.snap})
			require.Equal(t, tt.status, string(c.Status))
			require.Equal(t, tt.reason, c.Reason)
			require.Equal(t, tt.message, c.Message)
		})
	}

	t.Run("not observed", func(t *testing.T) {
		c := gatewaysCondition(nil, Observed{ObserveErr: sysobs.ErrNoServers})
		require.Equal(t, "Unknown", string(c.Status))
		require.Equal(t, ReasonObservationFailed, c.Reason)
	})
}

// TestComputeStatus_West pins the gateway part of the status story 6 shows
// for west: GatewaysConnected, status.gateways and endpoints.gateway.
func TestComputeStatus_West(t *testing.T) {
	b, err := os.ReadFile("../../docs/content/docs/stories/06-supercluster/01-status-natscluster-west.yaml")
	require.NoError(t, err)
	b, err = e2e.StripPlaceholders(b)
	require.NoError(t, err)
	var want clusterv1beta1.NatsCluster
	require.NoError(t, yaml.UnmarshalStrict(b, &want))

	nc := storySupercluster(t, "west")
	nc.Generation = 1
	plan, err := Render(nc, Inputs{})
	require.NoError(t, err)
	snap := settledSnapshot(plan, plan.Revision, "west-0")
	for i := range snap.Servers {
		snap.Servers[i].Gateways = &sysobs.Gateways{Outbound: []string{"east"}, Inbound: map[string]int{"east": 1}}
	}
	got := computeStatus(nc, plan, Observed{StatefulSets: readySets(plan, true, true, true), Snapshot: snap})

	requireConditions(t, want.Status.Conditions, got.Conditions)
	require.Equal(t, want.Status.Gateways, got.Gateways)
	require.Equal(t, want.Status.Endpoints.Gateway, got.Endpoints.Gateway)
	require.Equal(t, want.Status.Endpoints.Client, got.Endpoints.Client)

	nc.Spec.Gateway = nil
	got = computeStatus(&clusterv1beta1.NatsCluster{ObjectMeta: nc.ObjectMeta, Spec: nc.Spec, Status: got}, plan, Observed{StatefulSets: readySets(plan, true, true, true), Snapshot: snap})
	require.Nil(t, got.Gateways)
	require.Empty(t, got.Endpoints.Gateway)
	require.Nil(t, meta.FindStatusCondition(got.Conditions, ConditionGatewaysConnected))
}

// requireConditions requires every condition in want to be in got with its
// status and reason, and its message where want has one.
func requireConditions(t *testing.T, want, got []metav1.Condition) {
	t.Helper()
	for _, w := range want {
		c := meta.FindStatusCondition(got, w.Type)
		require.NotNil(t, c, w.Type)
		require.Equal(t, w.Status, c.Status, w.Type)
		require.Equal(t, w.Reason, c.Reason, w.Type)
		if w.Message != "" {
			require.Equal(t, w.Message, c.Message, w.Type)
		}
	}
}

// member is one NATS cluster of story 6 booted in-process, and the cluster
// controller's system connection to it.
type member struct {
	nc   *clusterv1beta1.NatsCluster
	plan *Plan
	sys  *SystemConnections
	url  string
}

// freePorts returns n loopback ports free at the time of the call.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	var out []int
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		out = append(out, l.Addr().(*net.TCPAddr).Port)
		require.NoError(t, l.Close())
	}
	return out
}

// supercluster boots story 6's east and west on loopback under one NATS
// operator, with gateway TLS from one CA and each server advertising its own
// listener; mutate adjusts both NatsClusters before they render.
func supercluster(t *testing.T, mutate func(east, west *clusterv1beta1.NatsCluster)) (east, west *member) {
	t.Helper()
	p := mintPlane(t)
	ncs := map[string]*clusterv1beta1.NatsCluster{"east": storySupercluster(t, "east"), "west": storySupercluster(t, "west")}
	ports := map[string][]int{}
	for name, nc := range ncs {
		nc.Spec.JetStream.Limits = &clusterv1beta1.JetStreamLimits{MaxMemoryStore: quantity("256Mi"), MaxFileStore: quantity("1Gi")}
		nc.Spec.Gateway.Advertise = ""
		ports[name] = freePorts(t, int(nc.Spec.Replicas))
	}
	for _, nc := range ncs {
		for i, r := range nc.Spec.Gateway.Remotes {
			u, err := url.Parse(r.URL)
			require.NoError(t, err)
			u.Host = fmt.Sprintf("127.0.0.1:%d", ports[r.Name][0])
			nc.Spec.Gateway.Remotes[i].URL = u.String()
		}
	}
	mutate(ncs["east"], ncs["west"])

	gwTLS := t.TempDir()
	secret, err := selfSignedRouteSecret(ncs["east"], []string{"127.0.0.1"}, time.Now())
	require.NoError(t, err)
	for k, v := range secret.Data {
		require.NoError(t, os.WriteFile(filepath.Join(gwTLS, k), v, 0o600))
	}

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	pool := natsconn.NewPool(natsconn.WithPreset(jwtplane.PresetClusterController))
	t.Cleanup(pool.Close)
	start := func(name string) *member {
		m := &member{nc: ncs[name]}
		in := Inputs{Trust: p.trust}
		m.plan, err = Render(m.nc, in)
		require.NoError(t, err)
		_, _, m.url, _ = startRenderedWith(t, m.nc, in, m.plan.Revision, func(i int, l *Layout) {
			l.GatewayListen = fmt.Sprintf("127.0.0.1:%d", ports[name][i])
			l.GatewayTLSDir = gwTLS
		})
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: m.nc.Namespace, Name: m.nc.Spec.Auth.SystemCredentials.SecretKeyRef.Name},
			Data:       map[string][]byte{natsconn.DefaultCredentialsKey: p.systemCreds(t, jwtplane.PresetClusterController)},
		}
		m.sys = &SystemConnections{
			Client:  fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build(),
			Pool:    pool,
			Servers: func(*clusterv1beta1.NatsCluster) []string { return []string{m.url} },
		}
		return m
	}
	return start("east"), start("west")
}

// status observes m over $SYS and computes its status, every server Ready.
func (m *member) status() (clusterv1beta1.NatsClusterStatus, error) {
	snap, err := m.sys.Observe(context.Background(), m.nc)
	if err != nil {
		return clusterv1beta1.NatsClusterStatus{}, err
	}
	return computeStatus(m.nc, m.plan, Observed{StatefulSets: readySets(m.plan, true, true, true), Snapshot: snap}), nil
}

// eventuallyStatus waits until m's status passes ok, and returns it; on
// timeout it fails with the last status or error.
func (m *member) eventuallyStatus(t *testing.T, ok func(clusterv1beta1.NatsClusterStatus) bool) clusterv1beta1.NatsClusterStatus {
	t.Helper()
	var got clusterv1beta1.NatsClusterStatus
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st, err := m.status()
		require.NoError(c, err)
		require.True(c, ok(st), "%s: last %+v", m.nc.Name, st)
		got = st
	}, 60*time.Second, 250*time.Millisecond, "status never passed")
	return got
}

func gatewaysConnected(st clusterv1beta1.NatsClusterStatus) bool {
	return meta.IsStatusConditionTrue(st.Conditions, ConditionGatewaysConnected) &&
		meta.IsStatusConditionTrue(st.Conditions, ConditionSettled)
}

// TestSupercluster_Explicit pins story 6 as rendered: east and west, each
// listing both, form gateways over TLS both ways, and west reports the
// status the story shows.
func TestSupercluster_Explicit(t *testing.T) {
	east, west := supercluster(t, func(_, _ *clusterv1beta1.NatsCluster) {})

	b, err := os.ReadFile("../../docs/content/docs/stories/06-supercluster/01-status-natscluster-west.yaml")
	require.NoError(t, err)
	b, err = e2e.StripPlaceholders(b)
	require.NoError(t, err)
	var want clusterv1beta1.NatsCluster
	require.NoError(t, yaml.UnmarshalStrict(b, &want))

	got := west.eventuallyStatus(t, func(st clusterv1beta1.NatsClusterStatus) bool {
		return gatewaysConnected(st) && st.Gateways[0].Inbound == 3
	})
	requireConditions(t, want.Status.Conditions, got.Conditions)
	require.Equal(t, want.Status.Gateways, got.Gateways)

	got = east.eventuallyStatus(t, func(st clusterv1beta1.NatsClusterStatus) bool {
		return gatewaysConnected(st) && st.Gateways[0].Inbound == 3
	})
	require.Equal(t, []clusterv1beta1.GatewayStatus{{Name: "west", Connected: true, Inbound: 3, Outbound: 3}}, got.Gateways)
}

// TestSupercluster_Seeds pins what discovery does with a list naming only
// a seed: east lists only itself and west lists east. Under Gossip east
// learns west from its inbound connections and dials it back; under
// Explicit east's reject_unknown refuses west.
func TestSupercluster_Seeds(t *testing.T) {
	seedsOnly := func(discovery clusterv1beta1.GatewayDiscovery) func(east, west *clusterv1beta1.NatsCluster) {
		return func(east, west *clusterv1beta1.NatsCluster) {
			east.Spec.Gateway.Remotes = east.Spec.Gateway.Remotes[:1]
			east.Spec.Gateway.Discovery = discovery
			west.Spec.Gateway.Discovery = discovery
		}
	}

	t.Run("Gossip", func(t *testing.T) {
		east, west := supercluster(t, seedsOnly(clusterv1beta1.GatewayDiscoveryGossip))
		got := east.eventuallyStatus(t, func(st clusterv1beta1.NatsClusterStatus) bool {
			return gatewaysConnected(st) && len(st.Gateways) == 1 && st.Gateways[0].Inbound == 3
		})
		require.Equal(t, []clusterv1beta1.GatewayStatus{{Name: "west", Connected: true, Inbound: 3, Outbound: 3}}, got.Gateways)
		got = west.eventuallyStatus(t, func(st clusterv1beta1.NatsClusterStatus) bool {
			return gatewaysConnected(st) && st.Gateways[0].Inbound == 3
		})
		require.Equal(t, []clusterv1beta1.GatewayStatus{{Name: "east", Connected: true, Inbound: 3, Outbound: 3}}, got.Gateways)
	})

	t.Run("Explicit", func(t *testing.T) {
		east, west := supercluster(t, seedsOnly(clusterv1beta1.GatewayDiscoveryExplicit))
		west.eventuallyStatus(t, func(st clusterv1beta1.NatsClusterStatus) bool {
			c := meta.FindStatusCondition(st.Conditions, ConditionGatewaysConnected)
			return c != nil && c.Reason == ReasonMembersUnreachable
		})
		require.Never(t, func() bool {
			st, err := east.status()
			return err == nil && len(st.Gateways) > 0
		}, 5*time.Second, 250*time.Millisecond, "east took a gateway it does not list")
	})
}
