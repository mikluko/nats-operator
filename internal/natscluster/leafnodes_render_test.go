package natscluster

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/refindex"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// renderedConfig renders server 0 of nc in its pod as a decoded config.
func renderedConfig(t *testing.T, nc *clusterv1beta1.NatsCluster, trust *Trust, remotes ...LeafRemote) map[string]any {
	t.Helper()
	b, err := serverConfig(nc, Inputs{Trust: trust}, serverName(nc, 0), podLayout(nc), "r1", remotes...).Render()
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	return m
}

// TestRender_Hub pins what hub.yaml's leafnodes renders.
func TestRender_Hub(t *testing.T) {
	p := mintPlane(t)
	nc := storyLeafCluster(t, "hub.yaml", "prod-east")
	require.Equal(t, map[string]any{
		"listen":    "0.0.0.0:7422",
		"advertise": "leaf.prod-east.acme.example:7422",
		"tls":       map[string]any{"cert_file": "/etc/nats-leafnodes-tls/tls.crt", "key_file": "/etc/nats-leafnodes-tls/tls.key"},
	}, renderedConfig(t, nc, p.trust)["leafnodes"])

	plan, err := Render(nc, Inputs{Trust: p.trust})
	require.NoError(t, err)
	sts := plan.Servers[0].StatefulSet
	nats := container(t, sts, "nats")
	require.Contains(t, nats.Ports, corev1.ContainerPort{Name: "leafnodes", ContainerPort: PortLeafnodes})
	require.Contains(t, nats.VolumeMounts, corev1.VolumeMount{Name: "leafnodes-tls", MountPath: leafnodesTLSDir, ReadOnly: true})
	require.Contains(t, sts.Spec.Template.Spec.Volumes, corev1.Volume{Name: "leafnodes-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "prod-east-leafnodes-tls"}}})

	svc := leafnodesService(nc)
	require.Equal(t, "prod-east-leafnodes", svc.Name)
	require.Equal(t, corev1.ServiceTypeLoadBalancer, svc.Spec.Type)
	require.Equal(t, "leaf.prod-east.acme.example", svc.Annotations["external-dns.alpha.kubernetes.io/hostname"])
	require.Equal(t, []corev1.ServicePort{servicePort("leafnodes", PortLeafnodes)}, svc.Spec.Ports)
	require.Equal(t, clusterSelector(nc), svc.Spec.Selector)

	cert := leafnodesCertificate(nc, &nc.Spec.Leafnodes.TLS.CertManager.IssuerRef)
	spec := cert.Object["spec"].(map[string]any)
	require.Equal(t, "prod-east-leafnodes", cert.GetName())
	require.Equal(t, "prod-east-leafnodes-tls", spec["secretName"])
	require.Equal(t, []any{"leaf.prod-east.acme.example"}, spec["dnsNames"])
	require.Equal(t, map[string]any{"name": "letsencrypt", "kind": "ClusterIssuer", "group": "cert-manager.io"}, spec["issuerRef"])

	t.Run("without advertise the certificate names the Service", func(t *testing.T) {
		nc := nc.DeepCopy()
		nc.Spec.Leafnodes.Advertise = ""
		spec := leafnodesCertificate(nc, &nc.Spec.Leafnodes.TLS.CertManager.IssuerRef).Object["spec"].(map[string]any)
		require.Equal(t, []any{"prod-east-leafnodes.nats-system.svc", "prod-east-leafnodes.nats-system.svc.cluster.local"}, spec["dnsNames"])
	})
	t.Run("the template's source ranges and class reach the Service", func(t *testing.T) {
		nc := nc.DeepCopy()
		nc.Spec.Leafnodes.Service.LoadBalancerSourceRanges = []string{"10.20.0.0/16"}
		nc.Spec.Leafnodes.Service.LoadBalancerClass = ptr.To("eks.amazonaws.com/nlb")
		svc := leafnodesService(nc)
		require.Equal(t, []string{"10.20.0.0/16"}, svc.Spec.LoadBalancerSourceRanges)
		require.Equal(t, ptr.To("eks.amazonaws.com/nlb"), svc.Spec.LoadBalancerClass)
		require.Nil(t, leafnodesService(storyLeafCluster(t, "hub.yaml", "prod-east")).Spec.LoadBalancerClass)
	})
	t.Run("without leafnodes there is no listener", func(t *testing.T) {
		nc := nc.DeepCopy()
		nc.Spec.Leafnodes = nil
		require.NotContains(t, renderedConfig(t, nc, p.trust), "leafnodes")
		require.Nil(t, leafnodesService(nc))
		plan, err := Render(nc, Inputs{Trust: p.trust})
		require.NoError(t, err)
		for _, port := range container(t, plan.Servers[0].StatefulSet, "nats").Ports {
			require.NotEqual(t, "leafnodes", port.Name)
		}
	})
}

// TestRender_ConfigVolume pins that every server's config volume projects
// its ConfigMap and the optional leaf remotes Secret together.
func TestRender_ConfigVolume(t *testing.T) {
	plan, err := Render(storyCluster(t), Inputs{})
	require.NoError(t, err)
	for _, v := range plan.Servers[1].StatefulSet.Spec.Template.Spec.Volumes {
		if v.Name != "config" {
			continue
		}
		require.NotNil(t, v.Projected)
		require.Equal(t, "demo-1-config", v.Projected.Sources[0].ConfigMap.Name)
		require.Equal(t, "demo-leaf-remotes", v.Projected.Sources[1].Secret.Name)
		require.True(t, *v.Projected.Sources[1].Secret.Optional)
		return
	}
	require.Fail(t, "no config volume")
}

// leafFixture is a fake API holding story 10's leaf-side objects in
// nats-system under a minted plane.
type leafFixture struct {
	p         testPlane
	telemetry jwtplane.Keys
	telJWT    string
	creds     []byte
}

func newLeafFixture(t *testing.T) *leafFixture {
	t.Helper()
	f := &leafFixture{p: mintPlane(t), telemetry: newTestKeys(t, nkeys.PrefixByteAccount)}
	var err error
	f.telJWT, err = jwtplane.SignAccount(jwtplane.Account{Name: "telemetry", Keys: f.telemetry}, f.p.op, time.Now())
	require.NoError(t, err)
	f.creds = f.p.creds(t, f.telemetry, leafUser())
	return f
}

// objects are edge-operator.yaml's NatsConnections, NatsAccountTrust and
// Secrets, and edge.yaml's NatsConnection.
func (f *leafFixture) objects(t *testing.T) []client.Object {
	t.Helper()
	at := &natsv1beta1.NatsAccountTrust{}
	storyDoc(t, "edge-operator.yaml", "NatsAccountTrust", "telemetry", at)
	at.Spec.PublicKey, at.Spec.JWT = publicKey(t, f.telemetry.Identity), f.telJWT
	objs := []client.Object{at,
		storyConnection(t, "edge.yaml", "hub", "tls://hub:7422", false),
		storyConnection(t, "edge-operator.yaml", "hub-system", "tls://hub:7422", true),
		storyConnection(t, "edge-operator.yaml", "hub-telemetry", "tls://hub:7422", true),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "nats-system", Name: "hub-ca"}, Data: map[string][]byte{caKey: testCA(t)}},
	}
	for _, name := range []string{"edge-site-1-leaf-creds", "edge-site-2-leaf-creds"} {
		objs = append(objs, credsSecret("nats-system", name, f.creds))
	}
	return append(objs, credsSecret("nats-system", "edge-site-2-system-leaf-creds", f.p.systemCreds(t, jwtplane.PresetLeafnode)))
}

func (f *leafFixture) client(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(append(f.objects(t), objs...)...).Build()
}

func testCA(t *testing.T) []byte {
	t.Helper()
	s, err := selfSignedRouteSecret(storyCluster(t), []string{"hub"}, time.Now())
	require.NoError(t, err)
	return s.Data[caKey]
}

// TestRender_Leaf pins what edge.yaml and edge-operator.yaml render.
func TestRender_Leaf(t *testing.T) {
	f := newLeafFixture(t)
	c := f.client(t)
	ctx := context.Background()

	t.Run("edge.yaml", func(t *testing.T) {
		nc := storyLeafCluster(t, "edge.yaml", "edge-site-1")
		remotes, cond, err := readLeafRemotes(ctx, c, nc, nil)
		require.NoError(t, err)
		require.Nil(t, cond)
		require.Equal(t, publicKey(t, f.telemetry.Identity), remotes[0].HubAccount)
		require.Equal(t, map[string]any{"remotes": []any{map[string]any{
			"urls":        []any{"tls://hub:7422"},
			"credentials": "/etc/nats-config/nats-system_hub.creds",
		}}}, renderedConfig(t, nc, nil, remotes...)["leafnodes"])
		require.Equal(t, map[string][]byte{"nats-system_hub.creds": f.creds}, leafRemotesSecret(nc, remotes).Data)
	})

	t.Run("edge-operator.yaml", func(t *testing.T) {
		nc := storyLeafCluster(t, "edge-operator.yaml", "edge-site-2")
		remotes, cond, err := readLeafRemotes(ctx, c, nc, f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		tel := publicKey(t, f.telemetry.Identity)
		m := renderedConfig(t, nc, f.p.trust, remotes...)
		require.Equal(t, map[string]any{"remotes": []any{
			map[string]any{
				"urls":        []any{"tls://hub:7422"},
				"credentials": "/etc/nats-config/nats-system_hub-system.creds",
				"account":     f.p.trust.SystemAccount,
				"tls":         map[string]any{"ca_file": "/etc/nats-config/nats-system_hub-system.ca.crt"},
			},
			map[string]any{
				"urls":        []any{"tls://hub:7422"},
				"credentials": "/etc/nats-config/nats-system_hub-telemetry.creds",
				"account":     tel,
				"tls":         map[string]any{"ca_file": "/etc/nats-config/nats-system_hub-telemetry.ca.crt"},
			},
		}}, m["leafnodes"])
		require.Equal(t, map[string]any{"type": "full", "dir": "/data/resolver", "allow_delete": true}, m["resolver"])
		require.Equal(t, map[string]any{f.p.trust.SystemAccount: f.p.trust.SystemAccountJWT, tel: f.telJWT}, m["resolver_preload"])
		require.Len(t, leafRemotesSecret(nc, remotes).Data, 4)
	})

	t.Run("a remote no grant admits renders out, its preload kept", func(t *testing.T) {
		nc := storyLeafCluster(t, "edge-operator.yaml", "edge-site-2")
		nc.Spec.LeafRemotes[1].ConnectionRef.Namespace = "hubs"
		remotes, cond, err := readLeafRemotes(ctx, c, nc, f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		require.Len(t, remotes, 2)
		require.Nil(t, remotes[0].Refusal)
		require.NotNil(t, remotes[1].Refusal)
		require.Equal(t, "NoGrant", remotes[1].Refusal.Reason)
		require.Contains(t, remotes[1].Refusal.Message, "leafRemotes[1]: ")
		tel := publicKey(t, f.telemetry.Identity)
		m := renderedConfig(t, nc, f.p.trust, remotes...)
		leaf := m["leafnodes"].(map[string]any)["remotes"].([]any)
		require.Len(t, leaf, 1)
		require.Equal(t, f.p.trust.SystemAccount, leaf[0].(map[string]any)["account"])
		require.Equal(t, "full", m["resolver"].(map[string]any)["type"])
		require.Equal(t, map[string]any{f.p.trust.SystemAccount: f.p.trust.SystemAccountJWT, tel: f.telJWT}, m["resolver_preload"])
		data := leafRemotesSecret(nc, remotes).Data
		require.Len(t, data, 2)
		require.NotContains(t, data, "hubs_hub-telemetry.creds")

		nc.Spec.LeafRemotes[0].ConnectionRef.Namespace = "hubs"
		remotes, cond, err = readLeafRemotes(ctx, c, nc, f.p.trust)
		require.NoError(t, err)
		require.Nil(t, cond)
		require.NotContains(t, renderedConfig(t, nc, f.p.trust, remotes...), "leafnodes")
		require.Nil(t, leafRemotesSecret(nc, remotes))
	})
}

// TestResolverType pins the resolver a NatsCluster runs: auth.resolver
// when set, otherwise Cache on a leaf that preloads nothing, through a
// remote or auth.accountTrustRefs, and Full everywhere else.
func TestResolverType(t *testing.T) {
	preload := []LeafRemote{{LocalAccount: "A", PreloadJWT: "jwt"}}
	fetch := []LeafRemote{{LocalAccount: "SYS"}}
	accounts := []AccountPreload{{PublicKey: "B", JWT: "jwt"}}
	for _, tt := range []struct {
		name     string
		set      clusterv1beta1.ResolverType
		leaf     bool
		remotes  []LeafRemote
		accounts []AccountPreload
		expected clusterv1beta1.ResolverType
	}{
		{"not a leaf", "", false, nil, nil, clusterv1beta1.ResolverFull},
		{"not a leaf, preloading accounts", "", false, nil, accounts, clusterv1beta1.ResolverFull},
		{"a leaf preloading nothing", "", true, fetch, nil, clusterv1beta1.ResolverCache},
		{"a leaf preloading an account", "", true, append(fetch, preload...), nil, clusterv1beta1.ResolverFull},
		{"a leaf preloading through accountTrustRefs", "", true, fetch, accounts, clusterv1beta1.ResolverFull},
		{"Cache set on a preloading leaf", clusterv1beta1.ResolverCache, true, preload, accounts, clusterv1beta1.ResolverCache},
		{"Full set on a leaf preloading nothing", clusterv1beta1.ResolverFull, true, fetch, nil, clusterv1beta1.ResolverFull},
	} {
		t.Run(tt.name, func(t *testing.T) {
			nc := &clusterv1beta1.NatsCluster{Spec: clusterv1beta1.NatsClusterSpec{Auth: &clusterv1beta1.Auth{Resolver: tt.set}}}
			if tt.leaf {
				nc.Spec.LeafRemotes = []clusterv1beta1.LeafRemote{{ConnectionRef: natsv1beta1.ObjectReference{Name: "hub"}}}
			}
			require.Equal(t, tt.expected, resolverType(nc, tt.remotes, tt.accounts))
		})
	}
}

// TestReadLeafRemotes_Refusals pins the Progressing condition for each
// remote that cannot be rendered.
func TestReadLeafRemotes_Refusals(t *testing.T) {
	f := newLeafFixture(t)
	ctx := context.Background()
	other := mintPlane(t)
	foreignJWT, err := jwtplane.SignAccount(jwtplane.Account{Name: "telemetry", Keys: f.telemetry}, other.op, time.Now())
	require.NoError(t, err)

	operatorLeaf := func(t *testing.T) *clusterv1beta1.NatsCluster {
		return storyLeafCluster(t, "edge-operator.yaml", "edge-site-2")
	}
	for _, tt := range []struct {
		name   string
		nc     func(*testing.T) *clusterv1beta1.NatsCluster
		mutate func(t *testing.T, c client.Client)
		reason string
		msg    string
	}{
		{"NatsConnection missing", func(t *testing.T) *clusterv1beta1.NatsCluster {
			nc := storyLeafCluster(t, "edge.yaml", "edge-site-1")
			nc.Spec.LeafRemotes[0].ConnectionRef.Name = "nowhere"
			return nc
		}, nil, ReasonLeafRemoteNotFound, "NatsConnection nats-system/nowhere does not exist"},
		{"one NatsConnection named twice", func(t *testing.T) *clusterv1beta1.NatsCluster {
			nc := storyLeafCluster(t, "edge.yaml", "edge-site-1")
			again := nc.Spec.LeafRemotes[0]
			again.ConnectionRef.Namespace = "nats-system"
			nc.Spec.LeafRemotes = append(nc.Spec.LeafRemotes, again)
			return nc
		}, nil, ReasonUnsupportedSpec, "leafRemotes[0] and leafRemotes[1] both name NatsConnection nats-system/hub"},
		{"creds Secret missing", func(t *testing.T) *clusterv1beta1.NatsCluster {
			return storyLeafCluster(t, "edge.yaml", "edge-site-1")
		}, func(t *testing.T, c client.Client) {
			require.NoError(t, c.Delete(context.Background(), credsSecret("nats-system", "edge-site-1-leaf-creds", nil)))
		}, ReasonLeafRemoteNotFound, "edge-site-1-leaf-creds"},
		{"account trust not yet filled in", operatorLeaf, func(t *testing.T, c client.Client) {
			at := &natsv1beta1.NatsAccountTrust{}
			require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "nats-system", Name: "telemetry"}, at))
			at.Spec = natsv1beta1.NatsAccountTrustSpec{AccountRef: &natsv1beta1.ObjectReference{Name: "telemetry"}}
			require.NoError(t, c.Update(context.Background(), at))
		}, ReasonLeafRemoteNotReady, "leafRemotes[1]: NatsAccountTrust nats-system/telemetry has no public key"},
		{"preload signed by another operator", operatorLeaf, func(t *testing.T, c client.Client) {
			at := &natsv1beta1.NatsAccountTrust{}
			require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "nats-system", Name: "telemetry"}, at))
			at.Spec.JWT = foreignJWT
			require.NoError(t, c.Update(context.Background(), at))
		}, ReasonLeafRemoteInvalid, "not by operator"},
		{"preload of another account", operatorLeaf, func(t *testing.T, c client.Client) {
			at := &natsv1beta1.NatsAccountTrust{}
			require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "nats-system", Name: "telemetry"}, at))
			at.Spec.PublicKey = publicKey(t, newTestPair(t, nkeys.PrefixByteAccount))
			require.NoError(t, c.Update(context.Background(), at))
		}, ReasonLeafRemoteInvalid, "jwt is account"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := f.client(t)
			if tt.mutate != nil {
				tt.mutate(t, c)
			}
			nc := tt.nc(t)
			var trust *Trust
			if nc.Spec.Auth != nil {
				trust = f.p.trust
			}
			remotes, cond, err := readLeafRemotes(ctx, c, nc, trust)
			require.NoError(t, err)
			require.Nil(t, remotes)
			require.NotNil(t, cond)
			require.Equal(t, metav1.ConditionFalse, cond.Status)
			require.Equal(t, tt.reason, cond.Reason, cond.Message)
			require.Contains(t, cond.Message, tt.msg)
		})
	}
}

func TestUnsupportedLeafFields(t *testing.T) {
	remote := func(mutate func(*clusterv1beta1.LeafRemote)) clusterv1beta1.LeafRemote {
		r := clusterv1beta1.LeafRemote{ConnectionRef: natsv1beta1.ObjectReference{Name: "hub"}}
		mutate(&r)
		return r
	}
	auth := &clusterv1beta1.Auth{TrustRef: natsv1beta1.ObjectReference{Name: "acme"}}
	for _, tt := range []struct {
		name   string
		auth   *clusterv1beta1.Auth
		remote clusterv1beta1.LeafRemote
		want   []string
	}{
		{"global account", nil, remote(func(*clusterv1beta1.LeafRemote) {}), nil},
		{"system account without auth", nil, remote(func(r *clusterv1beta1.LeafRemote) { r.LocalSystemAccount = true }), []string{"leafRemotes[0].localSystemAccount without auth"}},
		{"account trust without auth", nil, remote(func(r *clusterv1beta1.LeafRemote) {
			r.LocalAccountTrustRef = &natsv1beta1.ObjectReference{Name: "t"}
		}), []string{"leafRemotes[0].localAccountTrustRef without auth"}},
		{"system account under auth", auth, remote(func(r *clusterv1beta1.LeafRemote) { r.LocalSystemAccount = true }), nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec := &clusterv1beta1.NatsClusterSpec{Auth: tt.auth, LeafRemotes: []clusterv1beta1.LeafRemote{tt.remote}}
			require.Equal(t, tt.want, unsupportedLeafFields(spec))
		})
	}
}

// TestRestartReason_Leafnodes pins the classification of leafnodes
// changes: remotes reload, the first and the last included, and the
// listener restarts.
func TestRestartReason_Leafnodes(t *testing.T) {
	remote := func(n string) any {
		return map[string]any{"urls": []any{"tls://" + n + ":7422"}, "credentials": "/etc/nats-config/" + n + ".creds"}
	}
	base := func() map[string]any {
		return map[string]any{"server_name": "s", "leafnodes": map[string]any{"remotes": []any{remote("a")}}}
	}
	for _, tt := range []struct {
		name   string
		from   func(m map[string]any)
		to     func(m map[string]any)
		reason string
	}{
		{"remote added", nil, func(m map[string]any) {
			setPath(m, "leafnodes.remotes", []any{remote("a"), remote("b")})
		}, ""},
		{"remote removed", func(m map[string]any) {
			setPath(m, "leafnodes.remotes", []any{remote("a"), remote("b")})
		}, nil, ""},
		{"first remote added", func(m map[string]any) { delete(m, "leafnodes") }, nil, ""},
		{"last remote removed", nil, func(m map[string]any) { delete(m, "leafnodes") }, ""},
		{"listener added", nil, func(m map[string]any) { setPath(m, "leafnodes.listen", "0.0.0.0:7422") }, "leafnodes.listen is restart-only"},
		{"listener added with the first remote", func(m map[string]any) { delete(m, "leafnodes") }, func(m map[string]any) {
			setPath(m, "leafnodes.listen", "0.0.0.0:7422")
		}, "leafnodes is restart-only"},
		{"advertise changed", func(m map[string]any) { setPath(m, "leafnodes.advertise", "a:7422") }, func(m map[string]any) {
			setPath(m, "leafnodes.advertise", "b:7422")
		}, "leafnodes.advertise is restart-only"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			from, to := base(), base()
			if tt.from != nil {
				tt.from(from)
			}
			if tt.to != nil {
				tt.to(to)
			}
			require.Equal(t, tt.reason, restartReason("2.15.0", encode(t, from), encode(t, to)))
		})
	}
}

// TestLeafStatus pins the per-remote counts and LeafnodesConnected from a
// leafz observation.
func TestLeafStatus(t *testing.T) {
	nc := storyLeafCluster(t, "edge.yaml", "edge-site-1")
	nc.Generation = 4
	remotes := []LeafRemote{
		{Connection: types.NamespacedName{Namespace: "nats-system", Name: "hub"}, HubAccount: "ATEL"},
		{Connection: types.NamespacedName{Namespace: "orders", Name: "hub"}, HubAccount: "AORD"},
		{Connection: types.NamespacedName{Namespace: "nats-system", Name: "hub-system"}, LocalAccount: "ASYS", HubAccount: "ASYS"},
	}
	plan, err := Render(nc, Inputs{})
	require.NoError(t, err)
	plan.LeafRemotes = remotes
	spoke := func(acc string) sysobs.Leaf { return sysobs.Leaf{Account: acc, Spoke: true} }
	all := []sysobs.Leaf{spoke(globalAccount), spoke(globalAccount), spoke("ASYS")}

	t.Run("every server holds every remote", func(t *testing.T) {
		var st clusterv1beta1.NatsClusterStatus
		leafStatus(&st, nc, plan, map[string][]sysobs.Leaf{"edge-site-1-0": all, "edge-site-1-1": all, "edge-site-1-2": all}, nil)
		require.Equal(t, []clusterv1beta1.LeafRemoteStatus{
			{ConnectionNamespace: "nats-system", ConnectionName: "hub", Connected: 3, Account: "ATEL"},
			{ConnectionNamespace: "orders", ConnectionName: "hub", Connected: 3, Account: "AORD"},
			{ConnectionNamespace: "nats-system", ConnectionName: "hub-system", Connected: 3, Account: "ASYS"},
		}, st.LeafRemotes)
		c := meta.FindStatusCondition(st.Conditions, ConditionLeafnodesConnected)
		require.Equal(t, metav1.ConditionTrue, c.Status)
		require.Equal(t, ReasonAllRemotesConnected, c.Reason)
		require.Equal(t, "3 of 3 servers connected to 3 remotes", c.Message)
		require.Equal(t, int64(4), c.ObservedGeneration)
	})
	t.Run("a server short of one global remote, another silent", func(t *testing.T) {
		var st clusterv1beta1.NatsClusterStatus
		leafStatus(&st, nc, plan, map[string][]sysobs.Leaf{
			"edge-site-1-0": all,
			"edge-site-1-1": {spoke(globalAccount), spoke("ASYS"), {Account: globalAccount}},
		}, nil)
		c := meta.FindStatusCondition(st.Conditions, ConditionLeafnodesConnected)
		require.Equal(t, metav1.ConditionFalse, c.Status)
		require.Equal(t, ReasonRemotesDisconnected, c.Reason)
		require.Equal(t, "nats-system/hub: 1 of 3 servers connected; orders/hub: 1 of 3 servers connected; nats-system/hub-system: 2 of 3 servers connected", c.Message)
	})
	t.Run("no observation keeps each remote's count", func(t *testing.T) {
		st := clusterv1beta1.NatsClusterStatus{LeafRemotes: []clusterv1beta1.LeafRemoteStatus{
			{ConnectionNamespace: "nats-system", ConnectionName: "hub", Connected: 2},
			{ConnectionNamespace: "orders", ConnectionName: "hub", Connected: 1},
		}}
		leafStatus(&st, nc, plan, nil, errors.New("no answer"))
		require.Equal(t, []int32{2, 1, 0}, []int32{st.LeafRemotes[0].Connected, st.LeafRemotes[1].Connected, st.LeafRemotes[2].Connected})
		require.Equal(t, "ATEL", st.LeafRemotes[0].Account)
		c := meta.FindStatusCondition(st.Conditions, ConditionLeafnodesConnected)
		require.Equal(t, metav1.ConditionUnknown, c.Status)
		require.Equal(t, "no answer", c.Message)
	})
	t.Run("a refused remote reads its refusal and counts no server", func(t *testing.T) {
		plan := *plan
		plan.LeafRemotes = slices.Clone(remotes)
		plan.LeafRemotes[1].HubAccount = ""
		plan.LeafRemotes[1].Refusal = &metav1.Condition{Reason: "NoGrant", Message: "leafRemotes[1]: no grant"}
		st := clusterv1beta1.NatsClusterStatus{LeafRemotes: []clusterv1beta1.LeafRemoteStatus{
			{ConnectionNamespace: "orders", ConnectionName: "hub", Connected: 3, Account: "AORD"},
		}}
		one := []sysobs.Leaf{spoke(globalAccount), spoke("ASYS")}
		leafStatus(&st, nc, &plan, map[string][]sysobs.Leaf{"edge-site-1-0": one, "edge-site-1-1": one, "edge-site-1-2": one}, nil)
		require.Equal(t, []clusterv1beta1.LeafRemoteStatus{
			{ConnectionNamespace: "nats-system", ConnectionName: "hub", Connected: 3, Account: "ATEL"},
			{ConnectionNamespace: "orders", ConnectionName: "hub"},
			{ConnectionNamespace: "nats-system", ConnectionName: "hub-system", Connected: 3, Account: "ASYS"},
		}, st.LeafRemotes)
		c := meta.FindStatusCondition(st.Conditions, ConditionLeafnodesConnected)
		require.Equal(t, metav1.ConditionFalse, c.Status)
		require.Equal(t, "NoGrant", c.Reason)
		require.Equal(t, "leafRemotes[1]: no grant", c.Message)
	})
	t.Run("every remote refused still reads its refusal", func(t *testing.T) {
		plan := &Plan{Servers: plan.Servers, LeafRemotes: []LeafRemote{{Connection: types.NamespacedName{Namespace: "orders", Name: "hub"},
			Refusal: &metav1.Condition{Reason: "NoGrant", Message: "leafRemotes[0]: no grant"}}}}
		var st clusterv1beta1.NatsClusterStatus
		(&Reconciler{Observer: &fakeObserver{}}).observeLeafs(context.Background(), nc, plan, &st)
		c := meta.FindStatusCondition(st.Conditions, ConditionLeafnodesConnected)
		require.Equal(t, metav1.ConditionFalse, c.Status)
		require.Equal(t, "NoGrant", c.Reason)
	})
	t.Run("no remotes clears both", func(t *testing.T) {
		st := clusterv1beta1.NatsClusterStatus{LeafRemotes: []clusterv1beta1.LeafRemoteStatus{{ConnectionNamespace: "nats-system", ConnectionName: "hub"}}}
		conditions.Set(&st.Conditions, 1, metav1.Condition{Type: ConditionLeafnodesConnected, Status: metav1.ConditionTrue, Reason: ReasonAllRemotesConnected})
		plan := &Plan{}
		(&Reconciler{Observer: &fakeObserver{}}).observeLeafs(context.Background(), nc, plan, &st)
		require.Nil(t, st.LeafRemotes)
		require.Nil(t, meta.FindStatusCondition(st.Conditions, ConditionLeafnodesConnected))
	})
}

// TestLeafWatches pins the watch mappings: a NatsConnection, a
// NatsAccountTrust, and a Secret a NatsConnection reads each enqueue the
// NatsClusters whose leafRemotes, or for a NatsAccountTrust whose
// auth.accountTrustRefs, name them, from any namespace.
func TestLeafWatches(t *testing.T) {
	leaf := func(ns, name string, remotes ...clusterv1beta1.LeafRemote) *clusterv1beta1.NatsCluster {
		return &clusterv1beta1.NatsCluster{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: clusterv1beta1.NatsClusterSpec{LeafRemotes: remotes}}
	}
	conn := func(ns, name, secret string) *natsv1beta1.NatsConnection {
		return &natsv1beta1.NatsConnection{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: natsv1beta1.NatsConnectionSpec{
			Servers:     []string{"tls://hub:7422"},
			Credentials: &natsv1beta1.Credentials{SecretKeyRef: natsv1beta1.CredentialsSecretKeySelector{Name: secret}},
		}}
	}
	b := fake.NewClientBuilder().WithScheme(leafScheme(t))
	for field, keys := range map[string]func(*clusterv1beta1.NatsCluster) []string{
		LeafConnectionField: leafConnectionKeys,
		AccountTrustField:   accountTrustKeys,
	} {
		b = b.WithIndex(&clusterv1beta1.NatsCluster{}, field, func(o client.Object) []string { return keys(o.(*clusterv1beta1.NatsCluster)) })
	}
	b = b.WithIndex(&natsv1beta1.NatsConnection{}, ConnectionSecretField, func(o client.Object) []string {
		return connectionSecretKeys(o.(*natsv1beta1.NatsConnection))
	})
	c := b.WithObjects(
		leaf("a", "same", clusterv1beta1.LeafRemote{ConnectionRef: natsv1beta1.ObjectReference{Name: "hub"}}),
		leaf("b", "cross", clusterv1beta1.LeafRemote{
			ConnectionRef:        natsv1beta1.ObjectReference{Name: "hub", Namespace: "a"},
			LocalAccountTrustRef: &natsv1beta1.ObjectReference{Name: "telemetry", Namespace: "a"},
		}),
		leaf("a", "other", clusterv1beta1.LeafRemote{ConnectionRef: natsv1beta1.ObjectReference{Name: "elsewhere"}}),
		&clusterv1beta1.NatsCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "c", Name: "preloading"}, Spec: clusterv1beta1.NatsClusterSpec{Auth: &clusterv1beta1.Auth{
			AccountTrustRefs: []natsv1beta1.ObjectReference{{Name: "telemetry", Namespace: "a"}},
		}}},
		conn("a", "hub", "hub-creds"), conn("a", "elsewhere", "other-creds"),
	).Build()
	r := &Reconciler{Client: c}
	names := func(reqs []reconcile.Request) []string {
		var out []string
		for _, q := range reqs {
			out = append(out, q.String())
		}
		return out
	}
	ctx := context.Background()
	clusters := &clusterv1beta1.NatsClusterList{}
	hubConn := &natsv1beta1.NatsConnection{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "hub"}}
	require.ElementsMatch(t, []string{"a/same", "b/cross"}, enqueued(t, refindex.EnqueueByField(c, clusters, LeafConnectionField), hubConn))
	telemetry := &natsv1beta1.NatsAccountTrust{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "telemetry"}}
	require.ElementsMatch(t, []string{"b/cross", "c/preloading"}, enqueued(t, refindex.EnqueueByField(c, clusters, AccountTrustField), telemetry))
	require.ElementsMatch(t, []string{"a/same", "b/cross"}, names(r.clustersReadingSecret(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "hub-creds"}})))
	require.Empty(t, r.clustersReadingSecret(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "b", Name: "hub-creds"}}))
	require.Empty(t, r.clustersReadingSecret(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: "unrelated"}}))
}
