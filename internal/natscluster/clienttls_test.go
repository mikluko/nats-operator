package natscluster

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

func withClientTLS(nc *clusterv1beta1.NatsCluster, src clusterv1beta1.CertificateSource) *clusterv1beta1.NatsCluster {
	nc.Spec.TLS = &clusterv1beta1.ListenerTLS{CertificateSource: src}
	return nc
}

// TestClientTLS_Render pins what spec.tls renders: the client listener's
// certificate, its mount, a tls:// client endpoint, and a cert-manager
// Certificate for the client Service's names.
func TestClientTLS_Render(t *testing.T) {
	t.Run("off", func(t *testing.T) {
		nc := storyCluster(t)
		require.Nil(t, serverConfig(nc, Inputs{}, "demo-0", podLayout(nc), "").TLS)
		require.Empty(t, clientCertSecret(nc))
		require.Equal(t, "nats://demo.nats-system.svc:4222", clientURL(nc))
	})

	nc := withClientTLS(storyCluster(t), clusterv1beta1.CertificateSource{SecretRef: &natsv1beta1.SecretReference{Name: "clients"}})
	require.Equal(t, &ListenerTLSConfig{CertFile: "/etc/nats-client-tls/tls.crt", KeyFile: "/etc/nats-client-tls/tls.key"},
		serverConfig(nc, Inputs{}, "demo-0", podLayout(nc), "").TLS)
	require.Equal(t, "tls://demo.nats-system.svc:4222", clientURL(nc))

	plan, err := Render(nc, Inputs{})
	require.NoError(t, err)
	pod := plan.Servers[0].StatefulSet.Spec.Template.Spec
	require.Contains(t, pod.Volumes, corev1.Volume{Name: "client-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "clients"}}})
	require.Contains(t, pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "client-tls", MountPath: clientTLSDir, ReadOnly: true})

	issued := withClientTLS(storyCluster(t), clusterv1beta1.CertificateSource{CertManager: &clusterv1beta1.CertManagerCertificate{
		IssuerRef: clusterv1beta1.IssuerReference{Name: "internal-ca", Kind: "ClusterIssuer"},
	}})
	require.Equal(t, "demo-client-tls", clientCertSecret(issued))
	cert := clientCertificate(issued, clientIssuer(issued))
	require.Equal(t, "demo-client", cert.GetName())
	require.Equal(t, map[string]any{
		"secretName": "demo-client-tls",
		"usages":     []any{"server auth"},
		"privateKey": map[string]any{"algorithm": "ECDSA", "size": int64(256)},
		"issuerRef":  map[string]any{"name": "internal-ca", "kind": "ClusterIssuer", "group": "cert-manager.io"},
		"dnsNames":   []any{"demo", "demo.nats-system", "demo.nats-system.svc", "demo.nats-system.svc.cluster.local"},
	}, cert.Object["spec"])
}

// TestClientTLS_Server boots nats-server on a config rendered with
// spec.tls: a client holding the issuing CA connects over TLS, and one
// without it is refused.
func TestClientTLS_Server(t *testing.T) {
	nc := withClientTLS(storyCluster(t), clusterv1beta1.CertificateSource{SecretRef: &natsv1beta1.SecretReference{Name: "clients"}})
	nc.Spec.JetStream = nil
	nc.Spec.Routes = &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{Enabled: new(bool)}}
	certDir := writeRouteCert(t, nc)
	dir := t.TempDir()
	port, route := freePort(t), freePort(t)
	l := Layout{
		ClientListen:  fmt.Sprintf("127.0.0.1:%d", port),
		RouteListen:   fmt.Sprintf("127.0.0.1:%d", route),
		Routes:        []string{fmt.Sprintf("nats-route://127.0.0.1:%d", route)},
		MonitorListen: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		PidFile:       filepath.Join(dir, "nats.pid"),
		ClientTLSDir:  certDir,
	}
	b, err := serverConfig(nc, Inputs{}, "demo-0", l, "").Render()
	require.NoError(t, err)
	f := filepath.Join(dir, "nats.conf")
	require.NoError(t, os.WriteFile(f, b, 0o600))
	startFile(t, f)

	url := fmt.Sprintf("tls://127.0.0.1:%d", port)
	conn, err := nats.Connect(url, nats.RootCAs(filepath.Join(certDir, caKey)), nats.Timeout(5*time.Second))
	require.NoError(t, err)
	require.True(t, conn.TLSRequired())
	conn.Close()

	_, err = nats.Connect(url, nats.Timeout(5*time.Second), nats.NoReconnect())
	require.Error(t, err, "a client without the CA connected")
}

// TestClientCA pins the CA the cluster controller's system connection
// trusts: the client certificate Secret's ca.crt under client TLS.
func TestClientCA(t *testing.T) {
	nc := withClientTLS(storyCluster(t), clusterv1beta1.CertificateSource{SecretRef: &natsv1beta1.SecretReference{Name: "clients"}})
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nc.Namespace, Name: "clients"}, Data: map[string][]byte{caKey: []byte("CA")}}
	s := &SystemConnections{Client: fake.NewClientBuilder().WithObjects(secret).Build()}

	ca, err := s.clientCA(t.Context(), nc)
	require.NoError(t, err)
	require.Equal(t, []byte("CA"), ca)

	ca, err = s.clientCA(t.Context(), storyCluster(t))
	require.NoError(t, err)
	require.Nil(t, ca, "a CA read without client TLS")

	delete(secret.Data, caKey)
	s.Client = fake.NewClientBuilder().WithObjects(secret).Build()
	ca, err = s.clientCA(t.Context(), nc)
	require.NoError(t, err)
	require.Nil(t, ca)

	s.Client = fake.NewClientBuilder().Build()
	_, err = s.clientCA(t.Context(), nc)
	require.ErrorContains(t, err, "read client certificate Secret clients")
}
