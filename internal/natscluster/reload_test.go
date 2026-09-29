package natscluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// TestReloadServer drives reloadServer through sysobs against a server
// running story 1's rendered config with a system user added.
func TestReloadServer(t *testing.T) {
	nc := limitedStoryCluster(t)
	m := renderedMap(t, nc, writeRouteCert(t, nc))
	m["accounts"] = map[string]any{"SYS": map[string]any{"users": []any{map[string]any{"user": "sys", "password": "sys"}}}}
	m["system_account"] = "SYS"
	f := filepath.Join(t.TempDir(), "nats.conf")
	require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
	o, err := server.ProcessConfigFile(f)
	require.NoError(t, err)
	o.NoLog, o.NoSigs = true, true
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))

	conn, err := nats.Connect(fmt.Sprintf("nats://sys:sys@%s", s.Addr()))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	obs := sysobs.New(conn, "demo")
	snap := &sysobs.Snapshot{Servers: []sysobs.Server{{Name: "demo-0", ID: s.ID()}}}
	ctx := context.Background()

	rendered := func(mutate func(map[string]any)) (Server, []byte) {
		mutate(m)
		b := encode(t, m)
		return Server{Name: "demo-0", ConfigMap: &corev1.ConfigMap{Data: map[string]string{configFile: string(b)}}}, b
	}
	tagged, taggedFile := rendered(func(m map[string]any) { m["server_tags"] = []any{"az:b"} })

	t.Run("file not yet updated", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, snap, tagged, Certs{})
		require.NoError(t, err)
		require.False(t, applied)
	})
	t.Run("server unobserved", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, &sysobs.Snapshot{Silent: []string{"demo-0"}, Servers: snap.Servers}, tagged, Certs{})
		require.NoError(t, err)
		require.False(t, applied)
	})
	t.Run("reloads", func(t *testing.T) {
		require.NoError(t, os.WriteFile(f, taggedFile, 0o600))
		applied, err := reloadServer(ctx, obs, snap, tagged, Certs{})
		require.NoError(t, err)
		require.True(t, applied)
		require.Equal(t, []string{"az:b"}, varzTags(t, s))
	})
	t.Run("already loaded", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, snap, tagged, Certs{})
		require.NoError(t, err)
		require.True(t, applied)
	})
	t.Run("restart-only change fails", func(t *testing.T) {
		domain, domainFile := rendered(func(m map[string]any) { m["jetstream"].(map[string]any)["domain"] = "hub" })
		require.NoError(t, os.WriteFile(f, domainFile, 0o600))
		applied, err := reloadServer(ctx, obs, snap, domain, Certs{})
		require.ErrorIs(t, err, sysobs.ErrServer)
		require.False(t, applied)
	})
}

func varzTags(t *testing.T, s *server.Server) []string {
	t.Helper()
	v, err := s.Varz(nil)
	require.NoError(t, err)
	return v.Tags
}

// TestReloadServerCertRotation rotates the route certificate under a
// running server as kubelet refreshes a mounted Secret, apart from its
// ConfigMap, and pins that a reload is confirmed only once the server
// serves the rotated certificate.
func TestReloadServerCertRotation(t *testing.T) {
	nc := limitedStoryCluster(t)
	tlsDir := t.TempDir()
	first, err := selfSignedRouteSecret(nc, []string{"127.0.0.1"}, time.Now())
	require.NoError(t, err)
	writeSecretFiles(t, tlsDir, first)
	m := renderedMap(t, nc, tlsDir)
	m["accounts"] = map[string]any{"SYS": map[string]any{"users": []any{map[string]any{"user": "sys", "password": "sys"}}}}
	m["system_account"] = "SYS"
	f := filepath.Join(t.TempDir(), "nats.conf")
	require.NoError(t, os.WriteFile(f, encode(t, m), 0o600))
	o, err := server.ProcessConfigFile(f)
	require.NoError(t, err)
	o.NoLog, o.NoSigs = true, true
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))

	conn, err := nats.Connect(fmt.Sprintf("nats://sys:sys@%s", s.Addr()))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	obs := sysobs.New(conn, "demo")
	snap := &sysobs.Snapshot{Servers: []sysobs.Server{{Name: "demo-0", ID: s.ID()}}}
	ctx := context.Background()
	routeAddr := s.ClusterAddr().String()
	require.Equal(t, leafSerial(t, first), servedSerial(t, routeAddr, tlsDir))

	rotated, err := selfSignedRouteSecret(nc, []string{"127.0.0.1"}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	certs := Certs{Routes: mountedCert(rotated)}
	require.NotEqual(t, mountedCert(first).NotAfter, certs.Routes.NotAfter)
	m["server_metadata"] = map[string]any{MetadataConfigRevision: "r2"}
	b := encode(t, m)
	next := Server{Name: "demo-0", ConfigMap: &corev1.ConfigMap{Data: map[string]string{configFile: string(b)}}}
	require.NoError(t, os.WriteFile(f, b, 0o600))

	t.Run("certificate not yet refreshed", func(t *testing.T) {
		applied, err := reloadServer(ctx, obs, snap, next, certs)
		require.NoError(t, err)
		require.False(t, applied)
		require.Equal(t, leafSerial(t, first), servedSerial(t, routeAddr, tlsDir))
	})
	t.Run("rotated certificate served", func(t *testing.T) {
		writeSecretFiles(t, tlsDir, rotated)
		applied, err := reloadServer(ctx, obs, snap, next, certs)
		require.NoError(t, err)
		require.True(t, applied)
		require.Equal(t, leafSerial(t, rotated), servedSerial(t, routeAddr, tlsDir))
	})
}

func writeSecretFiles(t *testing.T, dir string, s *corev1.Secret) {
	t.Helper()
	for k, v := range s.Data {
		require.NoError(t, os.WriteFile(filepath.Join(dir, k), v, 0o600))
	}
}

func leafSerial(t *testing.T, s *corev1.Secret) string {
	t.Helper()
	b, _ := pem.Decode(s.Data[corev1.TLSCertKey])
	require.NotNil(t, b)
	c, err := x509.ParseCertificate(b.Bytes)
	require.NoError(t, err)
	return c.SerialNumber.String()
}

// servedSerial is the serial of the certificate the route listener at addr
// serves, dialled with the client certificate in dir.
func servedSerial(t *testing.T, addr, dir string) string {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, corev1.TLSCertKey), filepath.Join(dir, corev1.TLSPrivateKeyKey))
	require.NoError(t, err)
	c, err := tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{cert}}) //nolint:gosec // the test reads the served certificate, it does not trust it
	require.NoError(t, err)
	serial := c.ConnectionState().PeerCertificates[0].SerialNumber.String()
	require.NoError(t, c.Close())
	return serial
}

// TestRunningVersion pins the tag read off each form the nats image takes.
func TestRunningVersion(t *testing.T) {
	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name, image, want string
	}{
		{"tag", "nats:2.15.0", "2.15.0"},
		{"tag and digest", "nats:2.15.0@" + digest, "2.15.0"},
		{"registry port, tag and digest", "registry.example:5000/nats:2.14.1@" + digest, "2.14.1"},
		{"digest only", "nats@" + digest, ""},
		{"registry port, no tag", "registry.example:5000/nats", ""},
		{"unparseable", "NATS:", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{}
			sts.Spec.Template.Spec.Containers = []corev1.Container{{Name: "exporter", Image: "x:9"}, {Name: "nats", Image: tc.image}}
			require.Equal(t, tc.want, runningVersion(sts))
		})
	}
}

// TestChangeRestartReasonDigest pins the version pair named when a
// digest-pinned server's version changes.
func TestChangeRestartReasonDigest(t *testing.T) {
	nc := &clusterv1beta1.NatsCluster{}
	nc.Spec.Version = "2.15.0"
	nc.Spec.Image = &clusterv1beta1.Image{Digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	cur := &appsv1.StatefulSet{}
	cur.Annotations = map[string]string{AnnotationSpecDigest: "old"}
	cur.Spec.Template.Spec.Containers = []corev1.Container{{Name: "nats", Image: "nats:2.14.1@" + nc.Spec.Image.Digest}}
	s := Server{StatefulSet: &appsv1.StatefulSet{}}
	s.StatefulSet.Annotations = map[string]string{AnnotationSpecDigest: "new"}
	require.Equal(t, "version 2.14.1 -> 2.15.0 is restart-only", changeRestartReason(nc, cur, &corev1.ConfigMap{}, s))
}
