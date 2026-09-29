package natscluster

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

func parseCert(t *testing.T, pemBytes []byte) *x509.Certificate {
	t.Helper()
	b, _ := pem.Decode(pemBytes)
	require.NotNil(t, b)
	c, err := x509.ParseCertificate(b.Bytes)
	require.NoError(t, err)
	return c
}

// verifies reports whether the route certificate in leaf verifies against
// the ca.crt in trust, as a route peer checks it.
func verifies(t *testing.T, leaf, trust *corev1.Secret, at time.Time) bool {
	t.Helper()
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(trust.Data[caKey]))
	_, err := parseCert(t, leaf.Data[corev1.TLSCertKey]).Verify(x509.VerifyOptions{
		DNSName:     "demo-0.demo-headless.nats-system.svc",
		Roots:       pool,
		CurrentTime: at,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	})
	return err == nil
}

func TestEnsureSelfSignedRouteSecret(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, clusterv1beta1.AddToScheme(scheme))
	t0 := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	setUp := func(t *testing.T, objs ...client.Object) (*Reconciler, *clusterv1beta1.NatsCluster, *time.Time) {
		t.Helper()
		nc := storyCluster(t)
		nc.UID = types.UID("demo-uid")
		clock := t0
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objs, nc)...).Build()
		return &Reconciler{Client: c, Now: func() time.Time { return clock }}, nc, &clock
	}
	get := func(t *testing.T, r *Reconciler, nc *clusterv1beta1.NatsCluster, name string) *corev1.Secret {
		t.Helper()
		s := &corev1.Secret{}
		require.NoError(t, r.Client.Get(context.Background(), types.NamespacedName{Namespace: nc.Namespace, Name: name}, s))
		return s
	}

	t.Run("issued for a year, renewed with the same CA key", func(t *testing.T) {
		r, nc, clock := setUp(t)
		ctx := context.Background()
		require.NoError(t, r.ensureSelfSignedRouteSecret(ctx, nc))
		first := get(t, r, nc, routesSecretName(nc))
		ca := get(t, r, nc, routesCASecretName(nc))
		require.True(t, metav1.IsControlledBy(first, nc))
		require.True(t, metav1.IsControlledBy(ca, nc))
		require.Equal(t, t0.Add(selfSignedValidity), parseCert(t, first.Data[corev1.TLSCertKey]).NotAfter)
		require.Equal(t, ca.Data[corev1.TLSCertKey], first.Data[caKey])

		*clock = t0.Add(selfSignedValidity - selfSignedRenewBefore - day)
		require.NoError(t, r.ensureSelfSignedRouteSecret(ctx, nc))
		require.Equal(t, first.Data, get(t, r, nc, routesSecretName(nc)).Data, "renewed before it was due")

		*clock = t0.Add(selfSignedValidity - selfSignedRenewBefore)
		require.NoError(t, r.ensureSelfSignedRouteSecret(ctx, nc))
		renewed := get(t, r, nc, routesSecretName(nc))
		require.Equal(t, clock.Add(selfSignedValidity), parseCert(t, renewed.Data[corev1.TLSCertKey]).NotAfter)
		require.Equal(t, ca.Data[corev1.TLSPrivateKeyKey], get(t, r, nc, routesCASecretName(nc)).Data[corev1.TLSPrivateKeyKey])
		require.True(t, verifies(t, renewed, first, *clock), "a server holding the previous CA refuses the renewed certificate")
		require.True(t, verifies(t, first, renewed, *clock), "a renewed server refuses the previous certificate")
	})

	t.Run("a route Secret without its CA is reissued", func(t *testing.T) {
		old, err := selfSignedRouteSecret(storyCluster(t), routeDNSNames(storyCluster(t)), t0)
		require.NoError(t, err)
		r, nc, _ := setUp(t)
		require.NoError(t, r.writeSecret(context.Background(), nc, nil, old))
		require.NoError(t, r.ensureSelfSignedRouteSecret(context.Background(), nc))
		got := get(t, r, nc, routesSecretName(nc))
		require.NotEqual(t, old.Data[corev1.TLSCertKey], got.Data[corev1.TLSCertKey])
		require.Equal(t, get(t, r, nc, routesCASecretName(nc)).Data[corev1.TLSCertKey], got.Data[caKey])
	})

	t.Run("a route Secret the NatsCluster does not control is left alone", func(t *testing.T) {
		nc := storyCluster(t)
		own, err := selfSignedRouteSecret(nc, routeDNSNames(nc), t0.Add(-selfSignedValidity))
		require.NoError(t, err)
		r, nc, _ := setUp(t, own)
		require.NoError(t, r.ensureSelfSignedRouteSecret(context.Background(), nc))
		require.Equal(t, own.Data, get(t, r, nc, routesSecretName(nc)).Data)
	})
}

// routeServer starts a server whose route listener serves and verifies
// against the files of routes, as a NATS config's cluster tls block
// configures it, routing to urls.
func routeServer(t *testing.T, name string, routes *corev1.Secret, urls ...string) *server.Server {
	t.Helper()
	dir := t.TempDir()
	writeSecretFiles(t, dir, routes)
	tc, err := server.GenTLSConfig(&server.TLSConfigOpts{
		CertFile: filepath.Join(dir, corev1.TLSCertKey),
		KeyFile:  filepath.Join(dir, corev1.TLSPrivateKeyKey),
		CaFile:   filepath.Join(dir, caKey),
		Verify:   true,
	})
	require.NoError(t, err)
	tc.ClientAuth, tc.RootCAs = tls.RequireAndVerifyClientCert, tc.ClientCAs
	o := &server.Options{
		ServerName: name, Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		Cluster: server.ClusterOpts{Name: "demo", Host: "127.0.0.1", Port: -1, TLSConfig: tc, TLSTimeout: 2},
	}
	for _, u := range urls {
		o.Routes = append(o.Routes, server.RoutesFromStr(u)...)
	}
	s, err := server.NewServer(o)
	require.NoError(t, err)
	go s.Start()
	t.Cleanup(s.Shutdown)
	require.True(t, s.ReadyForConnections(10*time.Second))
	return s
}

// TestRouteCertRenewal pins that a server on a renewed route certificate
// routes with one still on the certificate it replaces, and that a
// certificate from another CA is refused.
func TestRouteCertRenewal(t *testing.T) {
	nc := limitedStoryCluster(t)
	now := time.Now()
	hosts := []string{"127.0.0.1"}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca, previous, err := issueRouteSecrets(nc, hosts, key, now.Add(-(selfSignedValidity - selfSignedRenewBefore)))
	require.NoError(t, err)
	caCert, _ := routeCA(ca)
	require.True(t, routeRenewalDue(previous, caCert, now))
	_, renewed, err := issueRouteSecrets(nc, hosts, key, now)
	require.NoError(t, err)

	old := routeServer(t, "demo-0", previous)
	url := fmt.Sprintf("nats://127.0.0.1:%d", old.ClusterAddr().Port)
	fresh := routeServer(t, "demo-1", renewed, url)
	require.Eventually(t, func() bool { return old.NumRoutes() > 0 && fresh.NumRoutes() > 0 }, 10*time.Second, 50*time.Millisecond)

	foreign, err := selfSignedRouteSecret(nc, hosts, now)
	require.NoError(t, err)
	other := routeServer(t, "demo-2", foreign, url)
	require.Never(t, func() bool { return other.NumRoutes() > 0 }, 3*time.Second, 100*time.Millisecond)
}

// selfSignedRouteSecret returns a kubernetes.io/tls Secret holding a new CA
// and a certificate it signs for hosts, as issueRouteSecrets does.
func selfSignedRouteSecret(nc *clusterv1beta1.NatsCluster, hosts []string, now time.Time) (*corev1.Secret, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	_, routes, err := issueRouteSecrets(nc, hosts, key, now)
	return routes, err
}
