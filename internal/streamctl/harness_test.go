package streamctl

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// testNATS is a NATS cluster of in-process JetStream servers.
type testNATS struct {
	servers []*server.Server
	// opts are each server's options, for restart.
	opts []*server.Options
	urls []string
	// ca and creds are set on a secure cluster: client TLS signed by ca,
	// and operator mode with one JetStream account, creds being its user's.
	ca    []byte
	creds []byte
}

// startNATS runs n JetStream servers, routed into one NATS cluster when n
// is above one, secured as testNATS describes when secure is set.
func startNATS(t *testing.T, n int, secure bool) *testNATS {
	t.Helper()
	tn := &testNATS{}
	var configure func(*server.Options)
	if secure {
		configure = tn.secure(t)
	}
	var routes []*url.URL
	ports := make([]int, n)
	if n > 1 {
		for i := range ports {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			ports[i] = l.Addr().(*net.TCPAddr).Port
			require.NoError(t, l.Close())
			routes = append(routes, &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", ports[i])})
		}
	}
	for i := range n {
		opts := &server.Options{
			ServerName:        fmt.Sprintf("n%d", i),
			Host:              "127.0.0.1",
			Port:              -1,
			NoLog:             true,
			NoSigs:            true,
			JetStream:         true,
			StoreDir:          t.TempDir(),
			JetStreamMaxStore: 1 << 40,
		}
		if n > 1 {
			opts.Cluster = server.ClusterOpts{Name: "test", Host: "127.0.0.1", Port: ports[i]}
			opts.Routes = routes
		}
		if configure != nil {
			configure(opts)
		}
		srv, err := server.NewServer(opts)
		require.NoError(t, err)
		go srv.Start()
		t.Cleanup(srv.Shutdown)
		require.True(t, srv.ReadyForConnections(10*time.Second), "n%d did not start", i)
		tn.servers = append(tn.servers, srv)
		tn.urls = append(tn.urls, srv.ClientURL())
	}
	if n > 1 {
		require.Eventually(t, func() bool {
			for _, srv := range tn.servers {
				if srv.JetStreamIsLeader() {
					return len(srv.JetStreamClusterPeers()) == n
				}
			}
			return false
		}, 30*time.Second, 100*time.Millisecond, "no meta leader seeing all %d servers", n)
	}
	return tn
}

// startSupercluster runs one NATS cluster of three JetStream servers per
// name, <name>-0 onwards, each gatewayed to every other, and returns them by
// name. Every port is settled before any server starts, since a clustered
// JetStream server refuses to start with no route.
func startSupercluster(t *testing.T, names ...string) map[string]*testNATS {
	t.Helper()
	const size = 3
	listen := func() int {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer func() { require.NoError(t, l.Close()) }()
		return l.Addr().(*net.TCPAddr).Port
	}
	routes, routePorts, gatewayPorts := map[string][]*url.URL{}, map[string][]int{}, map[string][]int{}
	var remotes []*server.RemoteGatewayOpts
	for _, name := range names {
		var gateways []*url.URL
		for range size {
			rp, gp := listen(), listen()
			routePorts[name], gatewayPorts[name] = append(routePorts[name], rp), append(gatewayPorts[name], gp)
			routes[name] = append(routes[name], &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", rp)})
			gateways = append(gateways, &url.URL{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", gp)})
		}
		remotes = append(remotes, &server.RemoteGatewayOpts{Name: name, URLs: gateways})
	}
	out := map[string]*testNATS{}
	var all []*server.Server
	for _, name := range names {
		tn := &testNATS{}
		for i := range size {
			opts := &server.Options{
				ServerName:        fmt.Sprintf("%s-%d", name, i),
				Host:              "127.0.0.1",
				Port:              -1,
				NoLog:             true,
				NoSigs:            true,
				JetStream:         true,
				StoreDir:          t.TempDir(),
				JetStreamMaxStore: 1 << 30,
				Cluster:           server.ClusterOpts{Name: name, Host: "127.0.0.1", Port: routePorts[name][i]},
				Routes:            routes[name],
				Gateway:           server.GatewayOpts{Name: name, Host: "127.0.0.1", Port: gatewayPorts[name][i], Gateways: remotes},
			}
			srv, err := server.NewServer(opts)
			require.NoError(t, err)
			go srv.Start()
			t.Cleanup(srv.Shutdown)
			tn.servers, tn.opts = append(tn.servers, srv), append(tn.opts, opts)
			all = append(all, srv)
		}
		out[name] = tn
	}
	for _, name := range names {
		for _, srv := range out[name].servers {
			require.True(t, srv.ReadyForConnections(15*time.Second), "%s did not start", srv.Name())
			out[name].urls = append(out[name].urls, srv.ClientURL())
		}
	}
	require.Eventually(t, func() bool {
		for _, srv := range all {
			if srv.JetStreamIsLeader() {
				return len(srv.JetStreamClusterPeers()) == len(all)
			}
		}
		return false
	}, time.Minute, 100*time.Millisecond, "the supercluster elected no meta leader seeing all %d servers", len(all))
	return out
}

// stop shuts down the server named name, and returns what starts it again
// on its own store.
func (tn *testNATS) stop(t *testing.T, name string) (restart func()) {
	t.Helper()
	i := slices.IndexFunc(tn.servers, func(s *server.Server) bool { return s.Name() == name })
	require.GreaterOrEqual(t, i, 0, "no server %s", name)
	tn.servers[i].Shutdown()
	tn.servers[i].WaitForShutdown()
	return func() {
		srv, err := server.NewServer(tn.opts[i])
		require.NoError(t, err)
		go srv.Start()
		t.Cleanup(srv.Shutdown)
		require.True(t, srv.ReadyForConnections(15*time.Second), "%s did not restart", name)
		tn.servers[i] = srv
	}
}

// secure mints a NATS operator, a system account and one JetStream account
// with a user, writes a server certificate for 127.0.0.1, and returns what
// configures a server to require both.
func (tn *testNATS) secure(t *testing.T) func(*server.Options) {
	t.Helper()
	op := newKey(t, nkeys.CreateOperator)
	opClaims := jwt.NewOperatorClaims(publicKey(t, op))
	sys, acc := newKey(t, nkeys.CreateAccount), newKey(t, nkeys.CreateAccount)
	opClaims.SystemAccount = publicKey(t, sys)
	resolver := &server.MemAccResolver{}
	sysJWT, err := jwt.NewAccountClaims(publicKey(t, sys)).Encode(op)
	require.NoError(t, err)
	require.NoError(t, resolver.Store(publicKey(t, sys), sysJWT))
	ac := jwt.NewAccountClaims(publicKey(t, acc))
	ac.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1}
	accJWT, err := ac.Encode(op)
	require.NoError(t, err)
	require.NoError(t, resolver.Store(publicKey(t, acc), accJWT))

	u := newKey(t, nkeys.CreateUser)
	uc := jwt.NewUserClaims(publicKey(t, u))
	token, err := uc.Encode(acc)
	require.NoError(t, err)
	seed, err := u.Seed()
	require.NoError(t, err)
	tn.creds, err = jwt.FormatUserConfig(token, seed)
	require.NoError(t, err)

	var certFile, keyFile string
	tn.ca, certFile, keyFile = writeTLS(t, t.TempDir())
	tlsConfig, err := server.GenTLSConfig(&server.TLSConfigOpts{CertFile: certFile, KeyFile: keyFile})
	require.NoError(t, err)
	return func(o *server.Options) {
		o.TrustedOperators = []*jwt.OperatorClaims{opClaims}
		o.SystemAccount = opClaims.SystemAccount
		o.AccountResolver = resolver
		o.TLS, o.TLSConfig = true, tlsConfig
	}
}

// connect is a client of the cluster in its JetStream account.
func (tn *testNATS) connect(t *testing.T) jetstream.JetStream {
	t.Helper()
	var opts []nats.Option
	if tn.creds != nil {
		pool := x509.NewCertPool()
		require.True(t, pool.AppendCertsFromPEM(tn.ca))
		opts = append(opts, nats.UserCredentialBytes(tn.creds), nats.Secure(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}))
	}
	nc, err := nats.Connect(tn.urls[0], opts...)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	j, err := jetstream.New(nc)
	require.NoError(t, err)
	return j
}

func newKey(t *testing.T, create func() (nkeys.KeyPair, error)) nkeys.KeyPair {
	t.Helper()
	kp, err := create()
	require.NoError(t, err)
	return kp
}

func publicKey(t *testing.T, kp nkeys.KeyPair) string {
	t.Helper()
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	return pub
}

// writeTLS writes a server certificate for 127.0.0.1 into dir and returns
// the PEM of the CA that signed it.
func writeTLS(t *testing.T, dir string) (caPEM []byte, certFile, keyFile string) {
	t.Helper()
	caKey, caDER := newCert(t, &x509.Certificate{
		Subject:               pkix.Name{CommonName: "test CA"},
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}, nil, nil)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)
	srvKey, srvDER := newCert(t, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "nats"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, caCert, caKey)
	keyDER, err := x509.MarshalECPrivateKey(srvKey)
	require.NoError(t, err)
	certFile, keyFile = filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), certFile, keyFile
}

// newCert signs tmpl with parentKey, or self-signs it when parent is nil.
func newCert(t *testing.T, tmpl, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	require.NoError(t, err)
	tmpl.SerialNumber = serial
	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().Add(time.Hour)
	if parent == nil {
		parent, parentKey = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	require.NoError(t, err)
	return key, der
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, natsv1beta1.AddToScheme(s))
	require.NoError(t, js.AddToScheme(s))
	return s
}

// indexerFunc adapts a function to client.FieldIndexer.
type indexerFunc func(obj client.Object, field string, extract client.IndexerFunc)

func (f indexerFunc) IndexField(_ context.Context, obj client.Object, field string, extract client.IndexerFunc) error {
	f(obj, field, extract)
	return nil
}
