package natsconn

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// testNATS is a nats-server under a NATS operator requiring TLS, with one
// account and a user in it.
type testNATS struct {
	srv     *server.Server
	url     string
	ca      []byte
	account string
	creds   []byte
}

func startNATS(t *testing.T) *testNATS {
	t.Helper()
	dir := t.TempDir()
	caPEM, certFile, keyFile := writeTLS(t, dir)

	op := newKey(t, nkeys.CreateOperator)
	opPub := publicKey(t, op)
	opJWT, err := jwt.NewOperatorClaims(opPub).Encode(op)
	require.NoError(t, err)

	sys := newKey(t, nkeys.CreateAccount)
	sysPub := publicKey(t, sys)
	sysJWT, err := jwt.NewAccountClaims(sysPub).Encode(op)
	require.NoError(t, err)

	acc := newKey(t, nkeys.CreateAccount)
	accPub := publicKey(t, acc)
	accJWT, err := jwt.NewAccountClaims(accPub).Encode(op)
	require.NoError(t, err)

	conf := fmt.Sprintf(`listen: 127.0.0.1:-1
operator: %s
system_account: %s
resolver: MEMORY
resolver_preload: { %s: %s, %s: %s }
tls { cert_file: %q, key_file: %q }
`, opJWT, sysPub, sysPub, sysJWT, accPub, accJWT, certFile, keyFile)
	path := filepath.Join(dir, "nats.conf")
	require.NoError(t, os.WriteFile(path, []byte(conf), 0o600))
	opts, err := server.ProcessConfigFile(path)
	require.NoError(t, err)
	opts.NoLog, opts.NoSigs = true, true
	srv, err := server.NewServer(opts)
	require.NoError(t, err)
	srv.Start()
	t.Cleanup(srv.Shutdown)
	require.True(t, srv.ReadyForConnections(5*time.Second))

	return &testNATS{
		srv:     srv,
		url:     fmt.Sprintf("tls://%s", srv.Addr().String()),
		ca:      caPEM,
		account: accPub,
		creds:   userCreds(t, acc, accPub),
	}
}

func (n *testNATS) endpoint() Endpoint {
	return Endpoint{Servers: []string{n.url}, CA: n.ca, Creds: n.creds}
}

// userCreds returns a creds file for a new user of the account signed by
// acc.
func userCreds(t *testing.T, acc nkeys.KeyPair, accPub string) []byte {
	t.Helper()
	u := newKey(t, nkeys.CreateUser)
	uc := jwt.NewUserClaims(publicKey(t, u))
	uc.IssuerAccount = accPub
	token, err := uc.Encode(acc)
	require.NoError(t, err)
	seed, err := u.Seed()
	require.NoError(t, err)
	creds, err := jwt.FormatUserConfig(token, seed)
	require.NoError(t, err)
	return creds
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
	return s
}

func fakeClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&natsv1beta1.NatsConnection{}).
		Build()
}

func secret(namespace, name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Data: data}
}

// connection returns a NatsConnection to servers reading CA and creds from
// the Secrets "ca" and "creds" under their default keys.
func connection(namespace, name string, servers ...string) *natsv1beta1.NatsConnection {
	return &natsv1beta1.NatsConnection{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Generation: 1},
		Spec: natsv1beta1.NatsConnectionSpec{
			Servers:     servers,
			TLS:         &natsv1beta1.ConnectionTLS{CA: &natsv1beta1.CA{SecretKeyRef: natsv1beta1.CASecretKeySelector{Name: "ca"}}},
			Credentials: &natsv1beta1.Credentials{SecretKeyRef: natsv1beta1.CredentialsSecretKeySelector{Name: "creds"}},
		},
	}
}

// secrets returns the "ca" and "creds" Secrets connection reads, holding
// n's CA and user.
func (n *testNATS) secrets(namespace string) []client.Object {
	return []client.Object{
		secret(namespace, "ca", map[string][]byte{DefaultCAKey: n.ca}),
		secret(namespace, "creds", map[string][]byte{DefaultCredentialsKey: n.creds}),
	}
}
