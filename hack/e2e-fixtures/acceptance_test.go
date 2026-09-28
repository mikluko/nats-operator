package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// decodeDir strictly decodes every document of every file in dir into its
// typed object, and returns the Secrets by name.
func decodeDir(t *testing.T, dir string) map[string]*corev1.Secret {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme, natsv1beta1.AddToScheme, clusterv1beta1.AddToScheme,
		authv1beta1.AddToScheme, jetstreamv1beta1.AddToScheme,
	} {
		require.NoError(t, add(scheme))
	}
	decoder := serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDeserializer()

	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	secrets := map[string]*corev1.Secret{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		r := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(b)))
		for {
			doc, err := r.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			obj, _, err := decoder.Decode(doc, nil, nil)
			require.NoError(t, err, "%s", filepath.Base(f))
			if s, ok := obj.(*corev1.Secret); ok {
				secrets[s.Name] = s
			}
		}
	}
	return secrets
}

// pub returns the public key of the seed under key in secret, which must
// be of kind.
func pub(t *testing.T, secret *corev1.Secret, key string, kind nkeys.PrefixByte) string {
	t.Helper()
	require.NotNil(t, secret)
	kp, err := nkeys.FromSeed([]byte(secret.StringData[key]))
	require.NoError(t, err, "%s/%s", secret.Name, key)
	p, err := kp.PublicKey()
	require.NoError(t, err)
	require.True(t, nkeys.Prefix(p) == kind, "%s/%s is of kind %v", secret.Name, key, kind)
	return p
}

// requireCreds checks that the creds in secret are a user of account,
// signed by its signing key, whose seed matches the JWT.
func requireCreds(t *testing.T, secret *corev1.Secret, account, signing string) {
	t.Helper()
	require.NotNil(t, secret)
	creds := []byte(secret.StringData["user.creds"])
	token, err := jwt.ParseDecoratedJWT(creds)
	require.NoError(t, err)
	claims, err := jwt.DecodeUserClaims(token)
	require.NoError(t, err, "%s: the JWT verifies", secret.Name)
	require.Equal(t, signing, claims.Issuer, secret.Name)
	require.Equal(t, account, claims.IssuerAccount, secret.Name)
	kp, err := jwt.ParseDecoratedUserNKey(creds)
	require.NoError(t, err)
	p, err := kp.PublicKey()
	require.NoError(t, err)
	require.Equal(t, claims.Subject, p, "%s: the seed is the JWT's user", secret.Name)
}

// TestAcceptance pins story 9's fixtures: every document decodes strictly
// into its type, and the printed JWTs and the creds chain to the generated
// keys.
func TestAcceptance(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	require.NoError(t, acceptance(dir, &out))
	secrets := decodeDir(t, dir)

	operator := pub(t, secrets["acme-keys"], "identity", nkeys.PrefixByteOperator)
	operatorSigning := pub(t, secrets["acme-keys"], "signing-1", nkeys.PrefixByteOperator)
	system := pub(t, secrets["sys-keys"], "identity", nkeys.PrefixByteAccount)
	systemSigning := pub(t, secrets["sys-keys"], "signing-1", nkeys.PrefixByteAccount)
	monitoring := pub(t, secrets["monitoring-prod-keys"], "identity", nkeys.PrefixByteAccount)
	monitoringSigning := pub(t, secrets["monitoring-prod-keys"], "signing-1", nkeys.PrefixByteAccount)

	var patch struct {
		Patch struct {
			Spec struct {
				OperatorJWT      string `json:"operatorJWT"`
				SystemAccountJWT string `json:"systemAccountJWT"`
			} `json:"spec"`
		} `json:"patch"`
	}
	require.NoError(t, yaml.UnmarshalStrict(out.Bytes(), &patch))

	oc, err := jwt.DecodeOperatorClaims(patch.Patch.Spec.OperatorJWT)
	require.NoError(t, err)
	require.Equal(t, operator, oc.Subject)
	require.Equal(t, operator, oc.Issuer)
	require.Equal(t, system, oc.SystemAccount)
	require.Contains(t, oc.SigningKeys, operatorSigning)

	ac, err := jwt.DecodeAccountClaims(patch.Patch.Spec.SystemAccountJWT)
	require.NoError(t, err)
	require.Equal(t, system, ac.Subject)
	require.Equal(t, operatorSigning, ac.Issuer)
	require.Contains(t, ac.SigningKeys, systemSigning)

	requireCreds(t, secrets["dev-east-cluster-controller-creds"], system, systemSigning)
	requireCreds(t, secrets["prod-west-cluster-controller-creds"], system, systemSigning)
	requireCreds(t, secrets["monitoring-runtime-creds"], monitoring, monitoringSigning)
}
