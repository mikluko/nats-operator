package fixtures

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/conf"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/yaml"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// fixtureScheme returns a scheme holding Kubernetes' kinds and every kind of
// the four groups.
func fixtureScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme, natsv1beta1.AddToScheme, clusterv1beta1.AddToScheme,
		authv1beta1.AddToScheme, jetstreamv1beta1.AddToScheme,
	} {
		require.NoError(t, add(scheme))
	}
	return scheme
}

// decoded holds a directory's Secrets, ConfigMaps and Services by name.
type decoded struct {
	secrets    map[string]*corev1.Secret
	configMaps map[string]*corev1.ConfigMap
	services   map[string]*corev1.Service
}

// decodeDir strictly decodes every document of every file in dir into its
// typed object.
func decodeDir(t *testing.T, dir string) decoded {
	t.Helper()
	decoder := serializer.NewCodecFactory(fixtureScheme(t), serializer.EnableStrict).UniversalDeserializer()

	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	d := decoded{secrets: map[string]*corev1.Secret{}, configMaps: map[string]*corev1.ConfigMap{}, services: map[string]*corev1.Service{}}
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
			switch o := obj.(type) {
			case *corev1.Secret:
				d.secrets[o.Name] = o
			case *corev1.ConfigMap:
				d.configMaps[o.Name] = o
			case *corev1.Service:
				d.services[o.Name] = o
			}
		}
	}
	return d
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

// requireCreds checks that the creds under key in secret are a user of
// account, signed by signing, whose seed matches the JWT.
func requireCreds(t *testing.T, secret *corev1.Secret, key, account, signing string) {
	t.Helper()
	require.NotNil(t, secret)
	creds := []byte(secret.StringData[key])
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

// trustPatch is a generated patch file's spec, for the fields any of them
// sets.
type trustPatch struct {
	Spec struct {
		OperatorJWT      string `json:"operatorJWT"`
		SystemAccountJWT string `json:"systemAccountJWT"`
		PublicKey        string `json:"publicKey"`
		JWT              string `json:"jwt"`
	} `json:"spec"`
}

// readPatch strictly decodes dir/name.
func readPatch(t *testing.T, dir, name string) trustPatch {
	t.Helper()
	var p trustPatch
	raw, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	require.NoError(t, yaml.UnmarshalStrict(raw, &p))
	return p
}

// chain names the key Secrets, without their -keys suffix, that a story's
// JWTs and creds must chain to.
type chain struct {
	operator, system string
	// operatorTrust is whether natsoperatortrust.json is written.
	operatorTrust bool
	// accountTrust is the account natsaccounttrust.json names, if any.
	accountTrust string
	// creds maps each creds Secret to its user's account.
	creds map[string]string
}

// check requires that every JWT and creds file in dir chains to the key
// Secrets c names.
func (c chain) check(t *testing.T, dir string) {
	t.Helper()
	d := decodeDir(t, dir)
	identity := func(name string, kind nkeys.PrefixByte) string {
		return pub(t, d.secrets[name+"-keys"], "identity", kind)
	}
	signing := func(name string, kind nkeys.PrefixByte) string {
		return pub(t, d.secrets[name+"-keys"], "signing-1", kind)
	}
	operator, operatorSigning := identity(c.operator, nkeys.PrefixByteOperator), signing(c.operator, nkeys.PrefixByteOperator)
	system, systemSigning := identity(c.system, nkeys.PrefixByteAccount), signing(c.system, nkeys.PrefixByteAccount)

	if c.operatorTrust {
		p := readPatch(t, dir, "natsoperatortrust.json")
		oc, err := jwt.DecodeOperatorClaims(p.Spec.OperatorJWT)
		require.NoError(t, err)
		require.Equal(t, operator, oc.Subject)
		require.Equal(t, operator, oc.Issuer)
		require.Equal(t, system, oc.SystemAccount)
		require.Contains(t, oc.SigningKeys, operatorSigning)

		ac, err := jwt.DecodeAccountClaims(p.Spec.SystemAccountJWT)
		require.NoError(t, err)
		require.Equal(t, system, ac.Subject)
		require.Equal(t, operatorSigning, ac.Issuer)
		require.Contains(t, ac.SigningKeys, systemSigning)
	}
	if c.accountTrust != "" {
		p := readPatch(t, dir, "natsaccounttrust.json")
		account := identity(c.accountTrust, nkeys.PrefixByteAccount)
		require.Equal(t, account, p.Spec.PublicKey)
		ac, err := jwt.DecodeAccountClaims(p.Spec.JWT)
		require.NoError(t, err)
		require.Equal(t, account, ac.Subject)
		require.Equal(t, operatorSigning, ac.Issuer)
		require.Contains(t, ac.SigningKeys, signing(c.accountTrust, nkeys.PrefixByteAccount))
	}
	for secret, account := range c.creds {
		requireCreds(t, d.secrets[secret], "user.creds",
			identity(account, nkeys.PrefixByteAccount), signing(account, nkeys.PrefixByteAccount))
	}
}

// checkUnmanaged requires that story 3's NATS operator, accounts and creds chain
// through the server config, and that its server certificate chains to its
// CA and matches its key.
func checkUnmanaged(t *testing.T, dir string) {
	t.Helper()
	t.Setenv("POD_NAME", "nats-0")
	d := decodeDir(t, dir)
	cm := d.configMaps["nats-config"]
	require.NotNil(t, cm)
	cfg, err := conf.Parse(cm.Data["nats.conf"])
	require.NoError(t, err)

	operator, ok := cfg["operator"].(string)
	require.True(t, ok, "operator is a string")
	oc, err := jwt.DecodeOperatorClaims(operator)
	require.NoError(t, err)
	require.Equal(t, oc.Subject, oc.Issuer)
	system, ok := cfg["system_account"].(string)
	require.True(t, ok, "system_account is a string")
	require.Equal(t, system, oc.SystemAccount)
	preload, ok := cfg["resolver_preload"].(map[string]any)
	require.True(t, ok, "resolver_preload is a map")
	require.Len(t, preload, 2)
	var account string
	for pub, value := range preload {
		token, ok := value.(string)
		require.True(t, ok, "resolver_preload %s is a string", pub)
		ac, err := jwt.DecodeAccountClaims(token)
		require.NoError(t, err)
		require.Equal(t, pub, ac.Subject)
		require.Equal(t, oc.Subject, ac.Issuer)
		if pub != system {
			account = pub
		}
	}
	require.NotEmpty(t, account)
	for _, secret := range []string{"nats-client", "payments-secrets"} {
		requireCreds(t, d.secrets[secret], "nats.creds", account, account)
	}

	tlsSecret := d.secrets["nats-tls"]
	require.NotNil(t, tlsSecret)
	pair, err := tls.X509KeyPair([]byte(tlsSecret.StringData["tls.crt"]), []byte(tlsSecret.StringData["tls.key"]))
	require.NoError(t, err, "the server key matches its certificate")
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM([]byte(d.secrets["nats-client"].StringData["ca.crt"])))
	_, err = cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: "nats.messaging.svc"})
	require.NoError(t, err)
	require.Equal(t, d.secrets["nats-client"].StringData["ca.crt"], d.secrets["messaging-nats-ca"].StringData["ca.crt"])
}

// checkJoinSupercluster requires that story 13's existing NATS cluster
// preloads its system account and accounts under a MEMORY resolver, that its
// gateway admits west alone, in the clear and with no authorization, at the
// address the story's NatsCluster advertises, and that the trust patches and
// creds chain to what it preloads.
func checkJoinSupercluster(t *testing.T, dir string) {
	t.Helper()
	t.Setenv("POD_NAME", "nats-0")
	d := decodeDir(t, dir)
	cm := d.configMaps["nats-config"]
	require.NotNil(t, cm)
	cfg, err := conf.Parse(cm.Data["nats.conf"])
	require.NoError(t, err)

	operator, ok := cfg["operator"].(string)
	require.True(t, ok, "operator is a string")
	oc, err := jwt.DecodeOperatorClaims(operator)
	require.NoError(t, err)
	require.Equal(t, oc.Subject, oc.Issuer)
	system, ok := cfg["system_account"].(string)
	require.True(t, ok, "system_account is a string")
	require.Equal(t, oc.SystemAccount, system)
	require.Equal(t, "MEMORY", cfg["resolver"])

	preload, ok := cfg["resolver_preload"].(map[string]any)
	require.True(t, ok, "resolver_preload is a map")
	accounts := map[string]*jwt.AccountClaims{}
	for pub, value := range preload {
		token, ok := value.(string)
		require.True(t, ok, "resolver_preload %s is a string", pub)
		ac, err := jwt.DecodeAccountClaims(token)
		require.NoError(t, err)
		require.Equal(t, pub, ac.Subject)
		require.True(t, oc.DidSign(ac), "%s is signed by the NATS operator", ac.Name)
		accounts[ac.Name] = ac
	}
	require.Equal(t, []string{"SYS", "orders", "payments"}, slices.Sorted(maps.Keys(accounts)))
	require.Equal(t, system, accounts["SYS"].Subject)

	trust := readPatch(t, dir, "natsoperatortrust.json")
	require.Equal(t, operator, trust.Spec.OperatorJWT)
	require.Equal(t, preload[system], trust.Spec.SystemAccountJWT)
	for _, name := range []string{"orders", "payments"} {
		p := readPatch(t, dir, "natsaccounttrust-"+name+".json")
		ac := accounts[name]
		require.Equal(t, ac.Subject, p.Spec.PublicKey, name)
		require.Equal(t, preload[ac.Subject], p.Spec.JWT, name)
		require.NotZero(t, ac.Expires, "%s expires", name)
		require.True(t, ac.Limits.IsJSEnabled(), "%s has JetStream", name)
	}

	onlyKey := func(ac *jwt.AccountClaims) string {
		t.Helper()
		keys := ac.SigningKeys.Keys()
		require.Len(t, keys, 1, ac.Name)
		return keys[0]
	}
	requireCreds(t, d.secrets["west-cluster-controller-creds"], "user.creds", system, onlyKey(accounts["SYS"]))
	requireCreds(t, d.secrets["orders-creds"], "user.creds", accounts["orders"].Subject, onlyKey(accounts["orders"]))

	gateway, ok := cfg["gateway"].(map[string]any)
	require.True(t, ok, "gateway is a map")
	require.Equal(t, "central", gateway["name"])
	require.Equal(t, true, gateway["reject_unknown"])
	require.NotContains(t, gateway, "tls")
	require.NotContains(t, gateway, "authorization")
	remotes, ok := gateway["gateways"].([]any)
	require.True(t, ok, "gateway.gateways is a list")
	require.Len(t, remotes, 1)
	remote, ok := remotes[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "west", remote["name"])

	west := storyNatsCluster(t, "13-join-supercluster/01-west.yaml")
	require.Equal(t, []any{"nats://" + west.Spec.Gateway.Advertise}, remote["urls"])
	var central string
	for _, r := range west.Spec.Gateway.Remotes {
		if r.Name == "central" {
			central = r.URL
		}
	}
	host, _, _ := strings.Cut(strings.TrimPrefix(central, "nats://"), ":")
	require.Equal(t, host, d.services["nats-gateway"].Annotations["external-dns.alpha.kubernetes.io/hostname"])
}

// storyNatsCluster strictly decodes the NatsCluster in the story file at
// path under the stories directory.
func storyNatsCluster(t *testing.T, path string) *clusterv1beta1.NatsCluster {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../../docs/content/docs/stories", path))
	require.NoError(t, err)
	var nc clusterv1beta1.NatsCluster
	require.NoError(t, yaml.UnmarshalStrict(raw, &nc))
	return &nc
}

// TestChains pins every story's fixtures: every document decodes strictly
// into its type, and every JWT and creds file chains to the keys generated
// with it.
func TestChains(t *testing.T) {
	stories := map[string]func(*testing.T, string){
		"03-unmanaged": checkUnmanaged,
		"06-supercluster": chain{operator: "acme-operator", system: "sys", operatorTrust: true, creds: map[string]string{
			"west-cluster-controller-creds": "sys",
		}}.check,
		"09-acceptance": chain{operator: "acme", system: "sys", operatorTrust: true, creds: map[string]string{
			"dev-east-cluster-controller-creds":  "sys",
			"prod-west-cluster-controller-creds": "sys",
			"monitoring-runtime-creds":           "monitoring-prod",
		}}.check,
		"10-leafnodes": chain{operator: "acme-operator", system: "sys", operatorTrust: true, accountTrust: "telemetry", creds: map[string]string{
			"edge-site-1-leaf-creds":        "telemetry",
			"edge-site-2-system-leaf-creds": "sys",
			"edge-site-2-leaf-creds":        "telemetry",
		}}.check,
		"11-evacuation": chain{operator: "acme", system: "sys", creds: map[string]string{
			"orders-creds":   "orders",
			"payments-creds": "payments",
		}}.check,
		"13-join-supercluster": checkJoinSupercluster,
		"14-nsc-takeover":      checkTakeover,
	}
	require.ElementsMatch(t, slices.Collect(maps.Keys(generators)), slices.Collect(maps.Keys(stories)))
	for story, check := range stories {
		t.Run(story, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, generators[story](dir))
			check(t, dir)
		})
	}
}

// TestGenerate pins every story's generated files: each is readable by its
// owner alone, and each patch file, named for the kind it targets and
// optionally a qualifier after a hyphen, decodes strictly into that kind and
// patches its spec alone, or with the qualifier status its status alone.
func TestGenerate(t *testing.T) {
	scheme := fixtureScheme(t)
	kinds := map[string]schema.GroupVersionKind{}
	for gvk := range scheme.AllKnownTypes() {
		if gvk.Group == natsv1beta1.GroupVersion.Group || gvk.Group == authv1beta1.GroupVersion.Group {
			kinds[strings.ToLower(gvk.Kind)] = gvk
		}
	}
	root := t.TempDir()
	require.NoError(t, Generate(root))
	stories, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, stories, len(generators))
	for _, story := range stories {
		t.Run(story.Name(), func(t *testing.T) {
			dir := filepath.Join(root, story.Name(), "e2e")
			files, err := os.ReadDir(dir)
			require.NoError(t, err)
			for _, f := range files {
				info, err := f.Info()
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0o600), info.Mode(), f.Name())
			}
			patches, err := filepath.Glob(filepath.Join(dir, "*.json"))
			require.NoError(t, err)
			for _, path := range patches {
				kind, qualifier, _ := strings.Cut(strings.TrimSuffix(filepath.Base(path), ".json"), "-")
				gvk, ok := kinds[kind]
				require.True(t, ok, "%s names no kind of %s or %s", path, natsv1beta1.GroupVersion.Group, authv1beta1.GroupVersion.Group)
				patched := "spec"
				if qualifier == "status" {
					patched = "status"
				}
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				var patch map[string]any
				require.NoError(t, json.Unmarshal(raw, &patch))
				require.Equal(t, []string{patched}, slices.Sorted(maps.Keys(patch)), path)
				obj, err := scheme.New(gvk)
				require.NoError(t, err)
				require.NoError(t, yaml.UnmarshalStrict(raw, obj), path)
			}
		})
	}
}
