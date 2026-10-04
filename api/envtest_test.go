package api_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	"github.com/mikluko/nats-operator/internal/e2e/placeholders"
)

// TestEnvtest pins, against the generated CRDs in a real API server, that
// every story manifest is accepted, every CEL rule refuses what it names, and
// the schema defaults.
func TestEnvtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../config/crd"},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	c, err := client.New(cfg, client.Options{})
	require.NoError(t, err)

	t.Run("CRDs", func(t *testing.T) { testCRDsInstalled(t, env) })

	docs := storyManifests(t)
	namespaces := map[string]bool{"rules": true}
	for _, doc := range docs {
		namespaces[doc.obj.GetNamespace()] = true
	}
	for ns := range namespaces {
		require.NoError(t, c.Create(t.Context(), parse(t, fmt.Sprintf("apiVersion: v1\nkind: Namespace\nmetadata: {name: %s}\n", ns))))
	}

	t.Run("StoryManifests", func(t *testing.T) {
		for _, doc := range docs {
			t.Run(doc.name, func(t *testing.T) {
				require.NoError(t, c.Create(t.Context(), doc.obj.DeepCopy(), client.DryRunAll, client.FieldValidation("Strict")))
			})
		}
	})
	t.Run("StoryStatuses", func(t *testing.T) { testStoryStatuses(t, c) })
	t.Run("CreateRules", func(t *testing.T) { testCreateRules(t, c) })
	t.Run("TransitionRules", func(t *testing.T) { testTransitionRules(t, c) })
	t.Run("Defaults", func(t *testing.T) { testDefaults(t, c) })
}

// testCRDsInstalled pins one installed CRD per registered Nats kind.
func testCRDsInstalled(t *testing.T, env *envtest.Environment) {
	installed := map[string]bool{}
	for _, crd := range env.CRDs {
		installed[crd.Spec.Group+"/"+crd.Spec.Names.Kind] = true
	}
	var want int
	for gvk := range apiScheme(t).AllKnownTypes() {
		if !strings.HasPrefix(gvk.Kind, "Nats") || strings.HasSuffix(gvk.Kind, "List") {
			continue
		}
		want++
		require.True(t, installed[gvk.Group+"/"+gvk.Kind], "no CRD for %s", gvk)
	}
	require.Len(t, installed, want)
}

var apiVersions = map[string]string{
	"NatsOperatorTrust":  "nats-operator.io/v1beta1",
	"NatsAccountTrust":   "nats-operator.io/v1beta1",
	"NatsCluster":        "cluster.nats-operator.io/v1beta1",
	"NatsOperator":       "auth.nats-operator.io/v1beta1",
	"NatsSystemAccount":  "auth.nats-operator.io/v1beta1",
	"NatsAccount":        "auth.nats-operator.io/v1beta1",
	"NatsUser":           "auth.nats-operator.io/v1beta1",
	"NatsConnection":     "nats-operator.io/v1beta1",
	"NatsStream":         "jetstream.nats-operator.io/v1beta1",
	"NatsConsumer":       "jetstream.nats-operator.io/v1beta1",
	"NatsKeyValue":       "jetstream.nats-operator.io/v1beta1",
	"NatsObjectStore":    "jetstream.nats-operator.io/v1beta1",
	"NatsBalancer":       "jetstream.nats-operator.io/v1beta1",
	"NatsSystemBalancer": "jetstream.nats-operator.io/v1beta1",

	"NatsClusterEvacuation": "jetstream.nats-operator.io/v1beta1",
}

// statusSpecs are admissible specs, one per kind a story status file names.
var statusSpecs = map[string]string{
	"NatsCluster":           "{version: 2.15.0, replicas: 1}",
	"NatsOperator":          "{systemAccountRef: {name: sys}}",
	"NatsAccount":           "{operatorRef: {name: o}}",
	"NatsUser":              "{accountRef: {kind: NatsAccount, name: a}}",
	"NatsConnection":        "{servers: [nats://c:4222]}",
	"NatsStream":            "{connectionRef: {name: c}}",
	"NatsBalancer":          "{connectionRef: {name: c}}",
	"NatsSystemBalancer":    "{connectionRef: {name: c}}",
	"NatsClusterEvacuation": "{connectionRef: {name: c}, from: {cluster: a}, to: {serverTags: [b]}}",
}

// testStoryStatuses pins every story status file, placeholders' example
// values included, to the status subresource's schema.
func testStoryStatuses(t *testing.T, c client.Client) {
	s := apiScheme(t)
	_, statuses := storyFiles(t)
	for i, path := range statuses {
		rel, err := filepath.Rel(storiesDir, path)
		require.NoError(t, err)
		t.Run(rel, func(t *testing.T) {
			kind := statusKind(t, s, path).Kind
			spec, ok := statusSpecs[kind]
			require.True(t, ok, "no spec for %s", kind)
			obj := parse(t, manifest(kind, fmt.Sprintf("status-%d", i), spec))
			require.NoError(t, c.Create(t.Context(), obj))
			t.Cleanup(func() { require.NoError(t, c.Delete(context.Background(), obj)) })

			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			raw, err = placeholders.Strip(raw)
			require.NoError(t, err)
			var doc struct {
				Status map[string]any `json:"status"`
			}
			require.NoError(t, yaml.UnmarshalStrict(raw, &doc))
			conditions, _ := doc.Status["conditions"].([]any)
			for _, cond := range conditions {
				m := cond.(map[string]any)
				if _, ok := m["lastTransitionTime"]; !ok {
					m["lastTransitionTime"] = "2026-09-26T11:20:44Z"
				}
				if _, ok := m["message"]; !ok {
					m["message"] = ""
				}
			}
			obj.Object["status"] = doc.Status
			require.NoError(t, c.Status().Update(t.Context(), obj, client.FieldValidation("Strict")))
		})
	}
}

// manifest renders an object of kind in the rules namespace, its spec given
// as flow-style YAML.
func manifest(kind, name, spec string) string {
	return fmt.Sprintf("apiVersion: %s\nkind: %s\nmetadata: {name: %s, namespace: rules}\nspec: %s\n", apiVersions[kind], kind, name, spec)
}

func parse(t *testing.T, doc string) *unstructured.Unstructured {
	t.Helper()
	var m map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(doc), &m))
	return &unstructured.Unstructured{Object: m}
}

const (
	seed    = "{secretKeyRef: {name: s, key: k}}"
	signing = "[{name: a, secretKeyRef: {name: s, key: k}}]"
)

// testCreateRules pins each CEL rule that refuses a spec at create.
func testCreateRules(t *testing.T, c client.Client) {
	cluster := func(extra string) string {
		return manifest("NatsCluster", "c", "{version: 2.15.0, replicas: 1"+extra+"}")
	}
	user := func(kind, extra string) string {
		return manifest("NatsUser", "u", "{accountRef: {kind: "+kind+", name: a}"+extra+"}")
	}
	account := func(kind, extra string) string {
		return manifest(kind, "a", "{operatorRef: {name: o}"+extra+"}")
	}
	tests := []struct {
		name string
		obj  string
		want string
	}{
		{"operator trust with both forms", manifest("NatsOperatorTrust", "t", "{operatorRef: {name: o}, operatorJWT: x, systemAccountJWT: z}"), "set exactly one of operatorRef"},
		{"operator trust with neither form", manifest("NatsOperatorTrust", "t", "{}"), "set exactly one of operatorRef"},
		{"operator trust with half the literal form", manifest("NatsOperatorTrust", "t", "{operatorJWT: x}"), "operatorJWT and systemAccountJWT are set together"},
		{"account trust with both forms", manifest("NatsAccountTrust", "t", "{accountRef: {name: a}, publicKey: A}"), "set exactly one of accountRef and publicKey"},
		{"account trust with neither form", manifest("NatsAccountTrust", "t", "{}"), "set exactly one of accountRef and publicKey"},
		{"account trust with jwt beside accountRef", manifest("NatsAccountTrust", "t", "{accountRef: {name: a}, jwt: x}"), "jwt is set only beside publicKey"},

		{"version below minimum", manifest("NatsCluster", "c", "{version: 2.14.9, replicas: 1}"), "2.15.0 or later"},
		{"version not semver", manifest("NatsCluster", "c", "{version: latest, replicas: 1}"), "2.15.0 or later"},
		{"leaf JetStream without domain", cluster(", jetstream: {}, leafRemotes: [{connectionRef: {name: hub}}]"), "must set jetstream.domain"},
		{"leaf remote binding two accounts", cluster(", leafRemotes: [{connectionRef: {name: hub}, localAccountTrustRef: {name: t}, localSystemAccount: true}]"), "localAccountTrustRef and localSystemAccount are mutually exclusive"},
		{"leaf remote naming a local account", cluster(", leafRemotes: [{connectionRef: {name: hub}, localAccount: A}]"), `unknown field "spec.leafRemotes[0].localAccount"`},
		{"route TLS with two certificates", cluster(", routes: {tls: {secretRef: {name: s}, certManager: {issuerRef: {name: i}}}}"), "secretRef and certManager are mutually exclusive"},
		{"disabled route TLS with a certificate", cluster(", routes: {tls: {enabled: false, secretRef: {name: s}}}"), "only while route TLS is enabled"},
		{"gateway TLS without a certificate", cluster(", gateway: {discovery: Explicit, remotes: [{name: a, url: u}], tls: {}}"), "set exactly one of secretRef and certManager"},
		{"image digest that is not sha256", cluster(", image: {digest: 'latest'}"), "spec.image.digest"},
		{"exporter image digest that is not sha256", cluster(", exporter: {image: {digest: 'sha256:abc'}}"), "spec.exporter.image.digest"},
		{"leafnode TLS with two certificates", cluster(", leafnodes: {tls: {secretRef: {name: s}, certManager: {issuerRef: {name: i}}}}"), "set exactly one of secretRef and certManager"},
		{"gateway source ranges on a ClusterIP Service", cluster(", gateway: {discovery: Explicit, remotes: [{name: a, url: u}], service: {loadBalancerSourceRanges: [10.0.0.0/8]}}"), "loadBalancerSourceRanges is set only when type is LoadBalancer"},
		{"gateway source ranges on a NodePort Service", cluster(", gateway: {discovery: Explicit, remotes: [{name: a, url: u}], service: {type: NodePort, loadBalancerSourceRanges: [10.0.0.0/8]}}"), "loadBalancerSourceRanges is set only when type is LoadBalancer"},
		{"gateway class on a ClusterIP Service", cluster(", gateway: {discovery: Explicit, remotes: [{name: a, url: u}], service: {type: ClusterIP, loadBalancerClass: example.com/lb}}"), "loadBalancerClass is set only when type is LoadBalancer"},
		{"leafnode source ranges on a ClusterIP Service", cluster(", auth: {trustRef: {name: t}}, leafnodes: {service: {loadBalancerSourceRanges: [10.0.0.0/8]}}"), "loadBalancerSourceRanges is set only when type is LoadBalancer"},
		{"leafnode class on a NodePort Service", cluster(", auth: {trustRef: {name: t}}, leafnodes: {service: {type: NodePort, loadBalancerClass: example.com/lb}}"), "loadBalancerClass is set only when type is LoadBalancer"},
		{"source range that is not a CIDR", cluster(", gateway: {discovery: Explicit, remotes: [{name: a, url: u}], service: {type: LoadBalancer, loadBalancerSourceRanges: [10.0.0.1]}}"), "a source range must be a CIDR"},
		{"empty class", cluster(", gateway: {discovery: Explicit, remotes: [{name: a, url: u}], service: {type: LoadBalancer, loadBalancerClass: ''}}"), "spec.gateway.service.loadBalancerClass"},

		{"operator jwt beside identity key", manifest("NatsOperator", "o", "{systemAccountRef: {name: sys}, jwt: x, keys: {identity: "+seed+", signing: "+signing+"}}"), "jwt and keys.identity are mutually exclusive"},
		{"operator jwt without signing key", manifest("NatsOperator", "o", "{systemAccountRef: {name: sys}, jwt: x}"), "jwt requires at least one signing key"},
		{"account publicKey beside identity key", account("NatsAccount", ", publicKey: A, keys: {identity: "+seed+", signing: "+signing+"}"), "publicKey and keys.identity are mutually exclusive"},
		{"account publicKey without signing key", account("NatsAccount", ", publicKey: A"), "publicKey requires at least one signing key"},
		{"system account publicKey beside identity key", account("NatsSystemAccount", ", publicKey: A, keys: {identity: "+seed+", signing: "+signing+"}"), "publicKey and keys.identity are mutually exclusive"},
		{"system account publicKey without signing key", account("NatsSystemAccount", ", publicKey: A"), "publicKey requires at least one signing key"},
		{"export preset beside a name", account("NatsAccount", ", exports: [{preset: jetstream-stepdown, name: x}]"), "either preset alone"},
		{"export without subject", account("NatsAccount", ", exports: [{name: x, type: Stream}]"), "either preset alone"},
		{"export without name", account("NatsAccount", ", exports: [{type: Stream, subject: s}]"), "either preset alone"},
		{"export without type", account("NatsAccount", ", exports: [{name: x, subject: s}]"), "either preset alone"},
		{"response type on a stream export", account("NatsAccount", ", exports: [{name: x, type: Stream, subject: s, responseType: Singleton}]"), "responseType is set only on a Service export"},
		{"importers on a public export", account("NatsAccount", ", exports: [{name: x, type: Service, subject: s, importers: [{kind: NatsAccount, name: a}]}]"), "importers are listed only on a Private export"},

		{"user preset beside permissions", user("NatsAccount", ", preset: leafnode, permissions: {publish: {allow: [a]}}"), "preset and permissions are mutually exclusive"},
		{"user preset beside connectionTypes", user("NatsAccount", ", preset: leafnode, connectionTypes: [LEAFNODE]"), "preset and connectionTypes are mutually exclusive"},
		{"user publicKey beside credentials", user("NatsAccount", ", publicKey: U, credentials: {secretKeyRef: {name: s}}"), "publicKey and credentials are mutually exclusive"},
		{"controller preset on an ordinary account", user("NatsAccount", ", preset: cluster-controller"), "a controller preset is for a NatsSystemAccount user"},
		{"readonly preset on the system account", user("NatsSystemAccount", ", preset: readonly"), "the readonly preset is for a NatsAccount user"},
		{"unknown connection type", user("NatsAccount", ", connectionTypes: [CARRIER_PIGEON]"), "Unsupported value"},

		{"consumer with stream and streamRef", manifest("NatsConsumer", "k", "{connectionRef: {name: c}, stream: S, streamRef: {name: s}}"), "set exactly one of stream and streamRef"},
		{"consumer with neither stream nor streamRef", manifest("NatsConsumer", "k", "{connectionRef: {name: c}}"), "set exactly one of stream and streamRef"},
		{"consumer by stream name without connection", manifest("NatsConsumer", "k", "{stream: S}"), "connectionRef is required unless streamRef is set"},

		{"balancer with a zero interval", manifest("NatsBalancer", "b", "{connectionRef: {name: c}, interval: 0s}"), "interval must be a positive duration"},
		{"balancer with a negative interval", manifest("NatsBalancer", "b", "{connectionRef: {name: c}, interval: -1m}"), "interval must be a positive duration"},
		{"system balancer with a zero interval", manifest("NatsSystemBalancer", "b", "{connectionRef: {name: c}, interval: 0s}"), "interval must be a positive duration"},
		{"system balancer with a negative interval", manifest("NatsSystemBalancer", "b", "{connectionRef: {name: c}, interval: -1m}"), "interval must be a positive duration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.Create(t.Context(), parse(t, tt.obj), client.DryRunAll, client.FieldValidation("Strict"))
			require.ErrorContains(t, err, tt.want)
		})
	}
}

// testTransitionRules pins each transition rule: an empty want means the
// update is accepted.
func testTransitionRules(t *testing.T, c client.Client) {
	stream := func(name, spec string) string {
		return manifest("NatsStream", name, "{connectionRef: {name: c}"+spec+"}")
	}
	consumer := func(spec string) string {
		return manifest("NatsConsumer", "k", "{connectionRef: {name: c}, stream: S"+spec+"}")
	}
	cluster := func(version string) string {
		return manifest("NatsCluster", "c", "{version: "+version+", replicas: 1}")
	}
	user := func(ref, spec string) string {
		return manifest("NatsUser", "u", "{accountRef: "+ref+spec+"}")
	}
	evacuation := func(conn, from, tag string) string {
		return manifest("NatsClusterEvacuation", "e", "{connectionRef: {name: "+conn+"}, from: {cluster: "+from+"}, to: {serverTags: ["+tag+"]}}")
	}
	gateway := func(service string) string {
		return manifest("NatsCluster", "c", "{version: 2.15.0, replicas: 1, gateway: {discovery: Explicit, remotes: [{name: a, url: u}], service: "+service+"}}")
	}
	leafnodes := func(service string) string {
		return manifest("NatsCluster", "c", "{version: 2.15.0, replicas: 1, auth: {trustRef: {name: t}}, leafnodes: {service: "+service+"}}")
	}
	tests := []struct {
		name          string
		before, after string
		want          string
	}{
		{"patch upgrade", cluster("2.15.0"), cluster("2.15.3"), ""},
		{"patch downgrade", cluster("2.15.3"), cluster("2.15.1"), ""},
		{"one minor up", cluster("2.15.0"), cluster("2.16.0"), ""},
		{"two minors up", cluster("2.15.0"), cluster("2.17.0"), "at most one minor at a time"},
		{"one minor down", cluster("2.16.0"), cluster("2.15.9"), ""},
		{"two minors down", cluster("2.17.0"), cluster("2.15.9"), "at most one minor at a time"},
		{"major up", cluster("2.15.0"), cluster("3.0.0"), "at most one minor at a time"},

		{"gateway class changed", gateway("{type: LoadBalancer, loadBalancerClass: a.example/lb}"), gateway("{type: LoadBalancer, loadBalancerClass: b.example/lb}"), "loadBalancerClass cannot change while type stays LoadBalancer"},
		{"gateway class removed", gateway("{type: LoadBalancer, loadBalancerClass: a.example/lb}"), gateway("{type: LoadBalancer}"), "loadBalancerClass cannot change while type stays LoadBalancer"},
		{"gateway class set on a LoadBalancer", gateway("{type: LoadBalancer}"), gateway("{type: LoadBalancer, loadBalancerClass: a.example/lb}"), "loadBalancerClass cannot change while type stays LoadBalancer"},
		{"gateway class set with the type", gateway("{type: ClusterIP}"), gateway("{type: LoadBalancer, loadBalancerClass: a.example/lb}"), ""},
		{"gateway class removed with the type", gateway("{type: LoadBalancer, loadBalancerClass: a.example/lb}"), gateway("{type: NodePort}"), ""},
		{"gateway class unchanged, source ranges changed", gateway("{type: LoadBalancer, loadBalancerClass: a.example/lb, loadBalancerSourceRanges: [10.0.0.0/8]}"), gateway("{type: LoadBalancer, loadBalancerClass: a.example/lb, loadBalancerSourceRanges: ['10.20.0.0/16', '2001:db8:20::/56']}"), ""},
		{"leafnode class changed", leafnodes("{type: LoadBalancer, loadBalancerClass: a.example/lb}"), leafnodes("{type: LoadBalancer, loadBalancerClass: b.example/lb}"), "loadBalancerClass cannot change while type stays LoadBalancer"},
		{"leafnode class removed", leafnodes("{type: LoadBalancer, loadBalancerClass: a.example/lb}"), leafnodes("{type: LoadBalancer}"), "loadBalancerClass cannot change while type stays LoadBalancer"},

		{"user moved to another account", user("{kind: NatsAccount, name: a}", ""), user("{kind: NatsAccount, name: b}", ""), "accountRef is immutable"},
		{"user moved to another namespace", user("{kind: NatsAccount, name: a}", ""), user("{kind: NatsAccount, name: a, namespace: other}", ""), "accountRef is immutable"},
		{"user moved to the system account", user("{kind: NatsAccount, name: a}", ""), user("{kind: NatsSystemAccount, name: a}", ""), "accountRef is immutable"},
		{"user permissions changed", user("{kind: NatsAccount, name: a}", ", permissions: {publish: {allow: [a]}}"), user("{kind: NatsAccount, name: a}", ", permissions: {publish: {allow: [b]}}"), ""},

		{"stream renamed", stream("orders", ", name: ORDERS"), stream("orders", ", name: OTHER"), "the stream name is immutable"},
		{"stream name spelled as metadata.name", stream("orders", ""), stream("orders", ", name: orders"), ""},
		{"stream name set away from metadata.name", stream("orders", ""), stream("orders", ", name: OTHER"), "the stream name is immutable"},
		{"storage changed", stream("s", ", storage: File"), stream("s", ", storage: Memory"), "storage is immutable"},
		{"storage late-initialized", stream("s", ""), stream("s", ", storage: Memory"), ""},
		{"retention to WorkQueue", stream("s", ", retention: Limits"), stream("s", ", retention: WorkQueue"), "to or from WorkQueue"},
		{"retention from WorkQueue", stream("s", ", retention: WorkQueue"), stream("s", ", retention: Interest"), "to or from WorkQueue"},
		{"retention Limits to Interest", stream("s", ", retention: Limits"), stream("s", ", retention: Interest"), ""},
		{"mirror changed", stream("s", ", mirror: {name: A}"), stream("s", ", mirror: {name: B}"), "mirror cannot change"},
		{"mirror removed", stream("s", ", mirror: {name: A}"), stream("s", ""), ""},
		{"sealed", stream("s", ", sealed: false"), stream("s", ", sealed: true"), ""},
		{"unsealed", stream("s", ", sealed: true"), stream("s", ", sealed: false"), "sealed cannot be unset"},
		{"denyDelete unset", stream("s", ", denyDelete: true"), stream("s", ", denyDelete: false"), "denyDelete cannot be unset"},
		{"denyPurge unset", stream("s", ", denyPurge: true"), stream("s", ", denyPurge: false"), "denyPurge cannot be unset"},
		{"allowMsgTTL unset", stream("s", ", allowMsgTTL: true"), stream("s", ", allowMsgTTL: false"), "allowMsgTTL cannot be unset"},
		{"allowMsgSchedules unset", stream("s", ", allowMsgSchedules: true"), stream("s", ", allowMsgSchedules: false"), "allowMsgSchedules cannot be unset"},
		{"allowMsgCounter set", stream("s", ", allowMsgCounter: false"), stream("s", ", allowMsgCounter: true"), "allowMsgCounter is immutable"},
		{"persistMode changed", stream("s", ", persistMode: Default"), stream("s", ", persistMode: Async"), "persistMode is immutable"},

		{"consumer renamed", consumer(", name: a"), consumer(", name: b"), "the consumer name is immutable"},
		{"deliverPolicy changed", consumer(", deliverPolicy: All"), consumer(", deliverPolicy: New"), "deliverPolicy is immutable"},
		{"ackPolicy changed", consumer(", ackPolicy: Explicit"), consumer(", ackPolicy: None"), "ackPolicy is immutable"},
		{"replayPolicy changed", consumer(", replayPolicy: Instant"), consumer(", replayPolicy: Original"), "replayPolicy is immutable"},
		{"optStartSeq changed", consumer(", optStartSeq: 1"), consumer(", optStartSeq: 2"), "optStartSeq is immutable"},
		{"optStartTime changed", consumer(", optStartTime: '2026-01-01T00:00:00Z'"), consumer(", optStartTime: '2026-01-02T00:00:00Z'"), "optStartTime is immutable"},
		{"heartbeat changed", consumer(", heartbeat: 5s"), consumer(", heartbeat: 10s"), "heartbeat is immutable"},
		{"heartbeat respelled", consumer(", heartbeat: 5s"), consumer(", heartbeat: 5000ms"), ""},
		{"flowControl changed", consumer(", flowControl: false"), consumer(", flowControl: true"), "flowControl is immutable"},
		{"maxWaiting changed", consumer(", maxWaiting: 1"), consumer(", maxWaiting: 2"), "maxWaiting is immutable"},
		{"deliverPolicy changed with recreate", consumer(", deliverPolicy: All"), consumer(", deliverPolicy: New, recreateOnImmutableChange: true"), ""},
		{"ackWait changed", consumer(", ackWait: 30s"), consumer(", ackWait: 1m"), ""},

		{"bucket renamed", manifest("NatsKeyValue", "kv", "{connectionRef: {name: c}, name: a}"), manifest("NatsKeyValue", "kv", "{connectionRef: {name: c}, name: b}"), "the bucket name is immutable"},
		{"key-value storage changed", manifest("NatsKeyValue", "kv", "{connectionRef: {name: c}, storage: File}"), manifest("NatsKeyValue", "kv", "{connectionRef: {name: c}, storage: Memory}"), "storage is immutable"},
		{"key-value storage late-initialized", manifest("NatsKeyValue", "kv", "{connectionRef: {name: c}}"), manifest("NatsKeyValue", "kv", "{connectionRef: {name: c}, storage: Memory}"), ""},
		{"object store storage changed", manifest("NatsObjectStore", "os", "{connectionRef: {name: c}, storage: File}"), manifest("NatsObjectStore", "os", "{connectionRef: {name: c}, storage: Memory}"), "storage is immutable"},
		{"object store renamed", manifest("NatsObjectStore", "os", "{connectionRef: {name: c}, name: a}"), manifest("NatsObjectStore", "os", "{connectionRef: {name: c}, name: b}"), "the bucket name is immutable"},

		{"stream connection changed", stream("s", ""), manifest("NatsStream", "s", "{connectionRef: {name: d}}"), "connectionRef is immutable"},
		{"stream connection moved to another namespace", stream("s", ""), manifest("NatsStream", "s", "{connectionRef: {name: c, namespace: other}}"), "connectionRef is immutable"},
		{"stream connection unchanged", stream("s", ""), stream("s", ", maxMsgs: 5"), ""},
		{"key-value connection changed", manifest("NatsKeyValue", "kv", "{connectionRef: {name: c}}"), manifest("NatsKeyValue", "kv", "{connectionRef: {name: d}}"), "connectionRef is immutable"},
		{"key-value connection unchanged", manifest("NatsKeyValue", "kv", "{connectionRef: {name: c}}"), manifest("NatsKeyValue", "kv", "{connectionRef: {name: c}}"), ""},
		{"object store connection changed", manifest("NatsObjectStore", "os", "{connectionRef: {name: c}}"), manifest("NatsObjectStore", "os", "{connectionRef: {name: d}}"), "connectionRef is immutable"},
		{"object store connection unchanged", manifest("NatsObjectStore", "os", "{connectionRef: {name: c}}"), manifest("NatsObjectStore", "os", "{connectionRef: {name: c}}"), ""},
		{"consumer connection changed", consumer(""), manifest("NatsConsumer", "k", "{connectionRef: {name: d}, stream: S}"), "connectionRef is immutable"},
		{"consumer connection set", manifest("NatsConsumer", "k", "{streamRef: {name: s}}"), manifest("NatsConsumer", "k", "{connectionRef: {name: c}, streamRef: {name: s}}"), "connectionRef is immutable"},
		{"consumer connection removed", manifest("NatsConsumer", "k", "{connectionRef: {name: c}, streamRef: {name: s}}"), manifest("NatsConsumer", "k", "{streamRef: {name: s}}"), "connectionRef is immutable"},
		{"consumer stream changed", consumer(""), manifest("NatsConsumer", "k", "{connectionRef: {name: c}, stream: T}"), "stream is immutable"},
		{"consumer stream swapped for a streamRef", consumer(""), manifest("NatsConsumer", "k", "{connectionRef: {name: c}, streamRef: {name: s}}"), "stream is immutable"},
		{"consumer streamRef changed", manifest("NatsConsumer", "k", "{streamRef: {name: s}}"), manifest("NatsConsumer", "k", "{streamRef: {name: t}}"), "streamRef is immutable"},
		{"consumer stream unchanged", consumer(""), consumer(", ackWait: 1m"), ""},
		{"consumer streamRef unchanged", manifest("NatsConsumer", "k", "{streamRef: {name: s}}"), manifest("NatsConsumer", "k", "{streamRef: {name: s}, ackWait: 1m}"), ""},

		{"evacuation connection changed", evacuation("c", "a", "t"), evacuation("d", "a", "t"), "connectionRef is immutable"},
		{"evacuation source changed", evacuation("c", "a", "t"), evacuation("c", "b", "t"), "from is immutable"},
		{"evacuation target changed", evacuation("c", "a", "t"), evacuation("c", "a", "u"), "to is immutable"},
		{"evacuation unchanged", evacuation("c", "a", "t"), evacuation("c", "a", "t"), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := parse(t, tt.before)
			require.NoError(t, c.Create(t.Context(), before))
			t.Cleanup(func() { require.NoError(t, c.Delete(context.Background(), before)) })
			after := parse(t, tt.after)
			after.SetResourceVersion(before.GetResourceVersion())
			err := c.Update(t.Context(), after, client.DryRunAll)
			if tt.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.want)
		})
	}
}

// testDefaults pins the schema defaults the API server fills in.
func testDefaults(t *testing.T, c client.Client) {
	tests := []struct {
		name string
		obj  string
		path []string
		want any
	}{
		{"creds key", manifest("NatsConnection", "d", "{servers: [nats://x], credentials: {secretKeyRef: {name: s}}}"), []string{"spec", "credentials", "secretKeyRef", "key"}, "user.creds"},
		{"CA key", manifest("NatsConnection", "d", "{servers: [nats://x], tls: {ca: {secretKeyRef: {name: s}}}}"), []string{"spec", "tls", "ca", "secretKeyRef", "key"}, "ca.crt"},
		{"exporter on", manifest("NatsCluster", "d", "{version: 2.15.0, replicas: 1, exporter: {}}"), []string{"spec", "exporter", "enabled"}, true},
		{"route TLS on", manifest("NatsCluster", "d", "{version: 2.15.0, replicas: 1, routes: {tls: {}}}"), []string{"spec", "routes", "tls", "enabled"}, true},
		{"account JWT TTL", manifest("NatsAccount", "d", "{operatorRef: {name: o}}"), []string{"spec", "jwtTTL"}, "48h"},
		{"stream adoption", manifest("NatsStream", "d", "{connectionRef: {name: c}}"), []string{"spec", "adoptionPolicy"}, "Never"},
		{"stream terminal", manifest("NatsStream", "d", "{connectionRef: {name: c}}"), []string{"spec", "terminalPolicy"}, "Hold"},
		{"stream deletion", manifest("NatsStream", "d", "{connectionRef: {name: c}}"), []string{"spec", "deletionPolicy"}, "Retain"},
		{"consumer deletion", manifest("NatsConsumer", "d", "{connectionRef: {name: c}, stream: S}"), []string{"spec", "deletionPolicy"}, "Delete"},
		{"key-value deletion", manifest("NatsKeyValue", "d", "{connectionRef: {name: c}}"), []string{"spec", "deletionPolicy"}, "Retain"},
		{"object store deletion", manifest("NatsObjectStore", "d", "{connectionRef: {name: c}}"), []string{"spec", "deletionPolicy"}, "Retain"},
		{"account balancer leader moves", manifest("NatsBalancer", "d", "{connectionRef: {name: c}}"), []string{"spec", "moves", "leader"}, true},
		{"account balancer placement moves", manifest("NatsBalancer", "d", "{connectionRef: {name: c}}"), []string{"spec", "moves", "placement"}, false},
		{"system balancer leader moves", manifest("NatsSystemBalancer", "d", "{connectionRef: {name: c}}"), []string{"spec", "moves", "leader"}, true},
		{"system balancer placement moves", manifest("NatsSystemBalancer", "d", "{connectionRef: {name: c}}"), []string{"spec", "moves", "placement"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := parse(t, tt.obj)
			require.NoError(t, c.Create(t.Context(), obj, client.DryRunAll))
			got, found, err := unstructured.NestedFieldNoCopy(obj.Object, tt.path...)
			require.NoError(t, err)
			require.True(t, found, "%v not defaulted", tt.path)
			require.Equal(t, tt.want, got)
		})
	}
}
