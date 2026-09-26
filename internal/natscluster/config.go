package natscluster

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// Config is one server's nats-server configuration. Its JSON encoding is
// the config file: nats-server parses JSON as its own syntax.
type Config struct {
	ServerName     string            `json:"server_name"`
	Listen         string            `json:"listen"`
	HTTP           string            `json:"http"`
	PidFile        string            `json:"pid_file"`
	LameDuck       string            `json:"lame_duck_duration"`
	LameDuckGrace  string            `json:"lame_duck_grace_period"`
	ServerTags     []string          `json:"server_tags,omitempty"`
	ServerMetadata map[string]string `json:"server_metadata,omitempty"`
	Cluster        ClusterConfig     `json:"cluster"`
	JetStream      *JetStreamConfig  `json:"jetstream,omitempty"`

	Operator        string            `json:"operator,omitempty"`
	SystemAccount   string            `json:"system_account,omitempty"`
	Resolver        *ResolverConfig   `json:"resolver,omitempty"`
	ResolverPreload map[string]string `json:"resolver_preload,omitempty"`
}

// ResolverConfig is the account resolver, a directory of account JWTs.
type ResolverConfig struct {
	Type        string `json:"type"`
	Dir         string `json:"dir"`
	AllowDelete bool   `json:"allow_delete,omitempty"`
}

// ClusterConfig is the route listener and the routes to every server.
type ClusterConfig struct {
	Name   string     `json:"name"`
	Listen string     `json:"listen"`
	Routes []string   `json:"routes"`
	TLS    *TLSConfig `json:"tls,omitempty"`
}

// TLSConfig is a listener's certificate and the CA its peers are verified
// against.
type TLSConfig struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CAFile   string `json:"ca_file"`
	Verify   bool   `json:"verify"`
}

// JetStreamConfig is a server's JetStream block; a zero limit is left to
// nats-server's default.
type JetStreamConfig struct {
	StoreDir       string `json:"store_dir"`
	MaxMemoryStore int64  `json:"max_memory_store,omitempty"`
	MaxFileStore   int64  `json:"max_file_store,omitempty"`
	Domain         string `json:"domain,omitempty"`
}

// Render encodes c as a config file.
func (c *Config) Render() ([]byte, error) {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("render config for %s: %w", c.ServerName, err)
	}
	return append(b, '\n'), nil
}

// Layout is where a server listens, what it routes to, and where its files
// are: what differs between a pod and a test.
type Layout struct {
	ClientListen  string
	RouteListen   string
	MonitorListen string
	PidFile       string
	StoreDir      string
	ResolverDir   string

	// Routes are route URLs of every server, the server's own included.
	Routes []string

	// TLSDir holds tls.crt, tls.key and ca.crt for route TLS.
	TLSDir string
}

// Paths inside a server's pod.
const (
	configDir    = "/etc/nats-config"
	configFile   = "nats.conf"
	pidDir       = "/var/run/nats"
	dataDir      = "/data"
	resolverDir  = dataDir + "/resolver"
	routesTLSDir = "/etc/nats-routes-tls"
)

// podLayout is the Layout of every server of nc in its pod.
func podLayout(nc *clusterv1beta1.NatsCluster) Layout {
	l := Layout{
		ClientListen:  fmt.Sprintf("0.0.0.0:%d", PortClient),
		RouteListen:   fmt.Sprintf("0.0.0.0:%d", PortRoute),
		MonitorListen: fmt.Sprintf("0.0.0.0:%d", PortMonitor),
		PidFile:       pidDir + "/nats.pid",
		StoreDir:      dataDir + "/jetstream",
		ResolverDir:   resolverDir,
		TLSDir:        routesTLSDir,
	}
	for _, s := range serverNames(nc) {
		l.Routes = append(l.Routes, fmt.Sprintf("nats-route://%s:%d", podHost(nc, s), PortRoute))
	}
	return l
}

// Limits are the memory limits derived from spec: the effective JetStream
// store limits, and GOMEMLIMIT in bytes, zero when resources.limits.memory
// is unset.
type Limits struct {
	JetStream  clusterv1beta1.JetStreamLimits
	GoMemLimit int64
}

// deriveLimits derives from resources.limits.memory a GOMEMLIMIT of 90% and
// a memory store of 75%, and from the volume claim's storage request a file
// store of 95%; jetstream.limits overrides either store limit.
func deriveLimits(spec *clusterv1beta1.NatsClusterSpec) Limits {
	var l Limits
	if mem, ok := spec.Resources.Limits[corev1.ResourceMemory]; ok {
		l.GoMemLimit = mem.Value() * 9 / 10
		l.JetStream.MaxMemoryStore = resource.NewQuantity(mem.Value()*3/4, resource.BinarySI)
	}
	js := spec.JetStream
	if js == nil {
		return l
	}
	if vct := js.VolumeClaimTemplate; vct != nil {
		if size, ok := vct.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			l.JetStream.MaxFileStore = resource.NewQuantity(size.Value()*19/20, resource.BinarySI)
		}
	}
	if o := js.Limits; o != nil {
		if o.MaxMemoryStore != nil {
			l.JetStream.MaxMemoryStore = o.MaxMemoryStore
		}
		if o.MaxFileStore != nil {
			l.JetStream.MaxFileStore = o.MaxFileStore
		}
	}
	return l
}

// routeTLSEnabled reports whether routes run over TLS: on unless
// routes.tls.enabled is false.
func routeTLSEnabled(spec *clusterv1beta1.NatsClusterSpec) bool {
	if spec.Routes == nil || spec.Routes.TLS == nil || spec.Routes.TLS.Enabled == nil {
		return true
	}
	return *spec.Routes.TLS.Enabled
}

// serverConfig renders server's config within nc under layout l, reporting
// revision through server_metadata unless revision is empty. trust is nil
// exactly when nc has no auth plane.
func serverConfig(nc *clusterv1beta1.NatsCluster, trust *Trust, server string, l Layout, revision string) *Config {
	c := &Config{
		ServerName:    server,
		Listen:        l.ClientListen,
		HTTP:          l.MonitorListen,
		PidFile:       l.PidFile,
		LameDuck:      lameDuckDuration.String(),
		LameDuckGrace: lameDuckGracePeriod.String(),
		ServerTags:    serverTags(nc.Spec.ServerTags),
		Cluster: ClusterConfig{
			Name:   nc.Name,
			Listen: l.RouteListen,
			Routes: l.Routes,
		},
	}
	if revision != "" {
		c.ServerMetadata = map[string]string{MetadataConfigRevision: revision}
	}
	if routeTLSEnabled(&nc.Spec) {
		c.Cluster.TLS = &TLSConfig{
			CertFile: l.TLSDir + "/" + corev1.TLSCertKey,
			KeyFile:  l.TLSDir + "/" + corev1.TLSPrivateKeyKey,
			CAFile:   l.TLSDir + "/" + caKey,
			Verify:   true,
		}
	}
	if js := nc.Spec.JetStream; js != nil {
		limits := deriveLimits(&nc.Spec).JetStream
		c.JetStream = &JetStreamConfig{StoreDir: l.StoreDir, Domain: js.Domain}
		if q := limits.MaxMemoryStore; q != nil {
			c.JetStream.MaxMemoryStore = q.Value()
		}
		if q := limits.MaxFileStore; q != nil {
			c.JetStream.MaxFileStore = q.Value()
		}
	}
	if trust != nil {
		c.Operator = trust.OperatorJWT
		c.SystemAccount = trust.SystemAccount
		c.Resolver = resolverConfig(nc.Spec.Auth.Resolver, l.ResolverDir)
		c.ResolverPreload = map[string]string{trust.SystemAccount: trust.SystemAccountJWT}
	}
	return c
}

// resolverConfig renders resolver type t, Full when empty, over dir. A
// Full resolver allows deletes, so that an account deleted at home is
// removed from every server.
func resolverConfig(t clusterv1beta1.ResolverType, dir string) *ResolverConfig {
	if t == clusterv1beta1.ResolverCache {
		return &ResolverConfig{Type: "cache", Dir: dir}
	}
	return &ResolverConfig{Type: "full", Dir: dir, AllowDelete: true}
}

// serverTags renders tags as key:value, sorted by key.
func serverTags(tags map[string]string) []string {
	var out []string
	for _, k := range slices.Sorted(maps.Keys(tags)) {
		out = append(out, k+":"+tags[k])
	}
	return out
}
