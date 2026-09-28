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
	Gateway        *GatewayConfig    `json:"gateway,omitempty"`
	JetStream      *JetStreamConfig  `json:"jetstream,omitempty"`
	Leafnodes      *LeafnodesConfig  `json:"leafnodes,omitempty"`

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
// against; without a CA file, peers are verified against the system roots.
type TLSConfig struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CAFile   string `json:"ca_file,omitempty"`
	Verify   bool   `json:"verify"`
}

// GatewayConfig is the gateway listener and the remote gateways a server
// dials. RejectUnknown refuses a gateway Gateways does not name, and so
// turns gossip discovery off.
type GatewayConfig struct {
	Name          string                `json:"name"`
	Listen        string                `json:"listen"`
	Advertise     string                `json:"advertise,omitempty"`
	RejectUnknown bool                  `json:"reject_unknown"`
	TLS           *TLSConfig            `json:"tls,omitempty"`
	Gateways      []RemoteGatewayConfig `json:"gateways,omitempty"`
}

// RemoteGatewayConfig is one remote gateway a server dials.
type RemoteGatewayConfig struct {
	Name string   `json:"name"`
	URLs []string `json:"urls"`
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
	GatewayListen string
	PidFile       string
	StoreDir      string
	ResolverDir   string

	// Routes are route URLs of every server, the server's own included.
	Routes []string

	// TLSDir holds tls.crt, tls.key and ca.crt for route TLS.
	TLSDir string

	// GatewayTLSDir holds tls.crt and tls.key for gateway TLS, and ca.crt
	// when Inputs.GatewayCA is set.
	GatewayTLSDir   string
	LeafnodesListen string
	// LeafnodesTLSDir holds tls.crt and tls.key for the leafnode listener.
	LeafnodesTLSDir string
	// LeafRemotesDir holds the files of the leaf remotes Secret.
	LeafRemotesDir string
}

// Paths inside a server's pod.
const (
	configDir     = "/etc/nats-config"
	configFile    = "nats.conf"
	pidDir        = "/var/run/nats"
	dataDir       = "/data"
	resolverDir   = dataDir + "/resolver"
	routesTLSDir  = "/etc/nats-routes-tls"
	gatewayTLSDir = "/etc/nats-gateway-tls"
)

// podLayout is the Layout of every server of nc in its pod.
func podLayout(nc *clusterv1beta1.NatsCluster) Layout {
	l := Layout{
		ClientListen:  fmt.Sprintf("0.0.0.0:%d", PortClient),
		RouteListen:   fmt.Sprintf("0.0.0.0:%d", PortRoute),
		MonitorListen: fmt.Sprintf("0.0.0.0:%d", PortMonitor),
		GatewayListen: fmt.Sprintf("0.0.0.0:%d", PortGateway),
		PidFile:       pidDir + "/nats.pid",
		StoreDir:      dataDir + "/jetstream",
		ResolverDir:   resolverDir,
		TLSDir:        routesTLSDir,
		GatewayTLSDir: gatewayTLSDir,

		LeafnodesListen: fmt.Sprintf("0.0.0.0:%d", PortLeafnodes),
		LeafnodesTLSDir: leafnodesTLSDir,
		LeafRemotesDir:  configDir,
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

// Inputs is what a render reads besides the NatsCluster.
type Inputs struct {
	// Trust is nil exactly when the NatsCluster has no auth plane.
	Trust *Trust
	// GatewayCA reports whether the gateway certificate Secret holds
	// ca.crt; gateway peers are then verified against it, both ways.
	GatewayCA bool
	// Certs are the TLS Secrets the servers mount.
	Certs Certs
}

// serverConfig renders server's config within nc from in under layout l,
// reporting revision through server_metadata unless revision is empty;
// remotes are nc's leafRemotes resolved.
func serverConfig(nc *clusterv1beta1.NatsCluster, in Inputs, server string, l Layout, revision string, remotes ...LeafRemote) *Config {
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
	if g := nc.Spec.Gateway; g != nil {
		c.Gateway = gatewayConfig(nc.Name, g, l, in.GatewayCA)
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
	c.Leafnodes = leafnodesConfig(nc, remotes, l)
	if trust := in.Trust; trust != nil {
		c.Operator = trust.OperatorJWT
		c.SystemAccount = trust.SystemAccount
		c.Resolver = resolverConfig(resolverType(nc, remotes), l.ResolverDir)
		c.ResolverPreload = map[string]string{trust.SystemAccount: trust.SystemAccountJWT}
		for _, r := range remotes {
			if r.PreloadJWT != "" {
				c.ResolverPreload[r.LocalAccount] = r.PreloadJWT
			}
		}
	}
	return c
}

// gatewayConfig renders the gateway of the NATS cluster named name.
func gatewayConfig(name string, g *clusterv1beta1.Gateway, l Layout, withCA bool) *GatewayConfig {
	gc := &GatewayConfig{
		Name:          name,
		Listen:        l.GatewayListen,
		Advertise:     g.Advertise,
		RejectUnknown: g.Discovery != clusterv1beta1.GatewayDiscoveryGossip,
	}
	for _, r := range g.Remotes {
		if r.Name != name {
			gc.Gateways = append(gc.Gateways, RemoteGatewayConfig{Name: r.Name, URLs: []string{r.URL}})
		}
	}
	if g.TLS != nil {
		gc.TLS = &TLSConfig{
			CertFile: l.GatewayTLSDir + "/" + corev1.TLSCertKey,
			KeyFile:  l.GatewayTLSDir + "/" + corev1.TLSPrivateKeyKey,
		}
		if withCA {
			gc.TLS.CAFile = l.GatewayTLSDir + "/" + caKey
			gc.TLS.Verify = true
		}
	}
	return gc
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
