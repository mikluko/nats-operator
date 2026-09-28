package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// Imperatives on a NatsCluster. The cluster controller clears force-step and
// replace-server once it has acted; force-delete it reads only while the
// NatsCluster is being deleted, and never clears.
const (
	// AnnotationForceStep pushes the rollout step for the named server
	// through its gate.
	AnnotationForceStep = "cluster.nats.mikluko.io/force-step"
	// AnnotationReplaceServer replaces the named server under the rollout
	// gate.
	AnnotationReplaceServer = "cluster.nats.mikluko.io/replace-server"
	// AnnotationForceDelete lets deletion proceed while JetStream data
	// remains.
	AnnotationForceDelete = "cluster.nats.mikluko.io/force-delete"
)

// NatsClusterSpec is the desired state of a NATS cluster.
// +kubebuilder:validation:XValidation:rule="!has(self.leafRemotes) || size(self.leafRemotes) == 0 || !has(self.jetstream) || has(self.jetstream.domain)",message="a leaf running JetStream must set jetstream.domain"
// +kubebuilder:validation:XValidation:rule="!has(self.leafnodes) || has(self.auth)",message="a leafnode listener requires auth: without it any leaf connects into the global account"
type NatsClusterSpec struct {
	// Version is the nats-server version rendered for.
	// +required
	// +kubebuilder:validation:XValidation:rule="isSemver(self) && semver(self).compareTo(semver('2.15.0')) >= 0",message="version must be a semantic version, 2.15.0 or later"
	// +kubebuilder:validation:XValidation:rule="!isSemver(self) || !isSemver(oldSelf) || (semver(self).major() == semver(oldSelf).major() && semver(self).minor() >= semver(oldSelf).minor() - 1 && semver(self).minor() <= semver(oldSelf).minor() + 1)",message="version moves at most one minor at a time, up or down"
	Version string `json:"version"`

	// Image is the nats-server image; its tag is always Version.
	// +optional
	Image *Image `json:"image,omitempty"`

	// Replicas is the number of servers.
	// +required
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas"`

	// Resources of the nats-server container. GOMEMLIMIT and the JetStream
	// memory store derive from limits.memory.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// JetStream enables JetStream on every server.
	// +optional
	JetStream *JetStream `json:"jetstream,omitempty"`

	// ServerTags are rendered as key:value server tags.
	// +optional
	ServerTags map[string]string `json:"serverTags,omitempty"`

	// PodTemplate is merged into every server's pod, over its security
	// context and automountServiceAccountToken too: whoever may write a
	// NatsCluster runs pods with any privilege its namespace admits.
	// +optional
	PodTemplate *PodTemplate `json:"podTemplate,omitempty"`

	// Exporter configures the prometheus-nats-exporter sidecar; absent, it
	// runs.
	// +optional
	Exporter *Exporter `json:"exporter,omitempty"`

	// Monitor configures access to the monitoring port, 8222, which has no
	// authentication.
	// +optional
	Monitor *Monitor `json:"monitor,omitempty"`

	// TLS on the client listener; absent, clients connect in the clear. The
	// cluster controller verifies it against the Secret's ca.crt, or the
	// system roots without one.
	// +optional
	TLS *ListenerTLS `json:"tls,omitempty"`

	// Routes configures the route listener; absent, route TLS is on and
	// self-signed.
	// +optional
	Routes *Routes `json:"routes,omitempty"`

	// Auth puts the NATS cluster under a NATS operator; absent, servers run
	// with no accounts and no client auth.
	// +optional
	Auth *Auth `json:"auth,omitempty"`

	// Gateway joins the NATS cluster into a supercluster under its own
	// name, the NatsCluster's name.
	// +optional
	Gateway *Gateway `json:"gateway,omitempty"`

	// Leafnodes opens a listener for leaf connections.
	// +optional
	Leafnodes *Leafnodes `json:"leafnodes,omitempty"`

	// LeafRemotes are the hubs this NATS cluster dials as a leaf.
	// +optional
	LeafRemotes []LeafRemote `json:"leafRemotes,omitempty"`

	// Rollout steers the restarts a spec change rolls out one server at a
	// time.
	// +optional
	Rollout *Rollout `json:"rollout,omitempty"`
}

// JetStream is the JetStream configuration of every server.
type JetStream struct {
	// Domain is the JetStream domain.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Domain string `json:"domain,omitempty"`

	// Limits override the store limits derived from resources and the
	// volume size.
	// +optional
	Limits *JetStreamLimits `json:"limits,omitempty"`

	// VolumeClaimTemplate is each server's file store volume; a change
	// replaces servers one at a time.
	// +optional
	VolumeClaimTemplate *VolumeClaimTemplate `json:"volumeClaimTemplate,omitempty"`
}

// JetStreamLimits are a server's JetStream store limits.
type JetStreamLimits struct {
	// MaxMemoryStore is the memory store limit.
	// +optional
	MaxMemoryStore *resource.Quantity `json:"maxMemoryStore,omitempty"`

	// MaxFileStore is the file store limit.
	// +optional
	MaxFileStore *resource.Quantity `json:"maxFileStore,omitempty"`
}

// EmbeddedObjectMetadata is the metadata a template passes through.
type EmbeddedObjectMetadata struct {
	// Labels added to the rendered object.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations added to the rendered object.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// VolumeClaimTemplate is a PersistentVolumeClaim template.
type VolumeClaimTemplate struct {
	// +optional
	Metadata EmbeddedObjectMetadata `json:"metadata,omitempty"`

	// +required
	Spec corev1.PersistentVolumeClaimSpec `json:"spec"`
}

// PodTemplate is merged into the pod the cluster controller renders.
type PodTemplate struct {
	// +optional
	Metadata EmbeddedObjectMetadata `json:"metadata,omitempty"`

	// Spec is a partial pod spec merged over the rendered one; the API
	// server does not validate it.
	// +optional
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Spec *corev1.PodSpec `json:"spec,omitempty"`
}

// CertificateSource names where a listener's certificate comes from.
type CertificateSource struct {
	// SecretRef names a kubernetes.io/tls Secret.
	// +optional
	SecretRef *natsv1beta1.SecretReference `json:"secretRef,omitempty"`

	// CertManager has cert-manager issue the certificate.
	// +optional
	CertManager *CertManagerCertificate `json:"certManager,omitempty"`
}

// CertManagerCertificate is a certificate cert-manager issues.
type CertManagerCertificate struct {
	// IssuerRef is copied into the Certificate the cluster controller
	// creates in the NatsCluster's namespace.
	// +required
	IssuerRef IssuerReference `json:"issuerRef"`
}

// IssuerReference names a cert-manager Issuer or ClusterIssuer.
type IssuerReference struct {
	// Name of an Issuer in the NatsCluster's namespace, or of a
	// ClusterIssuer.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Kind of the issuer, Issuer when omitted.
	// +optional
	Kind string `json:"kind,omitempty"`

	// Group of the issuer, cert-manager.io when omitted.
	// +optional
	Group string `json:"group,omitempty"`
}

// ListenerTLS is TLS on a listener that has it only when configured.
// +kubebuilder:validation:XValidation:rule="has(self.secretRef) != has(self.certManager)",message="set exactly one of secretRef and certManager"
type ListenerTLS struct {
	CertificateSource `json:",inline"`
}

// Routes configures the route listener.
type Routes struct {
	// TLS on routes.
	// +optional
	TLS *RoutesTLS `json:"tls,omitempty"`
}

// RoutesTLS is route TLS: on unless disabled, self-signed unless a
// certificate is named.
// +kubebuilder:validation:XValidation:rule="!(has(self.secretRef) && has(self.certManager))",message="secretRef and certManager are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.enabled) || self.enabled || (!has(self.secretRef) && !has(self.certManager))",message="a certificate is named only while route TLS is enabled"
type RoutesTLS struct {
	// Enabled turns route TLS off when false.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	CertificateSource `json:",inline"`
}

// Auth puts a NATS cluster under a NATS operator.
type Auth struct {
	// TrustRef names the NatsOperatorTrust holding the trust roots.
	// +required
	TrustRef natsv1beta1.ObjectReference `json:"trustRef"`

	// SystemCredentials are the system user the cluster controller connects
	// as.
	// +optional
	SystemCredentials *natsv1beta1.Credentials `json:"systemCredentials,omitempty"`

	// Resolver is the account resolver; the cluster controller renders Full
	// when omitted, and Cache for a leaf that preloads no account.
	// +optional
	Resolver ResolverType `json:"resolver,omitempty"`
}

// ResolverType is a NATS account resolver type.
// +kubebuilder:validation:Enum=Full;Cache
type ResolverType string

// Resolver types.
const (
	ResolverFull  ResolverType = "Full"
	ResolverCache ResolverType = "Cache"
)

// Gateway joins a NATS cluster into a supercluster.
type Gateway struct {
	// Discovery is how remotes are rendered: Explicit as gateway remotes
	// with reject_unknown on, Gossip as seeds with reject_unknown off.
	// +required
	Discovery GatewayDiscovery `json:"discovery"`

	// Remotes are every member of the supercluster; this NATS cluster's own
	// entry is skipped.
	// +required
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	Remotes []GatewayRemote `json:"remotes"`

	// TLS on the gateway listener; absent, gateways run in the clear. The
	// certificate's Secret must hold ca.crt, which peers are verified
	// against both ways.
	// +optional
	TLS *ListenerTLS `json:"tls,omitempty"`

	// Service is the template of the external gateway Service.
	// +optional
	Service *ServiceTemplate `json:"service,omitempty"`

	// Advertise is the host:port the servers advertise for gateways.
	// +optional
	Advertise string `json:"advertise,omitempty"`
}

// GatewayDiscovery is how gateway remotes are rendered.
// +kubebuilder:validation:Enum=Explicit;Gossip
type GatewayDiscovery string

// Gateway discovery modes.
const (
	GatewayDiscoveryExplicit GatewayDiscovery = "Explicit"
	GatewayDiscoveryGossip   GatewayDiscovery = "Gossip"
)

// GatewayRemote is one member of a supercluster.
type GatewayRemote struct {
	// Name is the member's gateway name.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// URL is where the member's gateway is dialled; on this NATS cluster's
	// own entry, its host is a name on the gateway certificate.
	// +required
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`
}

// ServiceTemplate is the template of an external Service.
type ServiceTemplate struct {
	// Type of the Service, ClusterIP when omitted.
	// +optional
	Type corev1.ServiceType `json:"type,omitempty"`

	// Annotations set on the Service.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// Leafnodes is the hub side of leaf connections.
type Leafnodes struct {
	// TLS on the leafnode listener; absent, leaf connections run in the
	// clear.
	// +optional
	TLS *ListenerTLS `json:"tls,omitempty"`

	// Service is the template of the external leafnode Service.
	// +optional
	Service *ServiceTemplate `json:"service,omitempty"`

	// Advertise is the host:port the servers advertise for leaf
	// connections.
	// +optional
	Advertise string `json:"advertise,omitempty"`
}

// LeafRemote is a hub a leaf dials, and the local account it binds.
// With neither localAccountTrustRef nor localSystemAccount it binds the
// global account.
// +kubebuilder:validation:XValidation:rule="!(has(self.localAccountTrustRef) && has(self.localSystemAccount) && self.localSystemAccount)",message="localAccountTrustRef and localSystemAccount are mutually exclusive"
type LeafRemote struct {
	// ConnectionRef names the NatsConnection holding the hub's URL, CA and
	// credentials.
	// +required
	ConnectionRef natsv1beta1.ObjectReference `json:"connectionRef"`

	// LocalAccountTrustRef names the NatsAccountTrust of the local account
	// the remote binds.
	// +optional
	LocalAccountTrustRef *natsv1beta1.ObjectReference `json:"localAccountTrustRef,omitempty"`

	// LocalSystemAccount binds the remote to the leaf's system account.
	// +optional
	LocalSystemAccount bool `json:"localSystemAccount,omitempty"`
}

// Exporter is the prometheus-nats-exporter sidecar in every server's pod,
// serving metrics on port 7777.
type Exporter struct {
	// Enabled turns the sidecar off when false.
	// +optional
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	// Image is the sidecar's image.
	// +optional
	Image *ExporterImage `json:"image,omitempty"`

	// From admits the metrics port from these peers besides the cluster
	// controller's namespace, under monitor.networkPolicy.
	// +optional
	// +listType=atomic
	From []networkingv1.NetworkPolicyPeer `json:"from,omitempty"`
}

// Image names the nats-server image of a NATS cluster's servers.
type Image struct {
	// Repository is the image repository; empty, it is nats.
	// +optional
	Repository string `json:"repository,omitempty"`

	// Digest pins the image to one manifest, rendered after the tag.
	// +optional
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	Digest string `json:"digest,omitempty"`
}

// ExporterImage names the prometheus-nats-exporter sidecar's image.
type ExporterImage struct {
	// Repository is the image repository; empty, it is
	// natsio/prometheus-nats-exporter.
	// +optional
	Repository string `json:"repository,omitempty"`

	// Tag is the image tag; empty, it is 0.17.3.
	// +optional
	Tag string `json:"tag,omitempty"`

	// Digest pins the image to one manifest, rendered after the tag.
	// +optional
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	Digest string `json:"digest,omitempty"`
}

// Monitor configures access to the monitoring port.
type Monitor struct {
	// NetworkPolicy renders a NetworkPolicy over the servers' pods that
	// admits the route port only from those pods, the monitoring port only
	// from the cluster controller's namespace, the metrics port from there
	// and exporter.from, and from anywhere the other ports the cluster
	// controller renders; a port podTemplate adds is not admitted.
	// +optional
	// +kubebuilder:default=true
	NetworkPolicy *bool `json:"networkPolicy,omitempty"`
}

// Rollout steers a NATS cluster's one-server-at-a-time restarts.
type Rollout struct {
	// Paused stops a rollout before its next step.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// NatsClusterStatus is the observed state of a NATS cluster.
type NatsClusterStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions: Ready, Settled, Progressing, Deleting, and where they
	// apply GatewaysConnected and LeafnodesConnected.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Version is the version every server has reached.
	// +optional
	Version string `json:"version,omitempty"`

	// Replicas is the number of servers.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of ready servers.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Endpoints are the addresses clients and peers reach the NATS cluster
	// at.
	// +optional
	Endpoints *Endpoints `json:"endpoints,omitempty"`

	// Config is the rendered config revision and how it was applied.
	// +optional
	Config *ConfigStatus `json:"config,omitempty"`

	// Rollout is the rollout in progress.
	// +optional
	Rollout *RolloutStatus `json:"rollout,omitempty"`

	// Removals are the servers whose removal or replacement has begun, and
	// how far each has gone.
	// +optional
	// +listType=map
	// +listMapKey=name
	Removals []ServerRemoval `json:"removals,omitempty"`

	// JetStream is the JetStream state of the NATS cluster.
	// +optional
	JetStream *JetStreamStatus `json:"jetstream,omitempty"`

	// Gateways are the connections to the other supercluster members.
	// +optional
	// +listType=map
	// +listMapKey=name
	Gateways []GatewayStatus `json:"gateways,omitempty"`

	// LeafRemotes are the connections to hubs.
	// +optional
	// +listType=map
	// +listMapKey=connectionNamespace
	// +listMapKey=connectionName
	LeafRemotes []LeafRemoteStatus `json:"leafRemotes,omitempty"`

	// Servers has one entry per server.
	// +optional
	// +listType=map
	// +listMapKey=name
	Servers []ServerStatus `json:"servers,omitempty"`
}

// Endpoints are a NATS cluster's addresses.
type Endpoints struct {
	// Client is the client URL.
	// +optional
	Client string `json:"client,omitempty"`

	// Monitor is the monitoring URL, on the headless Service.
	// +optional
	Monitor string `json:"monitor,omitempty"`

	// Gateway is the advertised gateway address.
	// +optional
	Gateway string `json:"gateway,omitempty"`
}

// ConfigApplyMethod is how a rendered config reached the servers.
// +kubebuilder:validation:Enum=Reload;Restart
type ConfigApplyMethod string

// Config apply methods.
const (
	ConfigAppliedByReload  ConfigApplyMethod = "Reload"
	ConfigAppliedByRestart ConfigApplyMethod = "Restart"
)

// ConfigStatus is the rendered config revision.
type ConfigStatus struct {
	// Revision is a digest of the rendered config, StatefulSets and
	// certificates; each server reports its own as config_revision.
	// +optional
	Revision string `json:"revision,omitempty"`

	// AppliedBy is how the revision is applied.
	// +optional
	AppliedBy ConfigApplyMethod `json:"appliedBy,omitempty"`

	// RestartReason names what made a restart necessary.
	// +optional
	RestartReason string `json:"restartReason,omitempty"`
}

// RolloutStatus is a rollout in progress.
type RolloutStatus struct {
	// TargetRevision is the config revision being rolled out.
	// +optional
	TargetRevision string `json:"targetRevision,omitempty"`

	// Updated are the servers on the target revision.
	// +optional
	Updated []string `json:"updated,omitempty"`

	// Current is the server being updated.
	// +optional
	Current string `json:"current,omitempty"`

	// Pending are the servers still to update.
	// +optional
	Pending []string `json:"pending,omitempty"`

	// Gate is what the rollout waits for before its next step.
	// +optional
	Gate *RolloutGate `json:"gate,omitempty"`
}

// RolloutGate is what a rollout waits for.
type RolloutGate struct {
	// WaitingFor names the condition the gate waits for.
	// +optional
	WaitingFor string `json:"waitingFor,omitempty"`

	// Since is when the gate closed.
	// +optional
	Since *metav1.Time `json:"since,omitempty"`
}

// RemovalPhase is how far a server's removal has gone.
// +kubebuilder:validation:Enum=Requested;Evacuating;Removed;Deleting;Rejoining
type RemovalPhase string

// Removal phases.
const (
	// RemovalRequested is a server replace-server named, waiting its turn.
	RemovalRequested RemovalPhase = "Requested"
	// RemovalEvacuating is a server whose evacuation the meta leader
	// accepted.
	RemovalEvacuating RemovalPhase = "Evacuating"
	// RemovalRemoved is a server whose removal from the meta group was
	// committed.
	RemovalRemoved RemovalPhase = "Removed"
	// RemovalDeleting is a server whose StatefulSet, data volume claim and,
	// beyond spec.replicas, ConfigMap are being deleted. A replaced server
	// is not recreated until its claim is gone.
	RemovalDeleting RemovalPhase = "Deleting"
	// RemovalRejoining is a server its replacement recreated, until the
	// rollout gate next opens.
	RemovalRejoining RemovalPhase = "Rejoining"
)

// ServerRemoval is one server's removal.
type ServerRemoval struct {
	// Name is the server_name.
	// +required
	Name string `json:"name"`

	// Phase is how far the removal has gone.
	// +required
	Phase RemovalPhase `json:"phase"`

	// Since is when the removal entered Phase.
	// +optional
	Since *metav1.Time `json:"since,omitempty"`
}

// JetStreamStatus is a NATS cluster's JetStream state.
type JetStreamStatus struct {
	// MetaLeader is the server name of the meta group's leader, empty while
	// none is known.
	// +optional
	MetaLeader string `json:"metaLeader,omitempty"`

	// Limits are the effective store limits.
	// +optional
	Limits *JetStreamLimits `json:"limits,omitempty"`
}

// GatewayStatus is the connection to one supercluster member.
type GatewayStatus struct {
	// Name is the member's gateway name.
	// +required
	Name string `json:"name"`

	// Connected reports whether the member is reachable.
	// +optional
	Connected bool `json:"connected,omitempty"`

	// Inbound is the number of inbound gateway connections.
	// +optional
	Inbound int32 `json:"inbound,omitempty"`

	// Outbound is the number of outbound gateway connections.
	// +optional
	Outbound int32 `json:"outbound,omitempty"`
}

// LeafRemoteStatus is the connection to one hub.
type LeafRemoteStatus struct {
	// ConnectionNamespace is the namespace of the remote's NatsConnection.
	// +required
	ConnectionNamespace string `json:"connectionNamespace"`

	// ConnectionName is the name of the remote's NatsConnection.
	// +required
	ConnectionName string `json:"connectionName"`

	// Connected is the number of servers connected to the hub.
	// +optional
	Connected int32 `json:"connected,omitempty"`

	// Account is the public key of the hub account the credentials sign
	// into.
	// +optional
	Account string `json:"account,omitempty"`
}

// ServerStatus is one server's state.
type ServerStatus struct {
	// Name is the server_name.
	// +required
	Name string `json:"name"`

	// Version is the nats-server version it runs.
	// +optional
	Version string `json:"version,omitempty"`

	// Ready reports whether its pod is Ready.
	// +optional
	Ready bool `json:"ready,omitempty"`

	// ConfigRevision is the config revision it reports.
	// +optional
	ConfigRevision string `json:"configRevision,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Settled",type=string,JSONPath=`.status.conditions[?(@.type=="Settled")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsCluster is a NATS cluster the cluster controller deploys, one
// StatefulSet per server.
type NatsCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsClusterSpec `json:"spec"`
	// +optional
	Status NatsClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsClusterList is a list of NatsCluster.
type NatsClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsCluster `json:"items"`
}

func init() {
	register(&NatsCluster{}, &NatsClusterList{})
}
