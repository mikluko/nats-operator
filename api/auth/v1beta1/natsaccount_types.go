package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// NatsAccountSpec is the desired state of an account.
// +kubebuilder:validation:XValidation:rule="!has(self.publicKey) || !has(self.keys) || !has(self.keys.identity)",message="publicKey and keys.identity are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.publicKey) || (has(self.keys) && has(self.keys.signing) && size(self.keys.signing) > 0)",message="publicKey requires at least one signing key"
type NatsAccountSpec struct {
	// OperatorRef names the NatsOperator that signs this account.
	// +required
	OperatorRef natsv1beta1.ObjectReference `json:"operatorRef"`

	// Keys adopts existing seeds.
	// +optional
	Keys *Keys `json:"keys,omitempty"`

	// PublicKey is the account's identity, keeping its identity key
	// offline.
	// +optional
	// +kubebuilder:validation:MinLength=1
	PublicKey string `json:"publicKey,omitempty"`

	// JWTTTL is the account JWT's lifetime, and the JWT is re-signed at half
	// of it; 0 signs a JWT that never expires.
	// +optional
	// +kubebuilder:default="48h"
	JWTTTL *metav1.Duration `json:"jwtTTL,omitempty"`

	// Limits are signed into the account JWT; an omitted limit is
	// unlimited.
	// +optional
	Limits *AccountLimits `json:"limits,omitempty"`

	// Exports are what other accounts may import from this one.
	// +optional
	// +kubebuilder:validation:MaxItems=1000
	Exports []Export `json:"exports,omitempty"`

	// Imports are exports of other accounts this one takes.
	// +optional
	Imports []Import `json:"imports,omitempty"`

	// Adoption is how an account the servers already hold a JWT for is
	// adopted at the first signing.
	// +optional
	Adoption *Adoption `json:"adoption,omitempty"`
}

// AccountLimits are an account's limits.
type AccountLimits struct {
	// Connections is the maximum number of client connections.
	// +optional
	Connections *int64 `json:"connections,omitempty"`

	// Subscriptions is the maximum number of subscriptions.
	// +optional
	Subscriptions *int64 `json:"subscriptions,omitempty"`

	// Payload is the maximum message payload.
	// +optional
	Payload *resource.Quantity `json:"payload,omitempty"`

	// JetStream enables JetStream for the account, within these limits.
	// +optional
	JetStream *AccountJetStreamLimits `json:"jetstream,omitempty"`
}

// AccountJetStreamLimits are an account's JetStream limits, either for the
// account as a whole or by tier.
// +kubebuilder:validation:XValidation:rule="!has(self.tiers) || size(self.tiers) == 0 || !(has(self.memoryStorage) || has(self.diskStorage) || has(self.streams) || has(self.consumers) || has(self.maxAckPending) || has(self.memoryMaxStreamBytes) || has(self.diskMaxStreamBytes) || (has(self.maxBytesRequired) && self.maxBytesRequired))",message="jetstream limits are set either for the account or by tier"
type AccountJetStreamLimits struct {
	// MemoryStorage is the memory store limit.
	// +optional
	MemoryStorage *resource.Quantity `json:"memoryStorage,omitempty"`

	// DiskStorage is the file store limit.
	// +optional
	DiskStorage *resource.Quantity `json:"diskStorage,omitempty"`

	// Streams is the maximum number of streams.
	// +optional
	Streams *int64 `json:"streams,omitempty"`

	// Consumers is the maximum number of consumers.
	// +optional
	Consumers *int64 `json:"consumers,omitempty"`

	// MaxAckPending is the highest max ack pending a consumer may set.
	// +optional
	MaxAckPending *int64 `json:"maxAckPending,omitempty"`

	// MemoryMaxStreamBytes is the highest max bytes a memory stream may set.
	// +optional
	MemoryMaxStreamBytes *resource.Quantity `json:"memoryMaxStreamBytes,omitempty"`

	// DiskMaxStreamBytes is the highest max bytes a file stream may set.
	// +optional
	DiskMaxStreamBytes *resource.Quantity `json:"diskMaxStreamBytes,omitempty"`

	// MaxBytesRequired refuses a stream that sets no max bytes.
	// +optional
	MaxBytesRequired bool `json:"maxBytesRequired,omitempty"`

	// Tiers are the limits by tier, in place of the limits for the account.
	// nats-server refuses a stream whose tier is not listed.
	// +optional
	// +listType=map
	// +listMapKey=name
	Tiers []AccountJetStreamTier `json:"tiers,omitempty"`
}

// JetStreamTierName names a JetStream tier: R followed by the replica count
// of the streams nats-server puts in it.
// +kubebuilder:validation:Enum=R1;R2;R3;R4;R5
type JetStreamTierName string

// JetStream tier names.
const (
	JetStreamTierR1 JetStreamTierName = "R1"
	JetStreamTierR2 JetStreamTierName = "R2"
	JetStreamTierR3 JetStreamTierName = "R3"
	JetStreamTierR4 JetStreamTierName = "R4"
	JetStreamTierR5 JetStreamTierName = "R5"
)

// AccountJetStreamTier are an account's JetStream limits for one tier.
type AccountJetStreamTier struct {
	// Name is the tier the limits apply to.
	Name JetStreamTierName `json:"name"`

	// MemoryStorage is the memory store limit.
	// +optional
	MemoryStorage *resource.Quantity `json:"memoryStorage,omitempty"`

	// DiskStorage is the file store limit.
	// +optional
	DiskStorage *resource.Quantity `json:"diskStorage,omitempty"`

	// Streams is the maximum number of streams.
	// +optional
	Streams *int64 `json:"streams,omitempty"`

	// Consumers is the maximum number of consumers.
	// +optional
	Consumers *int64 `json:"consumers,omitempty"`

	// MaxAckPending is the highest max ack pending a consumer may set.
	// +optional
	MaxAckPending *int64 `json:"maxAckPending,omitempty"`

	// MemoryMaxStreamBytes is the highest max bytes a memory stream may set.
	// +optional
	MemoryMaxStreamBytes *resource.Quantity `json:"memoryMaxStreamBytes,omitempty"`

	// DiskMaxStreamBytes is the highest max bytes a file stream may set.
	// +optional
	DiskMaxStreamBytes *resource.Quantity `json:"diskMaxStreamBytes,omitempty"`

	// MaxBytesRequired refuses a stream that sets no max bytes.
	// +optional
	MaxBytesRequired bool `json:"maxBytesRequired,omitempty"`
}

// ExportPreset is a named set of exports.
// +kubebuilder:validation:Enum=jetstream-stepdown
type ExportPreset string

// Export presets.
const (
	// ExportPresetJetStreamStepdown exports the stream and consumer leader
	// stepdown services, and imports them into the system account.
	ExportPresetJetStreamStepdown ExportPreset = "jetstream-stepdown"
)

// ExportType is the type of an export.
// +kubebuilder:validation:Enum=Stream;Service
type ExportType string

// Export types.
const (
	ExportTypeStream  ExportType = "Stream"
	ExportTypeService ExportType = "Service"
)

// ResponseType is how a service export responds.
// +kubebuilder:validation:Enum=Singleton;Stream;Chunked
type ResponseType string

// Response types.
const (
	ResponseTypeSingleton ResponseType = "Singleton"
	ResponseTypeStream    ResponseType = "Stream"
	ResponseTypeChunked   ResponseType = "Chunked"
)

// ExportAccess is who may import an export.
// +kubebuilder:validation:Enum=Public;Private
type ExportAccess string

// Export access levels.
const (
	ExportAccessPublic  ExportAccess = "Public"
	ExportAccessPrivate ExportAccess = "Private"
)

// Export is one export, or a preset expanding to several.
// +kubebuilder:validation:XValidation:rule="has(self.preset) ? !has(self.name) && !has(self.type) && !has(self.subject) && !has(self.responseType) && !has(self.access) && !has(self.importers) : has(self.name) && has(self.type) && has(self.subject)",message="an export sets either preset alone, or name, type and subject"
// +kubebuilder:validation:XValidation:rule="!has(self.responseType) || (has(self.type) && self.type == 'Service')",message="responseType is set only on a Service export"
// +kubebuilder:validation:XValidation:rule="!has(self.importers) || (has(self.access) && self.access == 'Private')",message="importers are listed only on a Private export"
type Export struct {
	// Preset expands to a fixed set of exports.
	// +optional
	Preset ExportPreset `json:"preset,omitempty"`

	// Name is what imports name the export by.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name,omitempty"`

	// Type is signed into the account JWT as the export's type.
	// +optional
	Type ExportType `json:"type,omitempty"`

	// Subject is signed into the account JWT as the export's subject.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Subject string `json:"subject,omitempty"`

	// ResponseType of a service export, Singleton when omitted.
	// +optional
	ResponseType ResponseType `json:"responseType,omitempty"`

	// Access is Public when omitted; a Private export is importable only by
	// its importers.
	// +optional
	Access ExportAccess `json:"access,omitempty"`

	// Importers of a Private export, each minted an activation token.
	// +optional
	Importers []AccountReference `json:"importers,omitempty"`
}

// Import takes another account's export by name, from a NatsAccount or
// NatsSystemAccount under this account's NatsOperator, or from an account
// named by public key.
// +kubebuilder:validation:XValidation:rule="has(self.accountRef) != has(self.publicKey)",message="an import names its exporter by accountRef or by publicKey"
// +kubebuilder:validation:XValidation:rule="has(self.publicKey) ? has(self.subject) && has(self.type) : !has(self.subject) && !has(self.type) && !has(self.activation)",message="subject and type are set with publicKey and only then; activation only with publicKey"
// +kubebuilder:validation:XValidation:rule="!has(self.share) || !self.share || !has(self.type) || self.type == 'Service'",message="share is set only on a Service import"
// +kubebuilder:validation:XValidation:rule="!has(self.allowTrace) || !self.allowTrace || !has(self.type) || self.type == 'Stream'",message="allowTrace is set only on a Stream import"
type Import struct {
	// AccountRef names the exporting NatsAccount or NatsSystemAccount.
	// +optional
	AccountRef *AccountReference `json:"accountRef,omitempty"`

	// PublicKey names the exporting account where no NatsAccount or
	// NatsSystemAccount under this account's NatsOperator describes it.
	// +optional
	// +kubebuilder:validation:MinLength=1
	PublicKey string `json:"publicKey,omitempty"`

	// Export is the name of the export taken.
	// +required
	// +kubebuilder:validation:MinLength=1
	Export string `json:"export"`

	// Subject is the exported subject, for an import by publicKey.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Subject string `json:"subject,omitempty"`

	// Type is the export's type, for an import by publicKey.
	// +optional
	Type ExportType `json:"type,omitempty"`

	// Activation is the token the exporter issued this account for a
	// Private export, for an import by publicKey.
	// +optional
	Activation *Activation `json:"activation,omitempty"`

	// LocalSubject is where the import appears in this account, the
	// exported subject when omitted.
	// +optional
	// +kubebuilder:validation:MinLength=1
	LocalSubject string `json:"localSubject,omitempty"`

	// Share lets the exporter of a Service import sample this account's
	// request latency.
	// +optional
	Share bool `json:"share,omitempty"`

	// AllowTrace lets message traces cross a Stream import.
	// +optional
	AllowTrace bool `json:"allowTrace,omitempty"`
}

// Activation is where an activation token is read from.
type Activation struct {
	// SecretKeyRef selects the token.
	// +required
	SecretKeyRef ActivationSecretKeySelector `json:"secretKeyRef"`
}

// ActivationSecretKeySelector selects an activation token from a Secret in
// the referrer's namespace.
type ActivationSecretKeySelector struct {
	// Name of a Secret in the referrer's namespace.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key within the Secret.
	// +required
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// NatsAccountStatus is the observed state of an account.
type NatsAccountStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions: Ready, ReferencesResolved, Distributed, and where it
	// applies RevocationsUnrecovered. Ready is False, reason PublicKeyInUse,
	// while the NatsOperator's NatsSystemAccount or another NatsAccount under
	// it holds the account's public key.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// PublicKey is the account's public key.
	// +optional
	PublicKey string `json:"publicKey,omitempty"`

	// JWT is the current account JWT; empty once the account is no longer
	// admitted to its NatsOperator and the NatsOperator records its deletion.
	// +optional
	JWT string `json:"jwt,omitempty"`

	// JWTHash identifies the current account JWT.
	// +optional
	JWTHash string `json:"jwtHash,omitempty"`

	// Distribution is how many servers hold the current JWT.
	// +optional
	Distribution *Distribution `json:"distribution,omitempty"`

	// Revocations are the user keys the account JWT revokes.
	// +optional
	// +listType=map
	// +listMapKey=publicKey
	Revocations []Revocation `json:"revocations,omitempty"`

	// Imports are the resolved imports.
	// +optional
	Imports []ImportStatus `json:"imports,omitempty"`
}

// ImportStatus is a resolved import.
type ImportStatus struct {
	// Export is the export taken, as account/export, or
	// namespace/account/export from another namespace, or
	// publicKey/export from an account named by public key.
	// +optional
	Export string `json:"export,omitempty"`

	// Subject is the exported subject.
	// +optional
	Subject string `json:"subject,omitempty"`

	// LocalSubject is where the import appears in this account.
	// +optional
	LocalSubject string `json:"localSubject,omitempty"`

	// Type is the type of the export taken.
	// +optional
	Type ExportType `json:"type,omitempty"`

	// Activation is the state of the activation token of a Private export.
	// +optional
	Activation ActivationState `json:"activation,omitempty"`
}

// ActivationState is the state of an import's activation token.
// +kubebuilder:validation:Enum=Signed;Supplied
type ActivationState string

// Activation states.
const (
	// ActivationSigned is an activation token the auth controller minted.
	ActivationSigned ActivationState = "Signed"
	// ActivationSupplied is an activation token read from the Secret the
	// import names.
	ActivationSupplied ActivationState = "Supplied"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsAccount is an account, its limits, and its exports and imports.
type NatsAccount struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsAccountSpec `json:"spec"`
	// +optional
	Status NatsAccountStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsAccountList is a list of NatsAccount.
type NatsAccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsAccount `json:"items"`
}

func init() {
	register(&NatsAccount{}, &NatsAccountList{})
}
