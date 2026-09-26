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

	// JWTTTL is the account JWT's lifetime; it is re-signed at half of it.
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

// AccountJetStreamLimits are an account's JetStream limits.
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

	// Type of the export.
	// +optional
	Type ExportType `json:"type,omitempty"`

	// Subject exported.
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

// Import takes another account's export by name.
type Import struct {
	// AccountRef names the exporting account.
	// +required
	AccountRef AccountReference `json:"accountRef"`

	// Export is the name of the export taken.
	// +required
	// +kubebuilder:validation:MinLength=1
	Export string `json:"export"`

	// LocalSubject is where the import appears in this account, the
	// exported subject when omitted.
	// +optional
	// +kubebuilder:validation:MinLength=1
	LocalSubject string `json:"localSubject,omitempty"`
}

// NatsAccountStatus is the observed state of an account.
type NatsAccountStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the account's state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// PublicKey is the account's public key.
	// +optional
	PublicKey string `json:"publicKey,omitempty"`

	// JWT is the current account JWT.
	// +optional
	JWT string `json:"jwt,omitempty"`

	// JWTHash identifies the current account JWT.
	// +optional
	JWTHash string `json:"jwtHash,omitempty"`

	// Distribution is how many servers hold the current JWT.
	// +optional
	Distribution *Distribution `json:"distribution,omitempty"`

	// Imports are the resolved imports.
	// +optional
	Imports []ImportStatus `json:"imports,omitempty"`
}

// ImportStatus is a resolved import.
type ImportStatus struct {
	// Export is the export taken, as account/export.
	// +optional
	Export string `json:"export,omitempty"`

	// Subject is the exported subject.
	// +optional
	Subject string `json:"subject,omitempty"`

	// LocalSubject is where the import appears in this account.
	// +optional
	LocalSubject string `json:"localSubject,omitempty"`

	// Type of the export.
	// +optional
	Type ExportType `json:"type,omitempty"`

	// Activation is the state of the activation token of a Private export.
	// +optional
	Activation string `json:"activation,omitempty"`
}

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
