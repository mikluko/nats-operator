package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// KeyValueConfig is nats.go's KeyValueConfig, with the bucket under Name.
// An omitted field takes the server's value.
type KeyValueConfig struct {
	// Name is the bucket, metadata.name when omitted.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name,omitempty"`

	// +optional
	Description string `json:"description,omitempty"`

	// +optional
	MaxValueSize *resource.Quantity `json:"maxValueSize,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	History *int32 `json:"history,omitempty"`

	// +optional
	TTL *metav1.Duration `json:"ttl,omitempty"`

	// +optional
	MaxBytes *resource.Quantity `json:"maxBytes,omitempty"`

	// Storage is immutable.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage is immutable"
	Storage *StorageType `json:"storage,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	Replicas *int32 `json:"replicas,omitempty"`

	// +optional
	Placement *Placement `json:"placement,omitempty"`

	// +optional
	Republish *Republish `json:"republish,omitempty"`

	// +optional
	Mirror *StreamSource `json:"mirror,omitempty"`

	// +optional
	Sources []StreamSource `json:"sources,omitempty"`

	// +optional
	Compression *bool `json:"compression,omitempty"`

	// +optional
	LimitMarkerTTL *metav1.Duration `json:"limitMarkerTTL,omitempty"`

	// Metadata is merged with the ownership marker, which the controller
	// owns.
	// +optional
	Metadata map[string]string `json:"metadata,omitempty"`
}

// NatsKeyValueSpec is the desired state of a key-value bucket. The fields of
// the inlined KeyValueConfig mirror nats.go's jetstream.KeyValueConfig, the
// config as clients see it, and mean what their like-named fields there
// mean: https://pkg.go.dev/github.com/nats-io/nats.go/jetstream#KeyValueConfig.
type NatsKeyValueSpec struct {
	// ConnectionRef names the NatsConnection whose credentials decide the
	// account; it is immutable.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="connectionRef is immutable"
	ConnectionRef natsv1beta1.ObjectReference `json:"connectionRef"`

	Policies `json:",inline"`

	// DeletionPolicy is what deleting the resource does to the bucket.
	// +optional
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	KeyValueConfig `json:",inline"`
}

// NatsKeyValueStatus is the observed state of a key-value bucket.
type NatsKeyValueStatus struct {
	SyncStatus `json:",inline"`

	// Server is the bucket's stream state on the server.
	// +optional
	Server *StreamServerStatus `json:"server,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="(has(self.spec.name) ? self.spec.name : self.metadata.name) == (has(oldSelf.spec.name) ? oldSelf.spec.name : oldSelf.metadata.name)",message="the bucket name is immutable"
// +kubebuilder:printcolumn:name="Bucket",type=string,JSONPath=`.spec.name`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsKeyValue is a JetStream key-value bucket.
type NatsKeyValue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsKeyValueSpec `json:"spec"`
	// +optional
	Status NatsKeyValueStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsKeyValueList is a list of NatsKeyValue.
type NatsKeyValueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsKeyValue `json:"items"`
}

func init() {
	register(&NatsKeyValue{}, &NatsKeyValueList{})
}
