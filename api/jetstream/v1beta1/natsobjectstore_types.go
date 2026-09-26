package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// ObjectStoreConfig is nats.go's ObjectStoreConfig, with the bucket under
// Name. An omitted field takes the server's value.
type ObjectStoreConfig struct {
	// Name is the bucket, metadata.name when omitted.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name,omitempty"`

	// +optional
	Description string `json:"description,omitempty"`

	// +optional
	TTL *metav1.Duration `json:"ttl,omitempty"`

	// +optional
	MaxBytes *resource.Quantity `json:"maxBytes,omitempty"`

	// +optional
	Storage *StorageType `json:"storage,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	Replicas *int32 `json:"replicas,omitempty"`

	// +optional
	Placement *Placement `json:"placement,omitempty"`

	// +optional
	Compression *bool `json:"compression,omitempty"`

	// Metadata is merged with the ownership marker, which the controller
	// owns.
	// +optional
	Metadata map[string]string `json:"metadata,omitempty"`
}

// NatsObjectStoreSpec is the desired state of an object store.
type NatsObjectStoreSpec struct {
	// ConnectionRef names the NatsConnection whose credentials decide the
	// account.
	// +required
	ConnectionRef natsv1beta1.ObjectReference `json:"connectionRef"`

	Policies `json:",inline"`

	// DeletionPolicy is what deleting the resource does to the object
	// store.
	// +optional
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	ObjectStoreConfig `json:",inline"`
}

// NatsObjectStoreStatus is the observed state of an object store.
type NatsObjectStoreStatus struct {
	SyncStatus `json:",inline"`

	// Server is the object store's stream state on the server.
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

// NatsObjectStore is a JetStream object store.
type NatsObjectStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsObjectStoreSpec `json:"spec"`
	// +optional
	Status NatsObjectStoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsObjectStoreList is a list of NatsObjectStore.
type NatsObjectStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsObjectStore `json:"items"`
}

func init() {
	register(&NatsObjectStore{}, &NatsObjectStoreList{})
}
