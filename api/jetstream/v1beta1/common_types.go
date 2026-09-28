package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// AdoptionPolicy is what the JetStream controller does with an object on
// the server that it does not own.
// +kubebuilder:validation:Enum=Never;Adopt;AdoptOrCreate
type AdoptionPolicy string

// Adoption policies.
const (
	// AdoptionNever creates the object, and goes Terminal on one it does
	// not own.
	AdoptionNever AdoptionPolicy = "Never"
	// AdoptionAdopt requires the object to exist and writes its config into
	// spec.
	AdoptionAdopt AdoptionPolicy = "Adopt"
	// AdoptionAdoptOrCreate applies spec whether or not the object exists,
	// and late-initializes omitted fields from the server.
	AdoptionAdoptOrCreate AdoptionPolicy = "AdoptOrCreate"
)

// DeletionPolicy is what deleting the resource does to the object on the
// server.
// +kubebuilder:validation:Enum=Retain;Delete
type DeletionPolicy string

// Deletion policies.
const (
	DeletionRetain DeletionPolicy = "Retain"
	DeletionDelete DeletionPolicy = "Delete"
)

// TerminalPolicy is what a Terminal condition waits for.
// +kubebuilder:validation:Enum=Hold;Retry
type TerminalPolicy string

// Terminal policies.
const (
	// TerminalHold waits for an edit to the resource.
	TerminalHold TerminalPolicy = "Hold"
	// TerminalRetry rechecks every resync period.
	TerminalRetry TerminalPolicy = "Retry"
)

// Policies are the lifecycle policies every JetStream resource carries
// beside its deletion policy, whose default differs by kind.
type Policies struct {
	// AdoptionPolicy is what happens to an object of the same name the
	// controller does not own.
	// +optional
	// +kubebuilder:default=Never
	AdoptionPolicy AdoptionPolicy `json:"adoptionPolicy,omitempty"`

	// TerminalPolicy is what a Terminal condition waits for.
	// +optional
	// +kubebuilder:default=Hold
	TerminalPolicy TerminalPolicy `json:"terminalPolicy,omitempty"`
}

// StorageType is a JetStream storage backend.
// +kubebuilder:validation:Enum=File;Memory
type StorageType string

// Storage types.
const (
	StorageFile   StorageType = "File"
	StorageMemory StorageType = "Memory"
)

// Placement is where a stream's replicas are placed.
type Placement struct {
	// Cluster is the NATS cluster the replicas are placed in.
	// +optional
	Cluster string `json:"cluster,omitempty"`

	// Tags are server tags every replica's server carries.
	// +optional
	Tags []string `json:"tags,omitempty"`

	// Preferred is the server preferred as leader.
	// +optional
	Preferred string `json:"preferred,omitempty"`
}

// StreamSource is a stream a mirror or source copies from.
type StreamSource struct {
	// Name is the origin stream's server-side name.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// OptStartSeq is the origin sequence to start at.
	// +optional
	OptStartSeq *int64 `json:"optStartSeq,omitempty"`

	// OptStartTime is the origin time to start at.
	// +optional
	OptStartTime *metav1.Time `json:"optStartTime,omitempty"`

	// FilterSubject filters the origin's messages.
	// +optional
	FilterSubject string `json:"filterSubject,omitempty"`

	// SubjectTransforms filter and transform the origin's subjects.
	// +optional
	SubjectTransforms []SubjectTransform `json:"subjectTransforms,omitempty"`

	// External qualifies an origin in another account or domain.
	// +optional
	External *ExternalStream `json:"external,omitempty"`

	// Consumer is a durable consumer on the origin used for sourcing.
	// +optional
	Consumer *StreamConsumerSource `json:"consumer,omitempty"`
}

// ExternalStream is the API and deliver prefixes of an origin in another
// account or domain.
type ExternalStream struct {
	// APIPrefix is the JetStream API prefix.
	// +required
	APIPrefix string `json:"apiPrefix"`

	// DeliverPrefix is the deliver subject prefix.
	// +optional
	DeliverPrefix string `json:"deliverPrefix,omitempty"`
}

// StreamConsumerSource is a durable consumer used for sourcing.
type StreamConsumerSource struct {
	// Name is the server's consumer name.
	// +optional
	Name string `json:"name,omitempty"`

	// DeliverSubject is the server's deliver_subject.
	// +optional
	DeliverSubject string `json:"deliverSubject,omitempty"`
}

// SubjectTransform maps a source subject to a destination subject.
type SubjectTransform struct {
	// Source is the server's src, the subjects transformed.
	// +optional
	Source string `json:"source,omitempty"`

	// Destination is the server's dest, the subject they become.
	// +required
	Destination string `json:"destination"`
}

// Republish republishes stored messages.
type Republish struct {
	// Source is the server's src, the stored subjects republished.
	// +optional
	Source string `json:"source,omitempty"`

	// Destination is the server's dest, the subject they are republished
	// to.
	// +required
	Destination string `json:"destination"`

	// HeadersOnly republishes headers without the payload.
	// +optional
	HeadersOnly *bool `json:"headersOnly,omitempty"`
}

// OwnershipOrigin is how the controller came to own an object.
// +kubebuilder:validation:Enum=Created;Adopted
type OwnershipOrigin string

// Ownership origins.
const (
	OwnershipCreated OwnershipOrigin = "Created"
	OwnershipAdopted OwnershipOrigin = "Adopted"
)

// Ownership is the marker naming the resource that owns an object on the
// server.
type Ownership struct {
	// Origin is how the object came to be owned.
	// +optional
	Origin OwnershipOrigin `json:"origin,omitempty"`

	// UID is the owning resource's UID, as written in the object's metadata.
	// +optional
	UID types.UID `json:"uid,omitempty"`
}

// SyncStatus is the status every JetStream object resource reports.
type SyncStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions: Ready, Synced, Terminal, Adopted.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// LastSyncedTime is when the server object was last compared to spec.
	// +optional
	LastSyncedTime *metav1.Time `json:"lastSyncedTime,omitempty"`

	// NextCheckTime is when a Terminal condition under the Retry policy is
	// next rechecked.
	// +optional
	NextCheckTime *metav1.Time `json:"nextCheckTime,omitempty"`

	// Ownership is the ownership marker on the server object.
	// +optional
	Ownership *Ownership `json:"ownership,omitempty"`
}

// ReplicaStatus is one replica of a Raft group.
type ReplicaStatus struct {
	// Name is the server_name of the server holding the replica.
	// +required
	Name string `json:"name"`

	// Current reports whether the replica is current.
	// +optional
	Current bool `json:"current,omitempty"`

	// Lag is how many operations the replica is behind.
	// +optional
	Lag int64 `json:"lag,omitempty"`
}

// StreamServerStatus is a stream's state as the server reports it.
type StreamServerStatus struct {
	// Created is when the stream was created on the server.
	// +optional
	Created *metav1.Time `json:"created,omitempty"`

	// Leader is the server leading the stream's group.
	// +optional
	Leader string `json:"leader,omitempty"`

	// Replicas are the followers.
	// +optional
	Replicas []ReplicaStatus `json:"replicas,omitempty"`

	// Messages is the number of messages stored.
	// +optional
	Messages int64 `json:"messages,omitempty"`

	// Bytes is the size of the messages stored.
	// +optional
	Bytes *resource.Quantity `json:"bytes,omitempty"`
}
