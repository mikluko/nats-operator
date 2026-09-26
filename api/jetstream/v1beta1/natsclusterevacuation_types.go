package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// NatsClusterEvacuationSpec is the desired state of an evacuation.
type NatsClusterEvacuationSpec struct {
	// ConnectionRef names a NatsConnection with system credentials.
	// +required
	ConnectionRef natsv1beta1.ObjectReference `json:"connectionRef"`

	// From is the NATS cluster emptied.
	// +required
	From EvacuationSource `json:"from"`

	// To is where the streams are moved.
	// +required
	To EvacuationTarget `json:"to"`
}

// EvacuationSource is the NATS cluster an evacuation empties.
type EvacuationSource struct {
	// Cluster is the NATS cluster's name.
	// +required
	// +kubebuilder:validation:MinLength=1
	Cluster string `json:"cluster"`
}

// EvacuationTarget is where an evacuation moves streams.
type EvacuationTarget struct {
	// ServerTags must match servers of the target only; the evacuation
	// refuses to start if a server of the source carries them.
	// +required
	// +kubebuilder:validation:MinItems=1
	ServerTags []string `json:"serverTags"`
}

// PinnedObject is a resource whose own spec pins the source cluster.
type PinnedObject struct {
	// Kind of the resource.
	// +required
	Kind string `json:"kind"`

	// Namespace of the resource.
	// +required
	Namespace string `json:"namespace"`

	// Name of the resource.
	// +required
	Name string `json:"name"`
}

// NatsClusterEvacuationStatus is the observed state of an evacuation.
type NatsClusterEvacuationStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions: Ready, Progressing.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Moved is the number of streams moved.
	// +optional
	Moved int32 `json:"moved,omitempty"`

	// InFlight is the number of moves in progress.
	// +optional
	InFlight int32 `json:"inFlight,omitempty"`

	// Pinned are the resources left in place; the evacuation is not Ready
	// while any remains.
	// +optional
	Pinned []PinnedObject `json:"pinned,omitempty"`

	// StalePlacement are the streams moved that no resource owns and whose
	// config still names the source cluster: while it exists, an update that
	// changes their placement returns them to it.
	// +optional
	StalePlacement []ServerStream `json:"stalePlacement,omitempty"`
}

// ServerStream names a stream on the server.
type ServerStream struct {
	// Account is the account's public key.
	// +required
	Account string `json:"account"`

	// Name of the stream.
	// +required
	Name string `json:"name"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="From",type=string,JSONPath=`.spec.from.cluster`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Moved",type=integer,JSONPath=`.status.moved`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsClusterEvacuation moves every stream, key-value bucket and object
// store in every account off one NATS cluster.
type NatsClusterEvacuation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsClusterEvacuationSpec `json:"spec"`
	// +optional
	Status NatsClusterEvacuationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsClusterEvacuationList is a list of NatsClusterEvacuation.
type NatsClusterEvacuationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsClusterEvacuation `json:"items"`
}

func init() {
	register(&NatsClusterEvacuation{}, &NatsClusterEvacuationList{})
}
