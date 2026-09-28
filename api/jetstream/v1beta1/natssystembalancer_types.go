package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// NatsSystemBalancerSpec is the desired state of a system balancer.
type NatsSystemBalancerSpec struct {
	// ConnectionRef names a NatsConnection with system credentials.
	// +required
	ConnectionRef natsv1beta1.ObjectReference `json:"connectionRef"`

	// Moves selects the kinds of move made.
	// +optional
	// +kubebuilder:default={}
	Moves *Moves `json:"moves,omitempty"`

	// Interval is the least time between two moves, 1m when omitted.
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="interval must be a positive duration"
	Interval *metav1.Duration `json:"interval,omitempty"`
}

// LeaderCapability is how far a system balancer can make leader moves.
// +kubebuilder:validation:Enum=Full;Partial;None
type LeaderCapability string

// Leader capabilities.
const (
	LeaderCapabilityFull    LeaderCapability = "Full"
	LeaderCapabilityPartial LeaderCapability = "Partial"
	LeaderCapabilityNone    LeaderCapability = "None"
)

// Capabilities are the moves a system balancer can make.
type Capabilities struct {
	// Placement reports whether placement moves are possible.
	// +optional
	Placement bool `json:"placement,omitempty"`

	// Leader is Full when every account holding a stream carries the
	// jetstream-stepdown export, None when none does, and Partial otherwise.
	// +optional
	Leader LeaderCapability `json:"leader,omitempty"`

	// LeaderReason explains a leader capability short of Full.
	// +optional
	LeaderReason string `json:"leaderReason,omitempty"`
}

// ServerLoad is one server's share of leaders and replicas.
type ServerLoad struct {
	// Name is the server's server_name.
	// +required
	Name string `json:"name"`

	// Leaders is the number of Raft groups the server leads.
	// +optional
	Leaders int32 `json:"leaders,omitempty"`

	// Replicas is the number of replicas the server holds.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
}

// Skew is the spread between the most and least loaded servers.
type Skew struct {
	// Leaders is the spread of leader counts.
	// +optional
	Leaders int32 `json:"leaders,omitempty"`

	// Replicas is the spread of replica counts.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
}

// MoveKind is a kind of balancer move.
// +kubebuilder:validation:Enum=Leader;Placement
type MoveKind string

// Move kinds.
const (
	MoveLeader    MoveKind = "Leader"
	MovePlacement MoveKind = "Placement"
)

// Move is a leader or placement move.
type Move struct {
	// Kind is Leader for a leader move, Placement for a placement move.
	// +optional
	Kind MoveKind `json:"kind,omitempty"`

	// Account is the public key of the account whose stream moved.
	// +optional
	Account string `json:"account,omitempty"`

	// Stream is the name of the stream moved, or of the stream whose
	// consumer's leader moved.
	// +optional
	Stream string `json:"stream,omitempty"`

	// Consumer is the name of the consumer whose leader moved; empty on a
	// stream's move.
	// +optional
	Consumer string `json:"consumer,omitempty"`

	// From is the server moved off.
	// +optional
	From string `json:"from,omitempty"`

	// To is the server moved to.
	// +optional
	To string `json:"to,omitempty"`

	// Time the move was requested.
	// +optional
	Time *metav1.Time `json:"time,omitempty"`
}

// NatsSystemBalancerStatus is the observed state of a system balancer.
type NatsSystemBalancerStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions: Ready, Holding.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Capabilities are the moves the balancer can make.
	// +optional
	Capabilities *Capabilities `json:"capabilities,omitempty"`

	// Servers report each server's load.
	// +optional
	// +listType=map
	// +listMapKey=name
	Servers []ServerLoad `json:"servers,omitempty"`

	// Skew is the spread across servers.
	// +optional
	Skew *Skew `json:"skew,omitempty"`

	// LastMove is the last move made.
	// +optional
	LastMove *Move `json:"lastMove,omitempty"`

	// Pending are moves requested and not yet complete.
	// +optional
	Pending []Move `json:"pending,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Holding",type=string,JSONPath=`.status.conditions[?(@.type=="Holding")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsSystemBalancer is the system balancer of one NATS cluster: it evens
// leaders and copies across its servers over every account.
type NatsSystemBalancer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsSystemBalancerSpec `json:"spec"`
	// +optional
	Status NatsSystemBalancerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsSystemBalancerList is a list of NatsSystemBalancer.
type NatsSystemBalancerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsSystemBalancer `json:"items"`
}

func init() {
	register(&NatsSystemBalancer{}, &NatsSystemBalancerList{})
}
