package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// Moves selects the kinds of move a balancer makes.
type Moves struct {
	// Leader enables leader moves.
	// +optional
	// +kubebuilder:default=true
	Leader *bool `json:"leader,omitempty"`

	// Placement enables placement moves.
	// +optional
	// +kubebuilder:default=false
	Placement *bool `json:"placement,omitempty"`
}

// Pool is a declared group of streams balanced apart from the account's
// others.
type Pool struct {
	// Name is unique among the balancer's pools and appears in status.pools.
	// +required
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Selector matches NatsStream, NatsKeyValue and NatsObjectStore
	// resources in the balancer's namespace by label.
	// +required
	Selector metav1.LabelSelector `json:"selector"`
}

// NatsBalancerSpec is the desired state of an account balancer.
type NatsBalancerSpec struct {
	// ConnectionRef names the NatsConnection whose credentials decide the
	// account.
	// +required
	ConnectionRef natsv1beta1.ObjectReference `json:"connectionRef"`

	// Pools are judged apart; a stream matching several belongs to the
	// first. With none declared the account is one pool.
	// +optional
	// +listType=map
	// +listMapKey=name
	Pools []Pool `json:"pools,omitempty"`

	// Moves selects the kinds of move made.
	// +optional
	// +kubebuilder:default={}
	Moves *Moves `json:"moves,omitempty"`

	// Interval is the least time between two moves, 1m when omitted.
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="interval must be a positive duration"
	Interval *metav1.Duration `json:"interval,omitempty"`
}

// PoolStatus is one pool's evenness.
type PoolStatus struct {
	// Name of the pool; the default pool is "(default)".
	// +required
	Name string `json:"name"`

	// Streams is the number of streams in the pool.
	// +optional
	Streams int32 `json:"streams,omitempty"`

	// LeaderSkew is the spread of leader counts across servers.
	// +optional
	LeaderSkew int32 `json:"leaderSkew,omitempty"`
}

// NatsBalancerStatus is the observed state of an account balancer.
type NatsBalancerStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions: Ready, Holding, Overlapping.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Pools report each pool's evenness.
	// +optional
	// +listType=map
	// +listMapKey=name
	Pools []PoolStatus `json:"pools,omitempty"`

	// LastMove is the last move made; the next waits for spec.interval
	// after its time.
	// +optional
	LastMove *Move `json:"lastMove,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Holding",type=string,JSONPath=`.status.conditions[?(@.type=="Holding")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsBalancer is an account balancer: it evens leaders and copies within
// the pools of one account, yielding to the system balancer.
type NatsBalancer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsBalancerSpec `json:"spec"`
	// +optional
	Status NatsBalancerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsBalancerList is a list of NatsBalancer.
type NatsBalancerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsBalancer `json:"items"`
}

func init() {
	register(&NatsBalancer{}, &NatsBalancerList{})
}
