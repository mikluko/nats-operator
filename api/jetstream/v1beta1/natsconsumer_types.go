package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// DeliverPolicy is where a consumer starts.
// +kubebuilder:validation:Enum=All;Last;New;ByStartSequence;ByStartTime;LastPerSubject
type DeliverPolicy string

// Deliver policies.
const (
	DeliverAll             DeliverPolicy = "All"
	DeliverLast            DeliverPolicy = "Last"
	DeliverNew             DeliverPolicy = "New"
	DeliverByStartSequence DeliverPolicy = "ByStartSequence"
	DeliverByStartTime     DeliverPolicy = "ByStartTime"
	DeliverLastPerSubject  DeliverPolicy = "LastPerSubject"
)

// AckPolicy is how a consumer's messages are acknowledged.
// +kubebuilder:validation:Enum=None;All;Explicit
type AckPolicy string

// Ack policies.
const (
	AckNone     AckPolicy = "None"
	AckAll      AckPolicy = "All"
	AckExplicit AckPolicy = "Explicit"
)

// ReplayPolicy is the pace a consumer replays at.
// +kubebuilder:validation:Enum=Instant;Original
type ReplayPolicy string

// Replay policies.
const (
	ReplayInstant  ReplayPolicy = "Instant"
	ReplayOriginal ReplayPolicy = "Original"
)

// PriorityPolicy is how a pull consumer picks among waiting clients.
// +kubebuilder:validation:Enum=None;Overflow;PinnedClient;Prioritized
type PriorityPolicy string

// Priority policies.
const (
	PriorityNone         PriorityPolicy = "None"
	PriorityOverflow     PriorityPolicy = "Overflow"
	PriorityPinnedClient PriorityPolicy = "PinnedClient"
	PriorityPrioritized  PriorityPolicy = "Prioritized"
)

// ConsumerConfig is nats.go's jetstream.ConsumerConfig: a push consumer when
// DeliverSubject is set, a pull consumer otherwise. An omitted field takes
// the server's value.
type ConsumerConfig struct {
	// Name is the server-side durable name, metadata.name when omitted.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name,omitempty"`

	// +optional
	Description string `json:"description,omitempty"`

	// +optional
	DeliverPolicy *DeliverPolicy `json:"deliverPolicy,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	OptStartSeq *int64 `json:"optStartSeq,omitempty"`

	// +optional
	OptStartTime *metav1.Time `json:"optStartTime,omitempty"`

	// +optional
	AckPolicy *AckPolicy `json:"ackPolicy,omitempty"`

	// +optional
	AckWait *metav1.Duration `json:"ackWait,omitempty"`

	// +optional
	MaxDeliver *int64 `json:"maxDeliver,omitempty"`

	// +optional
	BackOff []metav1.Duration `json:"backOff,omitempty"`

	// +optional
	FilterSubject string `json:"filterSubject,omitempty"`

	// +optional
	FilterSubjects []string `json:"filterSubjects,omitempty"`

	// +optional
	ReplayPolicy *ReplayPolicy `json:"replayPolicy,omitempty"`

	// RateLimit is in bits per second.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RateLimit *int64 `json:"rateLimit,omitempty"`

	// +optional
	SampleFrequency string `json:"sampleFrequency,omitempty"`

	// +optional
	MaxWaiting *int64 `json:"maxWaiting,omitempty"`

	// +optional
	MaxAckPending *int64 `json:"maxAckPending,omitempty"`

	// +optional
	FlowControl *bool `json:"flowControl,omitempty"`

	// +optional
	HeadersOnly *bool `json:"headersOnly,omitempty"`

	// +optional
	MaxRequestBatch *int64 `json:"maxRequestBatch,omitempty"`

	// +optional
	MaxRequestExpires *metav1.Duration `json:"maxRequestExpires,omitempty"`

	// +optional
	MaxRequestMaxBytes *int64 `json:"maxRequestMaxBytes,omitempty"`

	// +optional
	DeliverSubject string `json:"deliverSubject,omitempty"`

	// +optional
	DeliverGroup string `json:"deliverGroup,omitempty"`

	// +optional
	Heartbeat *metav1.Duration `json:"heartbeat,omitempty"`

	// +optional
	InactiveThreshold *metav1.Duration `json:"inactiveThreshold,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=5
	Replicas *int32 `json:"replicas,omitempty"`

	// +optional
	MemoryStorage *bool `json:"memoryStorage,omitempty"`

	// Metadata is merged with the ownership marker, which the controller
	// owns.
	// +optional
	Metadata map[string]string `json:"metadata,omitempty"`

	// +optional
	PauseUntil *metav1.Time `json:"pauseUntil,omitempty"`

	// +optional
	PriorityGroups []string `json:"priorityGroups,omitempty"`

	// +optional
	PriorityPolicy *PriorityPolicy `json:"priorityPolicy,omitempty"`

	// +optional
	PinnedTTL *metav1.Duration `json:"pinnedTTL,omitempty"`
}

// NatsConsumerSpec is the desired state of a consumer. deliverPolicy,
// ackPolicy, replayPolicy, optStartSeq, optStartTime, heartbeat, flowControl
// and maxWaiting are immutable unless recreateOnImmutableChange is set. The
// fields of the inlined ConsumerConfig mirror nats.go's
// jetstream.ConsumerConfig, the config as clients see it, and mean what
// their like-named fields there mean: https://pkg.go.dev/github.com/nats-io/nats.go/jetstream#ConsumerConfig.
// +kubebuilder:validation:XValidation:rule="has(self.stream) != has(self.streamRef)",message="set exactly one of stream and streamRef"
// +kubebuilder:validation:XValidation:rule="has(self.connectionRef) || has(self.streamRef)",message="connectionRef is required unless streamRef is set"
// +kubebuilder:validation:XValidation:rule="(has(self.recreateOnImmutableChange) && self.recreateOnImmutableChange) || !has(self.deliverPolicy) || !has(oldSelf.deliverPolicy) || self.deliverPolicy == oldSelf.deliverPolicy",message="deliverPolicy is immutable unless recreateOnImmutableChange is set"
// +kubebuilder:validation:XValidation:rule="(has(self.recreateOnImmutableChange) && self.recreateOnImmutableChange) || !has(self.ackPolicy) || !has(oldSelf.ackPolicy) || self.ackPolicy == oldSelf.ackPolicy",message="ackPolicy is immutable unless recreateOnImmutableChange is set"
// +kubebuilder:validation:XValidation:rule="(has(self.recreateOnImmutableChange) && self.recreateOnImmutableChange) || !has(self.replayPolicy) || !has(oldSelf.replayPolicy) || self.replayPolicy == oldSelf.replayPolicy",message="replayPolicy is immutable unless recreateOnImmutableChange is set"
// +kubebuilder:validation:XValidation:rule="(has(self.recreateOnImmutableChange) && self.recreateOnImmutableChange) || !has(self.optStartSeq) || !has(oldSelf.optStartSeq) || self.optStartSeq == oldSelf.optStartSeq",message="optStartSeq is immutable unless recreateOnImmutableChange is set"
// +kubebuilder:validation:XValidation:rule="(has(self.recreateOnImmutableChange) && self.recreateOnImmutableChange) || !has(self.optStartTime) || !has(oldSelf.optStartTime) || self.optStartTime == oldSelf.optStartTime",message="optStartTime is immutable unless recreateOnImmutableChange is set"
// +kubebuilder:validation:XValidation:rule="(has(self.recreateOnImmutableChange) && self.recreateOnImmutableChange) || !has(self.heartbeat) || !has(oldSelf.heartbeat) || duration(self.heartbeat) == duration(oldSelf.heartbeat)",message="heartbeat is immutable unless recreateOnImmutableChange is set"
// +kubebuilder:validation:XValidation:rule="(has(self.recreateOnImmutableChange) && self.recreateOnImmutableChange) || !has(self.flowControl) || !has(oldSelf.flowControl) || self.flowControl == oldSelf.flowControl",message="flowControl is immutable unless recreateOnImmutableChange is set"
// +kubebuilder:validation:XValidation:rule="(has(self.recreateOnImmutableChange) && self.recreateOnImmutableChange) || !has(self.maxWaiting) || !has(oldSelf.maxWaiting) || self.maxWaiting == oldSelf.maxWaiting",message="maxWaiting is immutable unless recreateOnImmutableChange is set"
type NatsConsumerSpec struct {
	// ConnectionRef names the NatsConnection whose credentials decide the
	// account, the stream's own when StreamRef is set and this is omitted.
	// +optional
	ConnectionRef *natsv1beta1.ObjectReference `json:"connectionRef,omitempty"`

	// Stream is the server-side name of a stream with no resource.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Stream string `json:"stream,omitempty"`

	// StreamRef names the NatsStream the consumer waits for and consumes.
	// +optional
	StreamRef *natsv1beta1.ObjectReference `json:"streamRef,omitempty"`

	Policies `json:",inline"`

	// DeletionPolicy is what deleting the resource does to the consumer.
	// +optional
	// +kubebuilder:default=Delete
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	// RecreateOnImmutableChange lets an immutable field change, by deleting
	// and recreating the consumer, which discards its delivery state.
	// +optional
	RecreateOnImmutableChange *bool `json:"recreateOnImmutableChange,omitempty"`

	ConsumerConfig `json:",inline"`
}

// ConsumerServerStatus is a consumer's state as the server reports it.
type ConsumerServerStatus struct {
	// Created is when the consumer was created on the server.
	// +optional
	Created *metav1.Time `json:"created,omitempty"`

	// Leader is the server leading the consumer's group.
	// +optional
	Leader string `json:"leader,omitempty"`

	// Replicas are the followers.
	// +optional
	Replicas []ReplicaStatus `json:"replicas,omitempty"`
}

// NatsConsumerStatus is the observed state of a consumer.
type NatsConsumerStatus struct {
	SyncStatus `json:",inline"`

	// Server is the consumer's state on the server.
	// +optional
	Server *ConsumerServerStatus `json:"server,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="(has(self.spec.name) ? self.spec.name : self.metadata.name) == (has(oldSelf.spec.name) ? oldSelf.spec.name : oldSelf.metadata.name)",message="the consumer name is immutable"
// +kubebuilder:printcolumn:name="Consumer",type=string,JSONPath=`.spec.name`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsConsumer is a JetStream consumer.
type NatsConsumer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsConsumerSpec `json:"spec"`
	// +optional
	Status NatsConsumerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsConsumerList is a list of NatsConsumer.
type NatsConsumerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsConsumer `json:"items"`
}

func init() {
	register(&NatsConsumer{}, &NatsConsumerList{})
}
