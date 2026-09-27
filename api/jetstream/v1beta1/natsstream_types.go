package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// RetentionPolicy is a stream's retention policy.
// +kubebuilder:validation:Enum=Limits;Interest;WorkQueue
type RetentionPolicy string

// Retention policies.
const (
	RetentionLimits    RetentionPolicy = "Limits"
	RetentionInterest  RetentionPolicy = "Interest"
	RetentionWorkQueue RetentionPolicy = "WorkQueue"
)

// DiscardPolicy is what a full stream discards.
// +kubebuilder:validation:Enum=Old;New
type DiscardPolicy string

// Discard policies.
const (
	DiscardOld DiscardPolicy = "Old"
	DiscardNew DiscardPolicy = "New"
)

// StoreCompression is a stream's storage compression.
// +kubebuilder:validation:Enum=None;S2
type StoreCompression string

// Store compressions.
const (
	CompressionNone StoreCompression = "None"
	CompressionS2   StoreCompression = "S2"
)

// PersistMode is a stream's persistence mode.
// +kubebuilder:validation:Enum=Default;Async
type PersistMode string

// Persist modes.
const (
	PersistDefault PersistMode = "Default"
	PersistAsync   PersistMode = "Async"
)

// StreamConfig is nats-server's StreamConfig. An omitted field takes the
// server's value, and the immutability rules compare a field only where
// both the old and the new spec set it.
type StreamConfig struct {
	// Name is the server-side stream name, metadata.name when omitted.
	// +optional
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name,omitempty"`

	// +optional
	Description string `json:"description,omitempty"`

	// +optional
	Subjects []string `json:"subjects,omitempty"`

	// Retention cannot change to or from WorkQueue.
	// +optional
	// +kubebuilder:validation:XValidation:rule="(self == 'WorkQueue') == (oldSelf == 'WorkQueue')",message="retention cannot change to or from WorkQueue"
	Retention *RetentionPolicy `json:"retention,omitempty"`

	// +optional
	MaxConsumers *int64 `json:"maxConsumers,omitempty"`

	// +optional
	MaxMsgs *int64 `json:"maxMsgs,omitempty"`

	// +optional
	MaxBytes *resource.Quantity `json:"maxBytes,omitempty"`

	// +optional
	Discard *DiscardPolicy `json:"discard,omitempty"`

	// +optional
	DiscardNewPerSubject *bool `json:"discardNewPerSubject,omitempty"`

	// +optional
	MaxAge *metav1.Duration `json:"maxAge,omitempty"`

	// +optional
	MaxMsgsPerSubject *int64 `json:"maxMsgsPerSubject,omitempty"`

	// +optional
	MaxMsgSize *resource.Quantity `json:"maxMsgSize,omitempty"`

	// Storage is immutable.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage is immutable"
	Storage *StorageType `json:"storage,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	Replicas *int32 `json:"replicas,omitempty"`

	// +optional
	NoAck *bool `json:"noAck,omitempty"`

	// +optional
	Duplicates *metav1.Duration `json:"duplicates,omitempty"`

	// Placement pins the stream; a changed cluster moves it.
	// +optional
	Placement *Placement `json:"placement,omitempty"`

	// Mirror cannot change; removing it promotes the mirror to a stream.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="mirror cannot change"
	Mirror *StreamSource `json:"mirror,omitempty"`

	// +optional
	Sources []StreamSource `json:"sources,omitempty"`

	// Sealed cannot be unset.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self || !oldSelf",message="sealed cannot be unset"
	Sealed *bool `json:"sealed,omitempty"`

	// DenyDelete cannot be unset.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self || !oldSelf",message="denyDelete cannot be unset"
	DenyDelete *bool `json:"denyDelete,omitempty"`

	// DenyPurge cannot be unset.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self || !oldSelf",message="denyPurge cannot be unset"
	DenyPurge *bool `json:"denyPurge,omitempty"`

	// +optional
	AllowRollup *bool `json:"allowRollup,omitempty"`

	// +optional
	Compression *StoreCompression `json:"compression,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	FirstSeq *int64 `json:"firstSeq,omitempty"`

	// +optional
	SubjectTransform *SubjectTransform `json:"subjectTransform,omitempty"`

	// +optional
	Republish *Republish `json:"republish,omitempty"`

	// +optional
	AllowDirect *bool `json:"allowDirect,omitempty"`

	// +optional
	MirrorDirect *bool `json:"mirrorDirect,omitempty"`

	// +optional
	ConsumerLimits *StreamConsumerLimits `json:"consumerLimits,omitempty"`

	// Metadata is merged with the ownership marker, which the controller
	// owns.
	// +optional
	Metadata map[string]string `json:"metadata,omitempty"`

	// AllowMsgTTL cannot be unset.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self || !oldSelf",message="allowMsgTTL cannot be unset"
	AllowMsgTTL *bool `json:"allowMsgTTL,omitempty"`

	// +optional
	SubjectDeleteMarkerTTL *metav1.Duration `json:"subjectDeleteMarkerTTL,omitempty"`

	// AllowMsgCounter is immutable.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="allowMsgCounter is immutable"
	AllowMsgCounter *bool `json:"allowMsgCounter,omitempty"`

	// +optional
	AllowAtomicPublish *bool `json:"allowAtomicPublish,omitempty"`

	// AllowMsgSchedules cannot be unset.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self || !oldSelf",message="allowMsgSchedules cannot be unset"
	AllowMsgSchedules *bool `json:"allowMsgSchedules,omitempty"`

	// PersistMode is immutable.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="persistMode is immutable"
	PersistMode *PersistMode `json:"persistMode,omitempty"`

	// +optional
	AllowBatchPublish *bool `json:"allowBatchPublish,omitempty"`
}

// StreamConsumerLimits are defaults for the stream's consumers.
type StreamConsumerLimits struct {
	// +optional
	InactiveThreshold *metav1.Duration `json:"inactiveThreshold,omitempty"`

	// +optional
	MaxAckPending *int64 `json:"maxAckPending,omitempty"`
}

// NatsStreamSpec is the desired state of a stream.
type NatsStreamSpec struct {
	// ConnectionRef names the NatsConnection whose credentials decide the
	// account.
	// +required
	ConnectionRef natsv1beta1.ObjectReference `json:"connectionRef"`

	Policies `json:",inline"`

	// DeletionPolicy is what deleting the resource does to the stream.
	// +optional
	// +kubebuilder:default=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy,omitempty"`

	StreamConfig `json:",inline"`
}

// NatsStreamStatus is the observed state of a stream.
type NatsStreamStatus struct {
	SyncStatus `json:",inline"`

	// Server is the stream's state on the server.
	// +optional
	Server *StreamServerStatus `json:"server,omitempty"`

	// Transfer is a move to another NATS cluster in progress.
	// +optional
	Transfer *StreamTransfer `json:"transfer,omitempty"`
}

// StreamTransfer is a stream's move between NATS clusters.
type StreamTransfer struct {
	// From is the NATS cluster the stream leaves.
	// +optional
	From string `json:"from,omitempty"`

	// To is the NATS cluster the stream moves to.
	// +optional
	To string `json:"to,omitempty"`

	// Started is when the move began.
	// +optional
	Started *metav1.Time `json:"started,omitempty"`

	// Replicas are the new replicas.
	// +optional
	Replicas []ReplicaStatus `json:"replicas,omitempty"`

	// Consumers is how many of the stream's consumers have moved.
	// +optional
	Consumers *TransferConsumers `json:"consumers,omitempty"`
}

// TransferConsumers counts the consumers moved with a stream.
type TransferConsumers struct {
	// Moved is the number moved.
	// +optional
	Moved int32 `json:"moved,omitempty"`

	// Total is the number to move.
	// +optional
	Total int32 `json:"total,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="(has(self.spec.name) ? self.spec.name : self.metadata.name) == (has(oldSelf.spec.name) ? oldSelf.spec.name : oldSelf.metadata.name)",message="the stream name is immutable"
// +kubebuilder:printcolumn:name="Stream",type=string,JSONPath=`.spec.name`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsStream is a JetStream stream.
type NatsStream struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsStreamSpec `json:"spec"`
	// +optional
	Status NatsStreamStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsStreamList is a list of NatsStream.
type NatsStreamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsStream `json:"items"`
}

func init() {
	register(&NatsStream{}, &NatsStreamList{})
}
