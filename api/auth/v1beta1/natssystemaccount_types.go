package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// NatsSystemAccountSpec is the desired state of a system account.
// +kubebuilder:validation:XValidation:rule="!has(self.publicKey) || !has(self.keys) || !has(self.keys.identity)",message="publicKey and keys.identity are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.publicKey) || (has(self.keys) && has(self.keys.signing) && size(self.keys.signing) > 0)",message="publicKey requires at least one signing key"
type NatsSystemAccountSpec struct {
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

	// Takeover is how a system account the servers already hold a JWT for
	// is taken over at the first signing.
	// +optional
	Takeover *Takeover `json:"takeover,omitempty"`
}

// NatsSystemAccountStatus is the observed state of a system account.
type NatsSystemAccountStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions: Ready, ReferencesResolved, Distributed.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// PublicKey is the account's public key.
	// +optional
	PublicKey string `json:"publicKey,omitempty"`

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
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsSystemAccount is a system account, signed only while a NatsOperator
// references it.
type NatsSystemAccount struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsSystemAccountSpec `json:"spec"`
	// +optional
	Status NatsSystemAccountStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsSystemAccountList is a list of NatsSystemAccount.
type NatsSystemAccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsSystemAccount `json:"items"`
}

func init() {
	register(&NatsSystemAccount{}, &NatsSystemAccountList{})
}
