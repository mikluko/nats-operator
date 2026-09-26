package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// NatsOperatorSpec is the desired state of a NATS operator.
// +kubebuilder:validation:XValidation:rule="!has(self.jwt) || !has(self.keys) || !has(self.keys.identity)",message="jwt and keys.identity are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.jwt) || (has(self.keys) && has(self.keys.signing) && size(self.keys.signing) > 0)",message="jwt requires at least one signing key"
type NatsOperatorSpec struct {
	// Keys adopts existing seeds.
	// +optional
	Keys *Keys `json:"keys,omitempty"`

	// JWT is an operator JWT signed elsewhere, keeping the identity key
	// offline.
	// +optional
	// +kubebuilder:validation:MinLength=1
	JWT string `json:"jwt,omitempty"`

	// SystemAccountRef names the NatsSystemAccount the operator JWT names.
	// +required
	SystemAccountRef natsv1beta1.ObjectReference `json:"systemAccountRef"`
}

// NatsOperatorStatus is the observed state of a NATS operator.
type NatsOperatorStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the operator's state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// PublicKey is the identity key's public key.
	// +optional
	PublicKey string `json:"publicKey,omitempty"`

	// SigningKeys are the signing keys' public keys.
	// +optional
	SigningKeys []string `json:"signingKeys,omitempty"`

	// SeedSecrets name the Secrets holding the generated seeds.
	// +optional
	SeedSecrets *SeedSecrets `json:"seedSecrets,omitempty"`

	// JWT is the operator JWT.
	// +optional
	JWT string `json:"jwt,omitempty"`

	// SystemAccount is the system account the operator JWT names.
	// +optional
	SystemAccount *SystemAccountStatus `json:"systemAccount,omitempty"`
}

// SeedSecrets name the Secrets holding generated seeds.
type SeedSecrets struct {
	// Identity names the identity seed's Secret.
	// +optional
	Identity string `json:"identity,omitempty"`

	// Signing name the signing seeds' Secrets.
	// +optional
	Signing []string `json:"signing,omitempty"`
}

// SystemAccountStatus is the system account an operator JWT names.
type SystemAccountStatus struct {
	// Name of the NatsSystemAccount.
	// +optional
	Name string `json:"name,omitempty"`

	// PublicKey of the system account.
	// +optional
	PublicKey string `json:"publicKey,omitempty"`

	// JWT of the system account.
	// +optional
	JWT string `json:"jwt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsOperator is a NATS operator the auth controller signs for.
type NatsOperator struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsOperatorSpec `json:"spec"`
	// +optional
	Status NatsOperatorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsOperatorList is a list of NatsOperator.
type NatsOperatorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsOperator `json:"items"`
}

func init() {
	register(&NatsOperator{}, &NatsOperatorList{})
}
