package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NatsAccountTrustSpec names an account either by reference to a live
// NatsAccount or by its literal public key.
// +kubebuilder:validation:XValidation:rule="has(self.accountRef) != has(self.publicKey)",message="set exactly one of accountRef and publicKey"
// +kubebuilder:validation:XValidation:rule="!has(self.jwt) || has(self.publicKey)",message="jwt is set only beside publicKey"
type NatsAccountTrustSpec struct {
	// AccountRef names a NatsAccount in this Kubernetes cluster; the auth
	// controller then writes its public key and JWT into this object's
	// status.
	// +optional
	AccountRef *ObjectReference `json:"accountRef,omitempty"`

	// PublicKey is the account's public key.
	// +optional
	// +kubebuilder:validation:MinLength=1
	PublicKey string `json:"publicKey,omitempty"`

	// JWT is the account JWT a NatsCluster preloads.
	// +optional
	// +kubebuilder:validation:MinLength=1
	JWT string `json:"jwt,omitempty"`
}

// NatsAccountTrustStatus is the observed state of a NatsAccountTrust.
type NatsAccountTrustStatus struct {
	// ObservedGeneration is the generation the conditions describe.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the trust object's state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// PublicKey is the referenced account's public key, written by the
	// auth controller in the reference form.
	// +optional
	PublicKey string `json:"publicKey,omitempty"`

	// JWT is the referenced account's JWT, written by the auth controller
	// in the reference form.
	// +optional
	JWT string `json:"jwt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsAccountTrust is an account a NatsCluster preloads or a leaf binds a
// remote to.
type NatsAccountTrust struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsAccountTrustSpec `json:"spec"`
	// +optional
	Status NatsAccountTrustStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsAccountTrustList is a list of NatsAccountTrust.
type NatsAccountTrustList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsAccountTrust `json:"items"`
}

func init() {
	register(&NatsAccountTrust{}, &NatsAccountTrustList{})
}
