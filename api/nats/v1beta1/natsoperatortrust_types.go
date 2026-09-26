package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NatsOperatorTrustSpec holds trust roots either by reference to a live
// NatsOperator or as literal JWTs.
// +kubebuilder:validation:XValidation:rule="has(self.operatorRef) != (has(self.operatorJWT) || has(self.systemAccountJWT))",message="set exactly one of operatorRef and the literal operatorJWT and systemAccountJWT"
// +kubebuilder:validation:XValidation:rule="has(self.operatorJWT) == has(self.systemAccountJWT)",message="operatorJWT and systemAccountJWT are set together"
type NatsOperatorTrustSpec struct {
	// OperatorRef names a NatsOperator in this Kubernetes cluster; the auth
	// controller then writes its JWTs into this object's status.
	// +optional
	OperatorRef *ObjectReference `json:"operatorRef,omitempty"`

	// OperatorJWT is the NATS operator JWT.
	// +optional
	// +kubebuilder:validation:MinLength=1
	OperatorJWT string `json:"operatorJWT,omitempty"`

	// SystemAccountJWT is the system account JWT.
	// +optional
	// +kubebuilder:validation:MinLength=1
	SystemAccountJWT string `json:"systemAccountJWT,omitempty"`
}

// NatsOperatorTrustStatus is the observed state of a NatsOperatorTrust.
type NatsOperatorTrustStatus struct {
	// ObservedGeneration is the generation the conditions describe.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the trust object's state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// OperatorJWT is the referenced operator's JWT, written by the auth
	// controller in the reference form.
	// +optional
	OperatorJWT string `json:"operatorJWT,omitempty"`

	// SystemAccountJWT is the referenced operator's system account JWT,
	// written by the auth controller in the reference form.
	// +optional
	SystemAccountJWT string `json:"systemAccountJWT,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsOperatorTrust is the trust roots a NatsCluster boots from: the NATS
// operator JWT and system account JWT.
type NatsOperatorTrust struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsOperatorTrustSpec `json:"spec"`
	// +optional
	Status NatsOperatorTrustStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsOperatorTrustList is a list of NatsOperatorTrust.
type NatsOperatorTrustList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsOperatorTrust `json:"items"`
}

func init() {
	register(&NatsOperatorTrust{}, &NatsOperatorTrustList{})
}
