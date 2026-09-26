package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NatsReferenceGrantSpec lists who may reference what in the grant's
// namespace.
type NatsReferenceGrantSpec struct {
	// From are the referrers admitted.
	// +required
	// +kubebuilder:validation:MinItems=1
	From []ReferenceGrantFrom `json:"from"`

	// To are the objects in this namespace they may reference.
	// +required
	// +kubebuilder:validation:MinItems=1
	To []ReferenceGrantTo `json:"to"`
}

// ReferenceGrantFrom names a kind of referrer in one namespace.
type ReferenceGrantFrom struct {
	// Group of the referrer.
	// +required
	// +kubebuilder:validation:MinLength=1
	Group string `json:"group"`

	// Kind of the referrer.
	// +required
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`

	// Namespace of the referrer.
	// +required
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
}

// ReferenceGrantTo names a kind of object in the grant's namespace.
type ReferenceGrantTo struct {
	// Group of the referenced object.
	// +required
	// +kubebuilder:validation:MinLength=1
	Group string `json:"group"`

	// Kind of the referenced object.
	// +required
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`

	// Name of the referenced object; omitted, every object of the kind.
	// +optional
	Name string `json:"name,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsReferenceGrant admits references into its own namespace from the
// namespaces it lists; it has no status, since nothing acts on it.
type NatsReferenceGrant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsReferenceGrantSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// NatsReferenceGrantList is a list of NatsReferenceGrant.
type NatsReferenceGrantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsReferenceGrant `json:"items"`
}

func init() {
	register(&NatsReferenceGrant{}, &NatsReferenceGrantList{})
}
