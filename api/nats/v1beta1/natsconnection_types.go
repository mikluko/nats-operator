package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NatsConnectionSpec is how a NATS cluster is reached and whom as.
type NatsConnectionSpec struct {
	// Servers are the NATS URLs to dial.
	// +required
	// +kubebuilder:validation:MinItems=1
	Servers []string `json:"servers"`

	// TLS configures the client side of TLS toward the servers.
	// +optional
	TLS *ConnectionTLS `json:"tls,omitempty"`

	// Credentials decide the account the connection lands in; without them
	// it lands wherever the server puts an unauthenticated client.
	// +optional
	Credentials *Credentials `json:"credentials,omitempty"`
}

// ConnectionTLS is the client side of TLS toward NATS servers.
type ConnectionTLS struct {
	// CA verifies the servers' certificates.
	// +optional
	CA *CA `json:"ca,omitempty"`
}

// NatsConnectionStatus is the observed state of a NatsConnection.
type NatsConnectionStatus struct {
	// ObservedGeneration is the generation the conditions describe.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the connection's state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsConnection is an address and an identity on a NATS cluster, managed
// or not; the only way the JetStream controller reaches one.
type NatsConnection struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsConnectionSpec `json:"spec"`
	// +optional
	Status NatsConnectionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsConnectionList is a list of NatsConnection.
type NatsConnectionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsConnection `json:"items"`
}

func init() {
	register(&NatsConnection{}, &NatsConnectionList{})
}
