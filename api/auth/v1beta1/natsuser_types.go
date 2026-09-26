package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// UserPreset is a named permission set.
// +kubebuilder:validation:Enum=cluster-controller;jetstream-controller;auth-controller;readonly;leafnode
type UserPreset string

// User presets. The three controller presets are for system account users;
// readonly is for ordinary accounts.
const (
	UserPresetClusterController   UserPreset = "cluster-controller"
	UserPresetJetStreamController UserPreset = "jetstream-controller"
	UserPresetAuthController      UserPreset = "auth-controller"
	UserPresetReadonly            UserPreset = "readonly"
	UserPresetLeafnode            UserPreset = "leafnode"
)

// ConnectionType is a NATS connection type a user may connect as.
// +kubebuilder:validation:Enum=STANDARD;WEBSOCKET;LEAFNODE;LEAFNODE_WS;MQTT;MQTT_WS;IN_PROCESS
type ConnectionType string

// Connection types, as NATS user JWTs spell them.
const (
	ConnectionTypeStandard   ConnectionType = "STANDARD"
	ConnectionTypeWebsocket  ConnectionType = "WEBSOCKET"
	ConnectionTypeLeafnode   ConnectionType = "LEAFNODE"
	ConnectionTypeLeafnodeWS ConnectionType = "LEAFNODE_WS"
	ConnectionTypeMqtt       ConnectionType = "MQTT"
	ConnectionTypeMqttWS     ConnectionType = "MQTT_WS"
	ConnectionTypeInProcess  ConnectionType = "IN_PROCESS"
)

// NatsUserSpec is the desired state of a user.
// +kubebuilder:validation:XValidation:rule="!has(self.preset) || !has(self.permissions)",message="preset and permissions are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.preset) || !has(self.connectionTypes)",message="preset and connectionTypes are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.publicKey) || !has(self.credentials)",message="publicKey and credentials are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!has(self.preset) || !(self.preset in ['cluster-controller', 'jetstream-controller', 'auth-controller']) || self.accountRef.kind == 'NatsSystemAccount'",message="a controller preset is for a NatsSystemAccount user"
// +kubebuilder:validation:XValidation:rule="!has(self.preset) || self.preset != 'readonly' || self.accountRef.kind == 'NatsAccount'",message="the readonly preset is for a NatsAccount user"
type NatsUserSpec struct {
	// AccountRef names the account the user belongs to; it cannot change.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="accountRef is immutable"
	AccountRef AccountReference `json:"accountRef"`

	// Permissions are the user's publish and subscribe permissions.
	// +optional
	Permissions *Permissions `json:"permissions,omitempty"`

	// ConnectionTypes restricts how the user may connect; empty allows any.
	// +optional
	// +listType=set
	ConnectionTypes []ConnectionType `json:"connectionTypes,omitempty"`

	// Preset is a named permission set in place of Permissions and
	// ConnectionTypes.
	// +optional
	Preset UserPreset `json:"preset,omitempty"`

	// PublicKey is a key whose seed the client holds; the user then gets a
	// signed JWT in status and no creds Secret.
	// +optional
	// +kubebuilder:validation:MinLength=1
	PublicKey string `json:"publicKey,omitempty"`

	// Credentials is where the user's creds are written, in the shape a
	// NatsConnection reads.
	// +optional
	Credentials *natsv1beta1.Credentials `json:"credentials,omitempty"`
}

// Permissions are a user's publish and subscribe permissions.
type Permissions struct {
	// Publish permissions.
	// +optional
	Publish *SubjectPermissions `json:"publish,omitempty"`

	// Subscribe permissions.
	// +optional
	Subscribe *SubjectPermissions `json:"subscribe,omitempty"`
}

// SubjectPermissions allow and deny subjects.
type SubjectPermissions struct {
	// Allow lists subjects allowed.
	// +optional
	Allow []string `json:"allow,omitempty"`

	// Deny lists subjects denied.
	// +optional
	Deny []string `json:"deny,omitempty"`
}

// NatsUserStatus is the observed state of a user.
type NatsUserStatus struct {
	// ObservedGeneration is the generation the status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the user's state.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// PublicKey is the user's public key.
	// +optional
	PublicKey string `json:"publicKey,omitempty"`

	// JWT is the user JWT, published for a user that brings its own key.
	// +optional
	JWT string `json:"jwt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Account",type=string,JSONPath=`.spec.accountRef.name`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NatsUser is a user of an account.
type NatsUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec NatsUserSpec `json:"spec"`
	// +optional
	Status NatsUserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsUserList is a list of NatsUser.
type NatsUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsUser `json:"items"`
}

func init() {
	register(&NatsUser{}, &NatsUserList{})
}
