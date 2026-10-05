package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// Keys adopts existing seeds; omitted, the auth controller generates keys
// into Secrets that outlive the object.
type Keys struct {
	// Identity is the identity key's seed.
	// +optional
	Identity *IdentityKey `json:"identity,omitempty"`

	// Signing are the signing keys' seeds.
	// +optional
	// +listType=map
	// +listMapKey=name
	Signing []SigningKey `json:"signing,omitempty"`
}

// IdentityKey is an identity key's seed.
type IdentityKey struct {
	// SecretKeyRef selects the seed.
	// +required
	SecretKeyRef SeedSecretKeySelector `json:"secretKeyRef"`
}

// SigningKey is a signing key's seed.
type SigningKey struct {
	// Name identifies the key within the list.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// SecretKeyRef selects the seed.
	// +required
	SecretKeyRef SeedSecretKeySelector `json:"secretKeyRef"`

	// Retiring keeps the key listed, so what it signed stays valid, and
	// signs nothing new with it. Nothing removes a retiring key from the list.
	// +optional
	Retiring bool `json:"retiring,omitempty"`

	// Scope makes the key a scoped signing key of an account: it signs only
	// the NatsUsers naming the scope's role, and the servers hold each of
	// them to the scope. A NATS operator's signing key takes none.
	// +optional
	Scope *SigningKeyScope `json:"scope,omitempty"`
}

// SigningKeyScope is what every user signed by a scoped signing key is
// held to, carried in the account JWT.
type SigningKeyScope struct {
	// Role names the scope; a NatsUser whose spec.role is this is signed by
	// the first key of the role that is not retiring.
	// +required
	// +kubebuilder:validation:MinLength=1
	Role string `json:"role"`

	// Permissions are the publish and subscribe permissions of the key's
	// users.
	// +optional
	Permissions *Permissions `json:"permissions,omitempty"`

	// ConnectionTypes restricts how the key's users may connect; empty
	// allows any.
	// +optional
	// +listType=set
	ConnectionTypes []ConnectionType `json:"connectionTypes,omitempty"`

	// Limits are the limits of each of the key's users; an omitted limit is
	// unlimited.
	// +optional
	Limits *UserLimits `json:"limits,omitempty"`
}

// UserLimits are the limits of one user's connection.
type UserLimits struct {
	// Subscriptions is the maximum number of subscriptions.
	// +optional
	Subscriptions *int64 `json:"subscriptions,omitempty"`

	// Payload is the maximum message payload.
	// +optional
	Payload *resource.Quantity `json:"payload,omitempty"`
}

// SeedSecretKeySelector selects an nkey seed from a Secret in the
// referrer's namespace.
type SeedSecretKeySelector struct {
	// Name of a Secret in the referrer's namespace.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key within the Secret.
	// +required
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// AccountKind is a kind that answers an account reference.
// +kubebuilder:validation:Enum=NatsAccount;NatsSystemAccount
type AccountKind string

// Account kinds.
const (
	AccountKindAccount       AccountKind = "NatsAccount"
	AccountKindSystemAccount AccountKind = "NatsSystemAccount"
)

// AccountReference names a NatsAccount or a NatsSystemAccount.
type AccountReference struct {
	// Kind of the account.
	// +required
	Kind AccountKind `json:"kind"`

	natsv1beta1.ObjectReference `json:",inline"`
}

// Distribution is how many servers hold an account's current JWT.
type Distribution struct {
	// Servers is how many servers trust the account's NATS operator.
	// +optional
	Servers int32 `json:"servers,omitempty"`

	// Current is the number of servers holding the current JWT.
	// +optional
	Current int32 `json:"current,omitempty"`

	// LastPushTime is when the JWT was last pushed.
	// +optional
	LastPushTime *metav1.Time `json:"lastPushTime,omitempty"`
}

// Revocation is a user key an account revokes.
type Revocation struct {
	// PublicKey is the revoked user's key, or `*` for every user of the
	// account.
	// +required
	PublicKey string `json:"publicKey"`

	// At revokes the user's JWTs issued at or before it.
	// +required
	At metav1.Time `json:"at"`

	// Issuers are the keys that may have signed a revoked JWT: the account's
	// signing keys when the revocation was recorded, or, for a revocation a
	// NatsAccount took from a JWT the servers held, that JWT's signing keys
	// and the account's identity key. The revocation is dropped once none
	// of them is the account's identity key or among its signing keys.
	// +optional
	Issuers []string `json:"issuers,omitempty"`
}
