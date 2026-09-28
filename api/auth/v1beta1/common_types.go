package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// Keys adopts existing seeds; omitted, the auth controller generates keys
// into Secrets it owns.
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

	// Retiring marks the key for removal once everything it signed has
	// been re-signed by another.
	// +optional
	Retiring bool `json:"retiring,omitempty"`
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
	// Servers is the number of servers in the roster.
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
	// PublicKey is the revoked user's key.
	// +required
	PublicKey string `json:"publicKey"`

	// At revokes the user's JWTs issued at or before it.
	// +required
	At metav1.Time `json:"at"`

	// Issuers are the account's signing keys when the revocation was
	// recorded, the keys that may have signed a revoked JWT. The revocation
	// is dropped once none of them is among the account's signing keys.
	// +optional
	Issuers []string `json:"issuers,omitempty"`
}
