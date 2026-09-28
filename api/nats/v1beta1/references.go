package v1beta1

// ObjectReference names an object whose kind the referring field fixes.
type ObjectReference struct {
	// Name of the referenced object.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace of the referenced object, the referrer's own when omitted.
	// Another namespace is admitted only by a NatsReferenceGrant there.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// SecretReference names a Secret in the referrer's namespace.
type SecretReference struct {
	// Name of a Secret in the referrer's namespace.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// CredentialsSecretKeySelector selects a NATS creds file from a Secret in
// the referrer's namespace.
type CredentialsSecretKeySelector struct {
	// Name of a Secret in the referrer's namespace.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key within the Secret, the key a NatsUser writes by default.
	// +optional
	// +kubebuilder:default=user.creds
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key,omitempty"`
}

// CASecretKeySelector selects a PEM CA bundle from a Secret in the
// referrer's namespace.
type CASecretKeySelector struct {
	// Name of a Secret in the referrer's namespace.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key within the Secret, the key cert-manager writes by default.
	// +optional
	// +kubebuilder:default=ca.crt
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key,omitempty"`
}

// Credentials is where a NATS creds file is read from or written to.
type Credentials struct {
	// SecretKeyRef selects the creds file.
	// +required
	SecretKeyRef CredentialsSecretKeySelector `json:"secretKeyRef"`
}

// CA is where a CA bundle is read from.
type CA struct {
	// SecretKeyRef selects the CA bundle.
	// +required
	SecretKeyRef CASecretKeySelector `json:"secretKeyRef"`
}
