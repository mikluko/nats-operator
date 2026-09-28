package authctl

// Condition types.
const (
	ConditionReady = "Ready"
	// ConditionRetiringKeysInUse is True on a NatsOperator while an account
	// JWT it manages is still signed by a signing key marked retiring, so
	// removing that key would invalidate it.
	ConditionRetiringKeysInUse = "RetiringKeysInUse"
	// ConditionDistributed is True on an account while every server
	// trusting its operator holds its current JWT.
	ConditionDistributed = "Distributed"
	// ConditionRevocationsUnrecovered is True on an account, or on a
	// NatsOperator for its system account, signed after its status lost
	// its JWT and revocations while not every server could be asked for them.
	ConditionRevocationsUnrecovered = "RevocationsUnrecovered"
)

// Condition reasons.
const (
	ReasonSigned             = "Signed"
	ReasonMirrored           = "Mirrored"
	ReasonKeysPending        = "KeysPending"
	ReasonInvalidKeys        = "InvalidKeys"
	ReasonInvalidJWT         = "InvalidJWT"
	ReasonNotFound           = "ReferenceNotFound"
	ReasonPending            = "Pending"
	ReasonNotReferenced      = "NotReferenced"
	ReasonOperatorMismatch   = "OperatorMismatch"
	ReasonResolved           = "Resolved"
	ReasonAllImportsResolved = "AllImportsResolved"
	ReasonImportsUnresolved  = "ImportsUnresolved"
	ReasonInUse              = "InUse"
	ReasonNoneInUse          = "NoneInUse"
	ReasonSecretConflict     = "SecretConflict"
	ReasonRevoking           = "Revoking"
	ReasonDistributing       = "Distributing"
	ReasonKicking            = "Kicking"
	ReasonDistributed        = "Distributed"
	ReasonAllServersCurrent  = "AllServersCurrent"
	ReasonServersBehind      = "ServersBehind"
	ReasonUnreachable        = "Unreachable"
	ReasonUnobserved         = "Unobserved"
	// ReasonRecovering is Ready's reason on an account whose status lost
	// its JWT and its revocations but records it distributed, while no
	// server can be asked for the JWT they are recovered from.
	ReasonRecovering = "RecoveringRevocations"
	// ReasonSeedLost is Ready's reason on an object whose status holds a
	// public key while the Secret its identity seed was generated into is
	// gone; no identity is generated in its place.
	ReasonSeedLost = "SeedLost"
	// ReasonPublicKeyInUse is Ready's reason on an account or user whose
	// public key another holds under the same NatsOperator or account.
	ReasonPublicKeyInUse = "PublicKeyInUse"
)
