package authctl

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/mikluko/nats-operator/internal/conditions"
)

// The condition types the reconcilers of this package set in status.
const (
	ConditionReady = "Ready"
	// ConditionRetiringKeysInUse is True on a NatsOperator while an account
	// JWT it manages is still signed by a signing key marked retiring.
	ConditionRetiringKeysInUse = "RetiringKeysInUse"
	// ConditionDistributed is True on an account while every server
	// trusting its NATS operator holds its current JWT. On a user it is only
	// ever False, reason NoSystemConnection.
	ConditionDistributed = "Distributed"
	// ConditionRevocationsUnrecovered is True on an account, or on a
	// NatsOperator for its system account, signed after its status lost
	// its JWT and revocations while not every server could be asked for them.
	ConditionRevocationsUnrecovered = "RevocationsUnrecovered"
)

// The reasons those conditions carry.
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
	// ReasonUntrustedSigner is Distributed's reason on an account whose JWT
	// is signed by a key that a server does not list in the NATS operator
	// JWT it runs under; such a server is not counted current.
	ReasonUntrustedSigner = "UntrustedSigner"
	// ReasonRecovering is Ready's reason on an account whose status lost
	// its JWT and its revocations but records it distributed, while no
	// server can be asked for the JWT they are recovered from.
	ReasonRecovering = "RecoveringRevocations"
	// ReasonSeedLost is Ready's reason on an object whose status holds a
	// public key while the Secret its identity seed was generated into is
	// gone; no identity is generated in its place.
	ReasonSeedLost = "SeedLost"
	// ReasonNoSystemConnection is Distributed's reason on an account or
	// user while the auth controller runs without --system-connection: no
	// JWT reaches a server and a deleted user's connections stay open.
	ReasonNoSystemConnection = "NoSystemConnection"
	// ReasonPublicKeyInUse is Ready's reason on an account or user whose
	// public key another holds under the same NatsOperator or account.
	ReasonPublicKeyInUse = "PublicKeyInUse"
	// ReasonAccountNotAdmitted is Ready's reason on a NatsUser whose
	// NatsAccount no NatsReferenceGrant admits to its NatsOperator; the user
	// is not signed.
	ReasonAccountNotAdmitted = "AccountNotAdmitted"
	// ReasonExporterPending is Ready's and ReferencesResolved's reason on a
	// NatsAccount importing from a NatsAccount that has no public key yet;
	// the importer is not signed until it has one.
	ReasonExporterPending = "ExporterPending"
	// ReasonReconcileError is Ready's reason on an object whose last
	// reconcile failed with an error no other reason names.
	ReasonReconcileError = "ReconcileError"
)

// observe records the outcome err of reconciling generation gen: success sets
// *observed to gen, and an error but a conflict turns Ready False, reason
// ReconcileError, unless Ready is already False at gen.
func observe(conds *[]metav1.Condition, observed *int64, gen int64, err error) {
	if err == nil {
		*observed = gen
		return
	}
	if apierrors.IsConflict(err) {
		return
	}
	if c := meta.FindStatusCondition(*conds, ConditionReady); c != nil && c.Status == metav1.ConditionFalse && c.ObservedGeneration == gen {
		return
	}
	conditions.Set(conds, gen, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonReconcileError, Message: err.Error()})
}
