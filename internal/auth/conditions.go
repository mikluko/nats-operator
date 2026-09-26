package auth

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition types.
const (
	ConditionReady = "Ready"
	// ConditionRetiringKeysInUse is True on a NatsOperator while an account
	// JWT it manages is still signed by a signing key marked retiring, so
	// removing that key would invalidate it.
	ConditionRetiringKeysInUse = "RetiringKeysInUse"
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
)

// setCondition sets a condition of generation gen on conds.
func setCondition(conds *[]metav1.Condition, gen int64, typ string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               typ,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: gen,
	})
}
