package auth

import (
	"cmp"
	"slices"
	"time"

	"github.com/nats-io/jwt/v2"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// accountRevocations returns the revocations an account JWT carries: every
// one prev, the account's current JWT, carries when it names the same
// account as pub, and one per user among users that has been signed and is
// being deleted or no longer admitted to the account. A key revoked twice is
// revoked at the later time. The JWT carried forward is the only record of a
// revocation whose user is gone; user JWTs never expire, so none is pruned.
func accountRevocations(prev, pub string, users []authv1beta1.NatsUser) []jwtplane.Revocation {
	at := map[string]time.Time{}
	revoke := func(key string, t time.Time) {
		if t.After(at[key]) {
			at[key] = t
		}
	}
	if c, err := jwt.DecodeAccountClaims(prev); err == nil && c.Subject == pub {
		for key, ts := range c.Revocations {
			revoke(key, time.Unix(ts, 0))
		}
	}
	for i := range users {
		if t, ok := userRevokedAt(&users[i]); ok {
			revoke(users[i].Status.PublicKey, t)
		}
	}
	out := make([]jwtplane.Revocation, 0, len(at))
	for key, t := range at {
		out = append(out, jwtplane.Revocation{PublicKey: key, At: t})
	}
	slices.SortFunc(out, func(a, b jwtplane.Revocation) int { return cmp.Compare(a.PublicKey, b.PublicKey) })
	return out
}

// userRevokedAt returns when u's key is revoked in its account: at its
// deletion while the revocation finalizer holds it, or when its reference to
// the account stopped being admitted. ok is false for a user never signed,
// or one that is neither.
func userRevokedAt(u *authv1beta1.NatsUser) (time.Time, bool) {
	if u.Status.PublicKey == "" {
		return time.Time{}, false
	}
	if u.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(u, UserFinalizer) {
		return u.DeletionTimestamp.Time, true
	}
	cond := meta.FindStatusCondition(u.Status.Conditions, grant.ConditionReferencesResolved)
	if cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == grant.ReasonNoGrant {
		return cond.LastTransitionTime.Time, true
	}
	return time.Time{}, false
}

// revokedSince reports whether accountJWT revokes the user key pub at t or
// later.
func revokedSince(accountJWT, pub string, t time.Time) bool {
	c, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return false
	}
	ts, ok := c.Revocations[pub]
	return ok && ts >= t.Unix()
}

// userRevoked reports whether accountJWT revokes userJWT.
func userRevoked(accountJWT, userJWT string) bool {
	ac, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return false
	}
	uc, err := jwt.DecodeUserClaims(userJWT)
	if err != nil {
		return false
	}
	return ac.IsClaimRevoked(uc)
}
