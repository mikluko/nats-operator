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

// accountRevocations returns the revocations an account with public key pub
// and signing keys signing records and signs: those recorded, those prev,
// its current JWT, carries when it names pub, and one per user among users
// that has been signed and is being deleted or no longer admitted. A
// revocation new to the record has as issuers the signing keys prev lists,
// or signing for a user's. A key revoked again later is revoked at the later
// time, by the issuers of both. A revocation none of whose issuers is in signing
// is dropped: no JWT it revokes is still valid.
func accountRevocations(recorded []authv1beta1.Revocation, prev, pub string, signing []string, users []authv1beta1.NatsUser) []authv1beta1.Revocation {
	byKey := map[string]*authv1beta1.Revocation{}
	revoke := func(key string, at time.Time, issuers []string) {
		r, ok := byKey[key]
		if ok && !at.After(r.At.Time) {
			return
		}
		if !ok {
			r = &authv1beta1.Revocation{PublicKey: key}
			byKey[key] = r
		}
		r.At = metav1.Time{Time: at}
		for _, k := range issuers {
			if !slices.Contains(r.Issuers, k) {
				r.Issuers = append(r.Issuers, k)
			}
		}
	}
	for _, r := range recorded {
		revoke(r.PublicKey, r.At.Time, r.Issuers)
	}
	if c, err := jwt.DecodeAccountClaims(prev); err == nil && c.Subject == pub {
		for key, ts := range c.Revocations {
			revoke(key, time.Unix(ts, 0), c.SigningKeys.Keys())
		}
	}
	for i := range users {
		if t, ok := userRevokedAt(&users[i]); ok {
			revoke(users[i].Status.PublicKey, t, signing)
		}
	}
	out := make([]authv1beta1.Revocation, 0, len(byKey))
	for _, r := range byKey {
		if !slices.ContainsFunc(r.Issuers, func(k string) bool { return slices.Contains(signing, k) }) {
			continue
		}
		slices.Sort(r.Issuers)
		out = append(out, *r)
	}
	slices.SortFunc(out, func(a, b authv1beta1.Revocation) int { return cmp.Compare(a.PublicKey, b.PublicKey) })
	return out
}

// signedRevocations are revs as jwtplane signs them.
func signedRevocations(revs []authv1beta1.Revocation) []jwtplane.Revocation {
	out := make([]jwtplane.Revocation, 0, len(revs))
	for _, r := range revs {
		out = append(out, jwtplane.Revocation{PublicKey: r.PublicKey, At: r.At.Time})
	}
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
