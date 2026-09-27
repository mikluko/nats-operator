package authctl

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/nats-io/jwt/v2"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
	"github.com/mikluko/nats-operator/internal/telemetry"
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

// recoveredRevocations is the revocations an account is signed with, and
// whether the servers were asked for them.
type recoveredRevocations struct {
	revocations []authv1beta1.Revocation
	// asked is set when the servers answered.
	asked bool
	// unasked is why no server could be asked for an account signed
	// regardless; it wraps ErrUnreachable.
	unasked error
}

// recoverRevocations returns the revocations to sign into the JWT of the
// account pub. They are accountRevocations', merged with those of the
// newest JWT d finds on the servers trusting operator where the status
// cannot be trusted to hold them all: recorded and prev both empty, or
// unrecovered set by an earlier signing that could not ask. Where no
// server can be asked, an account whose status records it distributed is
// not to be signed, and the error wraps ErrUnreachable; any other is
// signed with what the status and users give. A nil d is asked nothing.
func recoverRevocations(ctx context.Context, d Distributor, operator types.NamespacedName, recorded []authv1beta1.Revocation, prev, pub string, signing []string, users []authv1beta1.NatsUser, unrecovered, distributed bool) (recoveredRevocations, error) {
	revs := accountRevocations(recorded, prev, pub, signing, users)
	if d == nil || !unrecovered && (prev != "" || len(recorded) > 0) {
		return recoveredRevocations{revocations: revs}, nil
	}
	held, err := d.Lookup(ctx, operator, pub)
	switch {
	case errors.Is(err, ErrUnreachable) && !distributed:
		return recoveredRevocations{revocations: revs, unasked: err}, nil
	case err != nil:
		return recoveredRevocations{}, err
	}
	return recoveredRevocations{revocations: accountRevocations(revs, held, pub, signing, users), asked: true}, nil
}

// recordRecovery sets ConditionRevocationsUnrecovered on conds from s: True
// while an account was signed without asking the servers, removed once
// they answered.
func recordRecovery(conds *[]metav1.Condition, gen int64, s recoveredRevocations) {
	switch {
	case s.unasked != nil:
		conditions.Set(conds, gen, metav1.Condition{Type: ConditionRevocationsUnrecovered, Status: metav1.ConditionTrue, Reason: ReasonUnreachable,
			Message: "status held neither a JWT nor revocations and no server could be asked for the JWT to recover them from; " +
				"signed with the revocations its users give, and asked again once a server answers: " + s.unasked.Error()})
	case s.asked:
		meta.RemoveStatusCondition(conds, ConditionRevocationsUnrecovered)
	}
}

// unrecovered reports whether conds say an earlier signing could not ask
// the servers for the revocations.
func unrecovered(conds []metav1.Condition) bool {
	return meta.IsStatusConditionTrue(conds, ConditionRevocationsUnrecovered)
}

// everDistributed reports whether d records a push of the account or a
// server holding it.
func everDistributed(d *authv1beta1.Distribution) bool {
	return d != nil && (d.LastPushTime != nil || d.Current > 0)
}

// recoveryFailed reports err from recoverRevocations on an account that is
// therefore not signed, and returns how soon to try again.
func recoveryFailed(err error, notReady func(reason, msg string)) (time.Duration, error) {
	notReady(ReasonRecovering, "status holds neither a JWT nor revocations though the account was distributed, and the servers cannot be asked for the JWT to recover them from: "+err.Error())
	if errors.Is(err, ErrUnreachable) {
		return distributionRecheck, nil
	}
	return 0, err
}

// recordHeld records JWTHeld on obj for err from recoverRevocations, unless
// conds, its conditions before, already hold it with Ready's reason
// RecoveringRevocations.
func recordHeld(rec events.EventRecorder, obj runtime.Object, conds []metav1.Condition, err error) {
	if c := meta.FindStatusCondition(conds, ConditionReady); c != nil && c.Reason == ReasonRecovering {
		return
	}
	telemetry.Emit(rec, obj, telemetry.JWTHeld, "revocations cannot be recovered: %v", err)
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
