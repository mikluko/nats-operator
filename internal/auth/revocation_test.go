package auth

import (
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

func userKey(t *testing.T) string {
	t.Helper()
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	return pub
}

func TestAccountRevocations(t *testing.T) {
	op := testKeys(t, nkeys.PrefixByteOperator, false)
	acc := testKeys(t, nkeys.PrefixByteAccount, false)
	other := testKeys(t, nkeys.PrefixByteAccount, false)
	accPub, err := acc.Identity.PublicKey()
	require.NoError(t, err)

	t0 := time.Unix(1_800_000_000, 0)
	carried, deleting, denied, admitted, unsigned := userKey(t), userKey(t), userKey(t), userKey(t), userKey(t)
	signWith := func(keys jwtplane.Keys, revs ...jwtplane.Revocation) string {
		token, err := jwtplane.SignAccount(jwtplane.Account{Name: "a", Keys: keys, Revocations: revs}, op, t0)
		require.NoError(t, err)
		return token
	}
	prev := signWith(acc, jwtplane.Revocation{PublicKey: carried, At: t0}, jwtplane.Revocation{PublicKey: deleting, At: t0.Add(time.Hour)})

	user := func(pub string, mutate func(*authv1beta1.NatsUser)) authv1beta1.NatsUser {
		u := authv1beta1.NatsUser{Status: authv1beta1.NatsUserStatus{PublicKey: pub}}
		if mutate != nil {
			mutate(&u)
		}
		return u
	}
	deleted := func(at time.Time, finalizer bool) func(*authv1beta1.NatsUser) {
		return func(u *authv1beta1.NatsUser) {
			u.DeletionTimestamp = &metav1.Time{Time: at}
			if finalizer {
				u.Finalizers = []string{UserFinalizer}
			}
		}
	}
	noGrant := func(at time.Time) func(*authv1beta1.NatsUser) {
		return func(u *authv1beta1.NatsUser) {
			u.Status.Conditions = []metav1.Condition{{
				Type: grant.ConditionReferencesResolved, Status: metav1.ConditionFalse,
				Reason: grant.ReasonNoGrant, LastTransitionTime: metav1.Time{Time: at},
			}}
		}
	}

	tests := []struct {
		name  string
		prev  string
		users []authv1beta1.NatsUser
		want  map[string]time.Time
	}{
		{
			name: "nothing",
			want: map[string]time.Time{},
		},
		{
			name: "carried forward",
			prev: prev,
			want: map[string]time.Time{carried: t0, deleting: t0.Add(time.Hour)},
		},
		{
			name: "a JWT of another account is not carried",
			prev: signWith(other, jwtplane.Revocation{PublicKey: carried, At: t0}),
			want: map[string]time.Time{},
		},
		{
			name: "deleted and denied users are revoked; the later time wins",
			prev: prev,
			users: []authv1beta1.NatsUser{
				user(deleting, deleted(t0.Add(2*time.Hour), true)),
				user(denied, noGrant(t0.Add(3*time.Hour))),
				user(admitted, nil),
				user("", deleted(t0, true)),
			},
			want: map[string]time.Time{carried: t0, deleting: t0.Add(2 * time.Hour), denied: t0.Add(3 * time.Hour)},
		},
		{
			name:  "an earlier deletion does not move a revocation back",
			prev:  prev,
			users: []authv1beta1.NatsUser{user(deleting, deleted(t0, true))},
			want:  map[string]time.Time{carried: t0, deleting: t0.Add(time.Hour)},
		},
		{
			name:  "a deleted user whose finalizer is gone is not revoked afresh",
			users: []authv1beta1.NatsUser{user(unsigned, deleted(t0, false))},
			want:  map[string]time.Time{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := map[string]time.Time{}
			for _, r := range accountRevocations(tt.prev, accPub, tt.users) {
				require.Zero(t, r.Expires, "user JWTs never expire, so no revocation is ever pruned")
				got[r.PublicKey] = r.At
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRevokedSinceAndUserRevoked(t *testing.T) {
	op := testKeys(t, nkeys.PrefixByteOperator, false)
	acc := testKeys(t, nkeys.PrefixByteAccount, false)
	pub := userKey(t)
	userJWT, err := jwtplane.SignUser(jwtplane.User{Name: "u", PublicKey: pub}, acc)
	require.NoError(t, err)
	issued := time.Now()

	at := issued.Add(time.Minute)
	revoking, err := jwtplane.SignAccount(jwtplane.Account{Name: "a", Keys: acc, Revocations: []jwtplane.Revocation{{PublicKey: pub, At: at}}}, op, issued)
	require.NoError(t, err)
	plain, err := jwtplane.SignAccount(jwtplane.Account{Name: "a", Keys: acc}, op, issued)
	require.NoError(t, err)

	require.True(t, revokedSince(revoking, pub, at))
	require.False(t, revokedSince(revoking, pub, at.Add(time.Second)), "a deletion after the revocation needs one of its own")
	require.False(t, revokedSince(plain, pub, at))
	require.True(t, userRevoked(revoking, userJWT))
	require.False(t, userRevoked(plain, userJWT))
	require.False(t, userRevoked("", userJWT))
}

func TestUserClaims(t *testing.T) {
	pub := userKey(t)
	u := &authv1beta1.NatsUser{}
	u.Name = "svc"
	u.Spec.AccountRef.Kind = authv1beta1.AccountKindAccount
	u.Spec.ConnectionTypes = []authv1beta1.ConnectionType{authv1beta1.ConnectionTypeStandard}
	u.Spec.Permissions = &authv1beta1.Permissions{
		Publish:   &authv1beta1.SubjectPermissions{Allow: []string{"a.>"}, Deny: []string{"a.b"}},
		Subscribe: &authv1beta1.SubjectPermissions{Allow: []string{"_INBOX.>"}},
	}
	require.Equal(t, jwtplane.User{
		Name:                   "svc",
		PublicKey:              pub,
		AllowedConnectionTypes: []string{"STANDARD"},
		Permissions: &jwtplane.Permissions{
			Publish:   jwtplane.SubjectPermissions{Allow: []string{"a.>"}, Deny: []string{"a.b"}},
			Subscribe: jwtplane.SubjectPermissions{Allow: []string{"_INBOX.>"}},
		},
	}, userClaims(u, pub))

	sys := &authv1beta1.NatsUser{}
	sys.Name = "ctl"
	sys.Spec.AccountRef.Kind = authv1beta1.AccountKindSystemAccount
	sys.Spec.Preset = authv1beta1.UserPresetAuthController
	require.Equal(t, jwtplane.User{Name: "ctl", PublicKey: pub, SystemAccount: true, Preset: jwtplane.PresetAuthController}, userClaims(sys, pub))
}
