package authctl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

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
	acc := testKeys(t, nkeys.PrefixByteAccount, false, false)
	other := testKeys(t, nkeys.PrefixByteAccount, false)
	accPub := testPub(t, acc.Identity)
	keyA, keyB := testPub(t, acc.Signing[0].Pair), testPub(t, acc.Signing[1].Pair)
	keyC := testPub(t, testKeys(t, nkeys.PrefixByteAccount, false).Signing[0].Pair)

	t0 := time.Unix(1_800_000_000, 0)
	carried, deleting, denied, admitted := userKey(t), userKey(t), userKey(t), userKey(t)
	signWith := func(keys jwtplane.Keys, revs ...jwtplane.Revocation) string {
		token, err := jwtplane.SignAccount(jwtplane.Account{Name: "a", Keys: keys, Revocations: revs}, op, t0)
		require.NoError(t, err)
		return token
	}
	onlyA := acc
	onlyA.Signing = acc.Signing[:1]
	prev := signWith(onlyA, jwtplane.Revocation{PublicKey: carried, At: t0}, jwtplane.Revocation{PublicKey: deleting, At: t0.Add(time.Hour)})
	rev := func(key string, at time.Time, issuers ...string) authv1beta1.Revocation {
		slices.Sort(issuers)
		return authv1beta1.Revocation{PublicKey: key, At: metav1.Time{Time: at}, Issuers: issuers}
	}

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
		name     string
		recorded []authv1beta1.Revocation
		prev     string
		signing  []string
		users    []authv1beta1.NatsUser
		want     []authv1beta1.Revocation
	}{
		{
			name:    "nothing",
			signing: []string{keyA},
			want:    []authv1beta1.Revocation{},
		},
		{
			name:    "a lost record is rebuilt from the JWT, issued by the keys it lists",
			prev:    prev,
			signing: []string{keyA, keyB},
			want:    []authv1beta1.Revocation{rev(carried, t0, keyA), rev(deleting, t0.Add(time.Hour), keyA)},
		},
		{
			name:     "a lost JWT is rebuilt from the record",
			recorded: []authv1beta1.Revocation{rev(carried, t0, keyA)},
			signing:  []string{keyA},
			want:     []authv1beta1.Revocation{rev(carried, t0, keyA)},
		},
		{
			name:    "a JWT of another account is not carried",
			prev:    signWith(other, jwtplane.Revocation{PublicKey: carried, At: t0}),
			signing: []string{keyA},
			want:    []authv1beta1.Revocation{},
		},
		{
			name:     "the JWT does not widen the issuers of a revocation it carries at the same time",
			recorded: []authv1beta1.Revocation{rev(carried, t0, keyB)},
			prev:     prev,
			signing:  []string{keyA, keyB},
			want:     []authv1beta1.Revocation{rev(carried, t0, keyB), rev(deleting, t0.Add(time.Hour), keyA)},
		},
		{
			name:    "deleted and denied users are revoked by every signing key",
			prev:    prev,
			signing: []string{keyA, keyB},
			users: []authv1beta1.NatsUser{
				user(deleting, deleted(t0.Add(2*time.Hour), true)),
				user(denied, noGrant(t0.Add(3*time.Hour))),
				user(admitted, nil),
				user("", deleted(t0, true)),
			},
			want: []authv1beta1.Revocation{
				rev(carried, t0, keyA),
				rev(deleting, t0.Add(2*time.Hour), keyA, keyB),
				rev(denied, t0.Add(3*time.Hour), keyA, keyB),
			},
		},
		{
			name:     "an earlier deletion neither moves a revocation back nor widens its issuers",
			recorded: []authv1beta1.Revocation{rev(deleting, t0.Add(time.Hour), keyA)},
			signing:  []string{keyA, keyB},
			users:    []authv1beta1.NatsUser{user(deleting, deleted(t0, true))},
			want:     []authv1beta1.Revocation{rev(deleting, t0.Add(time.Hour), keyA)},
		},
		{
			name:    "a deleted user whose finalizer is gone is not revoked afresh",
			signing: []string{keyA},
			users:   []authv1beta1.NatsUser{user(admitted, deleted(t0, false))},
			want:    []authv1beta1.Revocation{},
		},
		{
			name:     "a revocation is kept while one of its issuers remains, retiring or not",
			recorded: []authv1beta1.Revocation{rev(carried, t0, keyA, keyB)},
			signing:  []string{keyB, keyC},
			want:     []authv1beta1.Revocation{rev(carried, t0, keyA, keyB)},
		},
		{
			name:     "rotating out every issuer drops the revocation, from the record and from the JWT",
			recorded: []authv1beta1.Revocation{rev(carried, t0, keyA), rev(denied, t0, keyB)},
			prev:     prev,
			signing:  []string{keyB, keyC},
			want:     []authv1beta1.Revocation{rev(denied, t0, keyB)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slices.SortFunc(tt.want, func(a, b authv1beta1.Revocation) int { return strings.Compare(a.PublicKey, b.PublicKey) })
			require.Equal(t, tt.want, accountRevocations(tt.recorded, tt.prev, accPub, tt.signing, tt.users))
		})
	}
}

// lookupOnly is a Distributor whose Lookup answers with jwt and err, and
// that records every account it was asked for.
type lookupOnly struct {
	jwt   string
	err   error
	asked []string
}

func (d *lookupOnly) Lookup(_ context.Context, _ types.NamespacedName, account string) (string, error) {
	d.asked = append(d.asked, account)
	return d.jwt, d.err
}

func (*lookupOnly) Push(context.Context, types.NamespacedName, string) error { return nil }

func (*lookupOnly) Current(context.Context, types.NamespacedName, string) (authv1beta1.Distribution, error) {
	return authv1beta1.Distribution{}, nil
}

func (*lookupOnly) Delete(context.Context, types.NamespacedName, string) error { return nil }

func TestRecoverRevocations(t *testing.T) {
	op := testKeys(t, nkeys.PrefixByteOperator, false)
	acc := testKeys(t, nkeys.PrefixByteAccount, false)
	accPub := testPub(t, acc.Identity)
	keyA := testPub(t, acc.Signing[0].Pair)
	t0 := time.Unix(1_800_000_000, 0)
	revoked, deleting := userKey(t), userKey(t)
	held, err := jwtplane.SignAccount(jwtplane.Account{Name: "a", Keys: acc, Revocations: []jwtplane.Revocation{{PublicKey: revoked, At: t0}}}, op, t0)
	require.NoError(t, err)
	current, err := jwtplane.SignAccount(jwtplane.Account{Name: "a", Keys: acc}, op, t0)
	require.NoError(t, err)
	rev := func(key string, at time.Time) authv1beta1.Revocation {
		return authv1beta1.Revocation{PublicKey: key, At: metav1.Time{Time: at}, Issuers: []string{keyA}}
	}
	deletingUser := authv1beta1.NatsUser{
		ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &metav1.Time{Time: t0.Add(time.Hour)}, Finalizers: []string{UserFinalizer}},
		Status:     authv1beta1.NatsUserStatus{PublicKey: deleting},
	}
	unreachable := fmt.Errorf("%w: no server answered STATSZ", ErrUnreachable)

	tests := []struct {
		name        string
		d           *lookupOnly
		recorded    []authv1beta1.Revocation
		prev        string
		users       []authv1beta1.NatsUser
		unrecovered bool
		distributed bool
		want        []authv1beta1.Revocation
		wantAsked   bool
		wantUnasked bool
		wantErr     error
	}{
		{
			name:      "claims found: the record is seeded from the servers' JWT",
			d:         &lookupOnly{jwt: held},
			users:     []authv1beta1.NatsUser{deletingUser},
			want:      []authv1beta1.Revocation{rev(revoked, t0), rev(deleting, t0.Add(time.Hour))},
			wantAsked: true,
		},
		{
			name:      "claims empty: a new account is signed with its users' revocations alone",
			d:         &lookupOnly{},
			users:     []authv1beta1.NatsUser{deletingUser},
			want:      []authv1beta1.Revocation{rev(deleting, t0.Add(time.Hour))},
			wantAsked: true,
		},
		{
			name:        "no connection, distributed: nothing to sign",
			d:           &lookupOnly{jwt: held, err: unreachable},
			distributed: true,
			wantErr:     ErrUnreachable,
		},
		{
			name:        "no connection, never distributed (fresh install): signed unasked",
			d:           &lookupOnly{err: unreachable},
			users:       []authv1beta1.NatsUser{deletingUser},
			want:        []authv1beta1.Revocation{rev(deleting, t0.Add(time.Hour))},
			wantUnasked: true,
		},
		{
			name:        "signed unasked earlier: the servers' JWT is merged into the one signed since",
			d:           &lookupOnly{jwt: held},
			prev:        current,
			users:       []authv1beta1.NatsUser{deletingUser},
			unrecovered: true,
			want:        []authv1beta1.Revocation{rev(revoked, t0), rev(deleting, t0.Add(time.Hour))},
			wantAsked:   true,
		},
		{
			name:        "signed unasked earlier, still no connection: still unasked",
			d:           &lookupOnly{err: unreachable},
			prev:        current,
			unrecovered: true,
			want:        []authv1beta1.Revocation{},
			wantUnasked: true,
		},
		{
			name:     "a record survives: the servers are not asked",
			d:        &lookupOnly{err: unreachable},
			recorded: []authv1beta1.Revocation{rev(revoked, t0)},
			want:     []authv1beta1.Revocation{rev(revoked, t0)},
		},
		{
			name: "a JWT survives: the servers are not asked",
			d:    &lookupOnly{err: unreachable},
			prev: current,
			want: []authv1beta1.Revocation{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slices.SortFunc(tt.want, func(a, b authv1beta1.Revocation) int { return strings.Compare(a.PublicKey, b.PublicKey) })
			got, err := recoverRevocations(t.Context(), tt.d, types.NamespacedName{Name: "op"}, tt.recorded, tt.prev, accPub, []string{keyA}, tt.users, tt.unrecovered, tt.distributed)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got.revocations)
			require.Equal(t, tt.wantAsked, got.asked)
			if tt.wantUnasked {
				require.ErrorIs(t, got.unasked, ErrUnreachable)
			} else {
				require.NoError(t, got.unasked)
			}
		})
	}

	t.Run("no Distributor: nothing to ask", func(t *testing.T) {
		got, err := recoverRevocations(t.Context(), nil, types.NamespacedName{Name: "op"}, nil, "", accPub, []string{keyA}, nil, true, true)
		require.NoError(t, err)
		require.Equal(t, recoveredRevocations{revocations: []authv1beta1.Revocation{}}, got)
	})
}

func TestRecordRecovery(t *testing.T) {
	var conds []metav1.Condition
	recordRecovery(&conds, 1, recoveredRevocations{unasked: fmt.Errorf("%w: down", ErrUnreachable)})
	require.True(t, unrecovered(conds))
	recordRecovery(&conds, 1, recoveredRevocations{})
	require.True(t, unrecovered(conds), "a signing that did not ask leaves it")
	recordRecovery(&conds, 1, recoveredRevocations{asked: true})
	require.Empty(t, conds)
}

func TestEverDistributed(t *testing.T) {
	require.False(t, everDistributed(nil))
	require.False(t, everDistributed(&authv1beta1.Distribution{Servers: 3}))
	require.True(t, everDistributed(&authv1beta1.Distribution{Servers: 3, Current: 1}))
	require.True(t, everDistributed(&authv1beta1.Distribution{LastPushTime: &metav1.Time{}}))
}

func TestRecoveryFailed(t *testing.T) {
	var reason string
	notReady := func(r, _ string) { reason = r }
	again, err := recoveryFailed(fmt.Errorf("%w: down", ErrUnreachable), notReady)
	require.NoError(t, err)
	require.Equal(t, distributionRecheck, again)
	require.Equal(t, ReasonRecovering, reason)

	other := errors.New("decode")
	_, err = recoveryFailed(other, notReady)
	require.ErrorIs(t, err, other)
}

func TestSignedRevocations(t *testing.T) {
	op := testKeys(t, nkeys.PrefixByteOperator, false)
	acc := testKeys(t, nkeys.PrefixByteAccount, false)
	pub := userKey(t)
	at := time.Unix(1_800_000_000, 0)
	token, err := jwtplane.SignAccount(jwtplane.Account{
		Name: "a", Keys: acc,
		Revocations: signedRevocations([]authv1beta1.Revocation{{PublicKey: pub, At: metav1.Time{Time: at}, Issuers: []string{"x"}}}),
	}, op, at)
	require.NoError(t, err)
	require.True(t, revokedSince(token, pub, at))
	require.False(t, revokedSince(token, pub, at.Add(time.Second)))
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
