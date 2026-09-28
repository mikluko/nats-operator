package authctl_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/authctl"
	"github.com/mikluko/nats-operator/internal/grant"
)

// notReady asserts Ready=False with reason on conds.
func notReady(ct *assert.CollectT, conds []metav1.Condition, reason string) {
	cond := meta.FindStatusCondition(conds, authctl.ConditionReady)
	if !assert.NotNil(ct, cond) {
		return
	}
	assert.Equal(ct, metav1.ConditionFalse, cond.Status)
	assert.Equal(ct, reason, cond.Reason, cond.Message)
}

// testLostSeed deletes the generated identity Secrets of an account, a
// system account and a NatsOperator already signed: each reads Ready False,
// SeedLost, keeps its public key, and no Secret is generated in its place.
func (e *env) testLostSeed(t *testing.T) {
	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata: {name: lost, namespace: lost}
spec:
  systemAccountRef: {name: sys}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: sys, namespace: lost}
spec:
  operatorRef: {name: lost}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: app, namespace: lost}
spec:
  operatorRef: {name: lost}
`)
	op, sys, acc := &authv1beta1.NatsOperator{}, &authv1beta1.NatsSystemAccount{}, &authv1beta1.NatsAccount{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("lost", "lost"), op)
		e.get(ct, key("lost", "sys"), sys)
		e.get(ct, key("lost", "app"), acc)
		ready(ct, op.Status.Conditions, op.Generation, authctl.ReasonSigned)
		ready(ct, sys.Status.Conditions, sys.Generation, authctl.ReasonSigned)
		ready(ct, acc.Status.Conditions, acc.Generation, authctl.ReasonSigned)
	})
	for _, tc := range []struct {
		secret string
		obj    client.Object
		status func() (string, []metav1.Condition)
	}{
		{"app-account-identity", acc, func() (string, []metav1.Condition) { return acc.Status.PublicKey, acc.Status.Conditions }},
		{"sys-systemaccount-identity", sys, func() (string, []metav1.Condition) { return sys.Status.PublicKey, sys.Status.Conditions }},
		{"lost-operator-identity", op, func() (string, []metav1.Condition) { return op.Status.PublicKey, op.Status.Conditions }},
	} {
		t.Run(tc.secret, func(t *testing.T) {
			pub, _ := tc.status()
			require.NotEmpty(t, pub)
			require.NoError(t, e.c.Delete(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "lost", Name: tc.secret}}))
			e.eventually(t, func(ct *assert.CollectT) {
				e.get(ct, client.ObjectKeyFromObject(tc.obj), tc.obj)
				got, conds := tc.status()
				notReady(ct, conds, authctl.ReasonSeedLost)
				assert.Equal(ct, pub, got)
			})
			err := e.c.Get(t.Context(), key("lost", tc.secret), &corev1.Secret{})
			require.True(t, apierrors.IsNotFound(err), "no new identity is generated: %v", err)
		})
	}
}

// testSeedsOutliveOwner deletes a signed NatsOperator, NatsSystemAccount
// and NatsAccount and applies them again: the generated seed Secrets carry
// no owner reference, stay, and the objects come back under the same keys.
func (e *env) testSeedsOutliveOwner(t *testing.T) {
	manifest := `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata: {name: keep, namespace: keep}
spec:
  systemAccountRef: {name: sys}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: sys, namespace: keep}
spec:
  operatorRef: {name: keep}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: app, namespace: keep}
spec:
  operatorRef: {name: keep}
`
	signed := func() (op authv1beta1.NatsOperator, sys authv1beta1.NatsSystemAccount, acc authv1beta1.NatsAccount) {
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, key("keep", "keep"), &op)
			e.get(ct, key("keep", "sys"), &sys)
			e.get(ct, key("keep", "app"), &acc)
			ready(ct, op.Status.Conditions, op.Generation, authctl.ReasonSigned)
			ready(ct, sys.Status.Conditions, sys.Generation, authctl.ReasonSigned)
			ready(ct, acc.Status.Conditions, acc.Generation, authctl.ReasonSigned)
		})
		return op, sys, acc
	}
	e.apply(t, manifest)
	op, sys, acc := signed()
	for _, name := range []string{"keep-operator-identity", "keep-operator-signing-1", "sys-systemaccount-identity", "sys-systemaccount-signing-1", "app-account-identity", "app-account-signing-1"} {
		var sec corev1.Secret
		require.NoError(t, e.c.Get(t.Context(), key("keep", name), &sec))
		require.Empty(t, sec.OwnerReferences, name)
	}

	for _, obj := range []client.Object{&acc, &sys, &op} {
		require.NoError(t, e.c.Delete(t.Context(), obj))
		e.eventually(t, func(ct *assert.CollectT) {
			err := e.c.Get(t.Context(), client.ObjectKeyFromObject(obj), obj)
			assert.True(ct, apierrors.IsNotFound(err), "%s is gone: %v", obj.GetName(), err)
		})
	}
	e.apply(t, manifest)
	op2, sys2, acc2 := signed()
	require.Equal(t, op.Status.PublicKey, op2.Status.PublicKey)
	require.Equal(t, op.Status.SigningKeys, op2.Status.SigningKeys)
	require.Equal(t, sys.Status.PublicKey, sys2.Status.PublicKey)
	require.Equal(t, acc.Status.PublicKey, acc2.Status.PublicKey)
}

// testAccountKeyHeld has a namespace granted the NatsOperator declare
// NatsAccounts with the public keys of an account and of the system
// account already signed under it: each is refused, Ready False,
// PublicKeyInUse naming the holder, records no key, and nothing signed
// with its signing key is pushed.
func (e *env) testAccountKeyHeld(t *testing.T) {
	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsOperator
metadata: {name: home, namespace: tenancy}
spec:
  systemAccountRef: {name: sys}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: sys, namespace: tenancy}
spec:
  operatorRef: {name: home}
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: victim, namespace: tenancy}
spec:
  operatorRef: {name: home}
---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsReferenceGrant
metadata: {name: thief, namespace: tenancy}
spec:
  from: [{group: auth.nats.mikluko.io, kind: NatsAccount, namespace: thief}]
  to: [{group: auth.nats.mikluko.io, kind: NatsOperator, name: home}]
`)
	home := key("tenancy", "home")
	sys, victim := &authv1beta1.NatsSystemAccount{}, &authv1beta1.NatsAccount{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("tenancy", "sys"), sys)
		e.get(ct, key("tenancy", "victim"), victim)
		ready(ct, sys.Status.Conditions, sys.Generation, authctl.ReasonSigned)
		ready(ct, victim.Status.Conditions, victim.Generation, authctl.ReasonSigned)
	})
	victimJWT := victim.Status.JWT
	thiefSigning := e.seedSecret(t, "thief", "thief-signing", nkeys.PrefixByteAccount)
	for _, tc := range []struct{ name, pub, holder string }{
		{"steal-account", victim.Status.PublicKey, "NatsAccount tenancy/victim"},
		{"steal-system-account", sys.Status.PublicKey, "NatsSystemAccount tenancy/sys"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e.apply(t, fmt.Sprintf(`
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: %s, namespace: thief}
spec:
  operatorRef: {name: home, namespace: tenancy}
  publicKey: %s
  keys:
    signing: [{name: s, secretKeyRef: {name: thief-signing, key: seed}}]
`, tc.name, tc.pub))
			acc := &authv1beta1.NatsAccount{}
			e.eventually(t, func(ct *assert.CollectT) {
				e.get(ct, key("thief", tc.name), acc)
				notReady(ct, acc.Status.Conditions, authctl.ReasonPublicKeyInUse)
				if cond := meta.FindStatusCondition(acc.Status.Conditions, authctl.ConditionReady); cond != nil {
					assert.Contains(ct, cond.Message, tc.holder)
				}
			})
			require.Empty(t, acc.Status.PublicKey)
			require.Empty(t, acc.Status.JWT)
			e.d.mu.Lock()
			pushes := slices.Clone(e.d.pushes[home])
			e.d.mu.Unlock()
			for _, token := range pushes {
				c, err := jwt.DecodeAccountClaims(token)
				require.NoError(t, err)
				require.False(t, c.SigningKeys.Contains(thiefSigning), "a JWT listing the thief's signing key was pushed for %s", c.Subject)
			}
		})
	}
	require.NoError(t, e.c.Get(t.Context(), key("tenancy", "victim"), victim))
	require.Equal(t, victimJWT, victim.Status.JWT)
}

// testAccountKeySquatted has a namespace no grant admits declare a
// NatsAccount under the NatsOperator, and a granted one declare a
// NatsSystemAccount naming it that the NatsOperator does not reference,
// each with the public key of an account about to be adopted: neither
// records the key, and the adopted accounts are signed.
func (e *env) testAccountKeySquatted(t *testing.T) {
	restored, fresh := accountPub(t), accountPub(t)
	e.seedSecret(t, "squat", "squat-signing", nkeys.PrefixByteAccount)
	e.seedSecret(t, "thief", "decoy-signing", nkeys.PrefixByteAccount)
	e.seedSecret(t, "tenancy", "restored-signing", nkeys.PrefixByteAccount)
	e.seedSecret(t, "tenancy", "fresh-signing", nkeys.PrefixByteAccount)
	e.apply(t, fmt.Sprintf(`
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: squatter, namespace: squat}
spec:
  operatorRef: {name: home, namespace: tenancy}
  publicKey: %s
  keys:
    signing: [{name: s, secretKeyRef: {name: squat-signing, key: seed}}]
---
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsSystemAccount
metadata: {name: decoy, namespace: thief}
spec:
  operatorRef: {name: home, namespace: tenancy}
  publicKey: %s
  keys:
    signing: [{name: s, secretKeyRef: {name: decoy-signing, key: seed}}]
`, restored, fresh))
	squatter := &authv1beta1.NatsAccount{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("squat", "squatter"), squatter)
		notReady(ct, squatter.Status.Conditions, grant.ReasonReferenceNotPermitted)
	})
	require.Empty(t, squatter.Status.PublicKey, "an account no grant admits records no key")

	for _, tc := range []struct{ name, pub string }{{"restored", restored}, {"fresh", fresh}} {
		e.apply(t, fmt.Sprintf(`
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsAccount
metadata: {name: %s, namespace: tenancy}
spec:
  operatorRef: {name: home}
  publicKey: %s
  keys:
    signing: [{name: s, secretKeyRef: {name: %s-signing, key: seed}}]
`, tc.name, tc.pub, tc.name))
	}
	for _, tc := range []struct{ name, pub string }{{"restored", restored}, {"fresh", fresh}} {
		acc := &authv1beta1.NatsAccount{}
		e.eventually(t, func(ct *assert.CollectT) {
			e.get(ct, key("tenancy", tc.name), acc)
			ready(ct, acc.Status.Conditions, acc.Generation, authctl.ReasonSigned)
			assert.Equal(ct, tc.pub, acc.Status.PublicKey)
		})
	}
}

func accountPub(t *testing.T) string {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	return pub
}

// testUserKeyHeld has a user granted an account set publicKey to the key
// of another user of it: it is refused, Ready False, PublicKeyInUse naming
// the holder, records no key, and deleting it revokes nothing.
func (e *env) testUserKeyHeld(t *testing.T) {
	e.apply(t, `
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata: {name: alice, namespace: tenancy}
spec:
  accountRef: {kind: NatsAccount, name: victim}
---
apiVersion: nats.mikluko.io/v1beta1
kind: NatsReferenceGrant
metadata: {name: thief-users, namespace: tenancy}
spec:
  from: [{group: auth.nats.mikluko.io, kind: NatsUser, namespace: thief}]
  to: [{group: auth.nats.mikluko.io, kind: NatsAccount, name: victim}]
`)
	alice := &authv1beta1.NatsUser{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("tenancy", "alice"), alice)
		ready(ct, alice.Status.Conditions, alice.Generation, authctl.ReasonSigned)
	})
	e.apply(t, fmt.Sprintf(`
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata: {name: mallory, namespace: thief}
spec:
  accountRef: {kind: NatsAccount, name: victim, namespace: tenancy}
  publicKey: %s
`, alice.Status.PublicKey))
	mallory := &authv1beta1.NatsUser{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("thief", "mallory"), mallory)
		notReady(ct, mallory.Status.Conditions, authctl.ReasonPublicKeyInUse)
		if cond := meta.FindStatusCondition(mallory.Status.Conditions, authctl.ConditionReady); cond != nil {
			assert.Contains(ct, cond.Message, "NatsUser tenancy/alice")
		}
	})
	require.Empty(t, mallory.Status.PublicKey)
	require.Empty(t, mallory.Status.JWT)

	require.NoError(t, e.c.Delete(t.Context(), mallory))
	e.eventually(t, func(ct *assert.CollectT) {
		err := e.c.Get(t.Context(), key("thief", "mallory"), &authv1beta1.NatsUser{})
		assert.True(ct, apierrors.IsNotFound(err), "mallory is gone: %v", err)
	})
	victim := &authv1beta1.NatsAccount{}
	require.NoError(t, e.c.Get(t.Context(), key("tenancy", "victim"), victim))
	require.False(t, revokesKey(victim.Status.JWT, alice.Status.PublicKey), "alice's key is not revoked")
	require.NoError(t, e.c.Get(t.Context(), key("tenancy", "alice"), alice))
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(alice.Status.Conditions, authctl.ConditionReady).Status)
}

// testReplacedUserKey changes a user's publicKey and deletes another's
// creds Secret: each previous key is revoked in the account JWT, and the
// user's replacedKeys empties once it is.
func (e *env) testReplacedUserKey(t *testing.T) {
	k1, k2 := userPub(t), userPub(t)
	e.apply(t, fmt.Sprintf(`
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata: {name: bob, namespace: tenancy}
spec:
  accountRef: {kind: NatsAccount, name: victim}
  publicKey: %s
`, k1))
	bob, alice := &authv1beta1.NatsUser{}, &authv1beta1.NatsUser{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("tenancy", "bob"), bob)
		e.get(ct, key("tenancy", "alice"), alice)
		ready(ct, bob.Status.Conditions, bob.Generation, authctl.ReasonSigned)
		ready(ct, alice.Status.Conditions, alice.Generation, authctl.ReasonSigned)
	})
	aliceKey := alice.Status.PublicKey

	e.update(t, key("tenancy", "bob"), &authv1beta1.NatsUser{}, func(o client.Object) {
		o.(*authv1beta1.NatsUser).Spec.PublicKey = k2
	})
	require.NoError(t, e.c.Delete(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "tenancy", Name: "alice-creds"}}))
	victim := &authv1beta1.NatsAccount{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("tenancy", "bob"), bob)
		e.get(ct, key("tenancy", "alice"), alice)
		e.get(ct, key("tenancy", "victim"), victim)
		ready(ct, bob.Status.Conditions, bob.Generation, authctl.ReasonSigned)
		assert.Equal(ct, k2, bob.Status.PublicKey)
		assert.NotEqual(ct, aliceKey, alice.Status.PublicKey)
		assert.True(ct, revokesKey(victim.Status.JWT, k1), "bob's previous key is revoked")
		assert.True(ct, revokesKey(victim.Status.JWT, aliceKey), "alice's previous key is revoked")
		assert.False(ct, revokesKey(victim.Status.JWT, k2))
		assert.Empty(ct, bob.Status.ReplacedKeys)
		assert.Empty(ct, alice.Status.ReplacedKeys)
	})
}

// testReplacedKeyRefused changes a signed user's publicKey to a key that
// is not a user key, then to one another user holds: each change is
// refused, and the key it held is revoked all the same.
func (e *env) testReplacedKeyRefused(t *testing.T) {
	k1 := userPub(t)
	e.apply(t, fmt.Sprintf(`
apiVersion: auth.nats.mikluko.io/v1beta1
kind: NatsUser
metadata: {name: carol, namespace: tenancy}
spec:
  accountRef: {kind: NatsAccount, name: victim}
  publicKey: %s
`, k1))
	carol, alice := &authv1beta1.NatsUser{}, &authv1beta1.NatsUser{}
	e.eventually(t, func(ct *assert.CollectT) {
		e.get(ct, key("tenancy", "carol"), carol)
		e.get(ct, key("tenancy", "alice"), alice)
		ready(ct, carol.Status.Conditions, carol.Generation, authctl.ReasonSigned)
		ready(ct, alice.Status.Conditions, alice.Generation, authctl.ReasonSigned)
	})
	for _, tc := range []struct {
		name, pub, reason string
	}{
		{"not a user key", "not-a-key", authctl.ReasonInvalidKeys},
		{"held by alice", alice.Status.PublicKey, authctl.ReasonPublicKeyInUse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e.update(t, key("tenancy", "carol"), &authv1beta1.NatsUser{}, func(o client.Object) {
				o.(*authv1beta1.NatsUser).Spec.PublicKey = tc.pub
			})
			victim := &authv1beta1.NatsAccount{}
			e.eventually(t, func(ct *assert.CollectT) {
				e.get(ct, key("tenancy", "carol"), carol)
				e.get(ct, key("tenancy", "victim"), victim)
				notReady(ct, carol.Status.Conditions, tc.reason)
				assert.Empty(ct, carol.Status.PublicKey)
				assert.Empty(ct, carol.Status.JWT)
				assert.True(ct, revokesKey(victim.Status.JWT, k1), "carol's previous key is revoked")
				assert.False(ct, revokesKey(victim.Status.JWT, alice.Status.PublicKey), "alice's key is not")
				assert.Empty(ct, carol.Status.ReplacedKeys)
			})
		})
	}
}

func userPub(t *testing.T) string {
	t.Helper()
	kp, err := nkeys.CreateUser()
	require.NoError(t, err)
	pub, err := kp.PublicKey()
	require.NoError(t, err)
	return pub
}
