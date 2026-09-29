package authctl

import (
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/jwt/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// deletedAccount is the record of deleting the account pub whose last JWT
// was token.
func deletedAccount(pub, token string) (authv1beta1.DeletedAccount, error) {
	c, err := jwt.DecodeAccountClaims(token)
	if err != nil {
		return authv1beta1.DeletedAccount{}, fmt.Errorf("decode account JWT: %w", err)
	}
	out := authv1beta1.DeletedAccount{PublicKey: pub}
	if c.Expires != 0 {
		out.Expires = &metav1.Time{Time: time.Unix(c.Expires, 0)}
	}
	return out, nil
}

// withdrawn reports whether acc is being deleted or is among refused, the
// accounts no NatsReferenceGrant admits to their NatsOperator.
func withdrawn(acc *authv1beta1.NatsAccount, refused map[types.NamespacedName]bool) bool {
	return acc.DeletionTimestamp != nil || refused[client.ObjectKeyFromObject(acc)]
}

// recordDeleting adds to list the record of every account in accounts that
// is withdrawn and has a JWT, replacing an earlier record of the same key.
// An account whose JWT does not decode is left out.
func recordDeleting(list []authv1beta1.DeletedAccount, accounts []authv1beta1.NatsAccount, refused map[types.NamespacedName]bool) []authv1beta1.DeletedAccount {
	for i := range accounts {
		acc := &accounts[i]
		if !withdrawn(acc, refused) || acc.Status.PublicKey == "" || acc.Status.JWT == "" {
			continue
		}
		d, err := deletedAccount(acc.Status.PublicKey, acc.Status.JWT)
		if err != nil {
			continue
		}
		list = slices.DeleteFunc(list, func(e authv1beta1.DeletedAccount) bool { return e.PublicKey == d.PublicKey })
		list = append(list, d)
	}
	return list
}

// deletionRecorded reports whether op's status records d, or would drop it
// from status.deletedAccounts as pruneDeleted does, given the public keys
// live in accounts, the NatsAccounts naming op, of which refused are not
// admitted to it.
func deletionRecorded(op *authv1beta1.NatsOperator, accounts []authv1beta1.NatsAccount, refused map[types.NamespacedName]bool, d authv1beta1.DeletedAccount, now time.Time) bool {
	if slices.ContainsFunc(op.Status.DeletedAccounts, func(e authv1beta1.DeletedAccount) bool {
		return e.PublicKey == d.PublicKey && e.Expires.Equal(d.Expires)
	}) {
		return true
	}
	kept, _ := pruneDeleted([]authv1beta1.DeletedAccount{d}, liveKeys(op, accounts, refused), now)
	return len(kept) == 0
}

// liveKeys is the public keys of op's system account and of every account
// in accounts not withdrawn.
func liveKeys(op *authv1beta1.NatsOperator, accounts []authv1beta1.NatsAccount, refused map[types.NamespacedName]bool) map[string]bool {
	live := map[string]bool{}
	if sys := op.Status.SystemAccount; sys != nil && sys.PublicKey != "" {
		live[sys.PublicKey] = true
	}
	for i := range accounts {
		if acc := &accounts[i]; !withdrawn(acc, refused) && acc.Status.PublicKey != "" {
			live[acc.Status.PublicKey] = true
		}
	}
	return live
}

// pruneDeleted drops from list every account whose JWT has expired at now
// or whose key is in live, and returns when the next remaining one expires,
// the zero time for never.
func pruneDeleted(list []authv1beta1.DeletedAccount, live map[string]bool, now time.Time) ([]authv1beta1.DeletedAccount, time.Time) {
	var out []authv1beta1.DeletedAccount
	var next time.Time
	for _, d := range list {
		if live[d.PublicKey] || (d.Expires != nil && !d.Expires.After(now)) {
			continue
		}
		out = append(out, d)
		if d.Expires != nil && (next.IsZero() || d.Expires.Before(&metav1.Time{Time: next})) {
			next = d.Expires.Time
		}
	}
	return out, next
}

// deleteRequest signs the request deleting list with operator's keys; ""
// for an empty list.
func deleteRequest(operator jwtplane.Keys, list []authv1beta1.DeletedAccount) (string, error) {
	if len(list) == 0 {
		return "", nil
	}
	keys := make([]string, 0, len(list))
	for _, d := range list {
		keys = append(keys, d.PublicKey)
	}
	return jwtplane.SignDelete(operator, keys)
}
