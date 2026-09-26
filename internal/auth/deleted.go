package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/jwt/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
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

// recordDeleted adds d to the deleted accounts of the NatsOperator at key,
// replacing an earlier record of the same key. An operator that does not
// exist records nothing.
func recordDeleted(ctx context.Context, c client.Client, key types.NamespacedName, d authv1beta1.DeletedAccount) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var op authv1beta1.NatsOperator
		if err := c.Get(ctx, key, &op); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		op.Status.DeletedAccounts = slices.DeleteFunc(op.Status.DeletedAccounts, func(e authv1beta1.DeletedAccount) bool { return e.PublicKey == d.PublicKey })
		op.Status.DeletedAccounts = append(op.Status.DeletedAccounts, d)
		return c.Status().Update(ctx, &op)
	})
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

// deleteErr is err from Distributor.Delete, less ErrUnreachable: the
// request is sent once a server can be asked.
func deleteErr(err error) error {
	if errors.Is(err, ErrUnreachable) {
		return nil
	}
	return err
}
