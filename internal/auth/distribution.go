package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
)

// distributionRecheck is how soon an account that is not on every server
// is looked at again.
const distributionRecheck = 5 * time.Second

// accountDistribution is the part of an account's status distribute keeps.
type accountDistribution struct {
	dist  **authv1beta1.Distribution
	conds *[]metav1.Condition
	gen   int64
}

// distribute counts the servers trusting operator that hold token, an
// account's current JWT, pushing it again when one does not, and sets the
// account's distribution and Distributed condition; with Ready True, a JWT
// on every server makes its reason Distributed. It returns how soon to
// look again, zero for not until something changes. With d nil it does
// nothing.
func distribute(ctx context.Context, d Distributor, operator types.NamespacedName, token string, a accountDistribution) (time.Duration, error) {
	if d == nil || token == "" {
		return 0, nil
	}
	set := func(status metav1.ConditionStatus, reason, msg string) {
		setCondition(a.conds, a.gen, ConditionDistributed, status, reason, msg)
	}
	got, err := d.Current(ctx, operator, token)
	if err == nil && got.Current < got.Servers {
		if err = d.Push(ctx, operator, token); err == nil {
			got, err = d.Current(ctx, operator, token)
		}
	}
	switch {
	case errors.Is(err, ErrUnreachable):
		set(metav1.ConditionFalse, ReasonUnreachable, err.Error())
		return distributionRecheck, nil
	case err != nil:
		set(metav1.ConditionUnknown, ReasonUnobserved, err.Error())
		return 0, err
	}
	if got.LastPushTime == nil && *a.dist != nil {
		got.LastPushTime = (*a.dist).LastPushTime
	}
	*a.dist = &got
	msg := fmt.Sprintf("%d of %d servers hold this JWT", got.Current, got.Servers)
	if got.Servers == 0 || got.Current < got.Servers {
		set(metav1.ConditionFalse, ReasonServersBehind, msg)
		return distributionRecheck, nil
	}
	set(metav1.ConditionTrue, ReasonAllServersCurrent, msg)
	if ready := meta.FindStatusCondition(*a.conds, ConditionReady); ready != nil && ready.Status == metav1.ConditionTrue {
		setCondition(a.conds, a.gen, ConditionReady, metav1.ConditionTrue, ReasonDistributed, "")
	}
	return 0, nil
}

// pushErr is err from a push of a newly signed JWT, less ErrUnreachable:
// a JWT no server can be asked to take is written to status all the same,
// and distribute pushes it once one can.
func pushErr(err error) error {
	if errors.Is(err, ErrUnreachable) {
		return nil
	}
	return err
}

// soonest is the earlier of two requeue delays, zero being none.
func soonest(a, b time.Duration) time.Duration {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	}
	return min(a, b)
}
