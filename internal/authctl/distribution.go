package authctl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
)

// distributionRecheck is how soon an account that is not on every server
// is looked at again.
const distributionRecheck = 5 * time.Second

// distribute pushes token again where a server trusting operator lacks it
// and returns the account's distribution, the Distributed condition and how
// soon to look again. With no token the condition has no Type; with d nil
// it is False, reason NoSystemConnection.
func distribute(ctx context.Context, d Distributor, operator types.NamespacedName, token string, prev *authv1beta1.Distribution) (*authv1beta1.Distribution, metav1.Condition, time.Duration, error) {
	cond := func(status metav1.ConditionStatus, reason, msg string) metav1.Condition {
		return metav1.Condition{Type: ConditionDistributed, Status: status, Reason: reason, Message: msg}
	}
	switch {
	case token == "":
		return prev, metav1.Condition{}, 0, nil
	case d == nil:
		return prev, cond(metav1.ConditionFalse, ReasonNoSystemConnection, "the auth controller runs without --system-connection: no server receives this JWT"), 0, nil
	}
	got, err := d.Current(ctx, operator, token)
	if err == nil && got.Current < got.Servers {
		if err = d.Push(ctx, operator, token); err == nil {
			got, err = d.Current(ctx, operator, token)
		}
	}
	switch {
	case errors.Is(err, ErrUnreachable):
		return prev, cond(metav1.ConditionFalse, ReasonUnreachable, err.Error()), distributionRecheck, nil
	case err != nil:
		return prev, cond(metav1.ConditionUnknown, ReasonUnobserved, err.Error()), 0, err
	}
	if got.LastPushTime == nil && prev != nil {
		got.LastPushTime = prev.LastPushTime
	}
	msg := fmt.Sprintf("%d of %d servers hold this JWT", got.Current, got.Servers)
	if got.Servers == 0 || got.Current < got.Servers {
		return &got, cond(metav1.ConditionFalse, ReasonServersBehind, msg), distributionRecheck, nil
	}
	return &got, cond(metav1.ConditionTrue, ReasonAllServersCurrent, msg), 0, nil
}

// recordDistribution sets cond, from distribute, on conds at gen; with
// every server current, a Ready True gets reason Distributed. A cond with
// no Type sets nothing.
func recordDistribution(conds *[]metav1.Condition, gen int64, cond metav1.Condition) {
	if cond.Type == "" {
		return
	}
	conditions.Set(conds, gen, metav1.Condition{Type: cond.Type, Status: cond.Status, Reason: cond.Reason, Message: cond.Message})
	if cond.Status != metav1.ConditionTrue {
		return
	}
	if ready := meta.FindStatusCondition(*conds, ConditionReady); ready != nil && ready.Status == metav1.ConditionTrue {
		conditions.Set(conds, gen, metav1.Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonDistributed})
	}
}

// repushStale pushes token where a server trusting operator lacks it and
// reports whether d refused it with ErrStaleJWT; false with d nil.
func repushStale(ctx context.Context, d Distributor, operator types.NamespacedName, token string) bool {
	if d == nil || token == "" {
		return false
	}
	got, err := d.Current(ctx, operator, token)
	if err != nil || got.Current >= got.Servers {
		return false
	}
	return errors.Is(d.Push(ctx, operator, token), ErrStaleJWT)
}

// ignoreUnreachable is err less ErrUnreachable: what no server can be
// asked to take now is sent once one can.
func ignoreUnreachable(err error) error {
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
