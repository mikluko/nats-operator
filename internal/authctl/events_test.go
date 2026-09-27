package authctl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
)

// drained is every event rec holds.
func drained(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// TestRecordHeld pins that JWTHeld is recorded as an account's revocations
// first fail to be recovered, and not again while Ready still says so.
func TestRecordHeld(t *testing.T) {
	unreachable := errors.New("no server answered")
	tests := []struct {
		name  string
		conds []metav1.Condition
		want  []string
	}{
		{name: "first failure", conds: []metav1.Condition{{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSigned}},
			want: []string{"Warning JWTHeld revocations cannot be recovered: no server answered"}},
		{name: "no status yet", want: []string{"Warning JWTHeld revocations cannot be recovered: no server answered"}},
		{name: "still held", conds: []metav1.Condition{{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonRecovering}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := events.NewFakeRecorder(10)
			recordHeld(rec, &authv1beta1.NatsAccount{}, tt.conds, unreachable)
			require.Equal(t, tt.want, drained(rec))
		})
	}
}

// kicker is Sessions closing n connections on each kick.
type kicker struct{ n int }

func (k kicker) Kick(context.Context, types.NamespacedName, string, string) (int, error) {
	return k.n, nil
}

// TestUserKicked pins that a deleted user's kick pass records UserKicked
// when it closes connections, and nothing when it finds none.
func TestUserKicked(t *testing.T) {
	op, err := nkeys.CreateOperator()
	require.NoError(t, err)
	acc, err := nkeys.CreateAccount()
	require.NoError(t, err)
	accPub, err := acc.PublicKey()
	require.NoError(t, err)
	user, err := nkeys.CreateUser()
	require.NoError(t, err)
	userPub, err := user.PublicKey()
	require.NoError(t, err)
	ac := jwt.NewAccountClaims(accPub)
	ac.Revocations = jwt.RevocationList{userPub: time.Now().Add(time.Hour).Unix()}
	token, err := ac.Encode(op)
	require.NoError(t, err)

	for _, tt := range []struct {
		closed int
		want   []string
	}{
		{closed: 2, want: []string{"Normal UserKicked closed 2 connections of " + userPub}},
		{closed: 0},
	} {
		s := runtime.NewScheme()
		require.NoError(t, clientgoscheme.AddToScheme(s))
		require.NoError(t, authv1beta1.AddToScheme(s))
		require.NoError(t, natsv1beta1.AddToScheme(s))
		now := metav1.Now()
		u := &authv1beta1.NatsUser{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "svc", Finalizers: []string{UserFinalizer}, DeletionTimestamp: &now},
			Spec: authv1beta1.NatsUserSpec{AccountRef: authv1beta1.AccountReference{
				Kind: authv1beta1.AccountKindAccount, ObjectReference: natsv1beta1.ObjectReference{Name: "orders"},
			}},
			Status: authv1beta1.NatsUserStatus{PublicKey: userPub},
		}
		a := &authv1beta1.NatsAccount{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "orders"},
			Spec:       authv1beta1.NatsAccountSpec{OperatorRef: natsv1beta1.ObjectReference{Name: "demo"}},
			Status: authv1beta1.NatsAccountStatus{PublicKey: accPub, JWT: token,
				Distribution: &authv1beta1.Distribution{Servers: 1, Current: 1}},
		}
		c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(u).WithObjects(u, a).Build()
		rec := events.NewFakeRecorder(10)
		r := &UserReconciler{Client: c, Sessions: kicker{tt.closed}, Recorder: rec}
		_, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(u)})
		require.NoError(t, err)
		require.Equal(t, tt.want, drained(rec), "%d closed", tt.closed)
	}
}
