package authctl

import (
	"testing"
	"time"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// testAccount is a NatsAccount with a signed JWT; deleting sets its
// deletion timestamp.
func testAccount(t *testing.T, op jwtplane.Keys, a jwtplane.Account, now time.Time, deleting bool) authv1beta1.NatsAccount {
	t.Helper()
	token, err := jwtplane.SignAccount(a, op, now)
	require.NoError(t, err)
	acc := authv1beta1.NatsAccount{}
	acc.Status.PublicKey = testPub(t, a.Keys.Identity)
	acc.Status.JWT = token
	if deleting {
		acc.DeletionTimestamp = &metav1.Time{Time: now}
	}
	return acc
}

func TestRecordDeleting(t *testing.T) {
	now := time.Unix(time.Now().Unix(), 0)
	op := testKeys(t, nkeys.PrefixByteOperator, false)
	live := testAccount(t, op, jwtplane.Account{Name: "live", Keys: testKeys(t, nkeys.PrefixByteAccount)}, now, false)
	gone := testAccount(t, op, jwtplane.Account{Name: "gone", Keys: testKeys(t, nkeys.PrefixByteAccount), TTL: time.Hour}, now, true)
	forever := testAccount(t, op, jwtplane.Account{Name: "forever", Keys: testKeys(t, nkeys.PrefixByteAccount), NoExpiry: true}, now, true)
	unsigned := authv1beta1.NatsAccount{}
	unsigned.Status.PublicKey = "AUNSIGNED"
	unsigned.DeletionTimestamp = &metav1.Time{Time: now}
	garbled := *gone.DeepCopy()
	garbled.Status.PublicKey, garbled.Status.JWT = "AGARBLED", "not a JWT"
	refusedAcc := testAccount(t, op, jwtplane.Account{Name: "refused", Keys: testKeys(t, nkeys.PrefixByteAccount), NoExpiry: true}, now, false)
	refusedAcc.Namespace, refusedAcc.Name = "tenant", "refused"

	goneRecord := authv1beta1.DeletedAccount{PublicKey: gone.Status.PublicKey, Expires: &metav1.Time{Time: now.Add(time.Hour)}}
	tests := []struct {
		name     string
		list     []authv1beta1.DeletedAccount
		accounts []authv1beta1.NatsAccount
		refused  map[types.NamespacedName]bool
		want     []authv1beta1.DeletedAccount
	}{
		{name: "live accounts add nothing", accounts: []authv1beta1.NatsAccount{live}},
		{name: "an account deleting with no JWT or a malformed one adds nothing", accounts: []authv1beta1.NatsAccount{unsigned, garbled}},
		{
			name:     "an account deleting adds its key and expiry",
			accounts: []authv1beta1.NatsAccount{live, gone, forever},
			want:     []authv1beta1.DeletedAccount{goneRecord, {PublicKey: forever.Status.PublicKey}},
		},
		{
			name:     "an account no grant admits adds its key",
			accounts: []authv1beta1.NatsAccount{refusedAcc},
			refused:  map[types.NamespacedName]bool{{Namespace: "tenant", Name: "refused"}: true},
			want:     []authv1beta1.DeletedAccount{{PublicKey: refusedAcc.Status.PublicKey}},
		},
		{
			name:     "an earlier record of the key is replaced",
			list:     []authv1beta1.DeletedAccount{{PublicKey: gone.Status.PublicKey}, {PublicKey: "AOTHER"}},
			accounts: []authv1beta1.NatsAccount{gone},
			want:     []authv1beta1.DeletedAccount{{PublicKey: "AOTHER"}, goneRecord},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, recordDeleting(tt.list, tt.accounts, tt.refused))
		})
	}
}

func TestDeletionRecorded(t *testing.T) {
	now := time.Unix(time.Now().Unix(), 0)
	opKeys := testKeys(t, nkeys.PrefixByteOperator, false)
	accKeys := testKeys(t, nkeys.PrefixByteAccount)
	gone := testAccount(t, opKeys, jwtplane.Account{Name: "gone", Keys: accKeys, TTL: time.Hour}, now, true)
	again := testAccount(t, opKeys, jwtplane.Account{Name: "again", Keys: accKeys, TTL: time.Hour}, now, false)
	again.Namespace, again.Name = "tenant", "again"
	d, err := deletedAccount(gone.Status.PublicKey, gone.Status.JWT)
	require.NoError(t, err)
	older := authv1beta1.DeletedAccount{PublicKey: d.PublicKey, Expires: &metav1.Time{Time: now.Add(time.Minute)}}

	operator := func(sys string, list ...authv1beta1.DeletedAccount) *authv1beta1.NatsOperator {
		op := &authv1beta1.NatsOperator{}
		op.Status.DeletedAccounts = list
		if sys != "" {
			op.Status.SystemAccount = &authv1beta1.SystemAccountStatus{PublicKey: sys}
		}
		return op
	}
	tests := []struct {
		name     string
		op       *authv1beta1.NatsOperator
		accounts []authv1beta1.NatsAccount
		refused  map[types.NamespacedName]bool
		at       time.Time
		want     bool
	}{
		{name: "not yet recorded", op: operator(""), accounts: []authv1beta1.NatsAccount{gone}, at: now},
		{name: "an older record of the key", op: operator("", older), accounts: []authv1beta1.NatsAccount{gone}, at: now},
		{name: "recorded", op: operator("", d), accounts: []authv1beta1.NatsAccount{gone}, at: now, want: true},
		{name: "its JWT expired, so the record is pruned", op: operator(""), accounts: []authv1beta1.NatsAccount{gone}, at: now.Add(2 * time.Hour), want: true},
		{name: "another account holds the key", op: operator(""), accounts: []authv1beta1.NatsAccount{gone, again}, at: now, want: true},
		{
			name: "another account holding the key is not admitted", op: operator(""), accounts: []authv1beta1.NatsAccount{gone, again},
			refused: map[types.NamespacedName]bool{{Namespace: "tenant", Name: "again"}: true}, at: now,
		},
		{name: "the system account holds the key", op: operator(d.PublicKey), accounts: []authv1beta1.NatsAccount{gone}, at: now, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, deletionRecorded(tt.op, tt.accounts, tt.refused, d, tt.at))
		})
	}
}
