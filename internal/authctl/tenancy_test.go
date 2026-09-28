package authctl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestKeyHolder(t *testing.T) {
	t0 := metav1.NewTime(time.Unix(1000, 0))
	t1 := metav1.NewTime(time.Unix(2000, 0))
	claim := func(name string, created metav1.Time, recorded string) keyClaim {
		return keyClaim{kind: "NatsAccount", key: types.NamespacedName{Namespace: "ns", Name: name}, uid: types.UID(name), created: created, recorded: recorded}
	}
	for _, tc := range []struct {
		name   string
		self   keyClaim
		others []keyClaim
		want   string
	}{
		{"no other", claim("a", t0, ""), nil, ""},
		{"another key", claim("a", t0, ""), []keyClaim{claim("b", t0, "K2")}, ""},
		{"itself", claim("a", t0, "K"), []keyClaim{claim("a", t0, "K")}, ""},
		{"held by an older", claim("a", t1, ""), []keyClaim{claim("b", t0, "K")}, "NatsAccount ns/b"},
		{"held by a newer, not recorded here", claim("a", t0, ""), []keyClaim{claim("b", t1, "K")}, "NatsAccount ns/b"},
		{"both record it, the other older", claim("a", t1, "K"), []keyClaim{claim("b", t0, "K")}, "NatsAccount ns/b"},
		{"both record it, this one older", claim("a", t0, "K"), []keyClaim{claim("b", t1, "K")}, ""},
		{"both record it, same second, name decides", claim("b", t0, "K"), []keyClaim{claim("a", t0, "K")}, "NatsAccount ns/a"},
		{"both record it, same second, this name first", claim("a", t0, "K"), []keyClaim{claim("b", t0, "K")}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, held := keyHolder(tc.self, "K", tc.others)
			require.Equal(t, tc.want != "", held)
			if held {
				require.Equal(t, tc.want, got.String())
			}
		})
	}
}
