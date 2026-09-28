package v1beta1

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

func TestObjectReference_ObjectKey(t *testing.T) {
	for _, tt := range []struct {
		name string
		ref  ObjectReference
		want types.NamespacedName
	}{
		{"own namespace", ObjectReference{Name: "demo"}, types.NamespacedName{Namespace: "apps", Name: "demo"}},
		{"named namespace", ObjectReference{Name: "demo", Namespace: "nats"}, types.NamespacedName{Namespace: "nats", Name: "demo"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.ref.ObjectKey("apps"))
		})
	}
}
