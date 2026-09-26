package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
)

func TestNamespacedName(t *testing.T) {
	tests := []struct {
		in      string
		want    types.NamespacedName
		wantErr bool
	}{
		{in: "nats-system/auth-controller", want: types.NamespacedName{Namespace: "nats-system", Name: "auth-controller"}},
		{in: "auth-controller", wantErr: true},
		{in: "/auth-controller", wantErr: true},
		{in: "nats-system/", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := namespacedName(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
