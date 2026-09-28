package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeExpected(t *testing.T) {
	got, err := decodeExpected([]byte("status:\n  uid: !any abc\n  replicas: 3\n  list: [a, !any b]\n"))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"status": map[string]any{
		"uid": placeholder{}, "replicas": 3, "list": []any{"a", placeholder{}},
	}}, got)

	_, err = decodeExpected([]byte("- a\n"))
	require.ErrorContains(t, err, "not a mapping")
}
