package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPageCurrent pins the page to what render makes of internal/telemetry.
func TestPageCurrent(t *testing.T) {
	got, err := os.ReadFile(filepath.Join("../..", page))
	require.NoError(t, err)
	want, err := render()
	require.NoError(t, err)
	require.Equal(t, want, string(got), "%s is stale: run just telemetry-docs", page)
}
