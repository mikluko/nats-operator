package balancectl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMoveLeases(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	var l MoveLeases
	require.Empty(t, l.take("C1", "a", now), "a free lease is granted")
	require.Empty(t, l.take("C2", "b", now), "leases are per NATS cluster")
	require.Equal(t, "a", l.take("C1", "b", now), "a held lease is not granted")
	require.Empty(t, l.take("C1", "a", now.Add(50*time.Second)), "the holder renews")
	require.Equal(t, "a", l.take("C1", "b", now.Add(moveLeaseTTL)), "a renewed lease outlives its first term")
	require.True(t, l.holds("C1", "a", now.Add(moveLeaseTTL)))
	require.Empty(t, l.take("C1", "b", now.Add(50*time.Second+moveLeaseTTL)), "a lease lapses a TTL after its last renewal")
	require.False(t, l.holds("C1", "a", now))

	l.release("C1", "a")
	require.True(t, l.holds("C1", "b", now), "only the holder releases")
	l.release("C1", "b")
	require.Empty(t, l.take("C1", "a", now))

	l.drop("a")
	require.False(t, l.holds("C1", "a", now))
	require.True(t, l.holds("C2", "b", now), "drop frees the holder's leases alone")
}
