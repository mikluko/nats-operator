package manager

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want Options
	}{
		{
			name: "defaults",
			args: nil,
			want: Options{MetricsAddr: ":8080", ProbeAddr: ":8081", LeaderElectionID: "cluster.nats.mikluko.io"},
		},
		{
			name: "overrides",
			args: []string{"-metrics-bind-address=0", "-leader-elect", "-leader-election-id=x"},
			want: Options{MetricsAddr: "0", ProbeAddr: ":8081", LeaderElection: true, LeaderElectionID: "x"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := flag.NewFlagSet(tt.name, flag.ContinueOnError)
			got := Flags(fs, "cluster.nats.mikluko.io")
			require.NoError(t, fs.Parse(tt.args))
			require.Equal(t, tt.want, *got)
		})
	}
}
