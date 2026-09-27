package manager

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
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

// TestNew pins that New builds its manager against the config it is
// given, reaching no API server until started.
func TestNew(t *testing.T) {
	scheme := runtime.NewScheme()
	mgr, err := New(&rest.Config{Host: "https://127.0.0.1:1"}, &Options{MetricsAddr: "0", ProbeAddr: "0"}, scheme)
	require.NoError(t, err)
	require.Same(t, scheme, mgr.GetScheme())
	require.Equal(t, "https://127.0.0.1:1", mgr.GetConfig().Host)
}
