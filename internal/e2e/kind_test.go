package e2e

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSubnet(t *testing.T) {
	for _, tt := range []struct {
		name    string
		inspect string
		want    string
		err     string
	}{
		{
			name:    "IPv4 after IPv6",
			inspect: `[{"name":"kind","subnets":[{"subnet":"fc00:f853:ccd:e793::/64"},{"subnet":"10.89.0.0/24","gateway":"10.89.0.1"}]}]`,
			want:    "10.89.0.0/24",
		},
		{
			name:    "IPv6 only",
			inspect: `[{"name":"kind","subnets":[{"subnet":"fc00:f853:ccd:e793::/64"}]}]`,
			err:     "network kind has no IPv4 subnet",
		},
		{name: "no network", inspect: `[]`, err: "0 networks"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSubnet([]byte(tt.inspect))
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, netip.MustParsePrefix(tt.want), got)
		})
	}
}

func kindKubeconfig(name, port string) string {
	return `apiVersion: v1
kind: Config
clusters:
- name: kind-` + name + `
  cluster: {server: "https://127.0.0.1:` + port + `", certificate-authority-data: Q0E=}
users:
- name: kind-` + name + `
  user: {client-certificate-data: Q0VSVA==, client-key-data: S0VZ}
contexts:
- name: kind-` + name + `
  context: {cluster: kind-` + name + `, user: kind-` + name + `}
current-context: kind-` + name + `
`
}

func TestMergeKubeconfigs(t *testing.T) {
	names := []string{"home", "home-2"}
	got, err := mergeKubeconfigs(names, []string{kindKubeconfig("home", "4001"), kindKubeconfig("home-2", "4002")})
	require.NoError(t, err)
	require.Equal(t, "home", got.CurrentContext)
	require.Len(t, got.Contexts, 2)
	for i, n := range names {
		require.Equal(t, n, got.Contexts[n].Cluster)
		require.Equal(t, n, got.Contexts[n].AuthInfo)
		require.Equal(t, "https://127.0.0.1:400"+string(rune('1'+i)), got.Clusters[n].Server)
		require.Equal(t, []byte("KEY"), got.AuthInfos[n].ClientKeyData)
	}

	_, err = mergeKubeconfigs([]string{"x"}, []string{"apiVersion: v1\nkind: Config\n"})
	require.ErrorContains(t, err, "no current context")
}

func TestPingPodman(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_ping" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte("OK"))
	}))
	t.Cleanup(srv.Close)
	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Listener.Addr().String())
	}
	refused := func(context.Context) (net.Conn, error) { return nil, errors.New("connection refused") }

	require.NoError(t, pingPodman(t.Context(), "/run/podman/podman.sock", dial))
	require.ErrorContains(t, pingPodman(t.Context(), "/run/podman/podman.sock", refused), "podman at /run/podman/podman.sock")
}
