package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"slices"
	"time"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/cluster/nodes"
	"sigs.k8s.io/kind/pkg/cluster/nodeutils"
	"sigs.k8s.io/kind/pkg/log"
)

// KindNodeImage is every kind cluster's node image.
const KindNodeImage = "kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed"

// KindNetwork is the podman network kind puts every cluster's nodes on.
const KindNetwork = "kind"

// PodmanSocket is where rootful podman serves its API.
const PodmanSocket = "/run/podman/podman.sock"

// PodmanRemedy is the one line that makes CheckPodman pass on a Linux host.
const PodmanRemedy = "run as root, with podman installed and `systemctl enable --now podman.socket`"

// CheckPodman returns nil when this process is root and rootful podman
// answers its API's ping on socket; kind drives the podman CLI, which is
// rootful only for root.
func CheckPodman(ctx context.Context, socket string) error {
	if os.Geteuid() != 0 {
		return errors.New("rootful podman needs root")
	}
	return pingPodman(ctx, socket, func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	})
}

// pingPodman returns nil when the podman API that dial reaches, named
// socket in errors, answers its ping.
func pingPodman(ctx context.Context, socket string, dial func(context.Context) (net.Conn, error)) error {
	hc := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dial(ctx) },
	}}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://podman/_ping", nil)
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("podman at %s: %w", socket, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("podman at %s: ping answered %s", socket, resp.Status)
	}
	return nil
}

// Kind creates, reaches and deletes kind clusters on podman, every one on
// KindNetwork.
type Kind struct {
	provider *cluster.Provider
	// kubeconfig is the file kind merges each Kubernetes cluster it creates
	// into and removes each one it deletes from.
	kubeconfig string
}

// NewKind returns a Kind logging to logger that keeps kind's own kubeconfig
// entries in the file kubeconfig.
func NewKind(logger log.Logger, kubeconfig string) *Kind {
	return &Kind{
		provider:   cluster.NewProvider(cluster.ProviderWithLogger(logger), cluster.ProviderWithPodman()),
		kubeconfig: kubeconfig,
	}
}

// Up creates each Kubernetes cluster in names that does not exist, from
// KindNodeImage with one node, and returns a kubeconfig with one context per
// name, named after it and reaching its API server from this host, names[0]
// current.
func (k *Kind) Up(names []string) (*clientcmdapi.Config, error) {
	existing, err := k.provider.List()
	if err != nil {
		return nil, fmt.Errorf("list kind clusters: %w", err)
	}
	for _, n := range names {
		if slices.Contains(existing, n) {
			continue
		}
		if err := k.provider.Create(n,
			cluster.CreateWithNodeImage(KindNodeImage),
			cluster.CreateWithKubeconfigPath(k.kubeconfig),
			cluster.CreateWithWaitForReady(3*time.Minute),
			cluster.CreateWithDisplayUsage(false),
			cluster.CreateWithDisplaySalutation(false),
		); err != nil {
			return nil, fmt.Errorf("create kind cluster %s: %w", n, err)
		}
	}
	kubeconfigs := make([]string, len(names))
	for i, n := range names {
		if kubeconfigs[i], err = k.provider.KubeConfig(n, false); err != nil {
			return nil, fmt.Errorf("kubeconfig of kind cluster %s: %w", n, err)
		}
	}
	return mergeKubeconfigs(names, kubeconfigs)
}

// mergeKubeconfigs returns one kubeconfig holding the current context of
// each of raws as a context, cluster and user named after the name at the
// same index, names[0] current.
func mergeKubeconfigs(names, raws []string) (*clientcmdapi.Config, error) {
	out := clientcmdapi.NewConfig()
	for i, n := range names {
		cfg, err := clientcmd.Load([]byte(raws[i]))
		if err != nil {
			return nil, fmt.Errorf("kubeconfig of %s: %w", n, err)
		}
		c, ok := cfg.Contexts[cfg.CurrentContext]
		if !ok {
			return nil, fmt.Errorf("kubeconfig of %s: no current context", n)
		}
		cl, ok := cfg.Clusters[c.Cluster]
		if !ok {
			return nil, fmt.Errorf("kubeconfig of %s: no cluster %q", n, c.Cluster)
		}
		u, ok := cfg.AuthInfos[c.AuthInfo]
		if !ok {
			return nil, fmt.Errorf("kubeconfig of %s: no user %q", n, c.AuthInfo)
		}
		out.Clusters[n] = cl
		out.AuthInfos[n] = u
		out.Contexts[n] = &clientcmdapi.Context{Cluster: n, AuthInfo: n}
	}
	if len(names) > 0 {
		out.CurrentContext = names[0]
	}
	return out, nil
}

// Down deletes every Kubernetes cluster in names that exists.
func (k *Kind) Down(names []string) error {
	existing, err := k.provider.List()
	if err != nil {
		return fmt.Errorf("list kind clusters: %w", err)
	}
	for _, n := range names {
		if !slices.Contains(existing, n) {
			continue
		}
		if err := k.provider.Delete(n, k.kubeconfig); err != nil {
			return fmt.Errorf("delete kind cluster %s: %w", n, err)
		}
	}
	return nil
}

// LoadImage imports the image archive at path into every node of each
// Kubernetes cluster in names, and tags the image named source it holds as
// each of tags.
func (k *Kind) LoadImage(names []string, path, source string, tags ...string) error {
	for _, n := range names {
		nodes, err := k.provider.ListInternalNodes(n)
		if err != nil {
			return fmt.Errorf("nodes of kind cluster %s: %w", n, err)
		}
		for _, node := range nodes {
			if err := loadArchive(node, path); err != nil {
				return fmt.Errorf("load %s into %s: %w", path, node, err)
			}
			id, err := nodeutils.ImageID(node, source)
			if err != nil {
				return fmt.Errorf("image %s on %s: %w", source, node, err)
			}
			for _, t := range tags {
				if err := nodeutils.ReTagImage(node, id, t); err != nil {
					return fmt.Errorf("tag %s as %s on %s: %w", source, t, node, err)
				}
			}
		}
	}
	return nil
}

func loadArchive(node nodes.Node, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return nodeutils.LoadImageArchive(node, f)
}

// KindSubnet returns the IPv4 subnet of podman network name.
func KindSubnet(ctx context.Context, name string) (netip.Prefix, error) {
	out, err := exec.CommandContext(ctx, "podman", "network", "inspect", name).Output()
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("podman network inspect %s: %w", name, err)
	}
	return parseSubnet(out)
}

// parseSubnet returns the first IPv4 subnet of the one network in the
// output of `podman network inspect`.
func parseSubnet(inspect []byte) (netip.Prefix, error) {
	var nets []struct {
		Name    string `json:"name"`
		Subnets []struct {
			Subnet string `json:"subnet"`
		} `json:"subnets"`
	}
	if err := json.Unmarshal(inspect, &nets); err != nil {
		return netip.Prefix{}, fmt.Errorf("podman network inspect: %w", err)
	}
	if len(nets) != 1 {
		return netip.Prefix{}, fmt.Errorf("podman network inspect: %d networks", len(nets))
	}
	for _, s := range nets[0].Subnets {
		p, err := netip.ParsePrefix(s.Subnet)
		if err != nil {
			return netip.Prefix{}, fmt.Errorf("network %s: %w", nets[0].Name, err)
		}
		if p.Addr().Is4() {
			return p.Masked(), nil
		}
	}
	return netip.Prefix{}, fmt.Errorf("network %s has no IPv4 subnet", nets[0].Name)
}
