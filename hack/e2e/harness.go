package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kindcmd "sigs.k8s.io/kind/pkg/cmd"

	"github.com/mikluko/nats-operator/internal/e2e"
)

const (
	release   = "nats-operator"
	releaseNS = "nats-operator"
	// authConnection is the NatsConnection the auth controller pushes
	// through, the one story 2 declares.
	authConnection = "nats-system/auth-controller"
)

// harness brings up the clusters and runs the stories on them, or with
// down deletes them. It runs where podman does: on a Linux host, or inside
// the darwin machine.
func harness(ctx context.Context, cfg config, root, imagesFile string, down bool) error {
	if err := e2e.CheckPodman(ctx, e2e.PodmanSocket); err != nil {
		return fmt.Errorf("%w; %s", err, e2e.PodmanRemedy)
	}
	work := filepath.Join(root, "bin", "e2e")
	if err := os.MkdirAll(work, 0o755); err != nil {
		return err
	}
	names := cfg.clusterNames()
	kind := e2e.NewKind(kindcmd.NewLogger(), filepath.Join(work, "kind.kubeconfig"))
	if down {
		logf("delete kind clusters %v", names)
		return kind.Down(names)
	}
	if _, err := exec.LookPath("helm"); err != nil {
		return errors.New("helm is not on PATH; hack/e2e/machine.sh installs the version the harness runs")
	}
	if imagesFile == "" {
		var err error
		if imagesFile, err = buildImages(ctx, root, work, "linux/"+runtime.GOARCH); err != nil {
			return err
		}
	}
	images, err := readImages(imagesFile)
	if err != nil {
		return err
	}

	logf("kind clusters %v", names)
	kc, err := kind.Up(names)
	if err != nil {
		return err
	}
	kubeconfig := filepath.Join(work, "kubeconfig")
	if err := clientcmd.WriteToFile(*kc, kubeconfig); err != nil {
		return err
	}
	clients, err := newClients(kc, names)
	if err != nil {
		return err
	}
	if err := addons(ctx, clients, names); err != nil {
		return err
	}
	for _, img := range images {
		logf("load image %s:%s", img.Repository, img.Tag)
		if err := kind.LoadImage(names, img.Archive, img.Source, img.Repository+":"+img.Tag); err != nil {
			return err
		}
	}
	for i, n := range names {
		enabled := cfg.controllers
		if i > 0 {
			enabled = cfg.peerControllers
		}
		logf("chart %s into %s/%s: %v", release, n, releaseNS, enabled)
		if err := installChart(ctx, clients[i], root, kubeconfig, n, enabled, images); err != nil {
			return err
		}
	}
	logf("stories %s", orAll(cfg.stories))
	return runStories(ctx, root, clients, cfg.stories, cfg.wait)
}

func orAll(s string) string {
	if s == "" {
		return "all"
	}
	return s
}

// newClients returns a client per context of kc named in names, in order.
func newClients(kc *clientcmdapi.Config, names []string) ([]client.Client, error) {
	clients := make([]client.Client, len(names))
	for i, n := range names {
		rc, err := clientcmd.NewDefaultClientConfig(*kc, &clientcmd.ConfigOverrides{CurrentContext: n}).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("context %s: %w", n, err)
		}
		if clients[i], err = client.New(rc, client.Options{Scheme: scheme.Scheme}); err != nil {
			return nil, fmt.Errorf("context %s: %w", n, err)
		}
	}
	return clients, nil
}

// addons gives Kubernetes cluster number i of clients MetalLB with address
// pool i on the kind network, and a CoreDNS serving the hostnames the runner
// publishes.
func addons(ctx context.Context, clients []client.Client, names []string) error {
	subnet, err := e2e.KindSubnet(ctx, e2e.KindNetwork)
	if err != nil {
		return err
	}
	manifest, err := e2e.FetchMetalLB(ctx)
	if err != nil {
		return err
	}
	for i, c := range clients {
		first, last, err := e2e.PoolRange(subnet, i)
		if err != nil {
			return err
		}
		logf("MetalLB in %s: %s-%s", names[i], first, last)
		if err := e2e.InstallMetalLB(ctx, c, manifest, first, last, 3*time.Minute, 5*time.Second); err != nil {
			return fmt.Errorf("%s: %w", names[i], err)
		}
		if err := e2e.ServeHosts(ctx, c); err != nil {
			return fmt.Errorf("%s: %w", names[i], err)
		}
	}
	return nil
}

// installChart installs the chart from root in context name of kubeconfig
// through c, with the controllers in enabled switched on and deployed from
// images, and runs its `helm test`. CRDs are applied first because helm
// installs a chart's crds/ only on first install.
func installChart(ctx context.Context, c client.Client, root, kubeconfig, name string, enabled []string, images []image) error {
	chart := filepath.Join(root, "charts", "nats-operator")
	crds, err := filepath.Glob(filepath.Join(chart, "crds", "*.yaml"))
	if err != nil {
		return err
	}
	for _, f := range crds {
		raw, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		objs, err := e2e.DecodeObjects(raw)
		if err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		if err := e2e.ApplyObjects(ctx, c, objs); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	args := []string{"upgrade", "--install", release, chart,
		"--kubeconfig", kubeconfig, "--kube-context", name,
		"--namespace", releaseNS, "--create-namespace", "--wait", "--timeout", "3m"}
	args = append(args, chartSets(enabled, images)...)
	if err := quietly(exec.CommandContext(ctx, "helm", args...)); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if err := quietly(exec.CommandContext(ctx, "helm", "test", release, "--kubeconfig", kubeconfig,
		"--kube-context", name, "--namespace", releaseNS, "--logs", "--timeout", "3m")); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

// chartSets are the --set flags that switch on the controllers in enabled,
// and no other, and deploy each controller from its image without pulling.
func chartSets(enabled []string, images []image) []string {
	sets := []string{"--set", "cluster.enabled=false", "--set", "auth.enabled=false", "--set", "jetstream.enabled=false",
		"--set", "auth.systemConnection=" + authConnection}
	for _, img := range images {
		sets = append(sets,
			"--set", img.Key+".image.repository="+img.Repository,
			"--set", img.Key+".image.tag="+img.Tag,
			"--set", img.Key+".image.pullPolicy=Never")
	}
	for _, k := range enabled {
		sets = append(sets, "--set", k+".enabled=true")
	}
	return sets
}

// quietly runs cmd, returning its combined output in the error when it
// fails.
func quietly(cmd *exec.Cmd) error {
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w\n%s", cmd.Args[0]+" "+cmd.Args[1], err, out.String())
	}
	return nil
}
