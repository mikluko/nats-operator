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
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
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

// harness brings up the kind clusters and runs the stories on them, or with
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

	generated, err := generateFixtures(work)
	if err != nil {
		return err
	}
	stories, err := selectBundles(root, generated, cfg.stories)
	if err != nil {
		return err
	}
	chart := chartValues{allowGatewayWithoutTLS: slices.ContainsFunc(stories, (*e2e.Bundle).DropsGatewayTLS)}
	if cfg.watchNamespaces {
		chart.watch = watchedNamespaces(stories, cfg.controllers)
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
	enabled := func(i int) []string {
		if i > 0 {
			return cfg.peerControllers
		}
		return cfg.controllers
	}
	for i, n := range names {
		if len(chart.watch) > 0 {
			logf("namespaces %v in %s", chart.watch, n)
			if err := createNamespaces(ctx, clients[i], chart.watch); err != nil {
				return fmt.Errorf("%s: %w", n, err)
			}
		}
		logf("chart %s into %s/%s: %v", release, n, releaseNS, enabled(i))
		if err := installChart(ctx, clients[i], root, kubeconfig, n, enabled(i), images, chart); err != nil {
			return err
		}
	}
	ci := &chartInstall{
		root: root, work: work, kubeconfig: kubeconfig, names: names, enabled: enabled, images: images,
		base: chart, installed: slices.Repeat([]chartValues{chart}, len(names)),
	}
	if slices.ContainsFunc(stories, func(b *e2e.Bundle) bool { return b.ScrapeMetrics }) {
		if ci.home, err = restConfig(kc, names[0]); err != nil {
			return err
		}
		if err := ci.prepareMetrics(ctx, clients[0], generated); err != nil {
			return fmt.Errorf("%s: %w", names[0], err)
		}
	}
	logf("stories %s", orAll(cfg.stories))
	return runStories(ctx, clients, stories, cfg.wait, ci)
}

// chartInstall is the chart as installed in each Kubernetes cluster of the
// run, and what installing it again takes.
type chartInstall struct {
	root, work, kubeconfig string
	names                  []string
	enabled                func(int) []string
	images                 []image
	// base is what every story runs under that asks for nothing more.
	base chartValues
	// installed is, by cluster, what the chart was last installed with.
	installed []chartValues
	// home reaches the home cluster; set where a story scrapes metrics.
	home *rest.Config
	// api and otherCA are what a story scraping metrics runs under and
	// fails to verify with.
	api     apiServer
	otherCA []byte
}

// prepareMetrics applies the generated metrics Secret through c, reads the
// API server's addresses, and keeps the CA that did not sign it.
func (ci *chartInstall) prepareMetrics(ctx context.Context, c client.Client, generated string) error {
	raw, err := os.ReadFile(filepath.Join(generated, metricsFixture))
	if err != nil {
		return err
	}
	objs, err := e2e.DecodeObjects(raw)
	if err != nil {
		return err
	}
	logf("Secret %s/%s", releaseNS, metricsSecret)
	if err := e2e.ApplyObjects(ctx, c, objs); err != nil {
		return err
	}
	if ci.api, err = findAPIServer(ctx, c); err != nil {
		return err
	}
	raw, err = os.ReadFile(filepath.Join(generated, otherMetricsFixture))
	if err != nil {
		return err
	}
	if objs, err = e2e.DecodeObjects(raw); err != nil {
		return err
	}
	ca, _, err := unstructured.NestedString(objs[0].Object, "stringData", "ca.crt")
	if err != nil || ca == "" {
		return fmt.Errorf("%s holds no ca.crt", otherMetricsFixture)
	}
	ci.otherCA = []byte(ca)
	return nil
}

// valuesFor returns what b runs under in cluster i: base, with, in the home
// cluster of a story that scrapes metrics, metricsValues for its
// namespaces written beside the run's other files.
func (ci *chartInstall) valuesFor(b *e2e.Bundle, i int) (chartValues, error) {
	vals := ci.base
	if i > 0 || !b.ScrapeMetrics {
		return vals, nil
	}
	vals.values = filepath.Join(ci.work, b.Name+"-values.yaml")
	return vals, writeValues(vals.values, metricsValues(ci.api, b.Namespaces()))
}

// upgrade installs the chart in cluster i with vals.
func (ci *chartInstall) upgrade(ctx context.Context, i int, vals chartValues) error {
	if err := upgradeChart(ctx, ci.root, ci.kubeconfig, ci.names[i], ci.enabled(i), ci.images, vals); err != nil {
		return err
	}
	ci.installed[i] = vals
	return nil
}

// before readies every cluster for b: where the chart watches the stories'
// namespaces, it returns the Runner.Fresh that installs it again, for their
// Roles, with what b runs under; otherwise it installs it again now where
// that differs from what it was last installed with.
func (ci *chartInstall) before(ctx context.Context, b *e2e.Bundle) (func(context.Context, int) error, error) {
	if len(ci.base.watch) > 0 {
		return func(ctx context.Context, i int) error {
			vals, err := ci.valuesFor(b, i)
			if err != nil {
				return err
			}
			logf("chart %s into %s/%s again, for its Roles", release, ci.names[i], releaseNS)
			return ci.upgrade(ctx, i, vals)
		}, nil
	}
	for i := range ci.names {
		vals, err := ci.valuesFor(b, i)
		if err != nil {
			return nil, err
		}
		if vals.values == ci.installed[i].values {
			continue
		}
		logf("chart %s into %s/%s again, for %s: %s", release, ci.names[i], releaseNS, b.Name, orNone(vals.values))
		if err := ci.upgrade(ctx, i, vals); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// after scrapes the home cluster's metrics where b asks for it.
func (ci *chartInstall) after(ctx context.Context, c client.Client, b *e2e.Bundle) error {
	if !b.ScrapeMetrics {
		return nil
	}
	logf("%s: scrape metrics", b.Name)
	args := chartSets(ci.enabled(0), ci.images, ci.installed[0])
	return scrapeMetrics(ctx, ci.home, c, ci.root, args, ci.otherCA)
}

func orNone(s string) string {
	if s == "" {
		return "default values"
	}
	return s
}

// restConfig returns the config reaching context name of kc.
func restConfig(kc *clientcmdapi.Config, name string) (*rest.Config, error) {
	rc, err := clientcmd.NewDefaultClientConfig(*kc, &clientcmd.ConfigOverrides{CurrentContext: name}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("context %s: %w", name, err)
	}
	return rc, nil
}

// watchedNamespaces are the namespaces the chart watches under
// E2E_WATCH_NAMESPACES: those stories declare objects in, plus the namespace
// of the auth controller's system connection where controllers has it.
func watchedNamespaces(stories []*e2e.Bundle, controllers []string) []string {
	var ns []string
	for _, b := range stories {
		ns = append(ns, b.Namespaces()...)
	}
	if slices.Contains(controllers, "auth") {
		ns = append(ns, strings.SplitN(authConnection, "/", 2)[0])
	}
	slices.Sort(ns)
	return slices.Compact(ns)
}

// createNamespaces creates each of names that c does not already hold.
func createNamespaces(ctx context.Context, c client.Client, names []string) error {
	for _, n := range names {
		err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n}})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create namespace %s: %w", n, err)
		}
	}
	return nil
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
		rc, err := restConfig(kc, n)
		if err != nil {
			return nil, err
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
// through c, as upgradeChart does, and runs its `helm test`. CRDs are
// applied first because helm installs a chart's crds/ only on first install.
func installChart(ctx context.Context, c client.Client, root, kubeconfig, name string, enabled []string, images []image, vals chartValues) error {
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
	if err := upgradeChart(ctx, root, kubeconfig, name, enabled, images, vals); err != nil {
		return err
	}
	if err := quietly(exec.CommandContext(ctx, "helm", "test", release, "--kubeconfig", kubeconfig,
		"--kube-context", name, "--namespace", releaseNS, "--logs", "--timeout", "3m")); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

// upgradeChart installs or upgrades the chart from root in context name of
// kubeconfig, with the controllers in enabled switched on and deployed from
// images, and set as vals; helm makes again any object of the release that
// has been deleted.
func upgradeChart(ctx context.Context, root, kubeconfig, name string, enabled []string, images []image, vals chartValues) error {
	args := []string{"upgrade", "--install", release, filepath.Join(root, "charts", "nats-operator"),
		"--kubeconfig", kubeconfig, "--kube-context", name,
		"--namespace", releaseNS, "--create-namespace", "--wait", "--timeout", "3m"}
	args = append(args, chartSets(enabled, images, vals)...)
	if err := quietly(exec.CommandContext(ctx, "helm", args...)); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

// chartValues are the chart values a run sets beyond the controllers and
// their images.
type chartValues struct {
	// watch are the namespaces the controllers watch; empty, every one.
	watch []string
	// allowGatewayWithoutTLS sets cluster.allowGatewayWithoutTLS, for a
	// story whose substitutions remove a gateway's tls.
	allowGatewayWithoutTLS bool
	// values is a values file, setting no value the fields above set; ""
	// is none.
	values string
}

// chartSets are the helm flags that switch on the controllers in enabled,
// and no other, deploy each controller from its image without pulling, and
// set vals.
func chartSets(enabled []string, images []image, vals chartValues) []string {
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
	if len(vals.watch) > 0 {
		sets = append(sets, "--set", "watchNamespaces={"+strings.Join(vals.watch, ",")+"}")
	}
	if vals.allowGatewayWithoutTLS {
		sets = append(sets, "--set", "cluster.allowGatewayWithoutTLS=true")
	}
	if vals.values != "" {
		sets = append(sets, "--values", vals.values)
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
