package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/mikluko/nats-operator/internal/e2e"
	"github.com/mikluko/nats-operator/internal/e2e/fixtures"
	"github.com/mikluko/nats-operator/internal/natscluster"
)

// What a story that scrapes metrics runs under: the Secret metrics are
// served under, the fixtures holding it and a Secret whose CA did not sign
// it, and the ServiceAccount that scrapes.
const (
	metricsSecret       = release + "-metrics-tls"
	metricsFixture      = "metrics-tls.yaml"
	otherMetricsFixture = "metrics-tls-other.yaml"
	scraperNamespace    = "monitoring"
	scraperName         = "prometheus"
)

// metricsDNSNames are the DNS names of every controller's metrics Service.
func metricsDNSNames() []string {
	var names []string
	for _, k := range []string{"cluster", "auth", "jetstream"} {
		names = append(names, fmt.Sprintf("%s-%s-controller-metrics.%s.svc", release, k, releaseNS))
	}
	return names
}

// generateMetricsFixtures writes metricsFixture and otherMetricsFixture into
// dir, each from a CA of its own.
func generateMetricsFixtures(dir string) error {
	for _, f := range []string{metricsFixture, otherMetricsFixture} {
		if err := fixtures.MetricsTLS(filepath.Join(dir, f), metricsSecret, releaseNS, metricsDNSNames()); err != nil {
			return err
		}
	}
	return nil
}

// apiServer is where the controllers reach the Kubernetes API server: the
// kubernetes Service's address, and its endpoints'.
type apiServer struct {
	service   netip.AddrPort
	endpoints []netip.AddrPort
}

// findAPIServer reads apiServer from default/kubernetes and its
// EndpointSlices through c.
func findAPIServer(ctx context.Context, c client.Client) (apiServer, error) {
	var api apiServer
	var svc corev1.Service
	if err := c.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: "kubernetes"}, &svc); err != nil {
		return api, fmt.Errorf("get Service default/kubernetes: %w", err)
	}
	addr, err := netip.ParseAddr(svc.Spec.ClusterIP)
	if err != nil || len(svc.Spec.Ports) != 1 {
		return api, fmt.Errorf("the kubernetes Service: want one address and one port, got %q and %d ports", svc.Spec.ClusterIP, len(svc.Spec.Ports))
	}
	api.service = netip.AddrPortFrom(addr, uint16(svc.Spec.Ports[0].Port))
	var eps discoveryv1.EndpointSliceList
	if err := c.List(ctx, &eps, client.InNamespace(metav1.NamespaceDefault),
		client.MatchingLabels{discoveryv1.LabelServiceName: "kubernetes"}); err != nil {
		return api, fmt.Errorf("list EndpointSlices of default/kubernetes: %w", err)
	}
	for _, s := range eps.Items {
		for _, p := range s.Ports {
			if p.Port == nil {
				continue
			}
			for _, ep := range s.Endpoints {
				for _, a := range ep.Addresses {
					addr, err := netip.ParseAddr(a)
					if err != nil {
						return api, fmt.Errorf("EndpointSlice %s: %w", s.Name, err)
					}
					api.endpoints = append(api.endpoints, netip.AddrPortFrom(addr, uint16(*p.Port)))
				}
			}
		}
	}
	if len(api.endpoints) == 0 {
		return api, errors.New("the kubernetes Service has no endpoints")
	}
	return api, nil
}

// metricsValues are the chart values a story that scrapes metrics installs:
// metrics served under metricsSecret on a Service, granted to the scraper;
// and a NetworkPolicy admitting the scraper's namespace to the metrics port,
// and egress to api, DNS, and the client and monitoring ports of the NATS
// servers in natsNamespaces, and to nothing else.
func metricsValues(api apiServer, natsNamespaces []string) map[string]any {
	namespace := func(ns string) map[string]any {
		return map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]any{corev1.LabelMetadataName: ns}}}
	}
	port := func(proto corev1.Protocol, p int) map[string]any {
		return map[string]any{"protocol": string(proto), "port": p}
	}
	ipBlock := func(a netip.Addr) map[string]any {
		return map[string]any{"ipBlock": map[string]any{"cidr": netip.PrefixFrom(a, a.BitLen()).String()}}
	}
	var addrs []netip.Addr
	var ports []uint16
	for _, ep := range api.endpoints {
		addrs = append(addrs, ep.Addr())
		ports = append(ports, ep.Port())
	}
	slices.SortFunc(addrs, netip.Addr.Compare)
	slices.Sort(ports)
	var endpointPeers, endpointPorts []any
	for _, a := range slices.Compact(addrs) {
		endpointPeers = append(endpointPeers, ipBlock(a))
	}
	for _, p := range slices.Compact(ports) {
		endpointPorts = append(endpointPorts, port(corev1.ProtocolTCP, int(p)))
	}
	dns := namespace(metav1.NamespaceSystem)
	dns["podSelector"] = map[string]any{"matchLabels": map[string]any{"k8s-app": "kube-dns"}}
	var nats []any
	for _, ns := range natsNamespaces {
		nats = append(nats, namespace(ns))
	}
	return map[string]any{
		"metrics": map[string]any{
			"service": map[string]any{"enabled": true},
			"tls":     map[string]any{"secretName": metricsSecret},
			"scraper": map[string]any{"serviceAccount": scraperNamespace + "/" + scraperName},
		},
		"networkPolicy": map[string]any{
			"enabled": true,
			"from":    []any{namespace(scraperNamespace)},
			"egress": []any{
				map[string]any{"to": []any{ipBlock(api.service.Addr())}, "ports": []any{port(corev1.ProtocolTCP, int(api.service.Port()))}},
				map[string]any{"to": endpointPeers, "ports": endpointPorts},
				map[string]any{"to": []any{dns}, "ports": []any{port(corev1.ProtocolUDP, 53), port(corev1.ProtocolTCP, 53)}},
				map[string]any{"to": nats, "ports": []any{port(corev1.ProtocolTCP, natscluster.PortClient), port(corev1.ProtocolTCP, natscluster.PortMonitor)}},
			},
		},
	}
}

// writeValues writes vals as YAML to path.
func writeValues(path string, vals map[string]any) error {
	raw, err := yaml.Marshal(vals)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

// renderChart returns the objects `helm template` renders from the chart
// under root with args.
func renderChart(ctx context.Context, root string, args []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "helm", append([]string{"template", release, filepath.Join(root, "charts", "nats-operator"),
		"--namespace", releaseNS}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("helm template: %w\n%s", err, stderr.String())
	}
	return out, nil
}

// scrapeMetrics scrapes every endpoint of the ServiceMonitors the chart
// under root renders with args and metrics.serviceMonitor.enabled, as
// Prometheus would: with the scraper's token, trusting only the CA its
// tlsConfig names and verifying serverName, through a port-forward to a
// ready pod behind it. It fails unless every scrape succeeds, and unless the
// same scrape trusting the CA of otherCA, a PEM file, fails verification.
func scrapeMetrics(ctx context.Context, rc *rest.Config, c client.Client, root string, args []string, otherCA []byte) error {
	rendered, err := renderChart(ctx, root, append(slices.Clone(args), "--set", "metrics.serviceMonitor.enabled=true"))
	if err != nil {
		return err
	}
	objs, err := e2e.DecodeObjects(rendered)
	if err != nil {
		return err
	}
	endpoints, err := e2e.MetricsEndpoints(objs)
	if err != nil {
		return err
	}
	if len(endpoints) == 0 {
		return errors.New("the chart renders no ServiceMonitor endpoint over https")
	}
	token, err := scraperToken(ctx, c)
	if err != nil {
		return err
	}
	other := x509.NewCertPool()
	if !other.AppendCertsFromPEM(otherCA) {
		return errors.New("the other CA holds no PEM certificate")
	}
	for _, ep := range endpoints {
		if err := scrapeEndpoint(ctx, rc, c, ep, token, other); err != nil {
			return fmt.Errorf("ServiceMonitor %s/%s: %w", ep.Namespace, ep.Monitor, err)
		}
	}
	return nil
}

// scrapeEndpoint scrapes ep as scrapeMetrics says.
func scrapeEndpoint(ctx context.Context, rc *rest.Config, c client.Client, ep e2e.MetricsEndpoint, token string, other *x509.CertPool) error {
	var ca corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: ep.Namespace, Name: ep.CA.Name}, &ca); err != nil {
		return fmt.Errorf("get CA Secret: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca.Data[ep.CA.Key]) {
		return fmt.Errorf("key %s of Secret %s holds no PEM certificate", ep.CA.Name, ep.CA.Key)
	}
	pod, port, err := endpointPod(ctx, c, ep)
	if err != nil {
		return err
	}
	addr, stop, err := forward(ctx, rc, pod, port)
	if err != nil {
		return fmt.Errorf("port-forward to %s/%s:%d: %w", pod.Namespace, pod.Name, port, err)
	}
	defer stop()
	if _, err := e2e.Scrape(ctx, addr, ep.Path, roots, ep.ServerName, token); err != nil {
		return fmt.Errorf("scrape %s as %s: %w", pod.Name, ep.ServerName, err)
	}
	_, err = e2e.Scrape(ctx, addr, ep.Path, other, ep.ServerName, token)
	switch _, unknown := errors.AsType[x509.UnknownAuthorityError](err); {
	case err == nil:
		return fmt.Errorf("scrape %s trusting another CA succeeded", pod.Name)
	case !unknown:
		return fmt.Errorf("scrape %s trusting another CA: want an unknown authority, got %w", pod.Name, err)
	}
	return nil
}

// endpointPod returns a ready pod behind the one Service ep selects that has
// ep's port, as Prometheus skips a selected Service without it, and the
// pod's port that Service port targets.
func endpointPod(ctx context.Context, c client.Client, ep e2e.MetricsEndpoint) (*corev1.Pod, int32, error) {
	var svcs corev1.ServiceList
	if err := c.List(ctx, &svcs, client.InNamespace(ep.Namespace), client.MatchingLabels(ep.Selector)); err != nil {
		return nil, 0, fmt.Errorf("list Services: %w", err)
	}
	named := func(p corev1.ServicePort) bool { return p.Name == ep.Port }
	svcs.Items = slices.DeleteFunc(svcs.Items, func(s corev1.Service) bool { return !slices.ContainsFunc(s.Spec.Ports, named) })
	if len(svcs.Items) != 1 {
		return nil, 0, fmt.Errorf("selects %d Services with port %s, want 1", len(svcs.Items), ep.Port)
	}
	svc := svcs.Items[0]
	target := svc.Spec.Ports[slices.IndexFunc(svc.Spec.Ports, named)].TargetPort
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(ep.Namespace), client.MatchingLabels(svc.Spec.Selector)); err != nil {
		return nil, 0, fmt.Errorf("list pods of Service %s: %w", svc.Name, err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !podReady(pod) {
			continue
		}
		if target.Type == intstr.Int {
			return pod, target.IntVal, nil
		}
		for _, ctr := range pod.Spec.Containers {
			for _, p := range ctr.Ports {
				if p.Name == target.StrVal {
					return pod, p.ContainerPort, nil
				}
			}
		}
		return nil, 0, fmt.Errorf("pod %s has no port %s", pod.Name, target.StrVal)
	}
	return nil, 0, fmt.Errorf("no ready pod behind Service %s", svc.Name)
}

func podReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	return slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool {
		return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
	})
}

// scraperToken returns a token of the scraper ServiceAccount, created with
// its namespace where either is missing.
func scraperToken(ctx context.Context, c client.Client) (string, error) {
	if err := createNamespaces(ctx, c, []string{scraperNamespace}); err != nil {
		return "", err
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: scraperNamespace, Name: scraperName}}
	if err := c.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create ServiceAccount %s/%s: %w", scraperNamespace, scraperName, err)
	}
	tr := &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: new(int64(600))}}
	if err := c.SubResource("token").Create(ctx, sa, tr); err != nil {
		return "", fmt.Errorf("token of ServiceAccount %s/%s: %w", scraperNamespace, scraperName, err)
	}
	return tr.Status.Token, nil
}

// forward forwards a local port to port of pod through the API server
// rc reaches, and returns the local address and the function that stops it.
func forward(ctx context.Context, rc *rest.Config, pod *corev1.Pod, port int32) (string, func(), error) {
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return "", nil, err
	}
	transport, upgrader, err := spdy.RoundTripperFor(rc)
	if err != nil {
		return "", nil, err
	}
	url := cs.CoreV1().RESTClient().Post().Resource("pods").Namespace(pod.Namespace).Name(pod.Name).SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)
	stop, ready := make(chan struct{}), make(chan struct{})
	fw, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"}, []string{"0:" + strconv.Itoa(int(port))}, stop, ready, io.Discard, io.Discard)
	if err != nil {
		return "", nil, err
	}
	done := make(chan error, 1)
	go func() { done <- fw.ForwardPorts() }()
	select {
	case <-ready:
	case err := <-done:
		return "", nil, err
	case <-ctx.Done():
		close(stop)
		return "", nil, ctx.Err()
	}
	ports, err := fw.GetPorts()
	if err != nil || len(ports) != 1 {
		close(stop)
		return "", nil, fmt.Errorf("forwarded ports %v: %w", ports, err)
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(int(ports[0].Local))), func() { close(stop) }, nil
}
