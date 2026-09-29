package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/mikluko/nats-operator/internal/e2e"
	"github.com/mikluko/nats-operator/internal/e2e/fixtures"
)

// What a story that scrapes metrics runs under: the Secret metrics are
// served under, the fixtures holding it and a Secret whose CA did not sign
// it, the ServiceAccount that scrapes, and the image it scrapes with.
const (
	metricsSecret       = release + "-metrics-tls"
	otherMetricsSecret  = release + "-metrics-tls-other"
	metricsFixture      = "metrics-tls.yaml"
	otherMetricsFixture = "metrics-tls-other.yaml"
	scraperNamespace    = "monitoring"
	scraperName         = "prometheus"
	scraperImage        = "docker.io/curlimages/curl:8.22.0@sha256:58adaa4e8dca9c988bae2aba4ab3434a0bb2da16bbe3f92dec39ec7785166777"
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
	for f, name := range map[string]string{metricsFixture: metricsSecret, otherMetricsFixture: otherMetricsSecret} {
		if err := fixtures.MetricsTLS(filepath.Join(dir, f), name, releaseNS, metricsDNSNames()); err != nil {
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
// page, the values its page shows, with every egress rule whose peers are all
// ipBlocks replaced by rules admitting api, and without ServiceMonitors. It
// fails unless page serves metrics under metricsSecret to the scraper's
// ServiceAccount and has such a rule.
func metricsValues(page map[string]any, api apiServer) (map[string]any, error) {
	vals := runtime.DeepCopyJSON(page)
	for path, want := range map[string]string{
		"metrics.tls.secretName":         metricsSecret,
		"metrics.scraper.serviceAccount": scraperNamespace + "/" + scraperName,
	} {
		got, _, err := unstructured.NestedString(vals, strings.Split(path, ".")...)
		if err != nil || got != want {
			return nil, fmt.Errorf("%s: want %q, got %q", path, want, got)
		}
	}
	if err := unstructured.SetNestedField(vals, false, "metrics", "serviceMonitor", "enabled"); err != nil {
		return nil, err
	}
	egress, _, err := unstructured.NestedSlice(vals, "networkPolicy", "egress")
	if err != nil {
		return nil, err
	}
	at := slices.IndexFunc(egress, onlyIPBlocks)
	if at < 0 {
		return nil, errors.New("networkPolicy.egress: no rule of ipBlocks alone, which the API server's rules replace")
	}
	egress = slices.DeleteFunc(egress, onlyIPBlocks)
	egress = slices.Insert(egress, at, apiServerRules(api)...)
	if err := unstructured.SetNestedSlice(vals, egress, "networkPolicy", "egress"); err != nil {
		return nil, err
	}
	return vals, nil
}

// onlyIPBlocks reports whether rule is an egress rule to ipBlock peers alone.
func onlyIPBlocks(rule any) bool {
	to, _, _ := unstructured.NestedSlice(rule.(map[string]any), "to")
	return len(to) > 0 && !slices.ContainsFunc(to, func(peer any) bool {
		p, ok := peer.(map[string]any)
		_, block := p["ipBlock"]
		return !ok || len(p) != 1 || !block
	})
}

// apiServerRules are the egress rules admitting api: its Service address
// and port, and its endpoints' addresses and ports.
func apiServerRules(api apiServer) []any {
	port := func(p uint16) any {
		return map[string]any{"protocol": string(corev1.ProtocolTCP), "port": int64(p)}
	}
	ipBlock := func(a netip.Addr) any {
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
	var peers, endpointPorts []any
	for _, a := range slices.Compact(addrs) {
		peers = append(peers, ipBlock(a))
	}
	for _, p := range slices.Compact(ports) {
		endpointPorts = append(endpointPorts, port(p))
	}
	return []any{
		map[string]any{"to": []any{ipBlock(api.service.Addr())}, "ports": []any{port(api.service.Port())}},
		map[string]any{"to": peers, "ports": endpointPorts},
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

// scrapeMetrics scrapes every https endpoint of the ServiceMonitors the
// chart renders with args, as Prometheus would; it fails unless each scrape
// succeeds and the same scrape trusting otherCA, PEM, fails verification.
func scrapeMetrics(ctx context.Context, cs kubernetes.Interface, c client.Client, root string, args []string, otherCA []byte) error {
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
	if err := createScraper(ctx, c); err != nil {
		return err
	}
	for _, ep := range endpoints {
		if err := scrapeEndpoint(ctx, cs, c, ep, otherCA); err != nil {
			return fmt.Errorf("ServiceMonitor %s/%s: %w", ep.Namespace, ep.Monitor, err)
		}
	}
	return nil
}

// Keys of the ConfigMap a scraper pod mounts at scraperCADir: the CA ep's
// tlsConfig names, and the one that did not sign the serving certificate.
const (
	scraperCADir   = "/etc/scrape"
	scraperCA      = "ca.crt"
	scraperOtherCA = "other-ca.crt"
)

// scrapeEndpoint scrapes ep from a pod in scraperNamespace running as the
// scraper, with ep's bearerTokenFile, to the IP of a ready pod behind it:
// once trusting only the CA ep's tlsConfig names, which must succeed, and
// once trusting otherCA alone, which must fail verification.
func scrapeEndpoint(ctx context.Context, cs kubernetes.Interface, c client.Client, ep e2e.MetricsEndpoint, otherCA []byte) error {
	if ep.BearerTokenFile == "" {
		return errors.New("the endpoint names no bearerTokenFile")
	}
	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: ep.Namespace, Name: ep.CA.Name}, &secret); err != nil {
		return fmt.Errorf("get CA Secret: %w", err)
	}
	ca := secret.Data[ep.CA.Key]
	if !x509.NewCertPool().AppendCertsFromPEM(ca) {
		return fmt.Errorf("key %s of Secret %s holds no PEM certificate", ep.CA.Key, ep.CA.Name)
	}
	target, port, err := endpointPod(ctx, c, ep)
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(target.Status.PodIP)
	if err != nil {
		return fmt.Errorf("pod %s: IP %q: %w", target.Name, target.Status.PodIP, err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: scraperNamespace, GenerateName: "scrape-"},
		Data:       map[string]string{scraperCA: string(ca), scraperOtherCA: string(otherCA)},
	}
	if err := c.Create(ctx, cm); err != nil {
		return fmt.Errorf("create ConfigMap in %s: %w", scraperNamespace, err)
	}
	defer func() { _ = c.Delete(context.WithoutCancel(ctx), cm) }()
	container := func(name, caKey string) corev1.Container {
		return corev1.Container{
			Name:         name,
			Image:        scraperImage,
			Command:      e2e.ScrapeCommand(ip, port, ep.ServerName, ep.Path, scraperCADir+"/"+caKey, ep.BearerTokenFile),
			VolumeMounts: []corev1.VolumeMount{{Name: "ca", MountPath: scraperCADir, ReadOnly: true}},
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: new(false),
				ReadOnlyRootFilesystem:   new(true),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
		}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: scraperNamespace, Name: cm.Name},
		Spec: corev1.PodSpec{
			ServiceAccountName: scraperName,
			RestartPolicy:      corev1.RestartPolicyNever,
			Containers:         []corev1.Container{container("verified", scraperCA), container("other-ca", scraperOtherCA)},
			Volumes: []corev1.Volume{{Name: "ca", VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cm.Name}},
			}}},
		},
	}
	if err := c.Create(ctx, pod); err != nil {
		return fmt.Errorf("create pod in %s: %w", scraperNamespace, err)
	}
	defer func() { _ = c.Delete(context.WithoutCancel(ctx), pod) }()
	exits, err := awaitExits(ctx, c, pod)
	if err != nil {
		return err
	}
	for name, want := range map[string]int32{"verified": 0, "other-ca": e2e.ExitUnverified} {
		if got := exits[name]; got != want {
			return fmt.Errorf("scrape of pod %s at %s as %s, trusting %s: curl exited %d, want %d: %s",
				target.Name, ip, ep.ServerName, name, got, want, containerLog(ctx, cs, pod, name))
		}
	}
	return nil
}

// scrapeTimeout bounds a scraper pod's run, pulling its image included.
const scrapeTimeout = 3 * time.Minute

// awaitExits waits until every container of pod has terminated and returns
// their exit codes by name.
func awaitExits(ctx context.Context, c client.Client, pod *corev1.Pod) (map[string]int32, error) {
	ctx, cancel := context.WithTimeout(ctx, scrapeTimeout)
	defer cancel()
	for {
		var live corev1.Pod
		if err := c.Get(ctx, client.ObjectKeyFromObject(pod), &live); err != nil {
			return nil, fmt.Errorf("pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		exits := map[string]int32{}
		var waiting []string
		for _, st := range live.Status.ContainerStatuses {
			switch {
			case st.State.Terminated != nil:
				exits[st.Name] = st.State.Terminated.ExitCode
			case st.State.Waiting != nil:
				waiting = append(waiting, st.Name+": "+st.State.Waiting.Reason)
			}
		}
		if len(exits) == len(pod.Spec.Containers) {
			return exits, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("pod %s/%s: containers still running after %s: %v", pod.Namespace, pod.Name, scrapeTimeout, waiting)
		case <-time.After(2 * time.Second):
		}
	}
}

// containerLog returns the log of container name of pod, or why it cannot.
func containerLog(ctx context.Context, cs kubernetes.Interface, pod *corev1.Pod, name string) string {
	raw, err := cs.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: name}).DoRaw(ctx)
	if err != nil {
		return fmt.Sprintf("no log: %v", err)
	}
	return strings.TrimSpace(string(raw))
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

// createScraper creates the scraper's namespace and ServiceAccount where
// either is missing.
func createScraper(ctx context.Context, c client.Client) error {
	if err := createNamespaces(ctx, c, []string{scraperNamespace}); err != nil {
		return err
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: scraperNamespace, Name: scraperName}}
	if err := c.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create ServiceAccount %s/%s: %w", scraperNamespace, scraperName, err)
	}
	return nil
}
