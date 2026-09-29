package e2e

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// SecretKey is one key of a Secret in the namespace of what names it.
type SecretKey struct {
	Name, Key string
}

// MetricsEndpoint is one HTTPS endpoint of a ServiceMonitor, as Prometheus
// scrapes it.
type MetricsEndpoint struct {
	// Namespace and Monitor name the ServiceMonitor.
	Namespace, Monitor string
	// Selector is the matchLabels selecting the Services scraped.
	Selector map[string]string
	// Port names the Service port scraped, and Path the path.
	Port, Path string
	// CA names the Secret key holding the PEM CA the serving certificate is
	// verified against.
	CA SecretKey
	// ServerName is the name the serving certificate is verified for.
	ServerName string
	// BearerTokenFile is the scraper's path to the token it sends; "" is
	// none.
	BearerTokenFile string
}

// MetricsEndpoints returns the endpoints of scheme https of the
// ServiceMonitors among objs; it fails on one whose tlsConfig does not name a
// CA Secret key and a serverName.
func MetricsEndpoints(objs []*unstructured.Unstructured) ([]MetricsEndpoint, error) {
	var out []MetricsEndpoint
	for _, o := range objs {
		if o.GetKind() != "ServiceMonitor" {
			continue
		}
		selector, _, err := unstructured.NestedStringMap(o.Object, "spec", "selector", "matchLabels")
		if err != nil {
			return nil, fmt.Errorf("ServiceMonitor %s: %w", key(o), err)
		}
		endpoints, _, err := unstructured.NestedSlice(o.Object, "spec", "endpoints")
		if err != nil {
			return nil, fmt.Errorf("ServiceMonitor %s: %w", key(o), err)
		}
		for i, raw := range endpoints {
			ep, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("ServiceMonitor %s: endpoint %d is not an object", key(o), i)
			}
			if scheme, _, _ := unstructured.NestedString(ep, "scheme"); scheme != "https" {
				continue
			}
			m := MetricsEndpoint{Namespace: o.GetNamespace(), Monitor: o.GetName(), Selector: selector}
			m.Port, _, _ = unstructured.NestedString(ep, "port")
			m.Path, _, _ = unstructured.NestedString(ep, "path")
			m.CA.Name, _, _ = unstructured.NestedString(ep, "tlsConfig", "ca", "secret", "name")
			m.CA.Key, _, _ = unstructured.NestedString(ep, "tlsConfig", "ca", "secret", "key")
			m.ServerName, _, _ = unstructured.NestedString(ep, "tlsConfig", "serverName")
			m.BearerTokenFile, _, _ = unstructured.NestedString(ep, "bearerTokenFile")
			if m.CA.Name == "" || m.CA.Key == "" || m.ServerName == "" {
				return nil, fmt.Errorf("ServiceMonitor %s: endpoint %d does not verify: tlsConfig names no ca.secret or serverName", key(o), i)
			}
			out = append(out, m)
		}
	}
	return out, nil
}

// ExitUnverified is the exit status of a ScrapeCommand whose server's
// certificate does not verify.
const ExitUnverified = 60

// ScrapeCommand is the curl command line that GETs path over HTTPS from
// serverName, resolved to addr, on port, through no proxy, with the bearer
// token in tokenFile, trusting caFile alone and verifying the certificate for
// serverName. curl exits 0 on a 2xx answer, ExitUnverified where the
// certificate does not verify, and 22 on an HTTP error.
func ScrapeCommand(addr netip.Addr, port int32, serverName, path, caFile, tokenFile string) []string {
	resolved := addr.String()
	if addr.Is6() {
		resolved = "[" + resolved + "]"
	}
	hostPort := net.JoinHostPort(serverName, strconv.Itoa(int(port)))
	return []string{
		"curl", "--silent", "--show-error", "--fail", "--max-time", "10", "--output", "/dev/null", "--noproxy", "*",
		"--cacert", caFile,
		"--resolve", hostPort + ":" + resolved,
		"--variable", "token@" + tokenFile,
		"--expand-header", "Authorization: Bearer {{token:trim}}",
		"https://" + hostPort + path,
	}
}
