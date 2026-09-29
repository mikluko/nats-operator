package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"

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
	// Port names the Service port scraped.
	Port, Path string
	// CA holds the PEM CA the serving certificate is verified against.
	CA SecretKey
	// ServerName is the name the serving certificate is verified for.
	ServerName string
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
			if m.CA.Name == "" || m.CA.Key == "" || m.ServerName == "" {
				return nil, fmt.Errorf("ServiceMonitor %s: endpoint %d does not verify: tlsConfig names no ca.secret or serverName", key(o), i)
			}
			out = append(out, m)
		}
	}
	return out, nil
}

// Scrape GETs path from addr over HTTPS with bearer token, trusting roots
// alone and verifying the certificate for serverName, and returns the body;
// it fails unless the answer is 200 OK.
func Scrape(ctx context.Context, addr, path string, roots *x509.CertPool, serverName, token string) ([]byte, error) {
	hc := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: serverName, MinVersion: tls.VersionTLS12},
	}}
	defer hc.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", path, resp.Status)
	}
	return body, nil
}
