package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MetalLB's native manifest and its SHA-256.
const (
	MetalLBManifestURL    = "https://raw.githubusercontent.com/metallb/metallb/v0.16.0/config/manifests/metallb-native.yaml"
	MetalLBManifestSHA256 = "b0b9be2802f10aa32d45308b4457d06cde0c70544712c8d0cf5511657ffd2b69"
)

// ApplyObjects server-side applies every object in objs through c, in
// order, taking ownership of every field it sets.
func ApplyObjects(ctx context.Context, c client.Client, objs []*unstructured.Unstructured) error {
	for _, o := range objs {
		if err := c.Apply(ctx, client.ApplyConfigurationFromUnstructured(o.DeepCopy()), client.FieldOwner(fieldOwner), client.ForceOwnership); err != nil {
			return fmt.Errorf("apply %s %s: %w", o.GetKind(), key(o), err)
		}
	}
	return nil
}

// FetchMetalLB returns the manifest at MetalLBManifestURL, failing unless
// its SHA-256 is MetalLBManifestSHA256.
func FetchMetalLB(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, MetalLBManifestURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch MetalLB manifest: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch MetalLB manifest: %s", resp.Status)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("fetch MetalLB manifest: %w", err)
	}
	if err := checkSHA256(raw, MetalLBManifestSHA256); err != nil {
		return nil, fmt.Errorf("MetalLB manifest: %w", err)
	}
	return raw, nil
}

func checkSHA256(raw []byte, want string) error {
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != want {
		return fmt.Errorf("sha256 %s, want %s", got, want)
	}
	return nil
}

// PoolRange returns the ten addresses of subnet that Kubernetes cluster
// number i, from 0, hands to LoadBalancer Services: host addresses 100+10i to
// 109+10i, clear of the node addresses podman assigns from the bottom of the
// subnet.
func PoolRange(subnet netip.Prefix, i int) (first, last netip.Addr, err error) {
	if !subnet.Addr().Is4() || subnet.Bits() > 24 {
		return first, last, fmt.Errorf("subnet %s is not an IPv4 subnet of /24 or wider", subnet)
	}
	if i < 0 || 110+10*i > 255 {
		return first, last, fmt.Errorf("cluster %d has no address pool in %s", i, subnet)
	}
	base := binary.BigEndian.Uint32(subnet.Masked().Addr().AsSlice())
	first = netip.AddrFrom4(be4(base + uint32(100+10*i)))
	last = netip.AddrFrom4(be4(base + uint32(109+10*i)))
	return first, last, nil
}

func be4(v uint32) [4]byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return b
}

// InstallMetalLB applies the MetalLB manifest through c, then a layer 2
// address pool of first to last, retrying the pool until timeout while
// MetalLB's CRDs and webhook come up.
func InstallMetalLB(ctx context.Context, c client.Client, manifest []byte, first, last netip.Addr, timeout, interval time.Duration) error {
	objs, err := DecodeObjects(manifest)
	if err != nil {
		return fmt.Errorf("MetalLB manifest: %w", err)
	}
	if err := ApplyObjects(ctx, c, objs); err != nil {
		return err
	}
	pool, err := DecodeObjects(fmt.Appendf(nil, metallbPool, first, last))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		err := ApplyObjects(ctx, c, pool)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("MetalLB address pool: %w", err)
		case <-time.After(interval):
		}
	}
}

const metallbPool = `apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: e2e
  namespace: metallb-system
spec:
  addresses: [%s-%s]
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: e2e
  namespace: metallb-system
spec:
  ipAddressPools: [e2e]
`

// Corefile is kube-system/coredns's Corefile on every Kubernetes cluster:
// kind's with a hosts plugin reading HostsKey ahead of the rest.
const Corefile = `.:53 {
    errors
    health {
       lameduck 5s
    }
    ready
    hosts /etc/coredns/` + HostsKey + ` {
       fallthrough
    }
    kubernetes cluster.local in-addr.arpa ip6.arpa {
       pods insecure
       fallthrough in-addr.arpa ip6.arpa
       ttl 30
    }
    prometheus :9153
    forward . /etc/resolv.conf {
       max_concurrent 1000
    }
    cache 30 {
       disable success cluster.local
       disable denial cluster.local
    }
    loop
    reload
    loadbalance
}
`

// ServeHosts makes CoreDNS resolve the names PublishHosts writes: it sets
// Corefile in kube-system/coredns, and mounts every key of that ConfigMap
// into the coredns Deployment rather than the Corefile alone.
func ServeHosts(ctx context.Context, c client.Client) error {
	var cm corev1.ConfigMap
	if err := c.Get(ctx, client.ObjectKey{Namespace: "kube-system", Name: "coredns"}, &cm); err != nil {
		return fmt.Errorf("get coredns ConfigMap: %w", err)
	}
	if cm.Data["Corefile"] != Corefile {
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data["Corefile"] = Corefile
		if err := c.Update(ctx, &cm); err != nil {
			return fmt.Errorf("update coredns ConfigMap: %w", err)
		}
	}
	var d appsv1.Deployment
	if err := c.Get(ctx, client.ObjectKey{Namespace: "kube-system", Name: "coredns"}, &d); err != nil {
		return fmt.Errorf("get coredns Deployment: %w", err)
	}
	changed := false
	for i := range d.Spec.Template.Spec.Volumes {
		v := &d.Spec.Template.Spec.Volumes[i]
		if v.ConfigMap != nil && v.ConfigMap.Name == "coredns" && v.ConfigMap.Items != nil {
			v.ConfigMap.Items = nil
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := c.Update(ctx, &d); err != nil {
		return fmt.Errorf("update coredns Deployment: %w", err)
	}
	return nil
}
