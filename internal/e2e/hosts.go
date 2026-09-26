package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HostnameAnnotation is external-dns's annotation naming a Service's
// hostnames, comma-separated.
const HostnameAnnotation = "external-dns.alpha.kubernetes.io/hostname"

// HostsKey is the key of kube-system/coredns that PublishHosts writes; the
// Corefile hack/e2e.sh installs reads it through the hosts plugin.
const HostsKey = "e2e.hosts"

// PublishHosts stands in for external-dns across the Kubernetes clusters
// clients reach: it maps every hostname a LoadBalancer Service in any of
// them carries in HostnameAnnotation to that Service's ingress IPs, and
// writes the mapping, in hosts file form, to HostsKey of kube-system/coredns
// in each. A Service with no ingress IP yet is left out; a cluster without
// that ConfigMap is left alone, and one whose ConfigMap changed underneath
// the write is left for the next call.
func PublishHosts(ctx context.Context, clients []client.Client) error {
	var lines []string
	for _, c := range clients {
		var svcs corev1.ServiceList
		if err := c.List(ctx, &svcs); err != nil {
			return fmt.Errorf("list services: %w", err)
		}
		for _, s := range svcs.Items {
			names := s.Annotations[HostnameAnnotation]
			if s.Spec.Type != corev1.ServiceTypeLoadBalancer || names == "" {
				continue
			}
			for _, in := range s.Status.LoadBalancer.Ingress {
				if in.IP == "" {
					continue
				}
				for n := range strings.SplitSeq(names, ",") {
					if n = strings.TrimSpace(n); n != "" {
						lines = append(lines, in.IP+" "+n)
					}
				}
			}
		}
	}
	slices.Sort(lines)
	hosts := strings.Join(slices.Compact(lines), "\n")
	if hosts != "" {
		hosts += "\n"
	}
	for _, c := range clients {
		var cm corev1.ConfigMap
		err := c.Get(ctx, client.ObjectKey{Namespace: "kube-system", Name: "coredns"}, &cm)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("get coredns ConfigMap: %w", err)
		}
		if cm.Data[HostsKey] == hosts {
			continue
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[HostsKey] = hosts
		if err := c.Update(ctx, &cm); err != nil && !apierrors.IsConflict(err) {
			return fmt.Errorf("update coredns ConfigMap: %w", err)
		}
	}
	return nil
}
