package natscluster

import (
	"fmt"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// Labels and annotations the cluster controller sets on what it renders.
const (
	LabelCluster = "cluster.nats.mikluko.io/cluster"
	LabelServer  = "cluster.nats.mikluko.io/server"

	// AnnotationConfigRevision on a server's StatefulSet and ConfigMap names
	// the config revision they were rendered at; on its pod template, the
	// revision its pod was last restarted for, so that every restart changes
	// the template.
	AnnotationConfigRevision = "cluster.nats.mikluko.io/config-revision"

	// AnnotationSpecDigest on a server's StatefulSet is a digest of the
	// spec it was rendered with; a change to it restarts the server.
	AnnotationSpecDigest = "cluster.nats.mikluko.io/spec-digest"

	// AnnotationVolumeDigest on a server's StatefulSet is a digest of the
	// volume claim templates it was created with; a change to it replaces
	// the server.
	AnnotationVolumeDigest = "cluster.nats.mikluko.io/volume-digest"

	// AnnotationConfigApply on a server's ConfigMap is how the revision it
	// holds is applied: Reload while the cluster controller reloads it,
	// Restart once the reload failed. A ConfigMap written for a restart
	// carries Restart or nothing; only Reload is ever reloaded.
	AnnotationConfigApply = "cluster.nats.mikluko.io/config-apply"

	// AnnotationRestartReason on a server's ConfigMap names what made its
	// revision restart-only.
	AnnotationRestartReason = "cluster.nats.mikluko.io/restart-reason"

	// AnnotationReloadSince on a server's ConfigMap is when it was written
	// for a reload, in RFC 3339.
	AnnotationReloadSince = "cluster.nats.mikluko.io/reload-since"

	// MetadataConfigRevision is the server_metadata key a server reports
	// its config revision under.
	MetadataConfigRevision = "config_revision"
)

// Ports every server listens on.
const (
	PortClient  = 4222
	PortRoute   = 6222
	PortMonitor = 8222
	PortGateway = 7222
	PortMetrics = 7777
)

// serverName is the server_name of the i-th server, which also names its
// StatefulSet.
func serverName(nc *clusterv1beta1.NatsCluster, i int) string {
	return fmt.Sprintf("%s-%d", nc.Name, i)
}

func serverNames(nc *clusterv1beta1.NatsCluster) []string {
	out := make([]string, nc.Spec.Replicas)
	for i := range out {
		out[i] = serverName(nc, i)
	}
	return out
}

func clientServiceName(nc *clusterv1beta1.NatsCluster) string { return nc.Name }

func headlessServiceName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-headless" }

func configMapName(server string) string { return server + "-config" }

func routesSecretName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-routes-tls" }

func gatewayServiceName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-gateway" }

// gatewaySecretName is the Secret cert-manager issues the gateway
// certificate into.
func gatewaySecretName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-gateway-tls" }

// podHost is the DNS name of a server's pod under the headless Service.
func podHost(nc *clusterv1beta1.NatsCluster, server string) string {
	return fmt.Sprintf("%s-0.%s.%s.svc", server, headlessServiceName(nc), nc.Namespace)
}

// routeDNSNames are the names a route certificate covers: every pod of the
// NATS cluster under the headless Service.
func routeDNSNames(nc *clusterv1beta1.NatsCluster) []string {
	base := fmt.Sprintf("%s.%s.svc", headlessServiceName(nc), nc.Namespace)
	return []string{"*." + base, "*." + base + ".cluster.local"}
}

func labels(nc *clusterv1beta1.NatsCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "nats",
		"app.kubernetes.io/instance":   nc.Name,
		"app.kubernetes.io/managed-by": "cluster-controller",
		LabelCluster:                   nc.Name,
	}
}

func serverLabels(nc *clusterv1beta1.NatsCluster, server string) map[string]string {
	l := labels(nc)
	l[LabelServer] = server
	return l
}

func clusterSelector(nc *clusterv1beta1.NatsCluster) map[string]string {
	return map[string]string{LabelCluster: nc.Name}
}

func serverSelector(nc *clusterv1beta1.NatsCluster, server string) map[string]string {
	return map[string]string{LabelCluster: nc.Name, LabelServer: server}
}

// dataClaimName is the PersistentVolumeClaim a server's StatefulSet creates
// from its data volume claim template.
func dataClaimName(server string) string { return "data-" + server + "-0" }
