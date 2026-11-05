package natscluster

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// clientURL is the URL of nc's client Service: tls:// under client TLS,
// nats:// without it or while plain clients are allowed.
func clientURL(nc *clusterv1beta1.NatsCluster) string {
	scheme := "tls"
	if clientPlain(nc) {
		scheme = "nats"
	}
	return fmt.Sprintf("%s://%s.%s.svc:%d", scheme, clientServiceName(nc), nc.Namespace, PortClient)
}

// clientPlain reports whether the cluster controller connects to nc's
// client Service in the clear: without client TLS, or with plain clients
// allowed.
func clientPlain(nc *clusterv1beta1.NatsCluster) bool {
	return nc.Spec.TLS == nil || nc.Spec.TLS.AllowNonTLS
}

func clientCertSecretName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-client-tls" }

func clientCertificateName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-client" }

// clientCertSecret names the Secret the client listener's certificate is
// mounted from, or "" when clients connect in the clear.
func clientCertSecret(nc *clusterv1beta1.NatsCluster) string {
	switch t := nc.Spec.TLS; {
	case t == nil:
		return ""
	case t.SecretRef != nil:
		return t.SecretRef.Name
	default:
		return clientCertSecretName(nc)
	}
}

// clientIssuer returns the issuer the client listener's certificate comes
// from, or nil when cert-manager does not issue it.
func clientIssuer(nc *clusterv1beta1.NatsCluster) *clusterv1beta1.IssuerReference {
	if t := nc.Spec.TLS; t != nil && t.CertManager != nil {
		return &t.CertManager.IssuerRef
	}
	return nil
}

// clientHosts are the names of nc's client Service.
func clientHosts(nc *clusterv1beta1.NatsCluster) []string {
	name, ns := clientServiceName(nc), nc.Namespace
	return []string{name, name + "." + ns, name + "." + ns + ".svc", name + "." + ns + ".svc.cluster.local"}
}

// clientCertificate is the cert-manager Certificate issuing the client
// listener's certificate for clientHosts.
func clientCertificate(nc *clusterv1beta1.NatsCluster, issuer *clusterv1beta1.IssuerReference) *unstructured.Unstructured {
	return certificate(nc, clientCertificateName(nc), clientCertSecretName(nc), issuer, clientHosts(nc), []string{"server auth"})
}
