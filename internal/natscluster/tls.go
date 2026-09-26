package natscluster

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// caKey is the key of the CA certificate in a TLS Secret.
const caKey = "ca.crt"

const selfSignedValidity = 10 * 365 * 24 * time.Hour

// selfSignedRouteSecret returns a kubernetes.io/tls Secret holding a new CA
// and a certificate it signs for hosts, for both server and client auth. A
// host that parses as an IP address is an IP SAN.
func selfSignedRouteSecret(nc *clusterv1beta1.NatsCluster, hosts []string, now time.Time) (*corev1.Secret, error) {
	caKeyPair, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: nc.Name + " route CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(selfSignedValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKeyPair.PublicKey, caKeyPair)
	if err != nil {
		return nil, fmt.Errorf("sign route CA: %w", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: nc.Name + " routes"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(selfSignedValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			leafTmpl.IPAddresses = append(leafTmpl.IPAddresses, ip)
		} else {
			leafTmpl.DNSNames = append(leafTmpl.DNSNames, h)
		}
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, ca, &leafKey.PublicKey, caKeyPair)
	if err != nil {
		return nil, fmt.Errorf("sign route certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return nil, err
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: routesSecretName(nc), Namespace: nc.Namespace, Labels: labels(nc)},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			caKey:                   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
			corev1.TLSCertKey:       pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
			corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		},
	}, nil
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		panic(err)
	}
	return n
}

// routesSecret names the Secret the route certificate is mounted from, or
// "" when route TLS is off.
func routesSecret(nc *clusterv1beta1.NatsCluster) string {
	if !routeTLSEnabled(&nc.Spec) {
		return ""
	}
	if tls := nc.Spec.Routes; tls != nil && tls.TLS != nil && tls.TLS.SecretRef != nil {
		return tls.TLS.SecretRef.Name
	}
	return routesSecretName(nc)
}

// certManagerIssuer returns the issuer route certificates come from, or nil
// when cert-manager does not issue them.
func certManagerIssuer(nc *clusterv1beta1.NatsCluster) *clusterv1beta1.IssuerReference {
	if !routeTLSEnabled(&nc.Spec) || nc.Spec.Routes == nil || nc.Spec.Routes.TLS == nil || nc.Spec.Routes.TLS.CertManager == nil {
		return nil
	}
	return &nc.Spec.Routes.TLS.CertManager.IssuerRef
}

// routesCertificate is the cert-manager Certificate that issues the route
// certificate into the Secret routesSecretName names.
func routesCertificate(nc *clusterv1beta1.NatsCluster, issuer *clusterv1beta1.IssuerReference) *unstructured.Unstructured {
	kind := issuer.Kind
	if kind == "" {
		kind = "Issuer"
	}
	group := issuer.Group
	if group == "" {
		group = "cert-manager.io"
	}
	var dnsNames []any
	for _, n := range routeDNSNames(nc) {
		dnsNames = append(dnsNames, n)
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata": map[string]any{
			"name":      nc.Name + "-routes",
			"namespace": nc.Namespace,
		},
		"spec": map[string]any{
			"secretName": routesSecretName(nc),
			"dnsNames":   dnsNames,
			"usages":     []any{"server auth", "client auth"},
			"privateKey": map[string]any{"algorithm": "ECDSA", "size": int64(256)},
			"issuerRef":  map[string]any{"name": issuer.Name, "kind": kind, "group": group},
		},
	}}
	u.SetLabels(labels(nc))
	return u
}
