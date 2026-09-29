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
	"net/url"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

const caKey = "ca.crt"

// A self-signed route certificate and its CA are valid for
// selfSignedValidity and renewed once less than selfSignedRenewBefore of it
// remains.
const (
	selfSignedValidity    = 365 * 24 * time.Hour
	selfSignedRenewBefore = selfSignedValidity / 3
)

// issueRouteSecrets returns the CA Secret of caKeyPair and the route Secret
// of a certificate it signs for hosts, both valid from now for
// selfSignedValidity. A certificate one call signs verifies against the CA
// of any other call with the same nc and caKeyPair.
func issueRouteSecrets(nc *clusterv1beta1.NatsCluster, hosts []string, caKeyPair *ecdsa.PrivateKey, now time.Time) (caSecret, routes *corev1.Secret, err error) {
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
		return nil, nil, fmt.Errorf("sign route CA: %w", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, err
	}
	caKeyDER, err := x509.MarshalECPrivateKey(caKeyPair)
	if err != nil {
		return nil, nil, err
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, fmt.Errorf("sign route certificate: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return nil, nil, err
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caSecret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: routesCASecretName(nc), Namespace: nc.Namespace, Labels: labels(nc)},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       caPEM,
			corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: caKeyDER}),
		},
	}
	routes = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: routesSecretName(nc), Namespace: nc.Namespace, Labels: labels(nc)},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			caKey:                   caPEM,
			corev1.TLSCertKey:       pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
			corev1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		},
	}
	return caSecret, routes, nil
}

// routeCA returns the CA certificate and key of caSecret, or nils when
// either is absent or does not parse.
func routeCA(caSecret *corev1.Secret) (*x509.Certificate, *ecdsa.PrivateKey) {
	cb, _ := pem.Decode(caSecret.Data[corev1.TLSCertKey])
	kb, _ := pem.Decode(caSecret.Data[corev1.TLSPrivateKeyKey])
	if cb == nil || kb == nil {
		return nil, nil
	}
	cert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, nil
	}
	key, err := x509.ParseECPrivateKey(kb.Bytes)
	if err != nil {
		return nil, nil
	}
	return cert, key
}

// routeRenewalDue reports whether the route certificate in routes must be
// reissued at now under ca.
func routeRenewalDue(routes *corev1.Secret, ca *x509.Certificate, now time.Time) bool {
	b, _ := pem.Decode(routes.Data[corev1.TLSCertKey])
	if b == nil {
		return true
	}
	leaf, err := x509.ParseCertificate(b.Bytes)
	if err != nil || leaf.CheckSignatureFrom(ca) != nil {
		return true
	}
	return !now.Before(leaf.NotAfter.Add(-selfSignedRenewBefore)) || !now.Before(ca.NotAfter.Add(-selfSignedRenewBefore))
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
	return certificate(nc, routesCertificateName(nc), routesSecretName(nc), issuer, routeDNSNames(nc), peerUsages)
}

// gatewaySecret names the Secret the gateway certificate is mounted from,
// or "" when gateways run in the clear.
func gatewaySecret(nc *clusterv1beta1.NatsCluster) string {
	g := nc.Spec.Gateway
	switch {
	case g == nil || g.TLS == nil:
		return ""
	case g.TLS.SecretRef != nil:
		return g.TLS.SecretRef.Name
	default:
		return gatewaySecretName(nc)
	}
}

// gatewayIssuer returns the issuer the gateway certificate comes from, or
// nil when cert-manager does not issue it.
func gatewayIssuer(nc *clusterv1beta1.NatsCluster) *clusterv1beta1.IssuerReference {
	if g := nc.Spec.Gateway; g != nil && g.TLS != nil && g.TLS.CertManager != nil {
		return &g.TLS.CertManager.IssuerRef
	}
	return nil
}

// gatewayHosts are the hosts other members dial this NATS cluster's
// gateway at: the host of its own entry in gateway.remotes and of
// gateway.advertise, in that order, each once.
func gatewayHosts(nc *clusterv1beta1.NatsCluster) []string {
	g := nc.Spec.Gateway
	if g == nil {
		return nil
	}
	var out []string
	add := func(h string) {
		if h != "" && !slices.Contains(out, h) {
			out = append(out, h)
		}
	}
	for _, r := range g.Remotes {
		if r.Name != nc.Name {
			continue
		}
		if u, err := url.Parse(r.URL); err == nil {
			add(u.Hostname())
		}
	}
	if g.Advertise != "" {
		if h, _, err := net.SplitHostPort(g.Advertise); err == nil {
			add(h)
		} else {
			add(g.Advertise)
		}
	}
	return out
}

// gatewayCertificate is the cert-manager Certificate that issues the
// gateway certificate for hosts into the Secret gatewaySecretName names.
func gatewayCertificate(nc *clusterv1beta1.NatsCluster, issuer *clusterv1beta1.IssuerReference, hosts []string) *unstructured.Unstructured {
	return certificate(nc, gatewayCertificateName(nc), gatewaySecretName(nc), issuer, hosts, peerUsages)
}

func routesCertificateName(nc *clusterv1beta1.NatsCluster) string  { return nc.Name + "-routes" }
func gatewayCertificateName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-gateway" }

var certificateGVK = schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Certificate"}

// peerUsages are the usages of a certificate a server both serves and
// dials its peers with.
var peerUsages = []string{"server auth", "client auth"}

// certificate is a cert-manager Certificate named name, issued by issuer
// into Secret secret for hosts with usages.
func certificate(nc *clusterv1beta1.NatsCluster, name, secret string, issuer *clusterv1beta1.IssuerReference, hosts, usages []string) *unstructured.Unstructured {
	kind := issuer.Kind
	if kind == "" {
		kind = "Issuer"
	}
	group := issuer.Group
	if group == "" {
		group = "cert-manager.io"
	}
	var dnsNames, ips []any
	for _, h := range hosts {
		if net.ParseIP(h) != nil {
			ips = append(ips, h)
		} else {
			dnsNames = append(dnsNames, h)
		}
	}
	spec := map[string]any{
		"secretName": secret,
		"usages":     toAny(usages),
		"privateKey": map[string]any{"algorithm": "ECDSA", "size": int64(256)},
		"issuerRef":  map[string]any{"name": issuer.Name, "kind": kind, "group": group},
	}
	if len(dnsNames) > 0 {
		spec["dnsNames"] = dnsNames
	}
	if len(ips) > 0 {
		spec["ipAddresses"] = ips
	}
	u := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	u.SetGroupVersionKind(certificateGVK)
	u.SetName(name)
	u.SetNamespace(nc.Namespace)
	u.SetLabels(labels(nc))
	return u
}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
