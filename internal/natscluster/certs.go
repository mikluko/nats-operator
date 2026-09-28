package natscluster

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// Certs are the TLS Secrets the servers mount, by listener; a listener
// without TLS has the zero MountedCert.
type Certs struct {
	Routes, Gateway, Leafnodes MountedCert
}

// MountedCert is what a render reads of a TLS Secret the servers mount.
type MountedCert struct {
	// Digest is a digest of the Secret's data: a rotation changes the
	// revision, so it reaches running servers.
	Digest string
	// NotAfter is the expiry of the Secret's tls.crt, zero when it does not
	// parse.
	NotAfter time.Time
	// CA reports whether the Secret holds ca.crt.
	CA bool
}

// mountedCert reads s, or returns the zero MountedCert for nil.
func mountedCert(s *corev1.Secret) MountedCert {
	if s == nil {
		return MountedCert{}
	}
	h := sha256.New()
	keys := make([]string, 0, len(s.Data))
	for k := range s.Data {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		h.Write(binary.BigEndian.AppendUint64(append([]byte(k), 0), uint64(len(s.Data[k]))))
		h.Write(s.Data[k])
	}
	m := MountedCert{Digest: hex.EncodeToString(h.Sum(nil))[:16], CA: len(s.Data[caKey]) > 0}
	if b, _ := pem.Decode(s.Data[corev1.TLSCertKey]); b != nil {
		if c, err := x509.ParseCertificate(b.Bytes); err == nil {
			m.NotAfter = c.NotAfter
		}
	}
	return m
}

// loaded reports whether a server reporting got has loaded c's
// certificates. A listener either side reports no expiry for is not
// compared.
func (c Certs) loaded(got sysobs.CertNotAfter) bool {
	same := func(want, got time.Time) bool { return want.IsZero() || got.IsZero() || want.Equal(got) }
	return same(c.Routes.NotAfter, got.Cluster) && same(c.Gateway.NotAfter, got.Gateway) && same(c.Leafnodes.NotAfter, got.Leafnode)
}

// ensureCerts makes every listener's certificate Secret exist and reads
// it. While one is not ready it also returns what the servers wait for and
// the condition reason naming the listener, routes before gateway before
// leafnodes.
func (r *Reconciler) ensureCerts(ctx context.Context, nc *clusterv1beta1.NatsCluster) (Certs, string, string, error) {
	var certs Certs
	var wait, reason string
	note := func(w, rsn string) {
		if wait == "" && w != "" {
			wait, reason = w, rsn
		}
	}

	if err := r.ensureSelfSignedRouteSecret(ctx, nc); err != nil {
		return certs, "", "", err
	}
	var want *unstructured.Unstructured
	if issuer := certManagerIssuer(nc); issuer != nil {
		want = routesCertificate(nc, issuer)
	}
	w, s, err := r.ensureCertSecret(ctx, nc, routesCertificateName(nc), want, routesSecret(nc), corev1.TLSCertKey, corev1.TLSPrivateKeyKey, caKey)
	if err != nil {
		return certs, "", "", err
	}
	note(w, ReasonRouteCertNotReady)
	certs.Routes = mountedCert(s)

	want = nil
	gatewayWait := ""
	if issuer := gatewayIssuer(nc); issuer != nil {
		if hosts := gatewayHosts(nc); len(hosts) == 0 {
			gatewayWait = "gateway.tls.certManager has no host to issue for: list this NATS cluster in gateway.remotes or set gateway.advertise"
		} else {
			want = gatewayCertificate(nc, issuer, hosts)
		}
	}
	if gatewayWait == "" {
		gatewayWait, s, err = r.ensureCertSecret(ctx, nc, gatewayCertificateName(nc), want, gatewaySecret(nc), corev1.TLSCertKey, corev1.TLSPrivateKeyKey)
		if err != nil {
			return certs, "", "", err
		}
		certs.Gateway = mountedCert(s)
	}
	note(gatewayWait, ReasonGatewayCertNotReady)

	want = nil
	if issuer := leafnodesIssuer(nc); issuer != nil {
		want = leafnodesCertificate(nc, issuer)
	}
	w, s, err = r.ensureCertSecret(ctx, nc, leafnodesCertificateName(nc), want, leafnodesCertSecret(nc), corev1.TLSCertKey, corev1.TLSPrivateKeyKey)
	if err != nil {
		return certs, "", "", err
	}
	note(w, ReasonLeafnodesCertNotReady)
	certs.Leafnodes = mountedCert(s)
	return certs, wait, reason, nil
}

// ensureCertSecret applies the cert-manager Certificate want, or deletes
// the one named name that nc owns when want is nil, then reads Secret
// secret. It returns what the servers wait for, "" once the Secret holds
// every key in keys, and the Secret, nil until then or when secret is "".
func (r *Reconciler) ensureCertSecret(ctx context.Context, nc *clusterv1beta1.NatsCluster, name string, want *unstructured.Unstructured, secret string, keys ...string) (string, *corev1.Secret, error) {
	if want == nil {
		if err := r.deleteCertificate(ctx, nc, name); err != nil {
			return "", nil, err
		}
	} else if wait, err := r.applyCertificate(ctx, nc, want); wait != "" || err != nil {
		return wait, nil, err
	}
	if secret == "" {
		return "", nil, nil
	}
	s := &corev1.Secret{}
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: nc.Namespace, Name: secret}, s)
	switch {
	case apierrors.IsNotFound(err):
		return fmt.Sprintf("Secret %s does not exist", secret), nil, nil
	case err != nil:
		return "", nil, fmt.Errorf("get secret %s: %w", secret, err)
	}
	for _, k := range keys {
		if len(s.Data[k]) == 0 {
			return fmt.Sprintf("Secret %s has no %s", secret, k), nil, nil
		}
	}
	return "", s, nil
}

// ensureSelfSignedRouteSecret issues the route certificate Secret, and the
// CA Secret it is signed from, when routes are self-signed and it is
// missing or due for renewal. A renewal keeps the CA's key, so servers
// still holding the previous certificates accept the renewed ones. A route
// Secret nc does not control is left alone.
func (r *Reconciler) ensureSelfSignedRouteSecret(ctx context.Context, nc *clusterv1beta1.NatsCluster) error {
	name := routesSecret(nc)
	if name == "" || name != routesSecretName(nc) || certManagerIssuer(nc) != nil {
		return nil
	}
	routes, err := r.getSecret(ctx, nc.Namespace, name)
	if err != nil {
		return err
	}
	if routes != nil && !metav1.IsControlledBy(routes, nc) {
		return nil
	}
	caSecret, err := r.getSecret(ctx, nc.Namespace, routesCASecretName(nc))
	if err != nil {
		return err
	}
	var ca *x509.Certificate
	var key *ecdsa.PrivateKey
	if caSecret != nil {
		ca, key = routeCA(caSecret)
	}
	now := r.now()
	if routes != nil && ca != nil && !routeRenewalDue(routes, ca, now) {
		return nil
	}
	if key == nil {
		if key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			return err
		}
	}
	wantCA, wantRoutes, err := issueRouteSecrets(nc, routeDNSNames(nc), key, now)
	if err != nil {
		return err
	}
	if err := r.writeSecret(ctx, nc, caSecret, wantCA); err != nil {
		return err
	}
	return r.writeSecret(ctx, nc, routes, wantRoutes)
}

// getSecret returns the Secret namespace/name, nil when it does not exist.
func (r *Reconciler) getSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	s := &corev1.Secret{}
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, s)
	switch {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("get secret %s: %w", name, err)
	}
	return s, nil
}

// writeSecret creates want controlled by nc when have is nil, and otherwise
// updates have to want's data.
func (r *Reconciler) writeSecret(ctx context.Context, nc *clusterv1beta1.NatsCluster, have, want *corev1.Secret) error {
	if have == nil {
		if err := controllerutil.SetControllerReference(nc, want, r.Client.Scheme()); err != nil {
			return err
		}
		if err := r.Client.Create(ctx, want); err != nil {
			return fmt.Errorf("create secret %s: %w", want.Name, err)
		}
		return nil
	}
	have.Labels = merged(have.Labels, want.Labels)
	have.Data = want.Data
	if err := controllerutil.SetControllerReference(nc, have, r.Client.Scheme()); err != nil {
		return err
	}
	if err := r.Client.Update(ctx, have); err != nil {
		return fmt.Errorf("update secret %s: %w", have.Name, err)
	}
	return nil
}

// applyCertificate creates or updates the cert-manager Certificate want.
// It returns what the servers wait for when cert-manager is not installed.
func (r *Reconciler) applyCertificate(ctx context.Context, nc *clusterv1beta1.NatsCluster, want *unstructured.Unstructured) (string, error) {
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(want.GroupVersionKind())
	cert.SetName(want.GetName())
	cert.SetNamespace(want.GetNamespace())
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cert, func() error {
		cert.SetLabels(merged(cert.GetLabels(), want.GetLabels()))
		cert.Object["spec"] = want.Object["spec"]
		return controllerutil.SetControllerReference(nc, cert, r.Client.Scheme())
	})
	if meta.IsNoMatchError(err) {
		return "cert-manager Certificate is not a known kind: cert-manager is not installed", nil
	}
	if err != nil {
		return "", fmt.Errorf("apply certificate %s: %w", want.GetName(), err)
	}
	return "", nil
}

// deleteCertificate deletes the cert-manager Certificate named name when
// nc owns it. Without cert-manager installed there is none to delete.
func (r *Reconciler) deleteCertificate(ctx context.Context, nc *clusterv1beta1.NatsCluster, name string) error {
	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(certificateGVK)
	cert.SetName(name)
	cert.SetNamespace(nc.Namespace)
	if err := r.Client.Get(ctx, client.ObjectKeyFromObject(cert), cert); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("get certificate %s: %w", name, err)
	}
	if !metav1.IsControlledBy(cert, nc) {
		return nil
	}
	if err := r.Client.Delete(ctx, cert); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete certificate %s: %w", name, err)
	}
	return nil
}
