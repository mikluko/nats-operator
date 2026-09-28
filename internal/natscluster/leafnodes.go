package natscluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"

	"github.com/nats-io/jwt/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/conditions"
	"github.com/mikluko/nats-operator/internal/grant"
	"github.com/mikluko/nats-operator/internal/natsconn"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// +kubebuilder:rbac:groups=nats.mikluko.io,resources=natsconnections;natsaccounttrusts,verbs=get;list;watch

// PortLeafnodes is the leafnode listener's port on a hub.
const PortLeafnodes = 7422

const leafnodesTLSDir = "/etc/nats-leafnodes-tls"

// globalAccount is the account a leaf without an auth plane binds its
// remotes to, as nats-server names it.
const globalAccount = "$G"

// Condition type and reasons of a leaf's remotes.
const (
	ConditionLeafnodesConnected = "LeafnodesConnected"

	ReasonAllRemotesConnected = "AllRemotesConnected"
	ReasonRemotesDisconnected = "RemotesDisconnected"

	ReasonLeafRemoteNotFound    = "LeafRemoteNotFound"
	ReasonLeafRemoteNotReady    = "LeafRemoteNotReady"
	ReasonLeafRemoteInvalid     = "LeafRemoteInvalid"
	ReasonLeafnodesCertNotReady = "LeafnodesCertificateNotReady"
)

// LeafnodesConfig is a server's leafnodes block: the hub's listener, the
// leaf's remotes, or both.
type LeafnodesConfig struct {
	Listen    string             `json:"listen,omitempty"`
	Advertise string             `json:"advertise,omitempty"`
	TLS       *ListenerTLSConfig `json:"tls,omitempty"`
	Remotes   []RemoteLeafConfig `json:"remotes,omitempty"`
}

// ListenerTLSConfig is the certificate a listener presents; peers are not
// asked for one.
type ListenerTLSConfig struct {
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

// RemoteLeafConfig is one hub a leaf dials. Without Account it binds the
// global account.
type RemoteLeafConfig struct {
	URLs        []string         `json:"urls"`
	Credentials string           `json:"credentials,omitempty"`
	Account     string           `json:"account,omitempty"`
	TLS         *RemoteTLSConfig `json:"tls,omitempty"`
}

// RemoteTLSConfig is the CA a leaf verifies its hub against.
type RemoteTLSConfig struct {
	CAFile string `json:"ca_file"`
}

// LeafRemote is one of spec.leafRemotes resolved to what a server renders
// and what its status reports.
type LeafRemote struct {
	// Ref is the remote's connectionRef as spec names it.
	Ref natsv1beta1.ObjectReference
	// Connection is the NatsConnection Ref resolves to.
	Connection types.NamespacedName
	Servers    []string
	CA         []byte
	Creds      []byte
	// LocalAccount is the public key of the local account the remote
	// binds; empty binds the global account.
	LocalAccount string
	// PreloadJWT is the local account's JWT, preloaded into the resolver;
	// empty preloads nothing.
	PreloadJWT string
	// HubAccount is the public key of the hub account Creds sign into,
	// empty without Creds.
	HubAccount string
}

// fileStem names the remote's files in the leaf remotes Secret: namespace
// and name joined by an underscore, which neither may contain.
func (lr *LeafRemote) fileStem() string {
	return lr.Connection.Namespace + "_" + lr.Connection.Name
}

// leafRemotesSecretName is the Secret holding every remote's creds and CA.
func leafRemotesSecretName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-leaf-remotes" }

func leafnodesServiceName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-leafnodes" }

func leafnodesCertSecretName(nc *clusterv1beta1.NatsCluster) string {
	return nc.Name + "-leafnodes-tls"
}

// leafnodesConfig renders nc's leafnode listener and remotes under layout
// l, or nil when nc has neither.
func leafnodesConfig(nc *clusterv1beta1.NatsCluster, remotes []LeafRemote, l Layout) *LeafnodesConfig {
	ln := nc.Spec.Leafnodes
	if ln == nil && len(remotes) == 0 {
		return nil
	}
	c := &LeafnodesConfig{}
	if ln != nil {
		c.Listen = l.LeafnodesListen
		c.Advertise = ln.Advertise
		if ln.TLS != nil {
			c.TLS = &ListenerTLSConfig{
				CertFile: l.LeafnodesTLSDir + "/" + corev1.TLSCertKey,
				KeyFile:  l.LeafnodesTLSDir + "/" + corev1.TLSPrivateKeyKey,
			}
		}
	}
	for i := range remotes {
		r := &remotes[i]
		rc := RemoteLeafConfig{URLs: r.Servers, Account: r.LocalAccount}
		if len(r.Creds) > 0 {
			rc.Credentials = l.LeafRemotesDir + "/" + r.fileStem() + ".creds"
		}
		if len(r.CA) > 0 {
			rc.TLS = &RemoteTLSConfig{CAFile: l.LeafRemotesDir + "/" + r.fileStem() + ".ca.crt"}
		}
		c.Remotes = append(c.Remotes, rc)
	}
	return c
}

// resolverType is the resolver nc runs: auth.resolver when set; otherwise
// Cache on a leaf that preloads no account and Full everywhere else.
func resolverType(nc *clusterv1beta1.NatsCluster, remotes []LeafRemote) clusterv1beta1.ResolverType {
	if t := nc.Spec.Auth.Resolver; t != "" {
		return t
	}
	if len(nc.Spec.LeafRemotes) > 0 && !preloads(remotes) {
		return clusterv1beta1.ResolverCache
	}
	return clusterv1beta1.ResolverFull
}

func preloads(remotes []LeafRemote) bool {
	return slices.ContainsFunc(remotes, func(r LeafRemote) bool { return r.PreloadJWT != "" })
}

// unsupportedLeafFields names the leafRemotes fields nc cannot render: a
// system account or account trust without an auth plane.
func unsupportedLeafFields(spec *clusterv1beta1.NatsClusterSpec) []string {
	var out []string
	for i, r := range spec.LeafRemotes {
		switch {
		case spec.Auth == nil && r.LocalSystemAccount:
			out = append(out, fmt.Sprintf("leafRemotes[%d].localSystemAccount without auth", i))
		case spec.Auth == nil && r.LocalAccountTrustRef != nil:
			out = append(out, fmt.Sprintf("leafRemotes[%d].localAccountTrustRef without auth", i))
		}
	}
	return out
}

// leafRefNamespaces are the namespaces nc's leafRemotes reach into.
func leafRefNamespaces(nc *clusterv1beta1.NatsCluster) []string {
	var out []string
	for _, r := range nc.Spec.LeafRemotes {
		out = append(out, r.ConnectionRef.Namespace)
		if r.LocalAccountTrustRef != nil {
			out = append(out, r.LocalAccountTrustRef.Namespace)
		}
	}
	return out
}

// readLeafRemotes resolves nc's leafRemotes under trust, which is nil
// exactly when nc has no auth plane. A remote whose NatsConnection, its
// Secrets or its NatsAccountTrust is absent, not admitted, not yet filled
// in or invalid returns nil and the Progressing condition saying so; so
// do two remotes naming one NatsConnection, and a leaf preloading accounts
// into a Full resolver with no jetstream.volumeClaimTemplate to keep it on.
func readLeafRemotes(ctx context.Context, r client.Reader, nc *clusterv1beta1.NatsCluster, trust *Trust) ([]LeafRemote, *metav1.Condition, error) {
	notProgressing := func(reason, format string, args ...any) *metav1.Condition {
		return &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: reason, Message: fmt.Sprintf(format, args...)}
	}
	from := grant.Referrer{Group: clusterv1beta1.GroupVersion.Group, Kind: "NatsCluster", Namespace: nc.Namespace}
	var out []LeafRemote
	seen := map[types.NamespacedName]int{}
	for i, spec := range nc.Spec.LeafRemotes {
		lr := LeafRemote{Ref: spec.ConnectionRef, Connection: types.NamespacedName{Namespace: spec.ConnectionRef.Namespace, Name: spec.ConnectionRef.Name}}
		if lr.Connection.Namespace == "" {
			lr.Connection.Namespace = nc.Namespace
		}
		if j, ok := seen[lr.Connection]; ok {
			return nil, notProgressing(ReasonUnsupportedSpec, "leafRemotes[%d] and leafRemotes[%d] both name NatsConnection %s", j, i, lr.Connection), nil
		}
		seen[lr.Connection] = i
		denied, err := grant.Admit(ctx, r, from, grant.Target{Group: natsv1beta1.GroupVersion.Group, Kind: natsconn.Kind, Namespace: lr.Connection.Namespace, Name: lr.Connection.Name})
		if err != nil {
			return nil, nil, err
		}
		if denied != nil {
			return nil, notProgressing(denied.Reason, "leafRemotes[%d]: %s", i, denied.Message), nil
		}
		conn := &natsv1beta1.NatsConnection{}
		if err := r.Get(ctx, lr.Connection, conn); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, notProgressing(ReasonLeafRemoteNotFound, "leafRemotes[%d]: NatsConnection %s does not exist", i, lr.Connection), nil
			}
			return nil, nil, fmt.Errorf("get NatsConnection %s: %w", lr.Connection, err)
		}
		ep, err := natsconn.ReadEndpoint(ctx, r, conn.Namespace, &conn.Spec)
		switch {
		case errors.Is(err, natsconn.ErrSecretNotFound) || errors.Is(err, natsconn.ErrKeyNotFound):
			return nil, notProgressing(ReasonLeafRemoteNotFound, "leafRemotes[%d]: NatsConnection %s: %v", i, lr.Connection, err), nil
		case errors.Is(err, natsconn.ErrInvalidCA) || errors.Is(err, natsconn.ErrInvalidCredentials):
			return nil, notProgressing(ReasonLeafRemoteInvalid, "leafRemotes[%d]: NatsConnection %s: %v", i, lr.Connection, err), nil
		case err != nil:
			return nil, nil, fmt.Errorf("read NatsConnection %s: %w", lr.Connection, err)
		}
		lr.Servers, lr.CA, lr.Creds = ep.Servers, ep.CA, ep.Creds
		if len(ep.Creds) > 0 {
			if lr.HubAccount, err = credsAccount(ep.Creds); err != nil {
				return nil, notProgressing(ReasonLeafRemoteInvalid, "leafRemotes[%d]: NatsConnection %s: %v", i, lr.Connection, err), nil
			}
		}

		switch {
		case spec.LocalSystemAccount:
			lr.LocalAccount = trust.SystemAccount
		case spec.LocalAccountTrustRef != nil:
			cond, err := readLocalAccount(ctx, r, from, nc, trust, *spec.LocalAccountTrustRef, &lr)
			if err != nil {
				return nil, nil, err
			}
			if cond != nil {
				cond.Message = fmt.Sprintf("leafRemotes[%d]: %s", i, cond.Message)
				return nil, cond, nil
			}
		}
		out = append(out, lr)
	}
	if trust != nil && preloads(out) && resolverType(nc, out) == clusterv1beta1.ResolverFull &&
		(nc.Spec.JetStream == nil || nc.Spec.JetStream.VolumeClaimTemplate == nil) {
		return nil, notProgressing(ReasonUnsupportedSpec, "a leaf preloading accounts keeps its Full resolver on jetstream.volumeClaimTemplate, which is not set; set it, or set auth.resolver: Cache"), nil
	}
	return out, nil, nil
}

// readLocalAccount fills lr's local account from the NatsAccountTrust ref
// names: its public key, and the JWT to preload when it carries one, which
// must be that account's, signed by trust's NATS operator.
func readLocalAccount(ctx context.Context, r client.Reader, from grant.Referrer, nc *clusterv1beta1.NatsCluster, trust *Trust, ref natsv1beta1.ObjectReference, lr *LeafRemote) (*metav1.Condition, error) {
	notProgressing := func(reason, format string, args ...any) *metav1.Condition {
		return &metav1.Condition{Type: ConditionProgressing, Status: metav1.ConditionFalse, Reason: reason, Message: fmt.Sprintf(format, args...)}
	}
	key := types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}
	if key.Namespace == "" {
		key.Namespace = nc.Namespace
	}
	denied, err := grant.Admit(ctx, r, from, grant.Target{Group: natsv1beta1.GroupVersion.Group, Kind: "NatsAccountTrust", Namespace: key.Namespace, Name: key.Name})
	if err != nil {
		return nil, err
	}
	if denied != nil {
		return notProgressing(denied.Reason, "%s", denied.Message), nil
	}
	at := &natsv1beta1.NatsAccountTrust{}
	if err := r.Get(ctx, key, at); err != nil {
		if apierrors.IsNotFound(err) {
			return notProgressing(ReasonLeafRemoteNotFound, "NatsAccountTrust %s does not exist", key), nil
		}
		return nil, fmt.Errorf("get NatsAccountTrust %s: %w", key, err)
	}
	pub, accJWT := at.Spec.PublicKey, at.Spec.JWT
	if at.Spec.AccountRef != nil {
		pub, accJWT = at.Status.PublicKey, at.Status.JWT
		if pub == "" {
			return notProgressing(ReasonLeafRemoteNotReady, "NatsAccountTrust %s has no public key in its status yet", key), nil
		}
	}
	lr.LocalAccount = pub
	if accJWT == "" {
		return nil, nil
	}
	if err := checkAccountJWT(trust, pub, accJWT); err != nil {
		return notProgressing(ReasonLeafRemoteInvalid, "NatsAccountTrust %s: %v", key, err), nil
	}
	lr.PreloadJWT = accJWT
	return nil, nil
}

// checkAccountJWT checks that accJWT is the account JWT of pub, signed by
// trust's NATS operator with its identity or one of its signing keys.
func checkAccountJWT(trust *Trust, pub, accJWT string) error {
	acc, err := jwt.DecodeAccountClaims(accJWT)
	if err != nil {
		return fmt.Errorf("jwt: %w", err)
	}
	if acc.Subject != pub {
		return fmt.Errorf("jwt is account %s, not %s", acc.Subject, pub)
	}
	op, err := jwt.DecodeOperatorClaims(trust.OperatorJWT)
	if err != nil {
		return err
	}
	if acc.Issuer != op.Subject && !slices.Contains(op.SigningKeys, acc.Issuer) {
		return fmt.Errorf("jwt is signed by %s, not by operator %s or its signing keys", acc.Issuer, op.Subject)
	}
	return nil
}

// credsAccount is the public key of the account a creds file's user
// belongs to.
func credsAccount(creds []byte) (string, error) {
	tok, err := jwt.ParseDecoratedJWT(creds)
	if err != nil {
		return "", err
	}
	u, err := jwt.DecodeUserClaims(tok)
	if err != nil {
		return "", fmt.Errorf("user jwt: %w", err)
	}
	if u.IssuerAccount != "" {
		return u.IssuerAccount, nil
	}
	return u.Issuer, nil
}

// leafRemotesSecret is the Secret holding each remote's creds as
// <stem>.creds and CA as <stem>.ca.crt, or nil when nc has no remotes.
func leafRemotesSecret(nc *clusterv1beta1.NatsCluster, remotes []LeafRemote) *corev1.Secret {
	if len(remotes) == 0 {
		return nil
	}
	data := map[string][]byte{}
	for i := range remotes {
		r := &remotes[i]
		if len(r.Creds) > 0 {
			data[r.fileStem()+".creds"] = r.Creds
		}
		if len(r.CA) > 0 {
			data[r.fileStem()+".ca.crt"] = r.CA
		}
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: leafRemotesSecretName(nc), Namespace: nc.Namespace, Labels: labels(nc)},
		Data:       data,
	}
}

// leafnodesService is the external leafnode Service rendered from
// leafnodes.service, or nil when nc has no leafnode listener.
func leafnodesService(nc *clusterv1beta1.NatsCluster) *corev1.Service {
	ln := nc.Spec.Leafnodes
	if ln == nil {
		return nil
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: leafnodesServiceName(nc), Namespace: nc.Namespace, Labels: labels(nc)},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: clusterSelector(nc),
			Ports:    []corev1.ServicePort{servicePort("leafnodes", PortLeafnodes)},
		},
	}
	if t := ln.Service; t != nil {
		if t.Type != "" {
			svc.Spec.Type = t.Type
		}
		svc.Annotations = t.Annotations
	}
	return svc
}

// addLeafnodesListener gives the nats container the leafnode port, and the
// listener certificate's mount when there is one.
func addLeafnodesListener(nc *clusterv1beta1.NatsCluster, c *corev1.Container) {
	if nc.Spec.Leafnodes == nil {
		return
	}
	c.Ports = append(c.Ports, corev1.ContainerPort{Name: "leafnodes", ContainerPort: PortLeafnodes})
	if leafnodesCertSecret(nc) != "" {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "leafnodes-tls", MountPath: leafnodesTLSDir, ReadOnly: true})
	}
}

func leafnodesVolumes(nc *clusterv1beta1.NatsCluster) []corev1.Volume {
	s := leafnodesCertSecret(nc)
	if s == "" {
		return nil
	}
	return []corev1.Volume{{Name: "leafnodes-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: s}}}}
}

// configVolume projects server's ConfigMap and the leaf remotes Secret,
// which may not exist, into one volume: kubelet swaps a projected volume's
// files together, so a reloaded config never names a remote file not yet
// written.
func configVolume(nc *clusterv1beta1.NatsCluster, server string) corev1.Volume {
	return corev1.Volume{Name: "config", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		Sources: []corev1.VolumeProjection{
			{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: configMapName(server)}}},
			{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: leafRemotesSecretName(nc)}, Optional: ptr.To(true)}},
		},
	}}}
}

// leafnodesCertSecret names the Secret the leafnode listener's certificate
// is mounted from, or "" when the listener has no TLS.
func leafnodesCertSecret(nc *clusterv1beta1.NatsCluster) string {
	ln := nc.Spec.Leafnodes
	if ln == nil || ln.TLS == nil {
		return ""
	}
	if ln.TLS.SecretRef != nil {
		return ln.TLS.SecretRef.Name
	}
	return leafnodesCertSecretName(nc)
}

func leafnodesCertificateName(nc *clusterv1beta1.NatsCluster) string { return nc.Name + "-leafnodes" }

// leafnodesCertificate is the cert-manager Certificate issuing the
// leafnode listener's certificate for leafnodesHosts.
func leafnodesCertificate(nc *clusterv1beta1.NatsCluster, issuer *clusterv1beta1.IssuerReference) *unstructured.Unstructured {
	return certificate(nc, leafnodesCertificateName(nc), leafnodesCertSecretName(nc), issuer, leafnodesHosts(nc), []string{"server auth"})
}

// leafnodesHosts are the hosts leaf nodes dial the listener at: the
// advertised host, or the leafnode Service's in-cluster names when nothing
// is advertised.
func leafnodesHosts(nc *clusterv1beta1.NatsCluster) []string {
	if host, _, err := net.SplitHostPort(nc.Spec.Leafnodes.Advertise); err == nil && host != "" {
		return []string{host}
	}
	base := fmt.Sprintf("%s.%s.svc", leafnodesServiceName(nc), nc.Namespace)
	return []string{base, base + ".cluster.local"}
}

// leafnodesIssuer returns the issuer the leafnode listener's certificate
// comes from, or nil when cert-manager does not issue it.
func leafnodesIssuer(nc *clusterv1beta1.NatsCluster) *clusterv1beta1.IssuerReference {
	if ln := nc.Spec.Leafnodes; ln != nil && ln.TLS != nil && ln.TLS.CertManager != nil {
		return &ln.TLS.CertManager.IssuerRef
	}
	return nil
}

// applyLeafnodes makes the leafnode Service and the leaf remotes Secret
// exist, each deleted once spec no longer asks for it.
func (r *Reconciler) applyLeafnodes(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan) error {
	var remotes, svc client.Object
	if s := leafRemotesSecret(nc, plan.LeafRemotes); s != nil {
		remotes = s
	}
	if s := leafnodesService(nc); s != nil {
		svc = s
	}
	if err := r.applyOwned(ctx, nc, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: leafRemotesSecretName(nc), Namespace: nc.Namespace}}, remotes, func(have, want client.Object) {
		have.(*corev1.Secret).Data = want.(*corev1.Secret).Data
	}); err != nil {
		return err
	}
	if err := r.applyOwned(ctx, nc, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: leafnodesServiceName(nc), Namespace: nc.Namespace}}, svc, func(have, want client.Object) {
		h, w := have.(*corev1.Service), want.(*corev1.Service)
		h.Annotations = merged(h.Annotations, w.Annotations)
		h.Spec.Type = w.Spec.Type
		h.Spec.Selector = w.Spec.Selector
		h.Spec.Ports = w.Spec.Ports
	}); err != nil {
		return err
	}
	return nil
}

// applyOwned creates or updates obj from want, owned by nc, copying what
// update sets; with want nil it deletes obj if nc owns it.
func (r *Reconciler) applyOwned(ctx context.Context, nc *clusterv1beta1.NatsCluster, obj, want client.Object, update func(have, want client.Object)) error {
	if want == nil {
		return r.deleteOwned(ctx, nc, obj)
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		obj.SetLabels(merged(obj.GetLabels(), want.GetLabels()))
		update(obj, want)
		return controllerutil.SetControllerReference(nc, obj, r.Client.Scheme())
	}); err != nil {
		return fmt.Errorf("apply %s: %w", want.GetName(), err)
	}
	return nil
}

// ObserveLeafs reads LEAFZ over $SYS when nc names a system user, and
// through Fallback otherwise.
func (s *SystemConnections) ObserveLeafs(ctx context.Context, nc *clusterv1beta1.NatsCluster) (map[string][]sysobs.Leaf, error) {
	if !hasSystemUser(nc) {
		return s.Fallback.ObserveLeafs(ctx, nc)
	}
	o, err := s.client(ctx, nc)
	if err != nil {
		return nil, err
	}
	return o.Leafz(ctx, serverNames(nc))
}

// observeLeafs sets st's leafRemotes and LeafnodesConnected from what
// r.Observer reports of nc's leafnode connections, or clears both when nc
// has no remotes.
func (r *Reconciler) observeLeafs(ctx context.Context, nc *clusterv1beta1.NatsCluster, plan *Plan, st *clusterv1beta1.NatsClusterStatus) {
	if len(plan.LeafRemotes) == 0 {
		st.LeafRemotes = nil
		meta.RemoveStatusCondition(&st.Conditions, ConditionLeafnodesConnected)
		return
	}
	leafs, err := r.Observer.ObserveLeafs(ctx, nc)
	leafStatus(st, nc, plan, leafs, err)
}

// leafStatus reports each remote's connected servers and hub account, and
// LeafnodesConnected: True once every server holds every remote. LEAFZ
// names no remote, so a server holds one once it has dialed as many
// connections in its local account as there are remotes binding it.
func leafStatus(st *clusterv1beta1.NatsClusterStatus, nc *clusterv1beta1.NatsCluster, plan *Plan, leafs map[string][]sysobs.Leaf, observeErr error) {
	gen := nc.Generation
	prev := map[types.NamespacedName]int32{}
	for _, s := range st.LeafRemotes {
		prev[types.NamespacedName{Namespace: s.ConnectionNamespace, Name: s.ConnectionName}] = s.Connected
	}
	account := func(lr *LeafRemote) string {
		if lr.LocalAccount == "" {
			return globalAccount
		}
		return lr.LocalAccount
	}
	need := map[string]int{}
	for i := range plan.LeafRemotes {
		need[account(&plan.LeafRemotes[i])]++
	}
	held := map[string]int32{}
	for _, s := range plan.Servers {
		dialed := map[string]int{}
		for _, l := range leafs[s.Name] {
			if l.Spoke {
				dialed[l.Account]++
			}
		}
		for acc, n := range need {
			if dialed[acc] >= n {
				held[acc]++
			}
		}
	}

	replicas := int32(len(plan.Servers))
	st.LeafRemotes = nil
	var short []string
	for i := range plan.LeafRemotes {
		lr := &plan.LeafRemotes[i]
		rs := clusterv1beta1.LeafRemoteStatus{
			ConnectionNamespace: lr.Connection.Namespace,
			ConnectionName:      lr.Connection.Name,
			Account:             lr.HubAccount,
			Connected:           prev[lr.Connection],
		}
		if leafs != nil {
			rs.Connected = held[account(lr)]
		}
		if rs.Connected < replicas {
			short = append(short, fmt.Sprintf("%s: %d of %d servers connected", lr.Connection, rs.Connected, replicas))
		}
		st.LeafRemotes = append(st.LeafRemotes, rs)
	}

	c := metav1.Condition{Type: ConditionLeafnodesConnected}
	switch {
	case leafs == nil:
		c.Status, c.Reason = metav1.ConditionUnknown, ReasonObservationFailed
		if observeErr != nil {
			c.Message = observeErr.Error()
		}
	case len(short) > 0:
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, ReasonRemotesDisconnected, strings.Join(short, "; ")
	default:
		noun := "remote"
		if len(plan.LeafRemotes) > 1 {
			noun = "remotes"
		}
		c.Status, c.Reason = metav1.ConditionTrue, ReasonAllRemotesConnected
		c.Message = fmt.Sprintf("%d of %d servers connected to %d %s", replicas, replicas, len(plan.LeafRemotes), noun)
	}
	conditions.Set(&st.Conditions, gen, c)
}
