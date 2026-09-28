package natscluster

import (
	"os"
	"testing"

	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/sysobs"
)

// TestEnvtestLeafnodes drives the reconciler against a real API server for
// story 10: the CEL rule on jetstream.domain, a leaf's remotes Secret and
// status, and a hub's leafnode Service, certificate wait and auth rule.
func TestEnvtestLeafnodes(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	c, err := client.New(cfg, client.Options{Scheme: leafScheme(t)})
	require.NoError(t, err)
	ctx := t.Context()
	obs := &fakeObserver{}
	r := &Reconciler{Client: c, Observer: obs}

	namespace := func(t *testing.T, ns string) {
		t.Helper()
		require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	}
	reconcile := func(t *testing.T, key types.NamespacedName) *clusterv1beta1.NatsCluster {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		got := &clusterv1beta1.NatsCluster{}
		require.NoError(t, c.Get(ctx, key, got))
		return got
	}
	condition := func(t *testing.T, nc *clusterv1beta1.NatsCluster, typ string, status metav1.ConditionStatus, reason string) *metav1.Condition {
		t.Helper()
		cond := meta.FindStatusCondition(nc.Status.Conditions, typ)
		require.NotNil(t, cond, typ)
		require.Equal(t, status, cond.Status, "%s: %s", typ, cond.Message)
		require.Equal(t, reason, cond.Reason, "%s: %s", typ, cond.Message)
		return cond
	}
	statefulSets := func(t *testing.T, ns string) []appsv1.StatefulSet {
		t.Helper()
		var list appsv1.StatefulSetList
		require.NoError(t, c.List(ctx, &list, client.InNamespace(ns)))
		return list.Items
	}

	t.Run("a leaf with JetStream and no domain is refused", func(t *testing.T) {
		namespace(t, "nodomain")
		nc := storyLeafCluster(t, "edge.yaml", "edge-site-1")
		nc.Namespace = "nodomain"
		nc.Spec.JetStream.Domain = ""
		err := c.Create(ctx, nc)
		require.True(t, apierrors.IsInvalid(err), "%v", err)
		require.ErrorContains(t, err, "a leaf running JetStream must set jetstream.domain")

		nc.Spec.LeafRemotes = nil
		require.NoError(t, c.Create(ctx, nc), "a NATS cluster that is not a leaf needs no domain")
	})

	t.Run("edge.yaml", func(t *testing.T) {
		namespace(t, "edge")
		p := mintPlane(t)
		telemetry := newTestKeys(t, nkeys.PrefixByteAccount)
		creds := p.creds(t, telemetry, leafUser())
		conn := storyConnection(t, "edge.yaml", "hub", "tls://leaf.prod-east.acme.example:7422", false)
		conn.Namespace = "edge"
		conn.Spec.Servers = []string{"tls://leaf.prod-east.acme.example:7422"}
		require.NoError(t, c.Create(ctx, conn))
		secret := credsSecret("edge", "edge-site-1-leaf-creds", creds)
		require.NoError(t, c.Create(ctx, secret))
		nc := storyLeafCluster(t, "edge.yaml", "edge-site-1")
		nc.Namespace = "edge"
		require.NoError(t, c.Create(ctx, nc))
		key := client.ObjectKeyFromObject(nc)

		got := reconcile(t, key)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
		require.Len(t, statefulSets(t, "edge"), 3)
		var remotes corev1.Secret
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "edge", Name: "edge-site-1-leaf-remotes"}, &remotes))
		require.Equal(t, map[string][]byte{"edge_hub.creds": creds}, remotes.Data)
		require.True(t, metav1.IsControlledBy(&remotes, got))
		var cm corev1.ConfigMap
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "edge", Name: "edge-site-1-0-config"}, &cm))
		require.Contains(t, cm.Data[configFile], `"credentials": "/etc/nats-config/edge_hub.creds"`)
		condition(t, got, ConditionLeafnodesConnected, metav1.ConditionUnknown, ReasonObservationFailed)

		spoke := []sysobs.Leaf{{Account: globalAccount, Spoke: true, Remote: "prod-east-0"}}
		obs.setLeafs(map[string][]sysobs.Leaf{"edge-site-1-0": spoke, "edge-site-1-1": spoke, "edge-site-1-2": spoke})
		got = reconcile(t, key)
		cond := condition(t, got, ConditionLeafnodesConnected, metav1.ConditionTrue, ReasonAllRemotesConnected)
		require.Equal(t, "3 of 3 servers connected to 1 remote", cond.Message)
		require.Equal(t, got.Generation, cond.ObservedGeneration)
		require.Equal(t, []clusterv1beta1.LeafRemoteStatus{{ConnectionNamespace: "edge", ConnectionName: "hub", Connected: 3, Account: publicKey(t, telemetry.Identity)}}, got.Status.LeafRemotes)

		t.Run("a missing creds Secret stops rendering", func(t *testing.T) {
			require.NoError(t, c.Delete(ctx, secret))
			got := reconcile(t, key)
			cond := condition(t, got, ConditionProgressing, metav1.ConditionFalse, ReasonLeafRemoteNotFound)
			require.Contains(t, cond.Message, "edge-site-1-leaf-creds")
			secret.ResourceVersion = ""
			require.NoError(t, c.Create(ctx, secret))
		})

		t.Run("removing the remotes removes their Secret and status", func(t *testing.T) {
			got := &clusterv1beta1.NatsCluster{}
			require.NoError(t, c.Get(ctx, key, got))
			got.Spec.LeafRemotes = nil
			require.NoError(t, c.Update(ctx, got))
			got = reconcile(t, key)
			require.Nil(t, got.Status.LeafRemotes)
			require.Nil(t, meta.FindStatusCondition(got.Status.Conditions, ConditionLeafnodesConnected))
			err := c.Get(ctx, types.NamespacedName{Namespace: "edge", Name: "edge-site-1-leaf-remotes"}, &corev1.Secret{})
			require.True(t, apierrors.IsNotFound(err), "%v", err)
		})
	})

	t.Run("hub leafnodes", func(t *testing.T) {
		namespace(t, "hub")
		hubNC := storyLeafCluster(t, "hub.yaml", "prod-east")
		nc := storyCluster(t)
		nc.Namespace = "hub"
		nc.Spec.Leafnodes = hubNC.Spec.Leafnodes
		nc.Spec.Leafnodes.TLS = &clusterv1beta1.ListenerTLS{CertificateSource: clusterv1beta1.CertificateSource{SecretRef: &natsv1beta1.SecretReference{Name: "leaf-cert"}}}
		err := c.Create(ctx, nc)
		require.True(t, apierrors.IsInvalid(err), "%v", err)
		require.ErrorContains(t, err, "a leafnode listener requires auth")

		nc.Spec.Auth = hubNC.Spec.Auth
		nc.Spec.Auth.SystemCredentials = nil
		trust := mintPlane(t).trust
		require.NoError(t, c.Create(ctx, &natsv1beta1.NatsOperatorTrust{
			ObjectMeta: metav1.ObjectMeta{Namespace: "hub", Name: nc.Spec.Auth.TrustRef.Name},
			Spec:       natsv1beta1.NatsOperatorTrustSpec{OperatorJWT: trust.OperatorJWT, SystemAccountJWT: trust.SystemAccountJWT},
		}))
		require.NoError(t, c.Create(ctx, nc))
		key := client.ObjectKeyFromObject(nc)

		got := reconcile(t, key)
		cond := condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonLeafnodesCertNotReady)
		require.Equal(t, "Secret leaf-cert does not exist", cond.Message)
		require.Empty(t, statefulSets(t, "hub"))
		var svc corev1.Service
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "hub", Name: "demo-leafnodes"}, &svc))
		require.Equal(t, corev1.ServiceTypeLoadBalancer, svc.Spec.Type)
		require.Equal(t, "leaf.prod-east.acme.example", svc.Annotations["external-dns.alpha.kubernetes.io/hostname"])
		require.True(t, metav1.IsControlledBy(&svc, got))

		cert, err := selfSignedRouteSecret(nc, []string{"leaf.prod-east.acme.example"}, r.now())
		require.NoError(t, err)
		cert.Name = "leaf-cert"
		require.NoError(t, c.Create(ctx, cert))
		got = reconcile(t, key)
		condition(t, got, ConditionProgressing, metav1.ConditionTrue, ReasonCreating)
		require.Len(t, statefulSets(t, "hub"), 3)
		require.Nil(t, meta.FindStatusCondition(got.Status.Conditions, ConditionLeafnodesConnected))
	})
}
