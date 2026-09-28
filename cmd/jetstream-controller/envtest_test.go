package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
	"github.com/mikluko/nats-operator/internal/manager"
	"github.com/mikluko/nats-operator/internal/manager/managertest"
	"github.com/mikluko/nats-operator/internal/manager/secretreads"
	"github.com/mikluko/nats-operator/internal/natsconn"
)

// TestEnvtestSecretMetadata pins that setup, in the manager New builds,
// reconciles a NatsConnection when the Secret it reads is created, sooner
// than its retry would, and never lists or watches whole Secrets.
func TestEnvtestSecretMetadata(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../config/crd"}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })

	scheme, err := manager.NewScheme(schemes...)
	require.NoError(t, err)
	recorded, reads := secretreads.Record(cfg)
	mgr, err := manager.New(recorded, &manager.Options{MetricsAddr: "0", ProbeAddr: "0"}, scheme, manager.Owned{})
	require.NoError(t, err)
	require.NoError(t, setup(t.Context(), mgr, time.Hour))
	go func() { _ = mgr.Start(t.Context()) }()

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	const ns = "payments"
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	conn := &natsv1beta1.NatsConnection{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "demo"},
		Spec: natsv1beta1.NatsConnectionSpec{
			Servers:     []string{"nats://127.0.0.1:1"},
			Credentials: &natsv1beta1.Credentials{SecretKeyRef: natsv1beta1.CredentialsSecretKeySelector{Name: "creds"}},
		},
	}
	require.NoError(t, c.Create(t.Context(), conn))
	requireReason := func(want string, within time.Duration) {
		t.Helper()
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			got := &natsv1beta1.NatsConnection{}
			if !assert.NoError(ct, c.Get(t.Context(), client.ObjectKeyFromObject(conn), got)) {
				return
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, natsconn.ConditionReady)
			if assert.NotNil(ct, cond) {
				assert.Equal(ct, want, cond.Reason, cond.Message)
			}
		}, within, 100*time.Millisecond)
	}
	requireReason(natsconn.ReasonSecretNotFound, 30*time.Second)

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "creds"},
		Data:       map[string][]byte{"not-" + natsconn.DefaultCredentialsKey: []byte("x")},
	}
	require.NoError(t, c.Create(t.Context(), secret))
	requireReason(natsconn.ReasonInvalidSecret, natsconn.DefaultRetryAfter/2)
	reads.RequireMetadataOnly(t)
}

// TestEnvtestReadyUnderRoles pins config/rbac/jetstream-controller as enough
// for the JetStream controller to become ready in namespace-scoped mode.
func TestEnvtestReadyUnderRoles(t *testing.T) {
	scheme, err := manager.NewScheme(schemes...)
	require.NoError(t, err)
	withResync := func(ctx context.Context, mgr ctrl.Manager) error {
		return setup(ctx, mgr, lifecycle.DefaultResync)
	}
	managertest.ReadyUnderRoles(t, scheme, manager.Owned{}, withResync, "../../config/rbac/jetstream-controller", "../../config/crd")
}
