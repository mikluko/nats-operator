package natscluster

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// podSecurityVariants are NatsClusters whose pods differ in containers and
// volumes.
func podSecurityVariants(t *testing.T) map[string]*clusterv1beta1.NatsCluster {
	t.Helper()
	noExporter := storyCluster(t)
	noExporter.Spec.Exporter = &clusterv1beta1.Exporter{Enabled: ptr.To(false)}
	bare := storyCluster(t)
	bare.Spec.JetStream = nil
	bare.Spec.Routes = &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{Enabled: ptr.To(false)}}
	emptyDir := storyCluster(t)
	emptyDir.Spec.JetStream.VolumeClaimTemplate = nil
	return map[string]*clusterv1beta1.NatsCluster{
		"story 1":                  storyCluster(t),
		"exporter off":             noExporter,
		"no JetStream, no TLS":     bare,
		"JetStream in an emptyDir": emptyDir,
	}
}

func TestRender_RestrictedPodSecurity(t *testing.T) {
	for name, nc := range podSecurityVariants(t) {
		t.Run(name, func(t *testing.T) {
			p, err := Render(nc, Inputs{})
			require.NoError(t, err)
			for _, s := range p.Servers {
				pod := s.StatefulSet.Spec.Template.Spec
				require.Equal(t, ptr.To(false), pod.AutomountServiceAccountToken)
				sc := pod.SecurityContext
				require.NotNil(t, sc)
				require.Equal(t, ptr.To(true), sc.RunAsNonRoot)
				require.NotZero(t, *sc.RunAsUser)
				require.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, sc.SeccompProfile.Type)
				for _, c := range pod.Containers {
					csc := c.SecurityContext
					require.NotNil(t, csc, c.Name)
					require.Equal(t, ptr.To(false), csc.AllowPrivilegeEscalation, c.Name)
					require.Equal(t, ptr.To(true), csc.ReadOnlyRootFilesystem, c.Name)
					require.Equal(t, []corev1.Capability{"ALL"}, csc.Capabilities.Drop, c.Name)
					require.Empty(t, csc.Capabilities.Add, c.Name)
				}
			}
		})
	}
}

// TestEnvtestRestrictedPodSecurity has the API server's PodSecurity
// admission judge each rendered pod against the restricted profile, and a
// podTemplate that escalates past it.
func TestEnvtestRestrictedPodSecurity(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is unset: run `just envtest` for the API-server-backed tests")
	}
	env := &envtest.Environment{}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := t.Context()

	const ns = "restricted"
	require.NoError(t, c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   ns,
		Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"},
	}}))
	admit := func(t *testing.T, nc *clusterv1beta1.NatsCluster) error {
		t.Helper()
		p, err := Render(nc, Inputs{})
		require.NoError(t, err)
		sts := p.Servers[0].StatefulSet
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: sts.Name, Namespace: ns}, Spec: sts.Spec.Template.Spec}
		for _, vct := range sts.Spec.VolumeClaimTemplates {
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: vct.Name, VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: vct.Name + "-" + sts.Name},
			}})
		}
		return c.Create(ctx, pod, client.DryRunAll)
	}

	for name, nc := range podSecurityVariants(t) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, admit(t, nc))
		})
	}
	t.Run("a podTemplate escalates past it", func(t *testing.T) {
		nc := storyCluster(t)
		nc.Spec.PodTemplate = &clusterv1beta1.PodTemplate{Spec: &corev1.PodSpec{Containers: []corev1.Container{{
			Name:            "nats",
			SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"NET_ADMIN"}}},
		}}}}
		err := admit(t, nc)
		require.True(t, apierrors.IsForbidden(err), "%v", err)
	})
}
