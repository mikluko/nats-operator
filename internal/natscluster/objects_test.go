package natscluster

import (
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

func container(t *testing.T, sts *appsv1.StatefulSet, name string) corev1.Container {
	t.Helper()
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == name {
			return c
		}
	}
	require.Failf(t, "no container", "%s in %s", name, sts.Name)
	return corev1.Container{}
}

func TestRender_Story1(t *testing.T) {
	nc := storyCluster(t)
	p, err := Render(nc)
	require.NoError(t, err)
	require.Len(t, p.Revision, 10)

	var names []string
	for _, s := range p.Servers {
		names = append(names, s.Name)
		sts := s.StatefulSet
		require.Equal(t, s.Name, sts.Name)
		require.Equal(t, int32(1), *sts.Spec.Replicas)
		require.Equal(t, "demo-headless", sts.Spec.ServiceName)
		require.Equal(t, map[string]string{LabelCluster: "demo", LabelServer: s.Name}, sts.Spec.Selector.MatchLabels)
		require.Equal(t, p.Revision, sts.Annotations[AnnotationConfigRevision])
		require.Equal(t, p.Revision, s.ConfigMap.Annotations[AnnotationConfigRevision])
		require.Equal(t, s.Name+"-config", s.ConfigMap.Name)
		require.Contains(t, s.ConfigMap.Data[configFile], `"server_name": "`+s.Name+`"`)
		require.Contains(t, s.ConfigMap.Data[configFile], `"config_revision": "`+p.Revision+`"`)

		nats := container(t, sts, "nats")
		require.Equal(t, "nats:2.15.0", nats.Image)
		require.Equal(t, []corev1.EnvVar{{Name: "GOMEMLIMIT", Value: "3865470566"}}, nats.Env)
		require.Equal(t, nc.Spec.Resources, nats.Resources)
		require.Equal(t, intstr.FromString("monitor"), nats.ReadinessProbe.HTTPGet.Port)
		exporter := container(t, sts, "exporter")
		require.Equal(t, ExporterImage, exporter.Image)

		require.Len(t, sts.Spec.VolumeClaimTemplates, 1)
		pvc := sts.Spec.VolumeClaimTemplates[0]
		require.Equal(t, "data", pvc.Name)
		size := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		require.Equal(t, "20Gi", size.String())
		require.Equal(t, "standard", *pvc.Spec.StorageClassName)
		require.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, pvc.Spec.AccessModes)

		var vols []string
		for _, v := range sts.Spec.Template.Spec.Volumes {
			vols = append(vols, v.Name)
		}
		require.Equal(t, []string{"config", "pid", "routes-tls"}, vols)
	}
	require.Equal(t, []string{"demo-0", "demo-1", "demo-2"}, names)

	require.Equal(t, corev1.ClusterIPNone, p.HeadlessService.Spec.ClusterIP)
	require.True(t, p.HeadlessService.Spec.PublishNotReadyAddresses)
	require.Equal(t, "demo", p.ClientService.Name)
	require.Equal(t, intstr.FromInt32(1), *p.PDB.Spec.MaxUnavailable)
	require.Equal(t, map[string]string{LabelCluster: "demo"}, p.PDB.Spec.Selector.MatchLabels)
}

func TestRender_Revision(t *testing.T) {
	base, err := Render(storyCluster(t))
	require.NoError(t, err)
	tests := []struct {
		name    string
		mutate  func(*clusterv1beta1.NatsCluster)
		changes bool
		spec    bool
	}{
		{"nothing", func(*clusterv1beta1.NatsCluster) {}, false, false},
		{"rollout paused", func(nc *clusterv1beta1.NatsCluster) { nc.Spec.Rollout = &clusterv1beta1.Rollout{Paused: true} }, false, false},
		{"version", func(nc *clusterv1beta1.NatsCluster) { nc.Spec.Version = "2.15.1" }, true, true},
		{"server tags", func(nc *clusterv1beta1.NatsCluster) { nc.Spec.ServerTags = map[string]string{"az": "a"} }, true, false},
		{"replicas", func(nc *clusterv1beta1.NatsCluster) { nc.Spec.Replicas = 5 }, true, false},
		{"pod template", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.PodTemplate = &clusterv1beta1.PodTemplate{Spec: &corev1.PodSpec{PriorityClassName: "high"}}
		}, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := storyCluster(t)
			tt.mutate(nc)
			p, err := Render(nc)
			require.NoError(t, err)
			require.Equal(t, tt.changes, p.Revision != base.Revision)
			digest := p.Servers[0].StatefulSet.Annotations[AnnotationSpecDigest]
			require.NotEmpty(t, digest)
			require.Equal(t, tt.spec, digest != base.Servers[0].StatefulSet.Annotations[AnnotationSpecDigest])
		})
	}
}

func TestRender_PodTemplate(t *testing.T) {
	nc := storyCluster(t)
	nc.Spec.JetStream.VolumeClaimTemplate.Metadata.Labels = map[string]string{"backup": "yes"}
	nc.Spec.PodTemplate = &clusterv1beta1.PodTemplate{
		Metadata: clusterv1beta1.EmbeddedObjectMetadata{
			Labels:      map[string]string{"team": "platform", LabelCluster: "hijack"},
			Annotations: map[string]string{"karpenter.sh/do-not-disrupt": "true"},
		},
		Spec: &corev1.PodSpec{
			NodeSelector: map[string]string{"pool": "nats"},
			Tolerations:  []corev1.Toleration{{Key: "dedicated", Value: "nats", Effect: corev1.TaintEffectNoSchedule}},
			Containers: []corev1.Container{
				{Name: "nats", Env: []corev1.EnvVar{{Name: "EXTRA", Value: "1"}}},
				{Name: "sidecar", Image: "busybox"},
			},
		},
	}
	p, err := Render(nc)
	require.NoError(t, err)
	sts := p.Servers[0].StatefulSet
	pod := sts.Spec.Template

	require.Equal(t, "platform", pod.Labels["team"])
	require.Equal(t, "demo", pod.Labels[LabelCluster], "a template label overrode the selector")
	require.Equal(t, "true", pod.Annotations["karpenter.sh/do-not-disrupt"])
	require.Equal(t, map[string]string{"pool": "nats"}, pod.Spec.NodeSelector)
	require.Len(t, pod.Spec.Tolerations, 1)
	require.Equal(t, int64(terminationGracePeriod), *pod.Spec.TerminationGracePeriodSeconds, "an unset field cleared the rendered one")

	var names []string
	for _, c := range pod.Spec.Containers {
		names = append(names, c.Name)
	}
	require.ElementsMatch(t, []string{"nats", "exporter", "sidecar"}, names)
	nats := container(t, sts, "nats")
	require.Equal(t, "nats:2.15.0", nats.Image)
	require.ElementsMatch(t, []corev1.EnvVar{{Name: "GOMEMLIMIT", Value: "3865470566"}, {Name: "EXTRA", Value: "1"}}, nats.Env)
	require.NotNil(t, nats.ReadinessProbe)

	pvc := sts.Spec.VolumeClaimTemplates[0]
	require.Equal(t, map[string]string{"backup": "yes", LabelCluster: "demo", LabelServer: "demo-0"}, pvc.Labels)
}

func TestRender_Volumes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*clusterv1beta1.NatsCluster)
		want   []string
		pvc    bool
	}{
		{"JetStream without a volume stores in an emptyDir", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.JetStream.VolumeClaimTemplate = nil
		}, []string{"config", "pid", "routes-tls", "data"}, false},
		{"no JetStream, no route TLS", func(nc *clusterv1beta1.NatsCluster) {
			nc.Spec.JetStream = nil
			off := false
			nc.Spec.Routes = &clusterv1beta1.Routes{TLS: &clusterv1beta1.RoutesTLS{Enabled: &off}}
		}, []string{"config", "pid"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := storyCluster(t)
			tt.mutate(nc)
			p, err := Render(nc)
			require.NoError(t, err)
			sts := p.Servers[0].StatefulSet
			var vols []string
			for _, v := range sts.Spec.Template.Spec.Volumes {
				vols = append(vols, v.Name)
			}
			require.Equal(t, tt.want, vols)
			require.Equal(t, tt.pvc, len(sts.Spec.VolumeClaimTemplates) > 0)
		})
	}
}

func TestSelfSignedRouteSecret(t *testing.T) {
	nc := storyCluster(t)
	s, err := selfSignedRouteSecret(nc, routeDNSNames(nc), time.Now())
	require.NoError(t, err)
	require.Equal(t, corev1.SecretTypeTLS, s.Type)
	require.Equal(t, "demo-routes-tls", s.Name)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(s.Data[caKey]))
	block, _ := pem.Decode(s.Data[corev1.TLSCertKey])
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		_, err = cert.Verify(x509.VerifyOptions{
			DNSName:   "demo-2-0.demo-headless.nats-system.svc",
			Roots:     pool,
			KeyUsages: []x509.ExtKeyUsage{usage},
		})
		require.NoError(t, err)
	}
}
