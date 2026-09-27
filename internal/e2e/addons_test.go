package e2e

import (
	"context"
	"fmt"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestPoolRange(t *testing.T) {
	for _, tt := range []struct {
		subnet      string
		i           int
		first, last string
		err         string
	}{
		{subnet: "10.89.0.0/24", i: 0, first: "10.89.0.100", last: "10.89.0.109"},
		{subnet: "10.89.0.0/24", i: 1, first: "10.89.0.110", last: "10.89.0.119"},
		{subnet: "10.89.0.7/24", i: 2, first: "10.89.0.120", last: "10.89.0.129"},
		{subnet: "172.18.0.0/16", i: 0, first: "172.18.0.100", last: "172.18.0.109"},
		{subnet: "10.89.0.0/24", i: 14, first: "10.89.0.240", last: "10.89.0.249"},
		{subnet: "10.89.0.0/24", i: 15, err: "cluster 15 has no address pool"},
		{subnet: "10.89.0.0/25", i: 0, err: "not an IPv4 subnet of /24 or wider"},
		{subnet: "fc00::/64", i: 0, err: "not an IPv4 subnet"},
	} {
		t.Run(fmt.Sprintf("%s/%d", tt.subnet, tt.i), func(t *testing.T) {
			first, last, err := PoolRange(netip.MustParsePrefix(tt.subnet), tt.i)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.first, first.String())
			require.Equal(t, tt.last, last.String())
		})
	}
}

func TestCheckSHA256(t *testing.T) {
	require.NoError(t, checkSHA256([]byte("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"))
	require.ErrorContains(t, checkSHA256([]byte("abd"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
		"want ba7816bf")
}

func TestMetalLBPool(t *testing.T) {
	objs, err := DecodeObjects(fmt.Appendf(nil, metallbPool, "10.89.0.100", "10.89.0.109"))
	require.NoError(t, err)
	require.Len(t, objs, 2)
	require.Equal(t, "IPAddressPool", objs[0].GetKind())
	require.Equal(t, []any{"10.89.0.100-10.89.0.109"}, objs[0].Object["spec"].(map[string]any)["addresses"])
	require.Equal(t, "L2Advertisement", objs[1].GetKind())
	require.Equal(t, []any{"e2e"}, objs[1].Object["spec"].(map[string]any)["ipAddressPools"])
}

// kindCoreDNS is the ConfigMap and Deployment kind creates, cut to what
// ServeHosts reads.
func kindCoreDNS() []client.Object {
	return []client.Object{
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"},
			Data:       map[string]string{"Corefile": ".:53 {\n    errors\n}\n"},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"},
			Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{{Name: "config-volume", VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "coredns"},
						Items:                []corev1.KeyToPath{{Key: "Corefile", Path: "Corefile"}},
					},
				}}},
			}}},
		},
	}
}

// TestServeHosts pins that CoreDNS is given the Corefile reading HostsKey
// and every key of its ConfigMap, and that a second call writes nothing.
func TestServeHosts(t *testing.T) {
	updates := 0
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(kindCoreDNS()...).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				updates++
				return c.Update(ctx, obj, opts...)
			},
		}).Build()

	require.NoError(t, ServeHosts(t.Context(), c))
	require.Equal(t, 2, updates)

	var cm corev1.ConfigMap
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "kube-system", Name: "coredns"}, &cm))
	require.Equal(t, Corefile, cm.Data["Corefile"])
	require.Contains(t, Corefile, "hosts /etc/coredns/"+HostsKey+" {\n       fallthrough\n    }\n    kubernetes")
	var d appsv1.Deployment
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "kube-system", Name: "coredns"}, &d))
	require.Nil(t, d.Spec.Template.Spec.Volumes[0].ConfigMap.Items)

	require.NoError(t, ServeHosts(t.Context(), c))
	require.Equal(t, 2, updates)
}
