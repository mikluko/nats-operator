package natscluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	clusterv1beta1 "github.com/mikluko/nats-operator/api/cluster/v1beta1"
)

// quickstartWithGateway is story 1's NatsCluster with a gateway that has no
// tls.
func quickstartWithGateway(t *testing.T) *clusterv1beta1.NatsCluster {
	t.Helper()
	b, err := os.ReadFile("../../docs/content/docs/stories/01-quickstart/01-natscluster.yaml")
	require.NoError(t, err)
	nc := &clusterv1beta1.NatsCluster{}
	require.NoError(t, yaml.UnmarshalStrict(b, nc))
	nc.Generation = 1
	nc.Spec.Gateway = &clusterv1beta1.Gateway{
		Discovery: clusterv1beta1.GatewayDiscoveryExplicit,
		Remotes:   []clusterv1beta1.GatewayRemote{{Name: "demo", URL: "nats://demo:7222"}, {Name: "east", URL: "nats://east:7222"}},
	}
	return nc
}

// rendered lists what a reconcile of nc left in its namespace: StatefulSets,
// ConfigMaps and Services, by kind and name, with their resource versions.
func rendered(t *testing.T, c client.Client, nc *clusterv1beta1.NatsCluster) map[string]string {
	t.Helper()
	out := map[string]string{}
	for kind, list := range map[string]client.ObjectList{
		"StatefulSet": &appsv1.StatefulSetList{}, "ConfigMap": &corev1.ConfigMapList{}, "Service": &corev1.ServiceList{},
	} {
		require.NoError(t, c.List(t.Context(), list, client.InNamespace(nc.Namespace)))
		require.NoError(t, meta.EachListItem(list, func(o runtime.Object) error {
			obj := o.(client.Object)
			out[kind+"/"+obj.GetName()] = obj.GetResourceVersion()
			return nil
		}))
	}
	return out
}

func reconcileOnce(t *testing.T, c client.Client, nc *clusterv1beta1.NatsCluster, allow bool) *clusterv1beta1.NatsCluster {
	t.Helper()
	r := &Reconciler{Client: c, Observer: unobservable{}, AllowGatewayWithoutTLS: allow}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
	require.NoError(t, err)
	got := &clusterv1beta1.NatsCluster{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(nc), got))
	return got
}

// TestReconcile_GatewayWithoutTLS pins that a NatsCluster whose gateway has
// no tls is refused with reason GatewayWithoutTLS, rendering nothing, unless
// the reconciler allows it.
func TestReconcile_GatewayWithoutTLS(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		nc := quickstartWithGateway(t)
		c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(nc).WithStatusSubresource(nc).Build()
		got := reconcileOnce(t, c, nc, false)
		for _, typ := range []string{ConditionReady, ConditionProgressing} {
			cond := meta.FindStatusCondition(got.Status.Conditions, typ)
			require.NotNil(t, cond, typ)
			require.Equal(t, metav1.ConditionFalse, cond.Status, typ)
			require.Equal(t, ReasonGatewayWithoutTLS, cond.Reason, typ)
			require.Contains(t, cond.Message, "--allow-gateway-without-tls", typ)
			require.Equal(t, nc.Generation, cond.ObservedGeneration, typ)
		}
		require.Empty(t, rendered(t, c, nc))
	})

	t.Run("allowed", func(t *testing.T) {
		nc := quickstartWithGateway(t)
		c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(nc).WithStatusSubresource(nc).Build()
		got := reconcileOnce(t, c, nc, true)
		cond := meta.FindStatusCondition(got.Status.Conditions, ConditionProgressing)
		require.NotNil(t, cond)
		require.Equal(t, ReasonCreating, cond.Reason, cond.Message)
		var sets appsv1.StatefulSetList
		require.NoError(t, c.List(t.Context(), &sets, client.InNamespace(nc.Namespace)))
		require.Len(t, sets.Items, int(nc.Spec.Replicas))
	})

	t.Run("a rendered NatsCluster is left as it is", func(t *testing.T) {
		nc := quickstartWithGateway(t)
		c := fake.NewClientBuilder().WithScheme(leafScheme(t)).WithObjects(nc).WithStatusSubresource(nc).Build()
		reconcileOnce(t, c, nc, true)
		before := rendered(t, c, nc)
		require.NotEmpty(t, before)

		got := reconcileOnce(t, c, nc, false)
		require.Equal(t, ReasonGatewayWithoutTLS, meta.FindStatusCondition(got.Status.Conditions, ConditionReady).Reason)
		require.Equal(t, before, rendered(t, c, nc))
	})
}

func TestGatewayWithoutTLS(t *testing.T) {
	withTLS := &clusterv1beta1.Gateway{TLS: &clusterv1beta1.ListenerTLS{}}
	tests := []struct {
		name    string
		gateway *clusterv1beta1.Gateway
		allow   bool
		want    bool
	}{
		{"no gateway", nil, false, false},
		{"gateway with tls", withTLS, false, false},
		{"gateway without tls", &clusterv1beta1.Gateway{}, false, true},
		{"gateway without tls, allowed", &clusterv1beta1.Gateway{}, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, gatewayWithoutTLS(&clusterv1beta1.NatsClusterSpec{Gateway: tt.gateway}, tt.allow))
		})
	}
}

// TestStories_GatewayTLS pins that every NatsCluster the stories apply with
// a gateway carries gateway tls, so none is refused by a cluster controller
// run without --allow-gateway-without-tls, save in story 13, whose existing
// supercluster runs its gateways in the clear and whose page names the flag.
func TestStories_GatewayTLS(t *testing.T) {
	inTheClear := map[string]bool{"13-join-supercluster": true}
	files, err := filepath.Glob("../../docs/content/docs/stories/*/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	gateways := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(b), 4096)
		for {
			var raw json.RawMessage
			err := dec.Decode(&raw)
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err, f)
			var nc clusterv1beta1.NatsCluster
			if len(raw) == 0 || string(raw) == "null" || json.Unmarshal(raw, &nc) != nil || nc.Kind != "NatsCluster" || nc.Spec.Gateway == nil {
				continue
			}
			gateways++
			story := filepath.Base(filepath.Dir(f))
			require.Equal(t, inTheClear[story], gatewayWithoutTLS(&nc.Spec, false), "%s: NatsCluster %s", f, nc.Name)
		}
	}
	require.NotZero(t, gateways, "no story applies a NatsCluster with a gateway")
}
