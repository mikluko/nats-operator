package natscluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCreateOrUpdate_RefusesUncontrolled(t *testing.T) {
	nc := storyCluster(t)
	nc.UID = types.UID("nc-uid")
	other := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "other", UID: "other-uid", Controller: ptr.To(true)}
	ours := metav1.OwnerReference{APIVersion: "cluster.nats.mikluko.io/v1beta1", Kind: "NatsCluster", Name: nc.Name, UID: nc.UID, Controller: ptr.To(true)}
	owner := metav1.OwnerReference{APIVersion: "cluster.nats.mikluko.io/v1beta1", Kind: "NatsCluster", Name: nc.Name, UID: nc.UID}

	for _, tc := range []struct {
		name    string
		have    []metav1.OwnerReference
		absent  bool
		refused bool
	}{
		{name: "absent", absent: true},
		{name: "controlled", have: []metav1.OwnerReference{ours}},
		{name: "unowned", refused: true},
		{name: "owned, not controlled", have: []metav1.OwnerReference{owner}, refused: true},
		{name: "controlled by another", have: []metav1.OwnerReference{other}, refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.NewClientBuilder().WithScheme(leafScheme(t))
			if !tc.absent {
				b = b.WithObjects(&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: nc.Namespace, OwnerReferences: tc.have},
					Data:       map[string]string{"k": "theirs"},
				})
			}
			r := &Reconciler{Client: b.Build()}
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: nc.Namespace}}
			err := r.createOrUpdate(t.Context(), nc, cm, func() { cm.Data = map[string]string{"k": "ours"} })

			got := &corev1.ConfigMap{}
			require.NoError(t, r.Client.Get(t.Context(), client.ObjectKeyFromObject(cm), got))
			if tc.refused {
				var nce *notControlledError
				require.ErrorAs(t, err, &nce)
				require.Equal(t, []string{"ConfigMap x"}, nce.Objects)
				require.Equal(t, "theirs", got.Data["k"])
				require.Equal(t, tc.have, got.OwnerReferences)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "ours", got.Data["k"])
			require.True(t, metav1.IsControlledBy(got, nc))
		})
	}
}

func TestRefusals(t *testing.T) {
	var f refusals
	require.NoError(t, f.err())
	require.NoError(t, f.add(nil))
	require.NoError(t, f.add(&notControlledError{Objects: []string{"Service demo"}}))
	require.NoError(t, f.add(&notControlledError{Objects: []string{"NetworkPolicy demo"}}))
	boom := errors.New("boom")
	require.ErrorIs(t, f.add(fmt.Errorf("apply pdb demo: %w", boom)), boom)
	require.EqualError(t, f.err(), "not controlled by this NatsCluster: Service demo, NetworkPolicy demo")
}
