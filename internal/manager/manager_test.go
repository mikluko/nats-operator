package manager

import (
	"context"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func TestFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    Options
		wantErr bool
	}{
		{
			name: "defaults",
			args: nil,
			want: Options{MetricsAddr: ":8080", ProbeAddr: ":8081", LeaderElectionID: "cluster.nats.mikluko.io"},
		},
		{
			name: "overrides",
			args: []string{"-metrics-bind-address=0", "-leader-elect", "-leader-election-id=x"},
			want: Options{MetricsAddr: "0", ProbeAddr: ":8081", LeaderElection: true, LeaderElectionID: "x"},
		},
		{
			name: "watch namespaces",
			args: []string{"-watch-namespaces= a,b ,,c"},
			want: Options{MetricsAddr: ":8080", ProbeAddr: ":8081", LeaderElectionID: "cluster.nats.mikluko.io", WatchNamespaces: []string{"a", "b", "c"}},
		},
		{
			name:    "watch namespaces naming none",
			args:    []string{"-watch-namespaces=,"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := flag.NewFlagSet(tt.name, flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			got := Flags(fs, "cluster.nats.mikluko.io")
			err := fs.Parse(tt.args)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, *got)
		})
	}
}

func TestSecretMetadata(t *testing.T) {
	now := metav1.Now()
	meta := metav1.ObjectMeta{
		Name: "creds", Namespace: "ns", UID: "uid", ResourceVersion: "7", Generation: 2,
		CreationTimestamp: now, DeletionTimestamp: &now,
		Labels:          map[string]string{"l": "v"},
		OwnerReferences: []metav1.OwnerReference{{Name: "owner"}},
		Finalizers:      []string{"f"},
	}
	typ := metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}
	in := &metav1.PartialObjectMetadata{TypeMeta: typ, ObjectMeta: *meta.DeepCopy()}
	in.Annotations = map[string]string{"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"k":"dg=="}}`}
	in.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "kubectl"}}
	got, err := secretMetadata(in)
	require.NoError(t, err)
	require.Equal(t, &metav1.PartialObjectMetadata{TypeMeta: typ, ObjectMeta: meta}, got)

	tombstone := cache.DeletedFinalStateUnknown{Key: "ns/creds"}
	got, err = secretMetadata(tombstone)
	require.NoError(t, err)
	require.Equal(t, tombstone, got)
}

func TestCacheSynced(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	require.NoError(t, cacheSynced(func(context.Context) bool { return true })(req))
	require.EqualError(t, cacheSynced(func(context.Context) bool { return false })(req), "cache is not synced")
}
