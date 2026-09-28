package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

// fetchOnly is an Object whose server object is info, whatever is asked.
type fetchOnly struct {
	info *Info
}

func (o fetchOnly) Describe() string                                     { return "stream S" }
func (o fetchOnly) Fetch(context.Context) (*Info, error)                 { return o.info, nil }
func (o fetchOnly) Desired() (Config, error)                             { return Config{}, nil }
func (o fetchOnly) Create(context.Context, Config) (*Info, error)        { return o.info, nil }
func (o fetchOnly) Update(context.Context, *Info, Config) (*Info, error) { return o.info, nil }
func (o fetchOnly) Delete(context.Context) error                         { return nil }
func (o fetchOnly) WriteSpec(context.Context, Config, bool) error        { return nil }

// TestSync_SettlingRecheck pins that a synced object whose Raft group is
// moving is read again after MovingRecheck, one whose group is not settled
// after SettlingRecheck, and a settled one, or one with no group, after the
// resync period.
func TestSync_SettlingRecheck(t *testing.T) {
	const owner = types.UID("5b1e0c4a")
	const resync = time.Hour
	peer := func(name string, current, offline bool) *jetstream.PeerInfo {
		return &jetstream.PeerInfo{Name: name, Current: current, Offline: offline}
	}
	tests := []struct {
		name    string
		cluster *jetstream.ClusterInfo
		moving  bool
		want    time.Duration
	}{
		{name: "no group", want: resync},
		{name: "settled", cluster: &jetstream.ClusterInfo{Leader: "a", Replicas: []*jetstream.PeerInfo{peer("b", true, false), peer("c", true, false)}}, want: resync},
		{name: "one server", cluster: &jetstream.ClusterInfo{Leader: "a"}, want: resync},
		{name: "no leader", cluster: &jetstream.ClusterInfo{Replicas: []*jetstream.PeerInfo{peer("b", true, false)}}, want: SettlingRecheck},
		{name: "member not current", cluster: &jetstream.ClusterInfo{Leader: "a", Replicas: []*jetstream.PeerInfo{peer("b", true, false), peer("c", false, false)}}, want: SettlingRecheck},
		{name: "member offline", cluster: &jetstream.ClusterInfo{Leader: "a", Replicas: []*jetstream.PeerInfo{peer("b", true, true)}}, want: SettlingRecheck},
		{name: "moving", cluster: &jetstream.ClusterInfo{Leader: "a", Replicas: []*jetstream.PeerInfo{peer("b", false, false)}}, moving: true, want: MovingRecheck},
		{name: "moving, every member current", cluster: &jetstream.ClusterInfo{Leader: "a", Replicas: []*jetstream.PeerInfo{peer("b", true, false)}}, moving: true, want: MovingRecheck},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{"metadata": map[string]any{OwnerKey: string(owner), OriginKey: string(jetstreamv1beta1.OwnershipCreated)}}
			obj := &metav1.ObjectMeta{UID: owner, Generation: 1}
			st := &jetstreamv1beta1.SyncStatus{}
			res, _, err := Syncer{Resync: resync}.Sync(t.Context(), Resource{Object: obj, Status: st},
				fetchOnly{info: &Info{Config: cfg, Cluster: tt.cluster, Moving: tt.moving}})
			require.NoError(t, err)
			require.Equal(t, tt.want, res.RequeueAfter)
		})
	}
}

// TestClassify pins which JetStream API errors are Terminal: a request the
// server refuses as invalid is, and one it cannot place for want of peers,
// which servers coming online resolve, is not.
func TestClassify(t *testing.T) {
	for _, tt := range []struct {
		name     string
		err      error
		terminal bool
	}{
		{name: "bad request", err: &jetstream.APIError{Code: 400, ErrorCode: 10058, Description: "stream name already in use with a different configuration"}, terminal: true},
		{name: "invalid stream config", err: &jetstream.APIError{Code: 500, ErrorCode: 10052, Description: "replicas > 1 not supported in non-clustered mode"}, terminal: true},
		{name: "no peers", err: &jetstream.APIError{Code: 400, ErrorCode: 10005, Description: "no suitable peers for placement, peer offline"}},
		{name: "server error", err: &jetstream.APIError{Code: 500, ErrorCode: 10008, Description: "JetStream system temporarily unavailable"}},
		{name: "not an API error", err: context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.err)
			var te *TerminalError
			require.Equal(t, tt.terminal, errors.As(got, &te))
			if !tt.terminal {
				require.Equal(t, tt.err, got)
			}
		})
	}
}
