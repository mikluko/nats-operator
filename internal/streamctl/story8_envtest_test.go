package streamctl

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	natsv1beta1 "github.com/mikluko/nats-operator/api/nats/v1beta1"
	"github.com/mikluko/nats-operator/internal/e2e/placeholders"
)

// testStory8 pins that the NatsStream of story 8 reads each of the story's
// status files, in east, transferring and in west, on a two-cluster
// supercluster, with the move held in flight by stopping two of the three
// new replicas.
func testStory8(t *testing.T, c client.Client, sc map[string]*testNATS) {
	const ns = "orders"
	require.NoError(t, c.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	require.NoError(t, c.Create(t.Context(), &natsv1beta1.NatsConnection{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "acme-orders"},
		Spec:       natsv1beta1.NatsConnectionSpec{Servers: sc["east"].urls},
	}))
	applyStory(t, c, sc["east"], "08-stream-transfer/01-natsstream-before.yaml")
	eventuallyStream(t, c, ns, "orders", func(ct *assert.CollectT, s *js.NatsStream) {
		assertStreamStatus(ct, s, "08-stream-transfer/01-status-natsstream-in-east.yaml")
		if assert.NotNil(ct, s.Status.Server) {
			assert.Regexp(ct, "^east-", s.Status.Server.Leader)
		}
	})

	j := sc["east"].connect(t)
	for _, name := range []string{"audit", "billing"} {
		_, err := j.CreateConsumer(t.Context(), "ORDERS", jetstreamConsumer(name))
		require.NoError(t, err)
	}
	payload := make([]byte, 4<<10)
	for i := range 20000 {
		j.PublishAsync(fmt.Sprintf("orders.%d", i%16), payload) //nolint:errcheck // completion is awaited below.
	}
	<-j.PublishAsyncComplete()

	after := readManifests(t, "08-stream-transfer/02-natsstream-after.yaml")[0]
	cluster, _, err := unstructured.NestedString(after.Object, "spec", "placement", "cluster")
	require.NoError(t, err)
	s := eventuallyStream(t, c, ns, "orders", func(*assert.CollectT, *js.NatsStream) {})
	s.Spec.Placement = &js.Placement{Cluster: cluster}
	require.NoError(t, c.Update(t.Context(), s))
	restart := stallMove(t, j, sc["west"], "ORDERS", cluster)

	eventuallyStream(t, c, ns, "orders", func(ct *assert.CollectT, s *js.NatsStream) {
		assertStreamStatus(ct, s, "08-stream-transfer/02-status-natsstream-transferring.yaml")
		synced := meta.FindStatusCondition(s.Status.Conditions, "Synced")
		if assert.NotNil(ct, synced) {
			assert.Regexp(ct, regexp.MustCompile(`^moving to NATS cluster west; [0-2] of 3 new replicas current$`), synced.Message)
		}
		if assert.NotNil(ct, s.Status.Server) {
			assert.Regexp(ct, "^east-", s.Status.Server.Leader)
		}
	})

	restart()
	eventuallyStream(t, c, ns, "orders", func(ct *assert.CollectT, s *js.NatsStream) {
		assertStreamStatus(ct, s, "08-stream-transfer/03-status-natsstream-in-west.yaml")
		assert.Nil(ct, s.Status.Transfer)
		if assert.NotNil(ct, s.Status.Server) {
			assert.Regexp(ct, "^west-", s.Status.Server.Leader)
		}
	})
}

// assertStreamStatus asserts that s's status holds what the story's status
// file shows, examples tagged !any aside: observedGeneration, each
// condition's status and reason, and of a transfer the NATS clusters, the new
// replicas' names and the consumer total.
func assertStreamStatus(ct *assert.CollectT, s *js.NatsStream, file string) {
	raw, err := os.ReadFile(filepath.Join(storiesDir, file))
	if !assert.NoError(ct, err) {
		return
	}
	raw, err = placeholders.Strip(raw)
	if !assert.NoError(ct, err) {
		return
	}
	var doc struct {
		Status js.NatsStreamStatus `json:"status"`
	}
	if !assert.NoError(ct, yaml.Unmarshal(raw, &doc)) {
		return
	}
	want := doc.Status
	assert.Equal(ct, want.ObservedGeneration, s.Status.ObservedGeneration)
	for _, w := range want.Conditions {
		got := meta.FindStatusCondition(s.Status.Conditions, w.Type)
		if assert.NotNil(ct, got, "no %s condition", w.Type) {
			assert.Equal(ct, w.Status, got.Status, w.Type)
			assert.Equal(ct, w.Reason, got.Reason, w.Type)
		}
	}
	if want.Transfer == nil {
		return
	}
	got := s.Status.Transfer
	if !assert.NotNil(ct, got, "no transfer") {
		return
	}
	assert.Equal(ct, want.Transfer.From, got.From)
	assert.Equal(ct, want.Transfer.To, got.To)
	assert.NotNil(ct, got.Started)
	var wantNames, gotNames []string
	for _, r := range want.Transfer.Replicas {
		wantNames = append(wantNames, r.Name)
	}
	for _, r := range got.Replicas {
		gotNames = append(gotNames, r.Name)
	}
	assert.Equal(ct, wantNames, gotNames)
	if assert.NotNil(ct, got.Consumers) {
		assert.Equal(ct, want.Transfer.Consumers.Total, got.Consumers.Total)
	}
}
