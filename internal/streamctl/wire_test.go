package streamctl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
)

func TestStreamWireRoundTrip(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 9, 26, 9, 0, 0, 0, time.UTC))
	in := js.StreamConfig{
		Name:                   "ORDERS",
		Description:            "orders",
		Subjects:               []string{"orders.>"},
		Retention:              ptr.To(js.RetentionWorkQueue),
		MaxConsumers:           ptr.To(int64(-1)),
		MaxMsgs:                ptr.To(int64(100)),
		MaxBytes:               ptr.To(resource.MustParse("5Gi")),
		Discard:                ptr.To(js.DiscardNew),
		DiscardNewPerSubject:   ptr.To(true),
		MaxAge:                 &metav1.Duration{Duration: 72 * time.Hour},
		MaxMsgsPerSubject:      ptr.To(int64(5)),
		MaxMsgSize:             ptr.To(resource.MustParse("1Mi")),
		Storage:                ptr.To(js.StorageMemory),
		Replicas:               ptr.To(int32(3)),
		NoAck:                  ptr.To(true),
		Duplicates:             &metav1.Duration{Duration: time.Minute},
		Placement:              &js.Placement{Cluster: "east", Tags: []string{"ssd"}, Preferred: "n1"},
		Sources:                []js.StreamSource{{Name: "A", OptStartSeq: ptr.To(int64(4)), OptStartTime: &start, FilterSubject: "a.>", SubjectTransforms: []js.SubjectTransform{{Source: "a.>", Destination: "b.>"}}, External: &js.ExternalStream{APIPrefix: "$JS.x.API", DeliverPrefix: "d"}, Consumer: &js.StreamConsumerSource{Name: "c"}}},
		Sealed:                 ptr.To(true),
		DenyDelete:             ptr.To(true),
		DenyPurge:              ptr.To(true),
		AllowRollup:            ptr.To(true),
		Compression:            ptr.To(js.CompressionS2),
		FirstSeq:               ptr.To(int64(7)),
		SubjectTransform:       &js.SubjectTransform{Source: "x", Destination: "y"},
		Republish:              &js.Republish{Source: "orders.>", Destination: "copy.>", HeadersOnly: ptr.To(true)},
		AllowDirect:            ptr.To(true),
		MirrorDirect:           ptr.To(true),
		ConsumerLimits:         &js.StreamConsumerLimits{InactiveThreshold: &metav1.Duration{Duration: time.Hour}, MaxAckPending: ptr.To(int64(10))},
		AllowMsgTTL:            ptr.To(true),
		SubjectDeleteMarkerTTL: &metav1.Duration{Duration: time.Minute},
		AllowMsgCounter:        ptr.To(true),
		AllowAtomicPublish:     ptr.To(true),
		AllowMsgSchedules:      ptr.To(true),
		PersistMode:            ptr.To(js.PersistAsync),
		AllowBatchPublish:      ptr.To(true),
	}
	cfg, err := lifecycle.ToConfig(streamToWire(&in))
	require.NoError(t, err)
	require.Equal(t, "workqueue", cfg["retention"])
	require.Equal(t, "memory", cfg["storage"])
	var w streamWire
	require.NoError(t, lifecycle.FromConfig(cfg, &w))
	out := streamFromWire(&w)
	require.Equal(t, in.MaxBytes.Value(), out.MaxBytes.Value())
	require.Equal(t, in.MaxMsgSize.Value(), out.MaxMsgSize.Value())
	out.MaxBytes, out.MaxMsgSize = in.MaxBytes, in.MaxMsgSize
	require.Equal(t, in, out)
}

func TestStreamFromWireDropsZeroes(t *testing.T) {
	cfg, err := lifecycle.DecodeConfig([]byte(`{"name":"S","retention":"limits","storage":"file","discard":"old","compression":"none",
		"max_consumers":-1,"max_msgs":-1,"max_bytes":-1,"max_age":0,"num_replicas":1,"sealed":false,"deny_delete":false,
		"allow_direct":false,"consumer_limits":{},"placement":{"cluster":""},"duplicate_window":120000000000}`))
	require.NoError(t, err)
	var w streamWire
	require.NoError(t, lifecycle.FromConfig(cfg, &w))
	got := streamFromWire(&w)
	want := js.StreamConfig{
		Name:         "S",
		Retention:    ptr.To(js.RetentionLimits),
		Storage:      ptr.To(js.StorageFile),
		Discard:      ptr.To(js.DiscardOld),
		Compression:  ptr.To(js.CompressionNone),
		MaxConsumers: ptr.To(int64(-1)),
		MaxMsgs:      ptr.To(int64(-1)),
		MaxBytes:     resource.NewQuantity(-1, resource.BinarySI),
		Replicas:     ptr.To(int32(1)),
		Duplicates:   &metav1.Duration{Duration: 2 * time.Minute},
	}
	require.Equal(t, want, got)
}

func TestConsumerWireRoundTrip(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 9, 26, 9, 0, 0, 0, time.UTC))
	in := js.ConsumerConfig{
		Description:        "audit",
		DeliverPolicy:      ptr.To(js.DeliverByStartTime),
		OptStartTime:       &start,
		AckPolicy:          ptr.To(js.AckAll),
		AckWait:            &metav1.Duration{Duration: 30 * time.Second},
		MaxDeliver:         ptr.To(int64(10)),
		BackOff:            []metav1.Duration{{Duration: time.Second}, {Duration: time.Minute}},
		FilterSubjects:     []string{"a.>", "b.>"},
		ReplayPolicy:       ptr.To(js.ReplayOriginal),
		RateLimit:          ptr.To(int64(1000)),
		SampleFrequency:    "50%",
		MaxAckPending:      ptr.To(int64(100)),
		FlowControl:        ptr.To(true),
		HeadersOnly:        ptr.To(true),
		DeliverSubject:     "deliver",
		DeliverGroup:       "group",
		Heartbeat:          &metav1.Duration{Duration: 5 * time.Second},
		InactiveThreshold:  &metav1.Duration{Duration: time.Hour},
		Replicas:           ptr.To(int32(3)),
		MemoryStorage:      ptr.To(true),
		PauseUntil:         &start,
		PriorityGroups:     []string{"g"},
		PriorityPolicy:     ptr.To(js.PriorityPinnedClient),
		PinnedTTL:          &metav1.Duration{Duration: time.Minute},
		MaxRequestBatch:    ptr.To(int64(5)),
		MaxRequestExpires:  &metav1.Duration{Duration: time.Second},
		MaxRequestMaxBytes: ptr.To(int64(1024)),
		MaxWaiting:         ptr.To(int64(3)),
		OptStartSeq:        ptr.To(int64(9)),
	}
	cfg, err := lifecycle.ToConfig(consumerToWire(&in, "AUDIT"))
	require.NoError(t, err)
	require.Equal(t, "AUDIT", cfg["durable_name"])
	require.Equal(t, "by_start_time", cfg["deliver_policy"])
	require.Equal(t, "pinned_client", cfg["priority_policy"])
	var w consumerWire
	require.NoError(t, lifecycle.FromConfig(cfg, &w))
	require.Equal(t, in, consumerFromWire(&w))
}

func TestServerStream(t *testing.T) {
	meta := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name} }
	for _, tc := range []struct {
		name string
		obj  client.Object
		want string
	}{
		{"stream by resource name", &js.NatsStream{ObjectMeta: meta("orders")}, "orders"},
		{"stream by spec name", &js.NatsStream{ObjectMeta: meta("orders"), Spec: js.NatsStreamSpec{StreamConfig: js.StreamConfig{Name: "ORDERS"}}}, "ORDERS"},
		{"key-value", &js.NatsKeyValue{ObjectMeta: meta("sessions")}, "KV_sessions"},
		{"key-value by spec name", &js.NatsKeyValue{ObjectMeta: meta("sessions"), Spec: js.NatsKeyValueSpec{KeyValueConfig: js.KeyValueConfig{Name: "cfg"}}}, "KV_cfg"},
		{"object store", &js.NatsObjectStore{ObjectMeta: meta("blobs")}, "OBJ_blobs"},
		{"other", &js.NatsConsumer{ObjectMeta: meta("c")}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ServerStream(tc.obj))
		})
	}
}
