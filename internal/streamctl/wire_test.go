package streamctl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
	"github.com/mikluko/nats-operator/internal/lifecycle"
)

func TestStreamWireRoundTrip(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 9, 26, 9, 0, 0, 0, time.UTC))
	in := js.StreamConfig{
		Name:                   "ORDERS",
		Description:            "orders",
		Subjects:               []string{"orders.>"},
		Retention:              ptrTo(js.RetentionWorkQueue),
		MaxConsumers:           ptrTo(int64(-1)),
		MaxMsgs:                ptrTo(int64(100)),
		MaxBytes:               ptrTo(resource.MustParse("5Gi")),
		Discard:                ptrTo(js.DiscardNew),
		DiscardNewPerSubject:   ptrTo(true),
		MaxAge:                 &metav1.Duration{Duration: 72 * time.Hour},
		MaxMsgsPerSubject:      ptrTo(int64(5)),
		MaxMsgSize:             ptrTo(resource.MustParse("1Mi")),
		Storage:                ptrTo(js.StorageMemory),
		Replicas:               ptrTo(int32(3)),
		NoAck:                  ptrTo(true),
		Duplicates:             &metav1.Duration{Duration: time.Minute},
		Placement:              &js.Placement{Cluster: "east", Tags: []string{"ssd"}, Preferred: "n1"},
		Sources:                []js.StreamSource{{Name: "A", OptStartSeq: ptrTo(int64(4)), OptStartTime: &start, FilterSubject: "a.>", SubjectTransforms: []js.SubjectTransform{{Source: "a.>", Destination: "b.>"}}, External: &js.ExternalStream{APIPrefix: "$JS.x.API", DeliverPrefix: "d"}, Consumer: &js.StreamConsumerSource{Name: "c"}}},
		Sealed:                 ptrTo(true),
		DenyDelete:             ptrTo(true),
		DenyPurge:              ptrTo(true),
		AllowRollup:            ptrTo(true),
		Compression:            ptrTo(js.CompressionS2),
		FirstSeq:               ptrTo(int64(7)),
		SubjectTransform:       &js.SubjectTransform{Source: "x", Destination: "y"},
		Republish:              &js.Republish{Source: "orders.>", Destination: "copy.>", HeadersOnly: ptrTo(true)},
		AllowDirect:            ptrTo(true),
		MirrorDirect:           ptrTo(true),
		ConsumerLimits:         &js.StreamConsumerLimits{InactiveThreshold: &metav1.Duration{Duration: time.Hour}, MaxAckPending: ptrTo(int64(10))},
		AllowMsgTTL:            ptrTo(true),
		SubjectDeleteMarkerTTL: &metav1.Duration{Duration: time.Minute},
		AllowMsgCounter:        ptrTo(true),
		AllowAtomicPublish:     ptrTo(true),
		AllowMsgSchedules:      ptrTo(true),
		PersistMode:            ptrTo(js.PersistAsync),
		AllowBatchPublish:      ptrTo(true),
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
		Retention:    ptrTo(js.RetentionLimits),
		Storage:      ptrTo(js.StorageFile),
		Discard:      ptrTo(js.DiscardOld),
		Compression:  ptrTo(js.CompressionNone),
		MaxConsumers: ptrTo(int64(-1)),
		MaxMsgs:      ptrTo(int64(-1)),
		MaxBytes:     resource.NewQuantity(-1, resource.BinarySI),
		Replicas:     ptrTo(int32(1)),
		Duplicates:   &metav1.Duration{Duration: 2 * time.Minute},
	}
	require.Equal(t, want, got)
}

func TestConsumerWireRoundTrip(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 9, 26, 9, 0, 0, 0, time.UTC))
	in := js.ConsumerConfig{
		Description:        "audit",
		DeliverPolicy:      ptrTo(js.DeliverByStartTime),
		OptStartTime:       &start,
		AckPolicy:          ptrTo(js.AckAll),
		AckWait:            &metav1.Duration{Duration: 30 * time.Second},
		MaxDeliver:         ptrTo(int64(10)),
		BackOff:            []metav1.Duration{{Duration: time.Second}, {Duration: time.Minute}},
		FilterSubjects:     []string{"a.>", "b.>"},
		ReplayPolicy:       ptrTo(js.ReplayOriginal),
		RateLimit:          ptrTo(int64(1000)),
		SampleFrequency:    "50%",
		MaxAckPending:      ptrTo(int64(100)),
		FlowControl:        ptrTo(true),
		HeadersOnly:        ptrTo(true),
		DeliverSubject:     "deliver",
		DeliverGroup:       "group",
		Heartbeat:          &metav1.Duration{Duration: 5 * time.Second},
		InactiveThreshold:  &metav1.Duration{Duration: time.Hour},
		Replicas:           ptrTo(int32(3)),
		MemoryStorage:      ptrTo(true),
		PauseUntil:         &start,
		PriorityGroups:     []string{"g"},
		PriorityPolicy:     ptrTo(js.PriorityPinnedClient),
		PinnedTTL:          &metav1.Duration{Duration: time.Minute},
		MaxRequestBatch:    ptrTo(int64(5)),
		MaxRequestExpires:  &metav1.Duration{Duration: time.Second},
		MaxRequestMaxBytes: ptrTo(int64(1024)),
		MaxWaiting:         ptrTo(int64(3)),
		OptStartSeq:        ptrTo(int64(9)),
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
