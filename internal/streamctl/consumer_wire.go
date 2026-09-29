package streamctl

import (
	"time"

	"k8s.io/utils/ptr"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

var (
	deliverPolicies = enum[js.DeliverPolicy]{
		js.DeliverAll: "all", js.DeliverLast: "last", js.DeliverNew: "new",
		js.DeliverByStartSequence: "by_start_sequence", js.DeliverByStartTime: "by_start_time",
		js.DeliverLastPerSubject: "last_per_subject",
	}
	ackPolicies      = enum[js.AckPolicy]{js.AckNone: "none", js.AckAll: "all", js.AckExplicit: "explicit"}
	replayPolicies   = enum[js.ReplayPolicy]{js.ReplayInstant: "instant", js.ReplayOriginal: "original"}
	priorityPolicies = enum[js.PriorityPolicy]{
		js.PriorityNone: "none", js.PriorityOverflow: "overflow",
		js.PriorityPinnedClient: "pinned_client", js.PriorityPrioritized: "prioritized",
	}
)

// consumerWire is the part of nats-server's ConsumerConfig JSON that
// ConsumerConfig models, every field omitted when unset.
type consumerWire struct {
	Name               string            `json:"name,omitempty"`
	Durable            string            `json:"durable_name,omitempty"`
	Description        *string           `json:"description,omitempty"`
	DeliverPolicy      *string           `json:"deliver_policy,omitempty"`
	OptStartSeq        *int64            `json:"opt_start_seq,omitempty"`
	OptStartTime       *time.Time        `json:"opt_start_time,omitempty"`
	AckPolicy          *string           `json:"ack_policy,omitempty"`
	AckWait            *int64            `json:"ack_wait,omitempty"`
	MaxDeliver         *int64            `json:"max_deliver,omitempty"`
	BackOff            []int64           `json:"backoff,omitempty"`
	FilterSubject      *string           `json:"filter_subject,omitempty"`
	FilterSubjects     []string          `json:"filter_subjects,omitempty"`
	ReplayPolicy       *string           `json:"replay_policy,omitempty"`
	RateLimit          *int64            `json:"rate_limit_bps,omitempty"`
	SampleFrequency    *string           `json:"sample_freq,omitempty"`
	MaxWaiting         *int64            `json:"max_waiting,omitempty"`
	MaxAckPending      *int64            `json:"max_ack_pending,omitempty"`
	FlowControl        *bool             `json:"flow_control,omitempty"`
	HeadersOnly        *bool             `json:"headers_only,omitempty"`
	MaxRequestBatch    *int64            `json:"max_batch,omitempty"`
	MaxRequestExpires  *int64            `json:"max_expires,omitempty"`
	MaxRequestMaxBytes *int64            `json:"max_bytes,omitempty"`
	DeliverSubject     *string           `json:"deliver_subject,omitempty"`
	DeliverGroup       *string           `json:"deliver_group,omitempty"`
	Heartbeat          *int64            `json:"idle_heartbeat,omitempty"`
	InactiveThreshold  *int64            `json:"inactive_threshold,omitempty"`
	Replicas           *int64            `json:"num_replicas,omitempty"`
	MemoryStorage      *bool             `json:"mem_storage,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	PauseUntil         *time.Time        `json:"pause_until,omitempty"`
	PriorityGroups     []string          `json:"priority_groups,omitempty"`
	PriorityPolicy     *string           `json:"priority_policy,omitempty"`
	PinnedTTL          *int64            `json:"priority_timeout,omitempty"`
}

// consumerToWire returns the fields c sets, spelled for the server, as the
// durable named name.
func consumerToWire(c *js.ConsumerConfig, name string) consumerWire {
	return consumerWire{
		Name:               name,
		Durable:            name,
		Description:        nonZero(&c.Description),
		DeliverPolicy:      deliverPolicies.wire(c.DeliverPolicy),
		OptStartSeq:        c.OptStartSeq,
		OptStartTime:       timeWire(c.OptStartTime),
		AckPolicy:          ackPolicies.wire(c.AckPolicy),
		AckWait:            durationWire(c.AckWait),
		MaxDeliver:         c.MaxDeliver,
		BackOff:            durationsWire(c.BackOff),
		FilterSubject:      nonZero(&c.FilterSubject),
		FilterSubjects:     c.FilterSubjects,
		ReplayPolicy:       replayPolicies.wire(c.ReplayPolicy),
		RateLimit:          c.RateLimit,
		SampleFrequency:    nonZero(&c.SampleFrequency),
		MaxWaiting:         c.MaxWaiting,
		MaxAckPending:      c.MaxAckPending,
		FlowControl:        c.FlowControl,
		HeadersOnly:        c.HeadersOnly,
		MaxRequestBatch:    c.MaxRequestBatch,
		MaxRequestExpires:  durationWire(c.MaxRequestExpires),
		MaxRequestMaxBytes: c.MaxRequestMaxBytes,
		DeliverSubject:     nonZero(&c.DeliverSubject),
		DeliverGroup:       nonZero(&c.DeliverGroup),
		Heartbeat:          durationWire(c.Heartbeat),
		InactiveThreshold:  durationWire(c.InactiveThreshold),
		Replicas:           int32Wire(c.Replicas),
		MemoryStorage:      c.MemoryStorage,
		Metadata:           c.Metadata,
		PauseUntil:         timeWire(c.PauseUntil),
		PriorityGroups:     c.PriorityGroups,
		PriorityPolicy:     priorityPolicies.wire(c.PriorityPolicy),
		PinnedTTL:          durationWire(c.PinnedTTL),
	}
}

// consumerFromWire returns w as a ConsumerConfig that sets every enum and
// every other field w holds a non-zero value for; name and metadata are
// left unset.
func consumerFromWire(w *consumerWire) js.ConsumerConfig {
	return js.ConsumerConfig{
		Description:        ptr.Deref(w.Description, ""),
		DeliverPolicy:      deliverPolicies.api(w.DeliverPolicy),
		OptStartSeq:        nonZero(w.OptStartSeq),
		OptStartTime:       timeAPI(w.OptStartTime),
		AckPolicy:          ackPolicies.api(w.AckPolicy),
		AckWait:            durationAPI(w.AckWait),
		MaxDeliver:         nonZero(w.MaxDeliver),
		BackOff:            durationsAPI(w.BackOff),
		FilterSubject:      ptr.Deref(w.FilterSubject, ""),
		FilterSubjects:     w.FilterSubjects,
		ReplayPolicy:       replayPolicies.api(w.ReplayPolicy),
		RateLimit:          nonZero(w.RateLimit),
		SampleFrequency:    ptr.Deref(w.SampleFrequency, ""),
		MaxWaiting:         nonZero(w.MaxWaiting),
		MaxAckPending:      nonZero(w.MaxAckPending),
		FlowControl:        nonZero(w.FlowControl),
		HeadersOnly:        nonZero(w.HeadersOnly),
		MaxRequestBatch:    nonZero(w.MaxRequestBatch),
		MaxRequestExpires:  durationAPI(w.MaxRequestExpires),
		MaxRequestMaxBytes: nonZero(w.MaxRequestMaxBytes),
		DeliverSubject:     ptr.Deref(w.DeliverSubject, ""),
		DeliverGroup:       ptr.Deref(w.DeliverGroup, ""),
		Heartbeat:          durationAPI(w.Heartbeat),
		InactiveThreshold:  durationAPI(w.InactiveThreshold),
		Replicas:           int32API(w.Replicas),
		MemoryStorage:      nonZero(w.MemoryStorage),
		PauseUntil:         timeAPI(w.PauseUntil),
		PriorityGroups:     w.PriorityGroups,
		PriorityPolicy:     priorityPolicies.api(w.PriorityPolicy),
		PinnedTTL:          durationAPI(w.PinnedTTL),
	}
}
