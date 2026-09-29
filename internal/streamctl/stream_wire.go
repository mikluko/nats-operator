package streamctl

import (
	"time"

	"k8s.io/utils/ptr"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

var (
	retentions   = enum[js.RetentionPolicy]{js.RetentionLimits: "limits", js.RetentionInterest: "interest", js.RetentionWorkQueue: "workqueue"}
	discards     = enum[js.DiscardPolicy]{js.DiscardOld: "old", js.DiscardNew: "new"}
	storages     = enum[js.StorageType]{js.StorageFile: "file", js.StorageMemory: "memory"}
	compressions = enum[js.StoreCompression]{js.CompressionNone: "none", js.CompressionS2: "s2"}
	persistModes = enum[js.PersistMode]{js.PersistDefault: "default", js.PersistAsync: "async"}
)

// streamWire is the part of nats-server's StreamConfig JSON that
// StreamConfig models, every field omitted when unset.
type streamWire struct {
	Name                   string              `json:"name,omitempty"`
	Description            *string             `json:"description,omitempty"`
	Subjects               []string            `json:"subjects,omitempty"`
	Retention              *string             `json:"retention,omitempty"`
	MaxConsumers           *int64              `json:"max_consumers,omitempty"`
	MaxMsgs                *int64              `json:"max_msgs,omitempty"`
	MaxBytes               *int64              `json:"max_bytes,omitempty"`
	Discard                *string             `json:"discard,omitempty"`
	DiscardNewPerSubject   *bool               `json:"discard_new_per_subject,omitempty"`
	MaxAge                 *int64              `json:"max_age,omitempty"`
	MaxMsgsPerSubject      *int64              `json:"max_msgs_per_subject,omitempty"`
	MaxMsgSize             *int64              `json:"max_msg_size,omitempty"`
	Storage                *string             `json:"storage,omitempty"`
	Replicas               *int64              `json:"num_replicas,omitempty"`
	NoAck                  *bool               `json:"no_ack,omitempty"`
	Duplicates             *int64              `json:"duplicate_window,omitempty"`
	Placement              *placementWire      `json:"placement,omitempty"`
	Mirror                 *sourceWire         `json:"mirror,omitempty"`
	Sources                []sourceWire        `json:"sources,omitempty"`
	Sealed                 *bool               `json:"sealed,omitempty"`
	DenyDelete             *bool               `json:"deny_delete,omitempty"`
	DenyPurge              *bool               `json:"deny_purge,omitempty"`
	AllowRollup            *bool               `json:"allow_rollup_hdrs,omitempty"`
	Compression            *string             `json:"compression,omitempty"`
	FirstSeq               *int64              `json:"first_seq,omitempty"`
	SubjectTransform       *transformWire      `json:"subject_transform,omitempty"`
	Republish              *republishWire      `json:"republish,omitempty"`
	AllowDirect            *bool               `json:"allow_direct,omitempty"`
	MirrorDirect           *bool               `json:"mirror_direct,omitempty"`
	ConsumerLimits         *consumerLimitsWire `json:"consumer_limits,omitempty"`
	Metadata               map[string]string   `json:"metadata,omitempty"`
	AllowMsgTTL            *bool               `json:"allow_msg_ttl,omitempty"`
	SubjectDeleteMarkerTTL *int64              `json:"subject_delete_marker_ttl,omitempty"`
	AllowMsgCounter        *bool               `json:"allow_msg_counter,omitempty"`
	AllowAtomicPublish     *bool               `json:"allow_atomic,omitempty"`
	AllowMsgSchedules      *bool               `json:"allow_msg_schedules,omitempty"`
	PersistMode            *string             `json:"persist_mode,omitempty"`
	AllowBatchPublish      *bool               `json:"allow_batched,omitempty"`
}

type placementWire struct {
	Cluster   string   `json:"cluster,omitempty"`
	Tags      []string `json:"tags,omitempty"`
	Preferred string   `json:"preferred,omitempty"`
}

type sourceWire struct {
	Name              string              `json:"name"`
	OptStartSeq       *int64              `json:"opt_start_seq,omitempty"`
	OptStartTime      *time.Time          `json:"opt_start_time,omitempty"`
	FilterSubject     string              `json:"filter_subject,omitempty"`
	SubjectTransforms []transformWire     `json:"subject_transforms,omitempty"`
	External          *externalWire       `json:"external,omitempty"`
	Consumer          *consumerSourceWire `json:"consumer,omitempty"`
}

type externalWire struct {
	APIPrefix     string `json:"api"`
	DeliverPrefix string `json:"deliver,omitempty"`
}

type consumerSourceWire struct {
	Name           string `json:"name,omitempty"`
	DeliverSubject string `json:"deliver_subject,omitempty"`
}

type transformWire struct {
	Source      string `json:"src,omitempty"`
	Destination string `json:"dest"`
}

type republishWire struct {
	Source      string `json:"src,omitempty"`
	Destination string `json:"dest"`
	HeadersOnly *bool  `json:"headers_only,omitempty"`
}

type consumerLimitsWire struct {
	InactiveThreshold *int64 `json:"inactive_threshold,omitempty"`
	MaxAckPending     *int64 `json:"max_ack_pending,omitempty"`
}

// streamToWire returns the fields c sets, spelled for the server.
func streamToWire(c *js.StreamConfig) streamWire {
	w := streamWire{
		Name:                   c.Name,
		Description:            nonZero(&c.Description),
		Subjects:               c.Subjects,
		Retention:              retentions.wire(c.Retention),
		MaxConsumers:           c.MaxConsumers,
		MaxMsgs:                c.MaxMsgs,
		MaxBytes:               quantityWire(c.MaxBytes),
		Discard:                discards.wire(c.Discard),
		DiscardNewPerSubject:   c.DiscardNewPerSubject,
		MaxAge:                 durationWire(c.MaxAge),
		MaxMsgsPerSubject:      c.MaxMsgsPerSubject,
		MaxMsgSize:             quantityWire(c.MaxMsgSize),
		Storage:                storages.wire(c.Storage),
		Replicas:               int32Wire(c.Replicas),
		NoAck:                  c.NoAck,
		Duplicates:             durationWire(c.Duplicates),
		Sealed:                 c.Sealed,
		DenyDelete:             c.DenyDelete,
		DenyPurge:              c.DenyPurge,
		AllowRollup:            c.AllowRollup,
		Compression:            compressions.wire(c.Compression),
		FirstSeq:               c.FirstSeq,
		AllowDirect:            c.AllowDirect,
		MirrorDirect:           c.MirrorDirect,
		Metadata:               c.Metadata,
		AllowMsgTTL:            c.AllowMsgTTL,
		SubjectDeleteMarkerTTL: durationWire(c.SubjectDeleteMarkerTTL),
		AllowMsgCounter:        c.AllowMsgCounter,
		AllowAtomicPublish:     c.AllowAtomicPublish,
		AllowMsgSchedules:      c.AllowMsgSchedules,
		PersistMode:            persistModes.wire(c.PersistMode),
		AllowBatchPublish:      c.AllowBatchPublish,
	}
	if p := c.Placement; p != nil {
		w.Placement = &placementWire{Cluster: p.Cluster, Tags: p.Tags, Preferred: p.Preferred}
	}
	if c.Mirror != nil {
		w.Mirror = ptr.To(sourceToWire(c.Mirror))
	}
	for i := range c.Sources {
		w.Sources = append(w.Sources, sourceToWire(&c.Sources[i]))
	}
	if t := c.SubjectTransform; t != nil {
		w.SubjectTransform = &transformWire{Source: t.Source, Destination: t.Destination}
	}
	if r := c.Republish; r != nil {
		w.Republish = &republishWire{Source: r.Source, Destination: r.Destination, HeadersOnly: r.HeadersOnly}
	}
	if l := c.ConsumerLimits; l != nil {
		w.ConsumerLimits = &consumerLimitsWire{InactiveThreshold: durationWire(l.InactiveThreshold), MaxAckPending: l.MaxAckPending}
	}
	return w
}

func sourceToWire(s *js.StreamSource) sourceWire {
	w := sourceWire{
		Name:          s.Name,
		OptStartSeq:   s.OptStartSeq,
		OptStartTime:  timeWire(s.OptStartTime),
		FilterSubject: s.FilterSubject,
	}
	for _, t := range s.SubjectTransforms {
		w.SubjectTransforms = append(w.SubjectTransforms, transformWire{Source: t.Source, Destination: t.Destination})
	}
	if e := s.External; e != nil {
		w.External = &externalWire{APIPrefix: e.APIPrefix, DeliverPrefix: e.DeliverPrefix}
	}
	if c := s.Consumer; c != nil {
		w.Consumer = &consumerSourceWire{Name: c.Name, DeliverSubject: c.DeliverSubject}
	}
	return w
}

// streamFromWire returns w as a StreamConfig that sets every enum and
// every other field w holds a non-zero value for; metadata is left unset.
func streamFromWire(w *streamWire) js.StreamConfig {
	c := js.StreamConfig{
		Name:                   w.Name,
		Description:            ptr.Deref(w.Description, ""),
		Subjects:               w.Subjects,
		Retention:              retentions.api(w.Retention),
		MaxConsumers:           nonZero(w.MaxConsumers),
		MaxMsgs:                nonZero(w.MaxMsgs),
		MaxBytes:               quantityAPI(w.MaxBytes),
		Discard:                discards.api(w.Discard),
		DiscardNewPerSubject:   nonZero(w.DiscardNewPerSubject),
		MaxAge:                 durationAPI(w.MaxAge),
		MaxMsgsPerSubject:      nonZero(w.MaxMsgsPerSubject),
		MaxMsgSize:             quantityAPI(w.MaxMsgSize),
		Storage:                storages.api(w.Storage),
		Replicas:               int32API(w.Replicas),
		NoAck:                  nonZero(w.NoAck),
		Duplicates:             durationAPI(w.Duplicates),
		Sealed:                 nonZero(w.Sealed),
		DenyDelete:             nonZero(w.DenyDelete),
		DenyPurge:              nonZero(w.DenyPurge),
		AllowRollup:            nonZero(w.AllowRollup),
		Compression:            compressions.api(w.Compression),
		FirstSeq:               nonZero(w.FirstSeq),
		AllowDirect:            nonZero(w.AllowDirect),
		MirrorDirect:           nonZero(w.MirrorDirect),
		AllowMsgTTL:            nonZero(w.AllowMsgTTL),
		SubjectDeleteMarkerTTL: durationAPI(w.SubjectDeleteMarkerTTL),
		AllowMsgCounter:        nonZero(w.AllowMsgCounter),
		AllowAtomicPublish:     nonZero(w.AllowAtomicPublish),
		AllowMsgSchedules:      nonZero(w.AllowMsgSchedules),
		PersistMode:            persistModes.api(w.PersistMode),
		AllowBatchPublish:      nonZero(w.AllowBatchPublish),
	}
	if p := w.Placement; p != nil && (p.Cluster != "" || len(p.Tags) > 0 || p.Preferred != "") {
		c.Placement = &js.Placement{Cluster: p.Cluster, Tags: p.Tags, Preferred: p.Preferred}
	}
	if w.Mirror != nil {
		c.Mirror = ptr.To(sourceFromWire(w.Mirror))
	}
	for i := range w.Sources {
		c.Sources = append(c.Sources, sourceFromWire(&w.Sources[i]))
	}
	if t := w.SubjectTransform; t != nil && t.Destination != "" {
		c.SubjectTransform = &js.SubjectTransform{Source: t.Source, Destination: t.Destination}
	}
	if r := w.Republish; r != nil && r.Destination != "" {
		c.Republish = &js.Republish{Source: r.Source, Destination: r.Destination, HeadersOnly: nonZero(r.HeadersOnly)}
	}
	if l := w.ConsumerLimits; l != nil && (ptr.Deref(l.InactiveThreshold, 0) != 0 || ptr.Deref(l.MaxAckPending, 0) != 0) {
		c.ConsumerLimits = &js.StreamConsumerLimits{InactiveThreshold: durationAPI(l.InactiveThreshold), MaxAckPending: nonZero(l.MaxAckPending)}
	}
	return c
}

func sourceFromWire(w *sourceWire) js.StreamSource {
	s := js.StreamSource{
		Name:          w.Name,
		OptStartSeq:   nonZero(w.OptStartSeq),
		OptStartTime:  timeAPI(w.OptStartTime),
		FilterSubject: w.FilterSubject,
	}
	for _, t := range w.SubjectTransforms {
		s.SubjectTransforms = append(s.SubjectTransforms, js.SubjectTransform{Source: t.Source, Destination: t.Destination})
	}
	if e := w.External; e != nil && e.APIPrefix != "" {
		s.External = &js.ExternalStream{APIPrefix: e.APIPrefix, DeliverPrefix: e.DeliverPrefix}
	}
	if c := w.Consumer; c != nil && (c.Name != "" || c.DeliverSubject != "") {
		s.Consumer = &js.StreamConsumerSource{Name: c.Name, DeliverSubject: c.DeliverSubject}
	}
	return s
}
