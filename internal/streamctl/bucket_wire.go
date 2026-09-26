package streamctl

import (
	"slices"
	"strings"

	js "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

// Stream name prefixes and the key subject template nats.go gives a bucket's
// stream.
const (
	kvStreamPrefix  = "KV_"
	objStreamPrefix = "OBJ_"
)

func kvSubjects(bucket string) string { return "$KV." + bucket + ".>" }

// kvWire is nats.go's KeyValueConfig JSON, every field omitted when unset.
type kvWire struct {
	Bucket         string            `json:"bucket,omitempty"`
	Description    *string           `json:"description,omitempty"`
	MaxValueSize   *int64            `json:"max_value_size,omitempty"`
	History        *int64            `json:"history,omitempty"`
	TTL            *int64            `json:"ttl,omitempty"`
	MaxBytes       *int64            `json:"max_bytes,omitempty"`
	Storage        *string           `json:"storage,omitempty"`
	Replicas       *int64            `json:"num_replicas,omitempty"`
	Placement      *placementWire    `json:"placement,omitempty"`
	Republish      *republishWire    `json:"republish,omitempty"`
	Mirror         *sourceWire       `json:"mirror,omitempty"`
	Sources        []sourceWire      `json:"sources,omitempty"`
	Compression    *bool             `json:"compression,omitempty"`
	LimitMarkerTTL *int64            `json:"limit_marker_ttl,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// objWire is nats.go's ObjectStoreConfig JSON, every field omitted when
// unset.
type objWire struct {
	Bucket      string            `json:"bucket,omitempty"`
	Description *string           `json:"description,omitempty"`
	TTL         *int64            `json:"max_age,omitempty"`
	MaxBytes    *int64            `json:"max_bytes,omitempty"`
	Storage     *string           `json:"storage,omitempty"`
	Replicas    *int64            `json:"num_replicas,omitempty"`
	Placement   *placementWire    `json:"placement,omitempty"`
	Compression *bool             `json:"compression,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// kvToWire returns the fields c sets, spelled as nats.go's KeyValueConfig,
// its mirror and sources in bucketSource's form.
func kvToWire(c *js.KeyValueConfig, bucket string) kvWire {
	w := kvWire{
		Bucket:         bucket,
		Description:    nonZero(&c.Description),
		MaxValueSize:   quantityWire(c.MaxValueSize),
		History:        int32Wire(c.History),
		TTL:            durationWire(c.TTL),
		MaxBytes:       quantityWire(c.MaxBytes),
		Storage:        storages.wire(c.Storage),
		Replicas:       int32Wire(c.Replicas),
		Placement:      placementToWire(c.Placement),
		Compression:    c.Compression,
		LimitMarkerTTL: durationWire(c.LimitMarkerTTL),
		Metadata:       c.Metadata,
	}
	if r := c.Republish; r != nil {
		w.Republish = &republishWire{Source: r.Source, Destination: r.Destination, HeadersOnly: r.HeadersOnly}
	}
	if c.Mirror != nil {
		w.Mirror = ptrTo(mirrorSource(sourceToWire(c.Mirror)))
	}
	for i := range c.Sources {
		w.Sources = append(w.Sources, bucketSource(sourceToWire(&c.Sources[i]), bucket))
	}
	return w
}

// kvFromWire returns w as a KeyValueConfig that sets every field w holds a
// non-zero value for; name and metadata are left unset.
func kvFromWire(w *kvWire) js.KeyValueConfig {
	c := js.KeyValueConfig{
		Description:    deref(w.Description),
		MaxValueSize:   quantityAPI(w.MaxValueSize),
		History:        int32API(w.History),
		TTL:            durationAPI(w.TTL),
		MaxBytes:       quantityAPI(w.MaxBytes),
		Storage:        storages.api(w.Storage),
		Replicas:       int32API(w.Replicas),
		Placement:      placementFromWire(w.Placement),
		Compression:    nonZero(w.Compression),
		LimitMarkerTTL: durationAPI(w.LimitMarkerTTL),
	}
	if r := w.Republish; r != nil && r.Destination != "" {
		c.Republish = &js.Republish{Source: r.Source, Destination: r.Destination, HeadersOnly: nonZero(r.HeadersOnly)}
	}
	if w.Mirror != nil {
		c.Mirror = ptrTo(sourceFromWire(w.Mirror))
	}
	for i := range w.Sources {
		c.Sources = append(c.Sources, sourceFromWire(&w.Sources[i]))
	}
	return c
}

// kvFromStream reads the KeyValueConfig nats.go would have built s from:
// the inverse of its prepareKeyValueConfig for every field KeyValueConfig
// carries, with its -1 "unlimited" values read as unset.
func kvFromStream(s *streamWire) kvWire {
	bucket := strings.TrimPrefix(s.Name, kvStreamPrefix)
	w := kvWire{
		Bucket:         bucket,
		Description:    nonZero(s.Description),
		MaxValueSize:   positive(s.MaxMsgSize),
		History:        positive(s.MaxMsgsPerSubject),
		TTL:            positive(s.MaxAge),
		MaxBytes:       positive(s.MaxBytes),
		Storage:        s.Storage,
		Replicas:       positive(s.Replicas),
		Placement:      s.Placement,
		Compression:    compressed(s.Compression),
		LimitMarkerTTL: positive(s.SubjectDeleteMarkerTTL),
		Metadata:       s.Metadata,
	}
	if r := s.Republish; r != nil && r.Destination != "" {
		w.Republish = r
	}
	if s.Mirror != nil {
		w.Mirror = ptrTo(mirrorSource(*s.Mirror))
	}
	for _, src := range s.Sources {
		w.Sources = append(w.Sources, bucketSource(src, bucket))
	}
	return w
}

// objToWire returns the fields c sets, spelled as nats.go's
// ObjectStoreConfig.
func objToWire(c *js.ObjectStoreConfig, bucket string) objWire {
	return objWire{
		Bucket:      bucket,
		Description: nonZero(&c.Description),
		TTL:         durationWire(c.TTL),
		MaxBytes:    quantityWire(c.MaxBytes),
		Storage:     storages.wire(c.Storage),
		Replicas:    int32Wire(c.Replicas),
		Placement:   placementToWire(c.Placement),
		Compression: c.Compression,
		Metadata:    c.Metadata,
	}
}

// objFromWire returns w as an ObjectStoreConfig that sets every field w
// holds a non-zero value for; name and metadata are left unset.
func objFromWire(w *objWire) js.ObjectStoreConfig {
	return js.ObjectStoreConfig{
		Description: deref(w.Description),
		TTL:         durationAPI(w.TTL),
		MaxBytes:    quantityAPI(w.MaxBytes),
		Storage:     storages.api(w.Storage),
		Replicas:    int32API(w.Replicas),
		Placement:   placementFromWire(w.Placement),
		Compression: nonZero(w.Compression),
	}
}

// objFromStream reads the ObjectStoreConfig nats.go would have built s
// from, as kvFromStream does for a bucket.
func objFromStream(s *streamWire) objWire {
	return objWire{
		Bucket:      strings.TrimPrefix(s.Name, objStreamPrefix),
		Description: nonZero(s.Description),
		TTL:         positive(s.MaxAge),
		MaxBytes:    positive(s.MaxBytes),
		Storage:     s.Storage,
		Replicas:    positive(s.Replicas),
		Placement:   s.Placement,
		Compression: compressed(s.Compression),
		Metadata:    s.Metadata,
	}
}

func placementToWire(p *js.Placement) *placementWire {
	if p == nil {
		return nil
	}
	return &placementWire{Cluster: p.Cluster, Tags: p.Tags, Preferred: p.Preferred}
}

func placementFromWire(p *placementWire) *js.Placement {
	if p == nil || (p.Cluster == "" && len(p.Tags) == 0 && p.Preferred == "") {
		return nil
	}
	return &js.Placement{Cluster: p.Cluster, Tags: p.Tags, Preferred: p.Preferred}
}

// mirrorSource is a bucket's mirror named by its bucket: nats.go prefixes
// a mirror's name with KV_ where it lacks it.
func mirrorSource(s sourceWire) sourceWire {
	s.Name = strings.TrimPrefix(s.Name, kvStreamPrefix)
	return s
}

// bucketSource is a source of bucket in the form nats.go builds it from: a
// source with the subject transform nats.go adds, or none where it adds
// none, is named by its bucket without the transform; any other source is
// left as it is, since nats.go passes it through.
func bucketSource(s sourceWire, bucket string) sourceWire {
	from := strings.TrimPrefix(s.Name, kvStreamPrefix)
	var added []transformWire
	if s.External == nil || from != bucket {
		added = []transformWire{{Source: kvSubjects(from), Destination: kvSubjects(bucket)}}
	}
	if len(s.SubjectTransforms) > 0 && !slices.Equal(s.SubjectTransforms, added) {
		return s
	}
	s.Name = from
	s.SubjectTransforms = nil
	return s
}

func positive(n *int64) *int64 {
	if n == nil || *n <= 0 {
		return nil
	}
	return n
}

func compressed(c *string) *bool {
	if c == nil || *c == "" || *c == "none" {
		return nil
	}
	return ptrTo(true)
}
