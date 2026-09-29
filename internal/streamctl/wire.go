package streamctl

import (
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// enum maps an API enum to its nats-server JSON spelling.
type enum[T ~string] map[T]string

// wire returns v's server spelling, nil when v is nil or unknown.
func (e enum[T]) wire(v *T) *string {
	if v == nil {
		return nil
	}
	s, ok := e[*v]
	if !ok {
		return nil
	}
	return &s
}

// api returns the API value spelled s on the wire, nil when s is nil or
// unknown.
func (e enum[T]) api(s *string) *T {
	if s == nil {
		return nil
	}
	for k, v := range e {
		if v == *s {
			return &k
		}
	}
	return nil
}

func durationWire(d *metav1.Duration) *int64 {
	if d == nil {
		return nil
	}
	n := int64(d.Duration)
	return &n
}

// durationAPI returns nil for a zero duration, as every *API helper does for
// a zero value.
func durationAPI(n *int64) *metav1.Duration {
	if n == nil || *n == 0 {
		return nil
	}
	return &metav1.Duration{Duration: time.Duration(*n)}
}

func quantityWire(q *resource.Quantity) *int64 {
	if q == nil {
		return nil
	}
	n := q.Value()
	return &n
}

func quantityAPI(n *int64) *resource.Quantity {
	if n == nil || *n == 0 {
		return nil
	}
	return resource.NewQuantity(*n, resource.BinarySI)
}

func timeWire(t *metav1.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func timeAPI(t *time.Time) *metav1.Time {
	if t == nil || t.IsZero() {
		return nil
	}
	m := metav1.NewTime(*t)
	return &m
}

func nonZero[T comparable](v *T) *T {
	var zero T
	if v == nil || *v == zero {
		return nil
	}
	return v
}

func int32Wire(v *int32) *int64 {
	if v == nil {
		return nil
	}
	return ptr.To(int64(*v))
}

func int32API(v *int64) *int32 {
	if v == nil || *v == 0 {
		return nil
	}
	return ptr.To(int32(*v)) //nolint:gosec // replica counts are bounded by the schema.
}

func durationsWire(ds []metav1.Duration) []int64 {
	if ds == nil {
		return nil
	}
	out := make([]int64, len(ds))
	for i, d := range ds {
		out[i] = int64(d.Duration)
	}
	return out
}

func durationsAPI(ns []int64) []metav1.Duration {
	if len(ns) == 0 {
		return nil
	}
	out := make([]metav1.Duration, len(ns))
	for i, n := range ns {
		out[i] = metav1.Duration{Duration: time.Duration(n)}
	}
	return out
}
