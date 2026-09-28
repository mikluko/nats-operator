package lifecycle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
)

// Config is a server object's config as the JetStream API carries it in
// JSON, numbers kept as json.Number so 64-bit values survive a round trip.
// Keys a kind does not model are carried through untouched.
type Config map[string]any

// metadataKey is the config key of the metadata map in every JetStream
// object config.
const metadataKey = "metadata"

// DecodeConfig decodes a JSON object into a Config.
func DecodeConfig(raw []byte) (Config, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var c Config
	if err := d.Decode(&c); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	return c, nil
}

// ToConfig encodes v, a struct with the server's JSON tags, into a Config.
func ToConfig(v any) (Config, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode config: %w", err)
	}
	return DecodeConfig(raw)
}

// FromConfig decodes c into v, a struct with the server's JSON tags.
func FromConfig(c Config, v any) error {
	raw, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	return nil
}

// Metadata returns c's metadata map, nil when it has none.
func (c Config) Metadata() map[string]string {
	m, _ := c[metadataKey].(map[string]any)
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// UserMetadata returns c's metadata without the ownership marker and
// without the keys nats-server reserves, nil when nothing is left.
func (c Config) UserMetadata() map[string]string {
	return userMetadata(c.Metadata())
}

// Overlay returns the config that applies desired to server, marked m:
// server's keys with each key of desired replaced, and a key desired maps to
// nil removed. The metadata is desired's without the marker and the
// reserved keys, or server's likewise when desired has none, plus m.
// Neither argument is modified; server may be nil.
func Overlay(server, desired Config, m Marker) Config {
	out := make(Config, len(server)+len(desired))
	for k, v := range server {
		out[k] = v
	}
	for k, v := range desired {
		if v == nil {
			delete(out, k)
			continue
		}
		out[k] = v
	}
	user := server.UserMetadata()
	if _, ok := desired[metadataKey]; ok {
		user = desired.UserMetadata()
	}
	meta := map[string]any{}
	for k, v := range withMarker(user, m) {
		meta[k] = v
	}
	out[metadataKey] = meta
	return out
}

// Drift returns, sorted, the keys server and want differ in. A key want
// lacks or maps to nil matches when server's value is absent or zero;
// metadata matches when both carry the same entries outside the reserved keys;
// any other value matches when server's holds it, a JSON object holding
// every key of want's and an absent value holding a zero one.
func Drift(server, want Config) []string {
	keys := map[string]bool{}
	for k := range want {
		keys[k] = true
	}
	for k := range server {
		keys[k] = true
	}
	return Changed(server, want, slices.Collect(maps.Keys(keys))...)
}

// Changed returns, sorted, those of keys server and want differ in, as
// Drift judges them.
func Changed(server, want Config, keys ...string) []string {
	var out []string
	for _, k := range keys {
		s := server[k]
		w, has := want[k]
		var ok bool
		switch {
		case !has || w == nil:
			ok = isZero(s)
		case k == metadataKey:
			ok = reflect.DeepEqual(unreserved(want.Metadata()), unreserved(server.Metadata()))
		default:
			ok = holds(s, w)
		}
		if !ok {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

func unreserved(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		if !strings.HasPrefix(k, reservedPrefix) {
			out[k] = v
		}
	}
	return out
}

// holds reports whether got holds want, per Drift.
func holds(got, want any) bool {
	if got == nil {
		return isZero(want)
	}
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range w {
			if !holds(g[k], v) {
				return false
			}
		}
		return true
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for i := range w {
			if !holds(g[i], w[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(got, want)
	}
}

func isZero(v any) bool {
	switch v := v.(type) {
	case nil:
		return true
	case bool:
		return !v
	case string:
		return v == ""
	case json.Number:
		return v == "0"
	case []any:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	}
	return false
}

// FillOmitted copies into dst every top-level JSON field src sets that dst
// omits, and reports whether it copied any.
func FillOmitted[T any](dst, src *T) (bool, error) {
	d, err := jsonObject(dst)
	if err != nil {
		return false, err
	}
	s, err := jsonObject(src)
	if err != nil {
		return false, err
	}
	var changed bool
	for k, v := range s {
		if _, ok := d[k]; !ok {
			d[k] = v
			changed = true
		}
	}
	if !changed {
		return false, nil
	}
	raw, err := json.Marshal(d)
	if err != nil {
		return false, fmt.Errorf("encode spec: %w", err)
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		return false, fmt.Errorf("decode spec: %w", err)
	}
	*dst = out
	return true, nil
}

func jsonObject(v any) (map[string]json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode spec: %w", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("decode spec: %w", err)
	}
	return m, nil
}
