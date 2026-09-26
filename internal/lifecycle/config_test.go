package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	jetstreamv1beta1 "github.com/mikluko/nats-operator/api/jetstream/v1beta1"
)

func config(t *testing.T, raw string) Config {
	t.Helper()
	c, err := DecodeConfig([]byte(raw))
	require.NoError(t, err)
	return c
}

func TestReadMarker(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta map[string]string
		want Marker
		ok   bool
	}{
		{"none", nil, Marker{}, false},
		{"created", map[string]string{OwnerKey: "u1", OriginKey: "Created"}, Marker{UID: "u1", Origin: jetstreamv1beta1.OwnershipCreated}, true},
		{"adopted", map[string]string{OwnerKey: "u1", OriginKey: "Adopted"}, Marker{UID: "u1", Origin: jetstreamv1beta1.OwnershipAdopted}, true},
		{"origin missing", map[string]string{OwnerKey: "u1"}, Marker{UID: "u1", Origin: jetstreamv1beta1.OwnershipCreated}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := ReadMarker(tc.meta)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, m)
		})
	}
}

func TestOverlay(t *testing.T) {
	m := Marker{UID: types.UID("u1"), Origin: jetstreamv1beta1.OwnershipCreated}
	marker := map[string]any{OwnerKey: "u1", OriginKey: "Created"}
	for _, tc := range []struct {
		name            string
		server, desired string
		want            map[string]any
	}{
		{
			name:    "create",
			desired: `{"name":"S","max_age":1}`,
			want:    map[string]any{"name": "S", "max_age": json.Number("1"), "metadata": marker},
		},
		{
			name:    "spec keys replace, others stay",
			server:  `{"name":"S","max_age":5,"max_msgs":-1,"unmodelled":true}`,
			desired: `{"name":"S","max_age":1}`,
			want:    map[string]any{"name": "S", "max_age": json.Number("1"), "max_msgs": json.Number("-1"), "unmodelled": true, "metadata": marker},
		},
		{
			name:    "nil removes",
			server:  `{"name":"C","deliver_subject":"d"}`,
			desired: `{"name":"C","deliver_subject":null}`,
			want:    map[string]any{"name": "C", "metadata": marker},
		},
		{
			name:    "server metadata kept when spec has none, reserved and foreign marker dropped",
			server:  `{"name":"S","metadata":{"team":"a","_nats.req.level":"0",` + `"` + OwnerKey + `":"old"}}`,
			desired: `{"name":"S"}`,
			want:    map[string]any{"name": "S", "metadata": map[string]any{"team": "a", OwnerKey: "u1", OriginKey: "Created"}},
		},
		{
			name:    "spec metadata replaces the server's",
			server:  `{"name":"S","metadata":{"team":"a","other":"x"}}`,
			desired: `{"name":"S","metadata":{"team":"b"}}`,
			want:    map[string]any{"name": "S", "metadata": map[string]any{"team": "b", OwnerKey: "u1", OriginKey: "Created"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var server Config
			if tc.server != "" {
				server = config(t, tc.server)
			}
			require.Equal(t, Config(tc.want), Overlay(server, config(t, tc.desired), m))
		})
	}
}

func TestDrift(t *testing.T) {
	for _, tc := range []struct {
		name         string
		server, want string
		drift        []string
	}{
		{"equal", `{"max_age":1}`, `{"max_age":1}`, nil},
		{"scalar", `{"max_age":1}`, `{"max_age":2}`, []string{"max_age"}},
		{"server-only zero", `{"max_age":1,"sealed":false}`, `{"max_age":1}`, nil},
		{"server-only value", `{"max_age":1,"description":"x"}`, `{"max_age":1}`, []string{"description"}},
		{"want nil, server absent", `{}`, `{"deliver_subject":null}`, nil},
		{"want nil, server set", `{"deliver_subject":"d"}`, `{"deliver_subject":null}`, []string{"deliver_subject"}},
		{"absent equals zero", `{}`, `{"flow_control":false}`, nil},
		{"object holds a subset", `{"placement":{"cluster":"c","tags":[]}}`, `{"placement":{"cluster":"c"}}`, nil},
		{"object differs", `{"placement":{"cluster":"c"}}`, `{"placement":{"cluster":"d"}}`, []string{"placement"}},
		{"list length", `{"subjects":["a"]}`, `{"subjects":["a","b"]}`, []string{"subjects"}},
		{"list of objects", `{"sources":[{"name":"a","external":null}]}`, `{"sources":[{"name":"a"}]}`, nil},
		{"large numbers", `{"max_bytes":9223372036854775807}`, `{"max_bytes":9223372036854775807}`, nil},
		{"metadata reserved ignored", `{"metadata":{"a":"1","_nats.ver":"2.15.0"}}`, `{"metadata":{"a":"1"}}`, nil},
		{"metadata extra key", `{"metadata":{"a":"1","b":"2"}}`, `{"metadata":{"a":"1"}}`, []string{"metadata"}},
		{"sorted", `{"b":1,"a":1}`, `{"b":2,"a":2}`, []string{"a", "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.drift, Drift(config(t, tc.server), config(t, tc.want)))
		})
	}
}

func TestChangedLimitsKeys(t *testing.T) {
	server := config(t, `{"deliver_policy":"all","max_deliver":1}`)
	want := config(t, `{"deliver_policy":"new","max_deliver":2}`)
	require.Equal(t, []string{"deliver_policy"}, Changed(server, want, "deliver_policy", "ack_policy"))
}

func TestFillOmitted(t *testing.T) {
	type spec struct {
		A *int    `json:"a,omitempty"`
		B *int    `json:"b,omitempty"`
		C *string `json:"c,omitempty"`
	}
	one, two, three := 1, 2, 3
	dst := spec{A: &one}
	changed, err := FillOmitted(&dst, &spec{A: &two, B: &three})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, 1, *dst.A, "set fields stay")
	require.Equal(t, 3, *dst.B)
	require.Nil(t, dst.C)

	changed, err = FillOmitted(&dst, &spec{A: &two})
	require.NoError(t, err)
	require.False(t, changed)
}
