package placeholders

import (
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestStrip(t *testing.T) {
	raw := []byte(`# comment
status:
  messages: !any 18204
  created: !any "2026-09-25T15:58:39Z"
  updated: !any [demo-2]
  ready: !any true
  name: demo-0
`)
	stripped, err := Strip(raw)
	require.NoError(t, err)
	var doc struct {
		Status struct {
			Messages int64    `json:"messages"`
			Created  string   `json:"created"`
			Updated  []string `json:"updated"`
			Ready    bool     `json:"ready"`
			Name     string   `json:"name"`
		} `json:"status"`
	}
	require.NoError(t, yaml.UnmarshalStrict(stripped, &doc))
	require.Equal(t, int64(18204), doc.Status.Messages)
	require.Equal(t, "2026-09-25T15:58:39Z", doc.Status.Created)
	require.Equal(t, []string{"demo-2"}, doc.Status.Updated)
	require.True(t, doc.Status.Ready)
	require.Equal(t, "demo-0", doc.Status.Name)
}
