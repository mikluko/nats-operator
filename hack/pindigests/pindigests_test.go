package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

const registry = "ghcr.io/mikluko/nats-operator"

func digest(c byte) string { return "sha256:" + strings.Repeat(string(c), 64) }

func released() []image {
	return []image{
		{registry + "/cluster-controller", digest('a')},
		{registry + "/auth-controller", digest('b')},
		{registry + "/jetstream-controller", digest('c')},
	}
}

// TestPin_ChartValues holds pinning the chart's values file to changing its
// three controller digest lines and no other byte.
func TestPin_ChartValues(t *testing.T) {
	src, err := os.ReadFile("../../charts/nats-operator/values.yaml")
	require.NoError(t, err)
	out, err := pin(src, registry, released())
	require.NoError(t, err)

	before := bytes.SplitAfter(src, []byte("\n"))
	after := bytes.SplitAfter(out, []byte("\n"))
	require.Len(t, after, len(before))
	var changed []string
	for i := range before {
		if !bytes.Equal(before[i], after[i]) {
			changed = append(changed, string(after[i]))
		}
	}
	require.Equal(t, []string{
		`    digest: "` + digest('a') + "\"\n",
		`    digest: "` + digest('b') + "\"\n",
		`    digest: "` + digest('c') + "\"\n",
	}, changed)

	type controller struct {
		Image struct{ Digest string } `yaml:"image"`
	}
	var values struct {
		Cluster   controller `yaml:"cluster"`
		Auth      controller `yaml:"auth"`
		JetStream controller `yaml:"jetstream"`
	}
	require.NoError(t, yaml.Unmarshal(out, &values))
	require.Equal(t, digest('a'), values.Cluster.Image.Digest)
	require.Equal(t, digest('b'), values.Auth.Image.Digest)
	require.Equal(t, digest('c'), values.JetStream.Image.Digest)
}

func TestPin_Refuses(t *testing.T) {
	const values = `a:
  image:
    repository: ` + registry + `/a-controller
    digest: ""
other:
  image:
    repository: example.com/other
    digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
`
	a := image{registry + "/a-controller", digest('a')}
	for _, tc := range []struct {
		name   string
		values string
		images []image
		err    string
	}{
		{"unlisted image", values, nil, "no digest for " + registry + "/a-controller"},
		{"listed image nothing pulls", values, []image{a, {registry + "/b-controller", digest('b')}}, "no image pulls " + registry + "/b-controller"},
		{"not a sha256 digest", values, []image{{a.Name, "sha256:abc"}}, "is not sha256:<hex>"},
		{"digest already set", strings.Replace(values, `digest: ""`, `digest: "`+digest('d')+`"`, 1), []image{a}, `image.digest is not ""`},
		{"digest missing", strings.Replace(values, "    digest: \"\"\n", "", 1), []image{a}, `image.digest is not ""`},
		{"digest unquoted", strings.Replace(values, `digest: ""`, `digest:`, 1), []image{a}, `image.digest is not ""`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pin([]byte(tc.values), registry, tc.images)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

// TestPin_Column holds the rewrite to the scalar yaml reports, past a
// multi-byte rune earlier on its line.
func TestPin_Column(t *testing.T) {
	src := "a: {image: {repository: " + registry + "/a-controller, x: \"é\", digest: \"\"}}\n"
	out, err := pin([]byte(src), registry, []image{{registry + "/a-controller", digest('a')}})
	require.NoError(t, err)
	require.Equal(t, strings.Replace(src, `digest: ""`, `digest: "`+digest('a')+`"`, 1), string(out))
}
