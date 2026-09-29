package e2e

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadChartValues(t *testing.T) {
	const front = "---\ntitle: T\nparams:\n  e2e:\n    scrapeMetrics: true\n---\n\n"
	for desc, tc := range map[string]struct {
		page string
		want map[string]any
		err  string
	}{
		"one block": {
			page: front + "## The certificate\n\n```yaml\nkind: Certificate\n```\n\n## The chart\n\n```yaml\nmetrics:\n  service:\n    enabled: true\n```\n\n```sh\nhelm install\n```\n\n## After\n\n```yaml\nother: 1\n```\n",
			want: map[string]any{"metrics": map[string]any{"service": map[string]any{"enabled": true}}},
		},
		"no section": {page: front + "## Other\n\n```yaml\na: 1\n```\n", err: `want one yaml code block under "The chart", got 0`},
		"two blocks": {page: front + "## The chart\n\n```yaml\na: 1\n```\n\n```yaml\nb: 1\n```\n", err: "got 2"},
		"not yaml":   {page: front + "## The chart\n\n```yaml\na: [\n```\n", err: "The chart"},
	} {
		t.Run(desc, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "index.md")
			require.NoError(t, os.WriteFile(path, []byte(tc.page), 0o644))
			got, err := readChartValues(path)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
