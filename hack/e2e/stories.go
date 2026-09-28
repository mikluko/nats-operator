package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mikluko/nats-operator/internal/e2e"
)

// Runner timings besides the per-step wait: how long a previous story's
// namespace may take to delete, its servers lame-ducking for two minutes,
// and how often a waiting step logs its diff.
const (
	teardown = 5 * time.Minute
	report   = 15 * time.Second
)

// runStories runs the story bundles under root numbered in only, all when
// it is empty, through clients, each step waiting wait unless its story
// sets its own, and prints the result table.
func runStories(ctx context.Context, root string, clients []client.Client, only string, wait time.Duration) error {
	selected, err := parseNumbers(only)
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "docs", "content", "docs", "stories")
	bundles, err := e2e.LoadBundles(dir)
	if err != nil {
		return err
	}
	r := &e2e.Runner{
		Clients: clients, Timeout: wait, Teardown: teardown, Interval: 2 * time.Second, Report: report,
		Namespaces: []string{releaseNS}, Log: os.Stderr, Publish: e2e.PublishHosts,
	}
	var results []e2e.Result
	failed := false
	for _, b := range bundles {
		if len(selected) > 0 && !selected[b.Number] {
			continue
		}
		res := r.Run(ctx, b)
		failed = failed || res.Outcome == e2e.Fail
		results = append(results, res)
	}
	if len(results) == 0 {
		return fmt.Errorf("no story in %s matches %q", dir, only)
	}
	fmt.Println()
	if err := e2e.WriteTable(os.Stdout, results); err != nil {
		return err
	}
	if failed {
		return errors.New("stories failed")
	}
	return nil
}

func parseNumbers(s string) (map[int]bool, error) {
	out := map[int]bool{}
	for f := range strings.SplitSeq(s, ",") {
		if f = strings.TrimSpace(f); f == "" {
			continue
		}
		n, err := strconv.Atoi(f)
		if err != nil {
			return nil, fmt.Errorf("story number %q: %w", f, err)
		}
		out[n] = true
	}
	return out, nil
}
