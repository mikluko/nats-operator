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
// namespace may take to delete, its servers lame-ducking for two minutes;
// how long it keeps the controllers' finalizers once its last Pod is gone;
// and how often a waiting step logs its diff.
const (
	teardown     = 5 * time.Minute
	releaseAfter = time.Minute
	report       = 15 * time.Second
)

// selectBundles returns the story bundles under root numbered in only, all
// when it is empty; it fails where none is.
func selectBundles(root, only string) ([]*e2e.Bundle, error) {
	selected, err := parseNumbers(only)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(root, "docs", "content", "docs", "stories")
	bundles, err := e2e.LoadBundles(dir)
	if err != nil {
		return nil, err
	}
	var out []*e2e.Bundle
	for _, b := range bundles {
		if len(selected) == 0 || selected[b.Number] {
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no story in %s matches %q", dir, only)
	}
	return out, nil
}

// runStories runs bundles through clients, each step waiting wait unless
// its story sets its own, fresh running as Runner.Fresh, and prints the
// result table.
func runStories(ctx context.Context, clients []client.Client, bundles []*e2e.Bundle, wait time.Duration, fresh func(context.Context, int) error) error {
	r := &e2e.Runner{
		Clients: clients, Timeout: wait, Teardown: teardown, Release: releaseAfter, Interval: 2 * time.Second, Report: report,
		Namespaces: []string{releaseNS}, Log: os.Stderr, Publish: e2e.PublishHosts, Fresh: fresh,
	}
	var results []e2e.Result
	failed := false
	for _, b := range bundles {
		res := r.Run(ctx, b)
		failed = failed || res.Outcome == e2e.Fail
		results = append(results, res)
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
