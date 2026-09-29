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
	"github.com/mikluko/nats-operator/internal/e2e/fixtures"
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

// generateFixtures writes the stories' generated fixtures, and the metrics
// fixtures, afresh under work/fixtures, and returns that directory.
func generateFixtures(work string) (string, error) {
	dir := filepath.Join(work, "fixtures")
	if err := os.RemoveAll(dir); err != nil {
		return "", err
	}
	if err := fixtures.Generate(dir); err != nil {
		return "", err
	}
	return dir, generateMetricsFixtures(dir)
}

// selectBundles returns the story bundles under root, with the fixtures
// under generated laid over them, numbered in only, all when it is empty;
// it fails where none is.
func selectBundles(root, generated, only string) ([]*e2e.Bundle, error) {
	selected, err := parseNumbers(only)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(root, "docs", "content", "docs", "stories")
	bundles, err := e2e.LoadBundles(dir, generated)
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
// its story sets its own, each story under the chart ci installs for it and
// checked by it once passed, and prints the result table.
func runStories(ctx context.Context, clients []client.Client, bundles []*e2e.Bundle, wait time.Duration, ci *chartInstall) error {
	r := &e2e.Runner{
		Clients: clients, Timeout: wait, Teardown: teardown, Release: releaseAfter, Interval: 2 * time.Second, Report: report,
		Namespaces: []string{releaseNS}, Log: os.Stderr, Publish: e2e.PublishHosts,
	}
	var results []e2e.Result
	failed := false
	for _, b := range bundles {
		res := runStory(ctx, r, clients[0], b, ci)
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

// runStory runs b through r under the chart ci installs for it, and fails
// it where ci's check after it fails; home reaches the home cluster.
func runStory(ctx context.Context, r *e2e.Runner, home client.Client, b *e2e.Bundle, ci *chartInstall) e2e.Result {
	start := time.Now()
	fresh, err := ci.before(ctx, b)
	if err != nil {
		return e2e.Result{Story: b.Name, Outcome: e2e.Fail, Elapsed: time.Since(start).Round(time.Second), Detail: fmt.Sprintf("chart: %v", err)}
	}
	r.Fresh = fresh
	res := r.Run(ctx, b)
	if res.Outcome != e2e.Pass {
		return res
	}
	if err := ci.after(ctx, home, b); err != nil {
		res.Outcome, res.Detail = e2e.Fail, err.Error()
	}
	res.Elapsed = time.Since(start).Round(time.Second)
	return res
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
