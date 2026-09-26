// Command e2e runs the story bundles against the Kubernetes cluster the
// current kubeconfig names and prints a per-story result table. It exits 1
// when any story fails.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/mikluko/nats-operator/internal/e2e"
)

// multiCluster are the stories that need more than one Kubernetes cluster.
var multiCluster = []int{6, 9, 10}

func main() {
	stories := flag.String("stories", "docs/content/stories", "directory holding the story bundles")
	only := flag.String("only", "", "comma-separated story numbers to run; empty runs all")
	timeout := flag.Duration("timeout", 5*time.Minute, "how long each story waits for its statuses")
	interval := flag.Duration("interval", 2*time.Second, "how often statuses are read")
	flag.Parse()

	if err := run(*stories, *only, *timeout, *interval); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "e2e:", err)
		os.Exit(1)
	}
}

func run(dir, only string, timeout, interval time.Duration) error {
	selected, err := parseNumbers(only)
	if err != nil {
		return err
	}
	bundles, err := e2e.LoadBundles(dir)
	if err != nil {
		return err
	}
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		return err
	}
	skip := map[int]string{}
	for _, n := range multiCluster {
		skip[n] = "needs more than one Kubernetes cluster"
	}
	r := &e2e.Runner{Client: c, Timeout: timeout, Interval: interval, Skip: skip, Log: os.Stderr}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
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
		return fmt.Errorf("stories failed")
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
