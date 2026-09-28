// Command e2e runs the story bundles end to end on kind clusters over
// rootful podman, and prints a per-story result table; it exits 1 when any
// story fails. Run from the repository root:
//
//	go run ./hack/e2e          create or reuse the clusters, run the stories
//	go run ./hack/e2e -down    delete the clusters
//
//	E2E_MACHINE           the darwin container machine              nats-operator-e2e
//	E2E_DNS               nameserver the machine resolves by        9.9.9.9
//	E2E_CLUSTER           home kind cluster; the others are         nats-operator-e2e
//	                      named <cluster>-2, <cluster>-3
//	E2E_CLUSTERS          how many Kubernetes clusters              2
//	E2E_CONTROLLERS       controllers the home chart enables        cluster auth jetstream
//	E2E_PEER_CONTROLLERS  controllers the others' chart enables     cluster jetstream
//	E2E_STORIES           comma-separated story numbers             all
//	E2E_WAIT              a step's wait, where its story sets none  90s
//	E2E_WATCH_NAMESPACES  true installs the chart with               false
//	                      watchNamespaces, the stories' namespaces
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// config is the run the E2E_* variables describe.
type config struct {
	machine         string
	dns             string
	cluster         string
	clusters        int
	controllers     []string
	peerControllers []string
	stories         string
	wait            time.Duration
	watchNamespaces bool
}

// loadConfig reads config from getenv, each unset or empty variable taking
// its default.
func loadConfig(getenv func(string) string) (config, error) {
	get := func(k, def string) string {
		if v := getenv(k); v != "" {
			return v
		}
		return def
	}
	c := config{
		machine:         get("E2E_MACHINE", "nats-operator-e2e"),
		dns:             get("E2E_DNS", "9.9.9.9"),
		cluster:         get("E2E_CLUSTER", "nats-operator-e2e"),
		controllers:     strings.Fields(get("E2E_CONTROLLERS", "cluster auth jetstream")),
		peerControllers: strings.Fields(get("E2E_PEER_CONTROLLERS", "cluster jetstream")),
		stories:         getenv("E2E_STORIES"),
	}
	var err error
	if c.clusters, err = strconv.Atoi(get("E2E_CLUSTERS", "2")); err != nil || c.clusters < 1 {
		return c, fmt.Errorf("E2E_CLUSTERS %q is not a positive number", getenv("E2E_CLUSTERS"))
	}
	if c.wait, err = time.ParseDuration(get("E2E_WAIT", "90s")); err != nil || c.wait <= 0 {
		return c, fmt.Errorf("E2E_WAIT %q is not a positive duration", getenv("E2E_WAIT"))
	}
	if c.watchNamespaces, err = strconv.ParseBool(get("E2E_WATCH_NAMESPACES", "false")); err != nil {
		return c, fmt.Errorf("E2E_WATCH_NAMESPACES %q is not a boolean", getenv("E2E_WATCH_NAMESPACES"))
	}
	return c, nil
}

// clusterNames are the kind clusters, the home cluster first.
func (c config) clusterNames() []string {
	names := []string{c.cluster}
	for i := 2; i <= c.clusters; i++ {
		names = append(names, fmt.Sprintf("%s-%d", c.cluster, i))
	}
	return names
}

func main() {
	down := flag.Bool("down", false, "delete the kind clusters instead of running the stories")
	images := flag.String("images", "", "images.json of controller images built elsewhere; empty builds them with ko")
	flag.Parse()

	if err := run(*down, *images); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "e2e:", err)
		os.Exit(1)
	}
}

func run(down bool, images string) error {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	switch runtime.GOOS {
	case "darwin":
		return viaMachine(ctx, cfg, root, down)
	case "linux":
		return harness(ctx, cfg, root, images, down)
	default:
		return fmt.Errorf("no e2e harness on %s", runtime.GOOS)
	}
}

func logf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "==> "+format+"\n", args...)
}
