controllers := "cluster-controller auth-controller jetstream-controller"
envtest_k8s_version := env("ENVTEST_K8S_VERSION", "1.36.x")
bin := justfile_directory() / "bin"

default: generate build test lint

# Build every controller binary into bin/.
build:
    #!/usr/bin/env sh
    set -eu
    for c in {{ controllers }}; do
        CGO_ENABLED=0 go build -o "{{ bin }}/$c" "./cmd/$c"
    done

test:
    go test -race ./...

lint:
    golangci-lint run

fmt:
    gofmt -s -w .

tidy:
    go mod tidy

# controller-gen from the tool directive in go.mod: deep-copy methods for
# every api/ package and CRDs into config/crd.
generate:
    go tool controller-gen object paths=./api/...
    go tool controller-gen crd paths=./api/... output:crd:artifacts:config=config/crd

# Fails when generated files are stale relative to their sources.
verify: generate
    git diff --exit-code -- api config

# envtest binaries for the API-server-backed tests; the tests themselves are
# ordinary `go test` runs that read KUBEBUILDER_ASSETS.
envtest:
    #!/usr/bin/env sh
    set -eu
    mkdir -p "{{ bin }}"
    KUBEBUILDER_ASSETS="$(go tool setup-envtest use {{ envtest_k8s_version }} --bin-dir "{{ bin }}" -p path)" \
        go test -race ./... -run Envtest

# The end-to-end run against a Kubernetes cluster on Apple containers; the
# script lands with the harness, and this recipe names it so `just e2e` is
# the one command from the start.
e2e:
    #!/usr/bin/env sh
    set -eu
    if [ ! -x hack/e2e.sh ]; then
        echo "hack/e2e.sh is not present; the e2e harness has not landed" >&2
        exit 1
    fi
    hack/e2e.sh

clean:
    rm -rf "{{ bin }}"
