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

# Build every controller image for platform with ko, pushing nothing, and
# print their references.
image platform=("linux/" + if arch() == "aarch64" { "arm64" } else { "amd64" }):
    #!/usr/bin/env sh
    set -eu
    set --
    for c in {{ controllers }}; do
        set -- "$@" "./cmd/$c"
    done
    KO_DOCKER_REPO=ghcr.io/mikluko/nats-operator ko build --push=false -B --platform "{{ platform }}" "$@"

# The OperatorReload tests skip under -race and run again without it.
test:
    go test -race ./...
    go test -run OperatorReload ./internal/natscluster

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

# The chart's crds/ is a copy of config/crd.
chart-crds:
    rm -f charts/nats-operator/crds/*.yaml
    mkdir -p charts/nats-operator/crds
    cp config/crd/*.yaml charts/nats-operator/crds/

# helm lint, and helm template for each single-controller subset and all three.
chart:
    #!/usr/bin/env sh
    set -eu
    helm lint --strict charts/nats-operator
    for sets in \
        "cluster.enabled=true,auth.enabled=false,jetstream.enabled=false" \
        "cluster.enabled=false,auth.enabled=true,jetstream.enabled=false" \
        "cluster.enabled=false,auth.enabled=false,jetstream.enabled=true" \
        "cluster.enabled=true,auth.enabled=true,jetstream.enabled=true"; do
        helm template nats-operator charts/nats-operator --set "$sets" > /dev/null
    done

# The API reference page, from the Go types under api/ and the templates
# under hack/api-docs/.
api-docs:
    go tool gen-crd-api-reference-docs \
        -api-dir=github.com/mikluko/nats-operator/api \
        -config=hack/api-docs/config.json \
        -template-dir=hack/api-docs/template \
        -out-file=docs/content/docs/reference/api.md

# The documentation site's NATS permissions page, from hack/permdocs and the
# presets in internal/jwtplane.
perm-docs:
    go run ./hack/permdocs

# Fails when generated files, tracked or not, are stale relative to their
# sources.
verify: generate chart-crds api-docs perm-docs
    git diff --exit-code -- api config charts docs/content/docs/reference
    test -z "$(git status --porcelain -- api config charts docs/content/docs/reference)"

# envtest binaries for the API-server-backed tests; the tests themselves are
# ordinary `go test` runs that read KUBEBUILDER_ASSETS.
envtest:
    #!/usr/bin/env sh
    set -eu
    mkdir -p "{{ bin }}"
    KUBEBUILDER_ASSETS="$(go tool setup-envtest use {{ envtest_k8s_version }} --bin-dir "{{ bin }}" -p path)" \
        go test -race ./... -run Envtest

# The story bundles end to end on minikube inside an Apple `container`
# machine; hack/e2e.sh lists the E2E_* variables it reads.
e2e:
    hack/e2e.sh

clean:
    rm -rf "{{ bin }}"
