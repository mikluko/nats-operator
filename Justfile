# The controllers are the commands under cmd/ named *-controller.
controllers := `cd cmd && echo *-controller`
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
# every api/ package, CRDs into config/crd, and each controller's ClusterRole
# into config/rbac/<controller> from the markers hack/rbac names.
generate:
    go tool controller-gen object paths=./api/...
    go tool controller-gen crd paths=./api/... output:crd:artifacts:config=config/crd
    go run ./hack/rbac

# The chart's crds/ is a copy of config/crd.
chart-crds:
    rm -f charts/nats-operator/crds/*.yaml
    mkdir -p charts/nats-operator/crds
    cp config/crd/*.yaml charts/nats-operator/crds/

# The chart's files/rbac/<controller>.yaml is a copy of
# config/rbac/<controller>/role.yaml.
chart-rbac:
    #!/usr/bin/env sh
    set -eu
    rm -f charts/nats-operator/files/rbac/*.yaml
    mkdir -p charts/nats-operator/files/rbac
    for c in {{ controllers }}; do
        cp "config/rbac/$c/role.yaml" "charts/nats-operator/files/rbac/$c.yaml"
    done

# helm lint under the defaults and each chart-testing values file, a lint that
# must fail on a misspelled key, and the chart's helm-unittest suites.
chart:
    #!/usr/bin/env sh
    set -eu
    helm lint --strict charts/nats-operator
    for f in charts/nats-operator/ci/*-values.yaml; do
        helm lint --strict charts/nats-operator -f "$f"
    done
    if helm lint --strict charts/nats-operator --set cluster.enabeld=true >/dev/null 2>&1; then
        echo "helm lint accepted the misspelled key cluster.enabeld" >&2
        exit 1
    fi
    helm unittest charts/nats-operator

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

# The documentation site's telemetry page, from hack/telemetrydocs and the
# instruments and events in internal/telemetry.
telemetry-docs:
    go run ./hack/telemetrydocs

# Fails when generated files, tracked or not, are stale relative to their
# sources.
verify: generate chart-crds chart-rbac api-docs perm-docs telemetry-docs
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
