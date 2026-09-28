controllers := `cd cmd && echo *-controller`
envtest_k8s_version := env("ENVTEST_K8S_VERSION", "1.36.x")
bin := justfile_directory() / "bin"
lychee_version := "0.24.2"

default: generate build test lint

build:
    #!/usr/bin/env sh
    set -eu
    for c in {{ controllers }}; do
        CGO_ENABLED=0 go build -o "{{ bin }}/$c" "./cmd/$c"
    done

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

generate:
    go tool controller-gen object paths=./api/...
    go tool controller-gen crd paths=./api/... output:crd:artifacts:config=config/crd
    go run ./hack/rbac

chart-crds:
    rm -f charts/nats-operator/crds/*.yaml
    mkdir -p charts/nats-operator/crds
    cp config/crd/*.yaml charts/nats-operator/crds/

chart-rbac:
    #!/usr/bin/env sh
    set -eu
    rm -f charts/nats-operator/files/rbac/*.yaml
    mkdir -p charts/nats-operator/files/rbac
    for c in {{ controllers }}; do
        cp "config/rbac/$c/role.yaml" "charts/nats-operator/files/rbac/$c.yaml"
    done

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

api-docs:
    go tool crd-ref-docs \
        --source-path=api \
        --config=hack/api-docs/config.yaml \
        --renderer=markdown \
        --log-level=ERROR \
        --templates-dir=hack/api-docs/template \
        --output-path=docs/content/docs/reference/api.md

perm-docs:
    go run ./hack/permdocs

telemetry-docs:
    go run ./hack/telemetrydocs

# bin/lychee at lychee_version; the checksums are the release archives'.
lychee:
    #!/usr/bin/env sh
    set -eu
    if [ "$("{{ bin }}/lychee" --version 2>/dev/null)" = "lychee {{ lychee_version }}" ]; then
        exit 0
    fi
    case "{{ os() }}-{{ arch() }}" in
        macos-aarch64) target=aarch64-apple-darwin sum=c9d3740ea2d891854d37116c9fba840f37b6e7c89d330e7db84ac333631c4977 ;;
        linux-x86_64) target=x86_64-unknown-linux-gnu sum=1f4e0ef7f6554a6ed33dd7ac144fb2e1bbed98598e7af973042fc5cd43951c9a ;;
        linux-aarch64) target=aarch64-unknown-linux-gnu sum=91a7bd65685da41b90ccb9bc867a3d649a7818042dae04ff405e55a25bddee4c ;;
        *) echo "no pinned lychee build for {{ os() }}-{{ arch() }}" >&2; exit 1 ;;
    esac
    tmp="$(mktemp -d "${TMPDIR:-/tmp}/lychee.XXXXXX")"
    trap 'rm -rf "$tmp"' EXIT
    curl -fsSLo "$tmp/lychee.tar.gz" \
        "https://github.com/lycheeverse/lychee/releases/download/lychee-v{{ lychee_version }}/lychee-$target.tar.gz"
    echo "$sum  $tmp/lychee.tar.gz" | shasum -a 256 -c -
    tar -xzf "$tmp/lychee.tar.gz" -C "$tmp"
    mkdir -p "{{ bin }}"
    mv "$tmp/lychee-$target/lychee" "{{ bin }}/lychee"

# Builds the site under a base path that is not the root, so a link that
# drops it is caught, and fails on any broken internal link or fragment.
site-check: lychee
    #!/usr/bin/env sh
    set -eu
    out="$(mktemp -d "${TMPDIR:-/tmp}/site-check.XXXXXX")"
    trap 'rm -rf "$out"' EXIT
    base=https://site.test/base/
    (cd docs && hugo --gc --minify --panicOnWarning --baseURL "$base" --destination "$out/base")
    "{{ bin }}/lychee" --offline --include-fragments --no-progress \
        --root-dir "$out" --index-files index.html \
        --remap "^https://site\.test/base/(.*)\$ file://$out/base/\$1" \
        "$out/base"

# Fails when generated files, tracked or not, are stale relative to their
# sources.
verify: generate chart-crds chart-rbac api-docs perm-docs telemetry-docs
    git diff --exit-code -- api config charts docs/content/docs/reference
    test -z "$(git status --porcelain -- api config charts docs/content/docs/reference)"

# The API-server-backed tests, with the envtest binaries they need.
envtest:
    #!/usr/bin/env sh
    set -eu
    mkdir -p "{{ bin }}"
    KUBEBUILDER_ASSETS="$(go tool setup-envtest use {{ envtest_k8s_version }} --bin-dir "{{ bin }}" -p path)" \
        go test -race ./... -run Envtest

# The story bundles end to end on kind, on darwin inside a `container` machine.
e2e:
    go run ./hack/e2e

e2e-down:
    go run ./hack/e2e -down

clean:
    rm -rf "{{ bin }}"
