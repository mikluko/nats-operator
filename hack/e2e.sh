#!/usr/bin/env bash
# Runs the story bundles end to end on a Kubernetes cluster from the Apple
# `container` CLI's k8s plugin: creates or reuses the cluster, builds and
# loads the controller images, installs the chart from the working tree and
# runs hack/e2e.
#
#   E2E_CLUSTER       cluster name                        nats-operator-e2e
#   E2E_CONTROLLERS   controllers the chart enables       cluster auth jetstream
#   E2E_STORIES       comma-separated story numbers       all
#   E2E_TIMEOUT       per-story wait for statuses         5m
set -euo pipefail

cluster=${E2E_CLUSTER:-nats-operator-e2e}
controllers=${E2E_CONTROLLERS:-cluster auth jetstream}
stories=${E2E_STORIES:-}
timeout=${E2E_TIMEOUT:-5m}

root=$(cd "$(dirname "$0")/.." && pwd)
work=$root/bin/e2e
release=nats-operator
release_ns=nats-operator
image_repo=localhost/nats-operator
case $(uname -m) in
	arm64 | aarch64) arch=arm64 ;;
	x86_64) arch=amd64 ;;
	*) echo "e2e: unsupported host architecture $(uname -m)" >&2; exit 1 ;;
esac

export KUBECONFIG=$work/kubeconfig

log() { printf '==> %s\n' "$*" >&2; }

# The cluster provider: these three functions are all that knows which one
# is in use.

# cluster_up starts the named cluster, creating it when it does not exist,
# and writes its kubeconfig to $KUBECONFIG.
cluster_up() {
	if ! container k8s start --name "$cluster" >/dev/null 2>&1; then
		container k8s create --name "$cluster"
	fi
	rm -f "$KUBECONFIG"
	container k8s write-config --name "$cluster" --kubeconfig "$KUBECONFIG" >/dev/null
	kubectl config use-context "$cluster" >/dev/null
}

# image_build builds image $2 from the directory $1 holding binary $3.
image_build() {
	quietly container build --platform "linux/$arch" \
		-f "$root/hack/controller.Containerfile" --build-arg "CONTROLLER=$3" \
		-t "$2" "$1"
}

# image_load makes image $1 available to the cluster's nodes.
image_load() {
	quietly container k8s load-image --name "$cluster" --platform "linux/$arch" "$1"
}

# quietly runs a command with no stdin, printing its output only when it
# fails. `container build --quiet` does not return on container 1.4.1.
quietly() {
	local out
	if ! out=$("$@" </dev/null 2>&1); then
		printf '%s\n' "$out" >&2
		return 1
	fi
}

# build_images cross-compiles all three controllers, builds each image and
# loads it, tagged by the binary's digest so an unchanged binary keeps its
# tag and a changed one rolls the Deployment. It prints one
# "<key> <repository> <tag>" line per controller.
build_images() {
	local key name ctx tag
	for key in cluster auth jetstream; do
		name=$key-controller
		ctx=$work/images/$name
		mkdir -p "$ctx"
		CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -o "$ctx/$name" "$root/cmd/$name"
		tag=$(shasum -a 256 "$ctx/$name" | cut -c1-12)
		log "image $image_repo/$name:$tag"
		image_build "$ctx" "$image_repo/$name:$tag" "$name"
		image_load "$image_repo/$name:$tag"
		echo "$key $image_repo/$name $tag"
	done
}

# install_chart installs the chart from the working tree with the controllers
# in $controllers enabled, reading image lines from stdin. CRDs are applied
# first because helm installs a chart's crds/ only on first install.
install_chart() {
	local key repo tag
	local sets=(--set cluster.enabled=false --set auth.enabled=false --set jetstream.enabled=false)
	while read -r key repo tag; do
		sets+=(--set "$key.image.repository=$repo" --set "$key.image.tag=$tag"
			--set "$key.image.pullPolicy=Never")
	done
	for key in $controllers; do
		sets+=(--set "$key.enabled=true")
	done
	kubectl apply --server-side --force-conflicts -f "$root/charts/nats-operator/crds" >/dev/null
	helm upgrade --install "$release" "$root/charts/nats-operator" \
		--namespace "$release_ns" --create-namespace \
		--wait --timeout 3m "${sets[@]}"
}

mkdir -p "$work"
log "cluster $cluster"
cluster_up
log "controllers: $controllers"
build_images >"$work/images.txt"
log "chart $release into $release_ns"
install_chart <"$work/images.txt" >/dev/null
log "stories ${stories:-all}"
cd "$root"
exec go run ./hack/e2e -only "$stories" -timeout "$timeout"
