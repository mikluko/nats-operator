#!/usr/bin/env bash
# Runs the story bundles end to end on a minikube cluster inside a Debian
# machine from the Apple `container` CLI: creates or reuses the machine and
# the cluster, builds the controller images into it, installs the chart from
# the working tree and runs hack/e2e from the host.
#
#   E2E_MACHINE       container machine name              nats-operator-e2e
#   E2E_CLUSTER       minikube profile name               nats-operator-e2e
#   E2E_DNS           nameserver the machine resolves by  1.1.1.1
#   E2E_CONTROLLERS   controllers the chart enables       cluster auth jetstream
#   E2E_STORIES       comma-separated story numbers       all
#   E2E_TIMEOUT       per-story wait for statuses         5m
set -euo pipefail

machine=${E2E_MACHINE:-nats-operator-e2e}
cluster=${E2E_CLUSTER:-nats-operator-e2e}
dns=${E2E_DNS:-1.1.1.1}
controllers=${E2E_CONTROLLERS:-cluster auth jetstream}
stories=${E2E_STORIES:-}
timeout=${E2E_TIMEOUT:-5m}

root=$(cd "$(dirname "$0")/.." && pwd)
work=$root/bin/e2e
release=nats-operator
release_ns=nats-operator
image_repo=localhost/nats-operator
machine_image=$image_repo/e2e-machine
minikube_version=v1.39.0
case $(uname -m) in
	arm64 | aarch64) arch=arm64 ;;
	x86_64) arch=amd64 ;;
	*) echo "e2e: unsupported host architecture $(uname -m)" >&2; exit 1 ;;
esac

export KUBECONFIG=$work/kubeconfig

log() { printf '==> %s\n' "$*" >&2; }

# The cluster provider: cluster_up, image_build and image_load, with the
# helpers they share, are all that knows which one is in use.

# minikube state lives on the machine's own disk: the home mount is
# read-only, and a writable one would put it in the host's home.
machine_home=/var/lib/nats-operator-e2e

# in_machine runs the sh script on stdin in the machine, as root when $1 is
# "root" and otherwise as the host-matched user in the docker group, with the
# remaining arguments as KEY=VALUE environment. `container machine run` joins
# and re-splits its command line, so scripts go through stdin; the shell's
# profile sets names such as VERSION, so the provider's own start with M_.
in_machine() {
	local who=$1 kv args=()
	shift
	if [[ $who == root ]]; then
		args+=(--root)
	else
		args+=(--gid "$docker_gid")
	fi
	for kv in "$@"; do
		args+=(-e "$kv")
	done
	container machine run -i -n "$machine" -w / "${args[@]}" -- sh -s 2> >(grep -v 'Operation not supported by device' >&2)
}

# machine_up boots the machine, creating it from hack/machine.Containerfile
# when it does not exist, points its resolver at $dns, installs minikube when
# absent and prepares $machine_home. A new machine is stopped once created:
# the first `container machine run` after create fails and stops it. The
# gateway resolver Apple `container` hands out does not answer on every host.
machine_up() {
	if ! container machine inspect "$machine" >/dev/null 2>&1; then
		quietly container build --dns "$dns" -t "$machine_image" \
			-f "$root/hack/machine.Containerfile" "$root/hack"
		if ! { container machine create "$machine_image" --name "$machine" \
			--cpus 6 --memory 12G --home-mount ro &&
			container machine stop "$machine"; } >"$work/machine.log" 2>&1; then
			cat "$work/machine.log" >&2
			return 1
		fi
	fi
	in_machine root M_DNS="$dns" M_VERSION="$minikube_version" M_ARCH="$arch" \
		M_HOME_DIR="$machine_home" M_OWNER="$(id -u)" <<-'EOF'
		set -eu
		echo "nameserver $M_DNS" >/etc/resolv.conf
		if ! command -v minikube >/dev/null; then
			url=https://storage.googleapis.com/minikube/releases/$M_VERSION/minikube-linux-$M_ARCH
			curl -fsSLo /tmp/minikube "$url"
			echo "$(curl -fsSL "$url.sha256")  /tmp/minikube" | sha256sum -c --quiet
			install -m 0755 /tmp/minikube /usr/local/bin/minikube
			rm /tmp/minikube
		fi
		install -d -o "$M_OWNER" "$M_HOME_DIR"
	EOF
	docker_gid=$(in_machine root <<<'getent group docker' | cut -d: -f3)
}

# cluster_up starts the minikube profile, creating it when it does not exist,
# and writes a kubeconfig reaching its API server at the machine's address to
# $KUBECONFIG.
cluster_up() {
	local ip port
	machine_up
	ip=$(container machine inspect "$machine" | jq -r '.[0].ipAddress')
	in_machine user MINIKUBE_HOME="$machine_home" KUBECONFIG="$machine_home/kubeconfig" \
		M_CLUSTER="$cluster" M_IP="$ip" <<-'EOF'
		set -eu
		log=$MINIKUBE_HOME/start.log
		if ! minikube start --profile "$M_CLUSTER" --driver docker --embed-certs \
			--listen-address 0.0.0.0 --apiserver-ips "$M_IP" --wait all >"$log" 2>&1; then
			cat "$log" >&2
			exit 1
		fi
	EOF
	port=$(in_machine user M_CLUSTER="$cluster" <<-'EOF'
		docker inspect --format '{{ (index (index .NetworkSettings.Ports "8443/tcp") 0).HostPort }}' "$M_CLUSTER"
	EOF
	)
	in_machine user <<<"cat $machine_home/kubeconfig" >"$KUBECONFIG"
	kubectl config set-cluster "$cluster" --server "https://$ip:$port" >/dev/null
	kubectl config use-context "$cluster" >/dev/null
	api_wait
}

# api_wait returns once the API server answers from the host, failing after
# a minute. A host's first TLS handshakes with a fresh cluster can stall.
api_wait() {
	local _
	for _ in $(seq 12); do
		if kubectl get --raw /readyz --request-timeout 5s >/dev/null 2>&1; then
			return 0
		fi
		sleep 5
	done
	echo "e2e: API server at $(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}') unreachable from the host" >&2
	return 1
}

# image_build builds image $2 from the directory $1 holding binary $3 with
# the machine's Docker Engine.
image_build() {
	in_machine user M_TAG="$2" M_FILE="$root/hack/controller.Containerfile" \
		M_CONTROLLER="$3" M_CONTEXT="$1" <<-'EOF' >/dev/null
		docker build -q -t "$M_TAG" -f "$M_FILE" --build-arg "CONTROLLER=$M_CONTROLLER" "$M_CONTEXT"
	EOF
}

# image_load makes image $1 available to the cluster's nodes.
image_load() {
	in_machine user MINIKUBE_HOME="$machine_home" M_CLUSTER="$cluster" M_TAG="$1" <<-'EOF' >/dev/null
		minikube --profile "$M_CLUSTER" image load "$M_TAG"
	EOF
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
