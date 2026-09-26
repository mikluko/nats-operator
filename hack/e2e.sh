#!/usr/bin/env bash
# Runs the story bundles end to end on minikube clusters inside a Debian
# machine from the Apple `container` CLI: creates or reuses the machine and
# the clusters, builds the controller images into them, installs the chart
# from the working tree in each, runs its `helm test` there and runs hack/e2e
# from the host.
#
# The clusters share one Docker network, named after the home profile, so a
# LoadBalancer Service's address, which MetalLB hands out from that network,
# is reachable from every cluster. hack/e2e publishes the hostnames such
# Services carry to every cluster's CoreDNS.
#
#   E2E_MACHINE           container machine name                nats-operator-e2e
#   E2E_CLUSTER           home minikube profile; the others     nats-operator-e2e
#                         are named <profile>-2, <profile>-3
#   E2E_CLUSTERS          how many Kubernetes clusters          2
#   E2E_DNS               nameserver the machine resolves by    1.1.1.1
#   E2E_CONTROLLERS       controllers the home chart enables    cluster auth jetstream
#   E2E_PEER_CONTROLLERS  controllers the others' chart enables cluster jetstream
#   E2E_STORIES           comma-separated story numbers         all
#   E2E_TIMEOUT           per-story wait for statuses           5m
set -euo pipefail

machine=${E2E_MACHINE:-nats-operator-e2e}
cluster=${E2E_CLUSTER:-nats-operator-e2e}
dns=${E2E_DNS:-1.1.1.1}
kube_clusters=${E2E_CLUSTERS:-2}
controllers=${E2E_CONTROLLERS:-cluster auth jetstream}
peer_controllers=${E2E_PEER_CONTROLLERS:-cluster jetstream}
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

# profiles are the minikube profiles, one per Kubernetes cluster, the home
# cluster first; each is also its kubeconfig context.
profiles=("$cluster")
for ((i = 2; i <= kube_clusters; i++)); do
	profiles+=("$cluster-$i")
done

log() { printf '==> %s\n' "$*" >&2; }

# The cluster provider: cluster_up, image_import and image_load, with the
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
# absent, prepares $machine_home and raises the inotify limits, whose
# defaults leave a second cluster's kube-proxy failing with "too many open
# files". A new machine is stopped once created: the first `container
# machine run` after create fails and stops it. The gateway resolver Apple
# `container` hands out does not answer on every host.
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
		printf 'fs.inotify.max_user_instances = 1024\nfs.inotify.max_user_watches = 524288\n' \
			>/etc/sysctl.d/90-nats-operator-e2e.conf
		sysctl -q -p /etc/sysctl.d/90-nats-operator-e2e.conf
	EOF
	docker_gid=$(in_machine root <<<'getent group docker' | cut -d: -f3)
}

# cluster_up starts every profile on the shared network, creating those that
# do not exist, gives each a MetalLB address pool and the CoreDNS hosts file
# hack/e2e writes, and writes a kubeconfig reaching each API server at the
# machine's address to $KUBECONFIG, the home cluster's context current.
cluster_up() {
	local ip i profile port prefix
	machine_up
	ip=$(container machine inspect "$machine" | jq -r '.[0].ipAddress')
	for i in "${!profiles[@]}"; do
		profile_up "$i" "$ip"
	done
	in_machine user <<<"cat $machine_home/kubeconfig" >"$KUBECONFIG"
	prefix=$(in_machine user M_NETWORK="$cluster" <<-'EOF'
		subnet=$(docker network inspect "$M_NETWORK" --format '{{ (index .IPAM.Config 0).Subnet }}')
		echo "${subnet%.*}"
	EOF
	)
	for i in "${!profiles[@]}"; do
		profile=${profiles[i]}
		port=$(in_machine user M_CLUSTER="$profile" <<-'EOF'
			docker inspect --format '{{ (index (index .NetworkSettings.Ports "8443/tcp") 0).HostPort }}' "$M_CLUSTER"
		EOF
		)
		kubectl config set-cluster "$profile" --server "https://$ip:$port" >/dev/null
		api_wait "$profile"
		metallb_pool "$profile" "$prefix.$((100 + 10 * i))-$prefix.$((109 + 10 * i))"
		coredns_hosts "$profile"
	done
	kubectl config use-context "$cluster" >/dev/null
}

# profile_up starts profile number $1 (from 0) on the Docker network named
# after the home profile, with the metallb addon. minikube gives every node
# on a network it did not create for that profile the same first address,
# so the others are pinned to the ones after it.
profile_up() {
	in_machine user MINIKUBE_HOME="$machine_home" KUBECONFIG="$machine_home/kubeconfig" \
		M_CLUSTER="${profiles[$1]}" M_INDEX="$1" M_NETWORK="$cluster" M_IP="$2" <<-'EOF'
		set -eu
		log=$MINIKUBE_HOME/start-$M_CLUSTER.log
		static=
		if [ "$M_INDEX" -gt 0 ]; then
			subnet=$(docker network inspect "$M_NETWORK" --format '{{ (index .IPAM.Config 0).Subnet }}')
			static="--static-ip ${subnet%.*}.$((2 + M_INDEX))"
		fi
		# shellcheck disable=SC2086
		if ! minikube start --profile "$M_CLUSTER" --driver docker --network "$M_NETWORK" $static \
			--embed-certs --listen-address 0.0.0.0 --apiserver-ips "$M_IP" \
			--addons metallb --wait all >"$log" 2>&1; then
			cat "$log" >&2
			exit 1
		fi
	EOF
}

# metallb_pool gives the MetalLB of context $1 the layer 2 address range $2.
metallb_pool() {
	kubectl --context "$1" apply -f - >/dev/null <<-EOF
		apiVersion: v1
		kind: ConfigMap
		metadata:
		  name: config
		  namespace: metallb-system
		data:
		  config: |
		    address-pools:
		    - name: default
		      protocol: layer2
		      addresses:
		      - $2
	EOF
}

# coredns_hosts makes the CoreDNS of context $1 resolve the names in the
# e2e.hosts key of its ConfigMap, which hack/e2e keeps current, before
# anything else: the Corefile is minikube's with a hosts plugin added, and
# the volume carries every key rather than the Corefile alone.
coredns_hosts() {
	local corefile=$work/Corefile
	cat >"$corefile" <<-'EOF'
		.:53 {
		    errors
		    health {
		       lameduck 5s
		    }
		    ready
		    hosts /etc/coredns/e2e.hosts {
		       fallthrough
		    }
		    kubernetes cluster.local in-addr.arpa ip6.arpa {
		       pods insecure
		       fallthrough in-addr.arpa ip6.arpa
		       ttl 30
		    }
		    prometheus :9153
		    forward . /etc/resolv.conf {
		       max_concurrent 1000
		    }
		    cache 30 {
		       disable success cluster.local
		       disable denial cluster.local
		    }
		    loop
		    reload
		    loadbalance
		}
	EOF
	kubectl --context "$1" -n kube-system create configmap coredns --dry-run=client -o yaml \
		--from-file=Corefile="$corefile" |
		kubectl --context "$1" apply --server-side --force-conflicts --field-manager nats-operator-e2e -f - >/dev/null
	kubectl --context "$1" -n kube-system patch deployment coredns --type json >/dev/null -p \
		'[{"op":"replace","path":"/spec/template/spec/volumes/0/configMap","value":{"name":"coredns","defaultMode":420}}]'
}

# api_wait returns once the API server of context $1 answers from the host,
# failing after a minute. A host's first TLS handshakes with a fresh cluster
# can stall.
api_wait() {
	local _
	for _ in $(seq 12); do
		if kubectl --context "$1" get --raw /readyz --request-timeout 5s >/dev/null 2>&1; then
			return 0
		fi
		sleep 5
	done
	echo "e2e: API server at $(kubectl config view --context "$1" --minify -o jsonpath='{.clusters[0].cluster.server}') unreachable from the host" >&2
	return 1
}

# image_import loads the image tarball $1 into the machine's Docker Engine
# and tags the image $2 it holds as $3.
image_import() {
	in_machine user M_TARBALL="$1" M_SOURCE="$2" M_TAG="$3" <<-'EOF' >/dev/null
		set -eu
		docker load -q -i "$M_TARBALL"
		docker tag "$M_SOURCE" "$M_TAG"
	EOF
}

# image_load makes image $1 available to every cluster's nodes.
image_load() {
	local profile
	for profile in "${profiles[@]}"; do
		in_machine user MINIKUBE_HOME="$machine_home" M_CLUSTER="$profile" M_TAG="$1" <<-'EOF' >/dev/null
			minikube --profile "$M_CLUSTER" image load "$M_TAG"
		EOF
	done
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

# build_images builds all three controller images with ko into tarballs on
# the host, imports each and loads it, tagged by its image digest so an
# unchanged image keeps its tag and a changed one rolls the Deployment. It
# prints one "<key> <repository> <tag>" line per controller.
build_images() {
	local key name tarball ref tag
	mkdir -p "$work/images"
	for key in cluster auth jetstream; do
		name=$key-controller
		tarball=$work/images/$name.tar
		if ! ref=$(cd "$root" && KO_DOCKER_REPO=$image_repo ko build --push=false -B \
			--platform "linux/$arch" --tags e2e --tarball "$tarball" "./cmd/$name" 2>"$work/ko.log"); then
			cat "$work/ko.log" >&2
			return 1
		fi
		tag=${ref##*@sha256:}
		tag=${tag:0:12}
		log "image $image_repo/$name:$tag"
		image_import "$tarball" "$image_repo/$name:e2e" "$image_repo/$name:$tag"
		image_load "$image_repo/$name:$tag"
		echo "$key $image_repo/$name $tag"
	done
}

# install_chart installs the chart from the working tree in context $1 with
# the controllers named in $2 enabled, reading image lines from stdin, and
# runs its `helm test`, printing the test pods' logs to stderr when it fails.
# CRDs are applied first because helm installs a chart's crds/ only on first
# install.
install_chart() {
	local key repo tag
	local sets=(--set cluster.enabled=false --set auth.enabled=false --set jetstream.enabled=false)
	while read -r key repo tag; do
		sets+=(--set "$key.image.repository=$repo" --set "$key.image.tag=$tag"
			--set "$key.image.pullPolicy=Never")
	done
	for key in $2; do
		sets+=(--set "$key.enabled=true")
	done
	kubectl --context "$1" apply --server-side --force-conflicts -f "$root/charts/nats-operator/crds" >/dev/null
	helm upgrade --install "$release" "$root/charts/nats-operator" --kube-context "$1" \
		--namespace "$release_ns" --create-namespace \
		--wait --timeout 3m "${sets[@]}"
	local out
	if ! out=$(helm test "$release" --kube-context "$1" --namespace "$release_ns" --logs --timeout 3m 2>&1); then
		printf '%s\n' "$out" >&2
		return 1
	fi
}

mkdir -p "$work"
log "clusters ${profiles[*]}"
cluster_up
build_images >"$work/images.txt"
for i in "${!profiles[@]}"; do
	enabled=$controllers
	if ((i > 0)); then
		enabled=$peer_controllers
	fi
	log "chart $release into ${profiles[i]}/$release_ns: $enabled"
	install_chart "${profiles[i]}" "$enabled" <"$work/images.txt" >/dev/null
done
log "stories ${stories:-all}"
cd "$root"
contexts=$(IFS=,; echo "${profiles[*]}")
exec go run ./hack/e2e -only "$stories" -timeout "$timeout" -contexts "$contexts"
