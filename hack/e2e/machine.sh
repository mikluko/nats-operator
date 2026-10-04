#!/bin/sh
# Provisions a Debian root filesystem for hack/e2e, idempotently, so it both
# builds the image and updates a running machine. On a booted system it
# points the resolver at M_DNS when that is set. Given the argument versions,
# it prints the helm version it installs and does nothing else.
set -eu

helm_version=v4.3.0
if [ "${1:-}" = versions ]; then
	echo "helm $helm_version"
	exit 0
fi
case $(uname -m) in
aarch64 | arm64)
	arch=arm64
	helm_sha256=31c5794dd55c66a51e6b7d2e2ac7a114ae8b1de41ff1d9ba51748ac973b06a08
	;;
x86_64)
	arch=amd64
	helm_sha256=86584a54def73570558f66f5111cc53dfed56689637ae32c1201205d494f54fb
	;;
*)
	echo "machine: unsupported architecture $(uname -m)" >&2
	exit 1
	;;
esac

booted=
if [ -d /run/systemd/system ]; then
	booted=1
fi
if [ -n "$booted" ] && [ -n "${M_DNS:-}" ]; then
	echo "nameserver $M_DNS" >/etc/resolv.conf
fi

missing=
for pkg in podman crun netavark aardvark-dns iptables curl ca-certificates procps; do
	dpkg-query -W -f '${Status}' "$pkg" 2>/dev/null | grep -q 'install ok installed' || missing="$missing $pkg"
done
if [ -n "$missing" ]; then
	apt-get update -yq >/dev/null
	# shellcheck disable=SC2086
	DEBIAN_FRONTEND=noninteractive apt-get install -yq $missing >/dev/null
fi

if ! helm version --short 2>/dev/null | grep -q "^$helm_version+"; then
	tmp=$(mktemp -d)
	tarball=helm-$helm_version-linux-$arch.tar.gz
	curl -fsSLo "$tmp/$tarball" "https://get.helm.sh/$tarball"
	echo "$helm_sha256  $tmp/$tarball" | sha256sum -c --quiet
	tar -xzf "$tmp/$tarball" -C "$tmp" "linux-$arch/helm"
	install -m 0755 "$tmp/linux-$arch/helm" /usr/local/bin/helm
	rm -rf "$tmp"
fi

mkdir -p /etc/sysctl.d /lib/modules
printf 'fs.inotify.max_user_instances = 1024\nfs.inotify.max_user_watches = 524288\n' \
	>/etc/sysctl.d/90-nats-operator-e2e.conf
systemctl enable podman.socket >/dev/null 2>&1

if [ -n "$booted" ]; then
	sysctl -q -p /etc/sysctl.d/90-nats-operator-e2e.conf
	for unit in docker.socket docker.service; do
		if systemctl is-enabled -q "$unit" 2>/dev/null || systemctl is-active -q "$unit"; then
			systemctl disable --now -q "$unit"
		fi
	done
	if iptables -n -L DOCKER-USER >/dev/null 2>&1; then
		iptables -P FORWARD ACCEPT
		iptables -F && iptables -X
		iptables -t nat -F && iptables -t nat -X
	fi
	systemctl start podman.socket
fi
