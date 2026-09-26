# The Debian container machine the e2e cluster runs in: the machine-debian
# base with a Docker Engine that starts on boot, minikube's driver.
FROM ghcr.io/mikluko/machine-debian

ARG DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/lib/apt,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    apt-get update -yq && \
    apt-get install -y docker.io iptables conntrack curl && \
    systemctl enable docker
