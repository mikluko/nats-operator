# The Debian container machine hack/e2e runs in on darwin: the
# machine-debian base brought up by hack/e2e/machine.sh, which the harness
# also runs on every start.
FROM ghcr.io/mikluko/machine-debian

COPY e2e/machine.sh /usr/local/libexec/nats-operator-e2e-machine
RUN --mount=type=cache,target=/var/lib/apt,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    /usr/local/libexec/nats-operator-e2e-machine
