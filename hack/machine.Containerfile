# The Debian container machine hack/e2e runs in on darwin: the
# machine-debian base brought up by hack/e2e/machine.sh, which the harness
# also runs on every start.
FROM ghcr.io/mikluko/machine-debian:0.16@sha256:a1e419fe228c6f042adcd0301fb8c6b5d8f9e89900c503f5211196753f3089b1

COPY e2e/machine.sh /usr/local/libexec/nats-operator-e2e-machine
RUN --mount=type=cache,target=/var/lib/apt,sharing=locked \
    --mount=type=cache,target=/var/cache/apt,sharing=locked \
    /usr/local/libexec/nats-operator-e2e-machine
