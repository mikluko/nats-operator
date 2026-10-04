# nats-operator

This chart installs the CRDs of the four API groups under `nats-operator.io` and any of three controllers:

- The cluster controller deploys NATS clusters and restarts them one server at a time.
- The auth controller creates their JWT auth plane and distributes it to the servers.
- The JetStream controller manages and balances JetStream.

## Install

You need Kubernetes 1.33, Helm 3.14 and nats-server 2.15.0, or later.

To install the chart with all three controllers, run this command with `<version>` replaced by a release version, such as `0.1.1`:

```sh
helm install nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
  --version <version> \
  --namespace nats-operator --create-namespace
```

To leave a controller out, set its switch to `false`.
The chart installs the CRDs whatever the switches are set to.

Table: The switch of each controller.

| Value               | Controller           | Default |
|---------------------|----------------------|---------|
| `cluster.enabled`   | cluster controller   | `true`  |
| `auth.enabled`      | auth controller      | `true`  |
| `jetstream.enabled` | JetStream controller | `true`  |

## Documentation

[Install](https://mikluko.github.io/nats-operator/docs/install/) has the prerequisites, and how to upgrade and uninstall.
[Chart and controller flags](https://mikluko.github.io/nats-operator/docs/reference/chart/) lists every value.
