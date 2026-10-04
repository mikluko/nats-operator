# nats-operator

nats-operator is three Kubernetes controllers that deploy NATS clusters, own their auth plane, and manage and balance JetStream.
You can install all three controllers or only the ones you need.

## Controllers

- The cluster controller deploys a NATS cluster from a `NatsCluster` and restarts it one server at a time.
  It also joins NATS clusters into a supercluster, or to a hub as leaves.
- The auth controller creates the JWT auth plane, which is a NATS operator with its accounts and users, and distributes it to the servers.
- The JetStream controller manages streams, consumers, key-value buckets and object stores, and balances JetStream leadership and placement.
  It works through a `NatsConnection` to any NATS cluster.

## Install

You need Kubernetes 1.33, Helm 3.14 and nats-server 2.15.0, or later.

To install the CRDs and all three controllers, and then test the release, run these commands with `<version>` replaced by a release version, such as `0.1.1`:

```sh
helm install nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
  --version <version> \
  --namespace nats-operator --create-namespace
helm test nats-operator --namespace nats-operator --logs
```

To leave a controller out, set `cluster.enabled`, `auth.enabled` or `jetstream.enabled` to `false`.

Then follow [the quickstart](https://mikluko.github.io/nats-operator/docs/stories/01-quickstart/) to deploy a NATS cluster with JetStream.

## Documentation

The site documents `main`.
For a release, read `docs/` at its tag.

- [Install](https://mikluko.github.io/nats-operator/docs/install/): prerequisites, values, flags, RBAC, upgrade and uninstall.
- [Stories](https://mikluko.github.io/nats-operator/docs/stories/): the quickstart tutorial, then one guide per task.
- Reference: [API](https://mikluko.github.io/nats-operator/docs/reference/api/), [NATS permissions](https://mikluko.github.io/nats-operator/docs/reference/nats-permissions/) and [telemetry](https://mikluko.github.io/nats-operator/docs/reference/telemetry/).
- [Design](docs/design/v1.md) and [ADRs](docs/adr/).
- [Security](SECURITY.md): how to report a vulnerability, the trust boundaries, and how to verify a release.

## Development

`just` generates, builds, tests and lints.
`just envtest` runs the tests that need an API server, and `just e2e` runs the stories on kind.
[`docs/README.md`](docs/README.md) has the steps to build and check the documentation site.

## License

[Apache-2.0](LICENSE).
