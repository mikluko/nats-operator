# nats-operator

Three Kubernetes controllers for NATS, each installed on its own:

- the **cluster controller** deploys NATS clusters from `NatsCluster`, rolls them one server at a time, and joins them into a supercluster or to a hub as leaves;
- the **auth controller** mints the JWT auth plane (NATS operator, accounts, users) and distributes it to the servers;
- the **JetStream controller** manages streams, consumers, key-value buckets and object stores, and balances JetStream leadership and placement, through a `NatsConnection` to any NATS cluster.

## Install

The chart installs the CRDs and all three controllers; `cluster.enabled`, `auth.enabled` or `jetstream.enabled` set to `false` leaves that controller out.

```sh
helm install nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
  --version <version> \
  --namespace nats-operator --create-namespace
helm test nats-operator --namespace nats-operator --logs
```

## Documentation

- [Install](https://mikluko.github.io/nats-operator/docs/install/): prerequisites, values, flags, RBAC, upgrade and uninstall.
- [Stories](https://mikluko.github.io/nats-operator/docs/stories/): the API, one user story at a time, starting from [the quickstart](https://mikluko.github.io/nats-operator/docs/stories/01-quickstart/).
- Reference: [API](https://mikluko.github.io/nats-operator/docs/reference/api/), [NATS permissions](https://mikluko.github.io/nats-operator/docs/reference/nats-permissions/), [telemetry](https://mikluko.github.io/nats-operator/docs/reference/telemetry/).
- [Design](docs/design/v1.md) and [ADRs](docs/adr/).

## Development

`just` generates, builds, tests and lints; `just envtest` runs the API-server-backed tests and `just e2e` the stories on kind.

## License

[Apache-2.0](LICENSE).
