# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Documentation site at <https://mikluko.github.io/nats-operator/>: the design, the stories and the ADRs.
- CRDs for every kind at `v1beta1`, under `config/crd/`: `NatsReferenceGrant`, `NatsOperatorTrust`, `NatsAccountTrust` and `NatsConnection` in `nats.mikluko.io`; `NatsCluster` in `cluster.nats.mikluko.io`; `NatsOperator`, `NatsSystemAccount`, `NatsAccount` and `NatsUser` in `auth.nats.mikluko.io`; `NatsStream`, `NatsConsumer`, `NatsKeyValue`, `NatsObjectStore`, `NatsBalancer`, `NatsSystemBalancer` and `NatsClusterEvacuation` in `jetstream.nats.mikluko.io`.
- The API server refuses mutually exclusive fields set together, a `NatsCluster` version below 2.15.0 or moving more than one minor at once, and changes nats-server would refuse to a stream's or consumer's immutable fields.

[Unreleased]: https://github.com/mikluko/nats-operator/commits/main
