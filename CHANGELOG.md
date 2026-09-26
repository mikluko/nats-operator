# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Documentation site at <https://mikluko.github.io/nats-operator/>: the design, the stories and the ADRs.
- CRDs for every kind at `v1beta1`, under `config/crd/`: `NatsReferenceGrant`, `NatsOperatorTrust`, `NatsAccountTrust` and `NatsConnection` in `nats.mikluko.io`; `NatsCluster` in `cluster.nats.mikluko.io`; `NatsOperator`, `NatsSystemAccount`, `NatsAccount` and `NatsUser` in `auth.nats.mikluko.io`; `NatsStream`, `NatsConsumer`, `NatsKeyValue`, `NatsObjectStore`, `NatsBalancer`, `NatsSystemBalancer` and `NatsClusterEvacuation` in `jetstream.nats.mikluko.io`.
- Helm chart `charts/nats-operator` installing the CRDs and any subset of the cluster, auth and JetStream controllers through `cluster.enabled`, `auth.enabled` and `jetstream.enabled`, each with its own ServiceAccount and RBAC limited to its own API group, `nats.mikluko.io` and the core objects it uses; images default to `ghcr.io/mikluko/nats-operator/<controller>` at the chart's `appVersion`.
- The cluster controller deploys a `NatsCluster` without `auth`, `gateway`, `leafnodes` or `leafRemotes`: one StatefulSet and ConfigMap per server, client and headless Services, a PodDisruptionBudget with `maxUnavailable: 1`, route TLS self-signed unless a certificate is named, and a `prometheus-nats-exporter` sidecar. Its status reports `Ready`, `Settled`, `Progressing`, `endpoints`, the config revision and one entry per server. A spec change after the servers exist is reported as `Progressing` with reason `RolloutPending` and is not applied.
- `NatsCluster` `status.config.restartReason` names what makes a spec change restart-only, such as `version 2.15.0 -> 2.15.1 is restart-only`.
- The API server refuses mutually exclusive fields set together, a `NatsCluster` version below 2.15.0 or moving more than one minor at once, and changes nats-server would refuse to a stream's or consumer's immutable fields.
- The auth controller signs `NatsOperator`, `NatsSystemAccount` and `NatsAccount` JWTs, adopting the keys `keys` names and generating the rest into Secrets it owns, and writes them to status; a reference-form `NatsOperatorTrust` or `NatsAccountTrust` mirrors them in its own status.
- `NatsAccount` imports are signed in with activation tokens for private exports, and only while a `NatsReferenceGrant` admits a cross-namespace one; `status.imports` and the `ReferencesResolved` condition report each.
- Signing keys marked `retiring` stay listed while every account is re-signed with another; the `NatsOperator` condition `RetiringKeysInUse` says when one can be removed.
- `NatsAccount.status.jwt` carries the account JWT, and `jwtTTL: 0s` signs one that never expires.
- The JetStream controller creates, updates and deletes the streams and consumers `NatsStream` and `NatsConsumer` declare, through their `NatsConnection`: it adopts existing ones under `adoptionPolicy`, goes Terminal on one it does not own, keeps or deletes them on resource deletion per `deletionPolicy`, and corrects drift every `--resync-period` (default 10m).

[Unreleased]: https://github.com/mikluko/nats-operator/commits/main
