# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Documentation site at <https://mikluko.github.io/nats-operator/>: the design, the stories and the ADRs.
- CRDs for every kind at `v1beta1`, under `config/crd/`: `NatsReferenceGrant`, `NatsOperatorTrust`, `NatsAccountTrust` and `NatsConnection` in `nats.mikluko.io`; `NatsCluster` in `cluster.nats.mikluko.io`; `NatsOperator`, `NatsSystemAccount`, `NatsAccount` and `NatsUser` in `auth.nats.mikluko.io`; `NatsStream`, `NatsConsumer`, `NatsKeyValue`, `NatsObjectStore`, `NatsBalancer`, `NatsSystemBalancer` and `NatsClusterEvacuation` in `jetstream.nats.mikluko.io`.
- Helm chart `charts/nats-operator` installing the CRDs and any subset of the cluster, auth and JetStream controllers through `cluster.enabled`, `auth.enabled` and `jetstream.enabled`, each with its own ServiceAccount and RBAC limited to its own API group, `nats.mikluko.io` and the core objects it uses; images default to `ghcr.io/mikluko/nats-operator/<controller>` at the chart's `appVersion`.
- The cluster controller deploys a `NatsCluster` without `auth`, `gateway`, `leafnodes` or `leafRemotes`: one StatefulSet and ConfigMap per server, client and headless Services, a PodDisruptionBudget with `maxUnavailable: 1`, route TLS self-signed unless a certificate is named, and a `prometheus-nats-exporter` sidecar. Its status reports `Ready`, `Settled`, `Progressing`, `endpoints`, the config revision and one entry per server.
- `NatsCluster` rolls a restart-only change, such as a version bump, one server at a time: highest ordinal first and the meta leader's server last, each step waiting until every server is Ready, every server on the new revision reports it, and the NATS cluster is Settled. `status.rollout` names the updated, current and pending servers and what the gate waits for; `Progressing` reads `RollingRestart`, `GateBlocked` once the gate has been closed for ten minutes, or `RolloutPaused`. `spec.rollout.paused` holds the next step, and the annotation `cluster.nats.mikluko.io/force-step: "<server>"` restarts that server at once.
- Servers render `lame_duck_duration: 2m` and `lame_duck_grace_period: 10s`, inside the pod's 300-second termination grace period.
- `NatsCluster` `status.config.restartReason` names what makes a spec change restart-only, such as `version 2.15.0 -> 2.15.1 is restart-only`.
- The API server refuses mutually exclusive fields set together, a `NatsCluster` version below 2.15.0 or moving more than one minor at once, and changes nats-server would refuse to a stream's or consumer's immutable fields.
- The auth controller signs `NatsOperator`, `NatsSystemAccount` and `NatsAccount` JWTs, adopting the keys `keys` names and generating the rest into Secrets it owns, and writes them to status; a reference-form `NatsOperatorTrust` or `NatsAccountTrust` mirrors them in its own status.
- `NatsAccount` imports are signed in with activation tokens for private exports, and only while a `NatsReferenceGrant` admits a cross-namespace one; `status.imports` and the `ReferencesResolved` condition report each.
- Signing keys marked `retiring` stay listed while every account is re-signed with another; the `NatsOperator` condition `RetiringKeysInUse` says when one can be removed.
- `NatsAccount.status.jwt` carries the account JWT, and `jwtTTL: 0s` signs one that never expires.
- The JetStream controller creates, updates and deletes the streams and consumers `NatsStream` and `NatsConsumer` declare, through their `NatsConnection`: it adopts existing ones under `adoptionPolicy`, goes Terminal on one it does not own, keeps or deletes them on resource deletion per `deletionPolicy`, and corrects drift every `--resync-period` (default 10m).
- The auth controller signs `NatsUser`s: creds land in the Secret `credentials` names, or `<name>-creds`, under `user.creds`; a user with `publicKey` gets its JWT in `status.jwt` and no Secret; `preset` signs the named permission set.
- A `NatsUser` whose `NatsReferenceGrant` is deleted is revoked in its account's JWT, and re-signed once a grant admits it again.
- The JetStream controller runs `NatsSystemBalancer`: on system credentials it evens stream and consumer leaders, and with `moves.placement` stream copies, across the servers of the NATS cluster its `NatsConnection` reaches, over every account. It makes one move per `interval` (default 1m) and none while that NATS cluster is not Settled or a `NatsClusterEvacuation` empties it; it moves leaders only for accounts carrying the `jetstream-stepdown` export, reporting the rest as `leader: partial` in `status.capabilities`; it never moves a stream whose `placement.cluster` names another NATS cluster; and a second `NatsSystemBalancer` for the same NATS cluster goes `Ready=False` with reason `DuplicateBalancer`.
- The cluster controller deploys a `NatsCluster` without `gateway`, `leafnodes` or `leafRemotes`: one StatefulSet and ConfigMap per server, client and headless Services, a PodDisruptionBudget with `maxUnavailable: 1`, route TLS self-signed unless a certificate is named, and a `prometheus-nats-exporter` sidecar. Its status reports `Ready`, `Settled`, `Progressing`, `endpoints`, the config revision and one entry per server.
- A `NatsCluster` with `auth` renders `operator`, `system_account` and the account resolver from the `NatsOperatorTrust` its `auth.trustRef` names, in either form, with the system account JWT preloaded. `auth.resolver` is `Full` (the default) or `Cache`. `Progressing` reads `TrustNotFound`, `TrustNotReady`, `TrustInvalid` or `NoGrant` while the trust roots cannot be read, and nothing is rendered.
- With `auth.systemCredentials`, the cluster controller observes the NATS cluster and reloads its servers as that system user over `$SYS`; without it, every config change restarts.
- A change to the trust roots, `system_account` or the resolver restarts servers one at a time.
- Deleting a `NatsUser` holds it until its account's JWT revokes its key and, with `--system-connection` set, until every server holds that JWT and no connection of the user remains; then its creds Secret is removed.
- `--system-connection` on the auth controller names a `NatsConnection` whose creds are a system user holding the `auth-controller` preset; through it account JWTs are pushed to the servers' resolvers on every change and at half their `jwtTTL`, and `status.distribution` and the `Distributed` condition report how many servers hold the current one.
- Deleting a `NatsAccount` deletes it from the servers' resolvers, and again from every server that joins later; `NatsOperator.status.deletedAccounts` lists such accounts until their last JWT expires.

[Unreleased]: https://github.com/mikluko/nats-operator/commits/main
