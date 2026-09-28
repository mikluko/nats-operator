# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Licensed under Apache-2.0; the chart carries `artifacthub.io/license: Apache-2.0`.
- Vulnerabilities are reported through the repository's GitHub private vulnerability reporting, as `SECURITY.md` states.
- `SECURITY.md` states the supported versions, the acknowledgement time for a report, and how to verify a release's signatures and provenance.
- Each release's controller images and chart are signed keylessly with cosign and carry a GitHub build provenance attestation.
- The cluster controller caches only the StatefulSets, ConfigMaps, Services, PersistentVolumeClaims and PodDisruptionBudgets labelled `cluster.nats.mikluko.io/cluster`; every controller reads Secrets from the API server rather than its cache.
- A controller's `/readyz` passes once its cache is synced.
- Every controller's OpenTelemetry resource carries its host name as `service.instance.id`, unless `OTEL_RESOURCE_ATTRIBUTES` sets one.
- Every controller serves its Prometheus metrics over HTTPS, to a bearer token of a user allowed `get` on the non-resource URL `/metrics`.
- Chart value `metrics.scraper.serviceAccount`, the `namespace/name` of a ServiceAccount granted `get` on `/metrics`.
- `helm test` on the chart checks every enabled controller's `/readyz`.
- A failed `NatsCluster` reconcile reads `Progressing=False, reason: ReconcileFailed` and records a `ReconcileFailed` Warning event.
- `NatsCluster` `status.removals` names each server being removed or replaced, its phase and since when; a replaced server is recreated only once its old volume claim is gone.
- CRDs for every kind at `v1beta1`, under `config/crd/`, in the API groups `nats.mikluko.io`, `cluster.nats.mikluko.io`, `auth.nats.mikluko.io` and `jetstream.nats.mikluko.io`.
- The API server refuses mutually exclusive fields set together, a `NatsCluster` version below 2.15.0 or a move of more than one minor, and changes nats-server would refuse to the immutable fields of JetStream objects.
- Helm chart `charts/nats-operator` installing the CRDs and any subset of the three controllers, each with its own ServiceAccount and a ClusterRole holding only the verbs it uses.
- The chart requires Kubernetes 1.29 or later.
- The chart refuses a value key it does not know, checked against `values.schema.json`.
- Chart values `nodeSelector`, `annotations`, `podAnnotations` and `affinity`, globally and per controller.
- Chart value `auth.systemConnection`, passed to the auth controller as `--system-connection`.
- Each release publishes the three controller images for linux/amd64 and linux/arm64, the chart as an OCI artifact, and a GitHub release carrying the version's changelog entry.
- Documentation site at <https://mikluko.github.io/nats-operator/>, of the latest release: the stories, the design and the ADRs.
- Documentation page `/docs/install/`: installing the chart, its values, the controllers' flags and RBAC, upgrade and uninstall.
- Documentation page `/docs/reference/api/`: every kind, field and enum value of the four API groups.
- Documentation page `/docs/reference/nats-permissions/`: the nats-server subjects each controller requests and the presets that grant them.
- Documentation page `/docs/reference/telemetry/`: the controllers' OpenTelemetry configuration, instruments, spans and Kubernetes events.
- Each controller exports OpenTelemetry metrics and traces once the SDK's environment names an exporter or endpoint.
- The controllers record Kubernetes events for balancer moves, evacuations, rollout steps, JWT pushes and user kicks.
- The cluster controller deploys a `NatsCluster` as one StatefulSet and ConfigMap per server, with Services, a PodDisruptionBudget, route TLS and a `prometheus-nats-exporter` sidecar.
- `NatsCluster` rolls a restart-only change one server at a time, each step gated on the NATS cluster being Settled.
- `spec.rollout.paused` holds a `NatsCluster` rollout, and the annotation `cluster.nats.mikluko.io/force-step` restarts one waiting server at once.
- Servers render `lame_duck_duration: 2m` and `lame_duck_grace_period: 10s`.
- `NatsCluster` `status.config.restartReason` names what makes a spec change restart-only.
- A `NatsCluster` with `auth` renders its trust roots and account resolver from the `NatsOperatorTrust` that `auth.trustRef` names.
- With `auth.systemCredentials` the cluster controller reloads servers over `$SYS`; without it every config change restarts.
- A self-signed route certificate is valid for one year and renewed under the same CA once a third of that remains; the CA is kept in the Secret `<name>-routes-ca`.
- A `NatsCluster`'s client Service serves only the client port; the monitoring port is on the headless Service, which `status.endpoints.monitor` names.
- A `NatsCluster`'s pods meet the restricted Pod Security Standard and mount no ServiceAccount token; `podTemplate` can still escalate what the pod runs.
- A change to the trust roots, `system_account` or the resolver restarts servers one at a time.
- `NatsCluster` `gateway` joins a supercluster, and status reports `GatewaysConnected`.
- `NatsCluster` `leafnodes` accepts leaf connections on port 7422.
- `NatsCluster` `leafRemotes` dials hubs as a leaf through `NatsConnection`s, and status reports `LeafnodesConnected`.
- A renewed route, gateway or leafnode certificate reaches running servers by reload.
- The cluster controller deletes a cert-manager `Certificate` it created once its listener no longer names it.
- The `cluster-controller` user preset may request `$SYS.REQ.SERVER.PING.GATEWAYZ` and `$SYS.REQ.SERVER.PING.LEAFZ`.
- Lowering `NatsCluster` `replicas` evacuates and removes servers one at a time.
- A change to `jetstream.volumeClaimTemplate`, or the annotation `cluster.nats.mikluko.io/replace-server`, replaces servers one at a time under the same name.
- Deleting a `NatsCluster` waits while its NATS cluster holds JetStream data, unless annotated `cluster.nats.mikluko.io/force-delete`.
- The auth controller signs `NatsOperator`, `NatsSystemAccount` and `NatsAccount` JWTs from adopted or generated keys.
- `NatsAccount` `jwtTTL: 0s` signs a JWT that never expires.
- `NatsAccount` imports carry activation tokens for private exports, and cross namespaces only where a `NatsReferenceGrant` admits them.
- A `NatsAccount` import from an account under another `NatsOperator` is left out, with `Ready` False, reason `ImportsUnresolved`.
- The `NatsOperator` condition `RetiringKeysInUse` says when a `retiring` signing key can be removed.
- The auth controller signs `NatsUser`s into creds Secrets, or into `status.jwt` for a user with `publicKey`.
- `NatsUser` `spec.accountRef` cannot change once set.
- A `NatsUser` whose `NatsReferenceGrant` is deleted is revoked.
- Deleting a `NatsUser` revokes it and, with `--system-connection` set, closes its connections before its creds Secret is removed, unless its account or the account's `NatsOperator` no longer exists.
- `--system-connection` on the auth controller pushes account JWTs to the servers, and `status.distribution` reports how many hold the current one.
- Deleting a `NatsAccount` deletes it from the servers' resolvers, also on servers that join later.
- `NatsAccount` and `NatsSystemAccount` `status.revocations` list the user keys the account revokes.
- An account whose status lost its JWT and revocations recovers them from the servers before it is signed again.
- The JetStream controller creates, adopts, updates and deletes the streams and consumers `NatsStream` and `NatsConsumer` declare, and corrects drift every `--resync-period`.
- `NatsStream` `status.transfer` reports a move to another NATS cluster while it runs.
- Deleting a JetStream resource whose `NatsConnection` is gone or no longer admitted leaves its server object.
- The JetStream controller manages the key-value buckets and object stores `NatsKeyValue` and `NatsObjectStore` declare.
- A JetStream object the servers refuse as invalid goes Terminal; one they cannot place is retried.
- `NatsSystemBalancer` evens leaders, and optionally copies, across the servers of one NATS cluster over every account.
- `NatsBalancer` evens leaders, and optionally copies, within pools of one account's streams.
- `NatsClusterEvacuation` moves every JetStream object off one NATS cluster to servers carrying the target tags.
- `NatsClusterEvacuation` `status.remaining` counts the streams still to leave the source cluster.
- A `NatsStream` whose stream matches spec but whose transfer or consumers cannot be read reads `Ready` False, reason `ObserveFailed`, and is retried with backoff.
- The API server refuses a `NatsBalancer` or `NatsSystemBalancer` `interval` that is not positive.
- A `NatsAccount` or `NatsUser` whose `publicKey` another account or user under the same `NatsOperator` holds reads `Ready` False, reason `PublicKeyInUse`.
- Generated seed Secrets are named `<name>-<operator|systemaccount|account>-<identity|signing-1>`; one the object does not own reads `Ready` False, reason `SecretConflict`.
- A generated identity Secret lost after `status.publicKey` recorded its key reads `Ready` False, reason `SeedLost`, and no new identity is minted.
- A key a `NatsUser` stops holding is revoked in its account and listed in `status.replacedKeys` until the account JWT carries the revocation.
- Revocation recovery reads the servers' JWT only when every server of the roster answers.
- A `NatsAccount`, `NatsSystemAccount` or `NatsUser` reads `Distributed` False, reason `NoSystemConnection`, while the auth controller runs without `--system-connection`.
- `NatsClusterEvacuation` makes no move while any server of its NATS system is down or the meta group has no leader, reporting `Ready` and `Progressing` False with reason `ServersDown`.
- The `NatsSystemBalancer` and the `NatsBalancer`s of one NATS cluster move one at a time; while one's move is in flight the others read `Holding`, reason `MoveLeaseHeld`.

### Security

- The `cluster-controller`, `jetstream-controller` and `auth-controller` presets subscribe only to their own inbox, `_INBOX.<preset>.>`; a JetStream controller account user whose `permissions` restrict subscriptions must allow `_INBOX.jetstream-controller.>`.

[Unreleased]: https://github.com/mikluko/nats-operator/commits/main
