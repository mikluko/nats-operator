# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Licensed under Apache-2.0; the chart carries `artifacthub.io/license: Apache-2.0`.
- Vulnerabilities are reported through the repository's GitHub private vulnerability reporting, as `SECURITY.md` states.
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
- `helm test` checks every enabled controller's `/healthz`.
- Each release publishes the three controller images for linux/amd64 and linux/arm64, the chart as an OCI artifact, and a GitHub release carrying the version's changelog entry.
- Documentation site at <https://mikluko.github.io/nats-operator/>: the stories, the design and the ADRs.
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
- A change to the trust roots, `system_account` or the resolver restarts servers one at a time.
- `NatsCluster` `gateway` joins a supercluster, and status reports `GatewaysConnected`.
- `NatsCluster` `leafnodes` accepts leaf connections on port 7422.
- `NatsCluster` `leafRemotes` dials hubs as a leaf through `NatsConnection`s, and status reports `LeafnodesConnected`.
- A renewed route, gateway or leafnode certificate reaches running servers by reload.
- The cluster controller deletes a cert-manager `Certificate` it created once its listener no longer names it.
- The `cluster-controller` user preset may request `$SYS.REQ.SERVER.PING.GATEWAYZ` and `$SYS.REQ.SERVER.PING.LEAFZ`.
<<<<<<< HEAD
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
- Deleting a `NatsUser` revokes it and, with `--system-connection` set, closes its connections before its creds Secret is removed.
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
=======
- Lowering `NatsCluster` `replicas` removes servers one at a time, highest ordinal first: each is evacuated, removed from the JetStream meta group, and deleted with its PVC and ConfigMap. A stream with more replicas than the new size blocks it with `Progressing=False, reason: ScaleDownBlocked`, as does JetStream without `auth.systemCredentials`.
- A change to `jetstream.volumeClaimTemplate`, or the annotation `cluster.nats.mikluko.io/replace-server: "<server>"`, replaces servers one at a time under the same name: evacuated, removed, deleted with its PVC and recreated, and the next step waits until the JetStream meta leader counts the recreated server as a peer, also when that leader is in another NATS cluster of the supercluster. `Progressing` reads `ScalingDown` or `ReplacingServer` while a step runs, `ClaimTerminating` while a server waits for its old PVC to be deleted, and `ReplacementBlocked` without `auth.systemCredentials`.
- Deleting a `NatsCluster` with JetStream waits while its NATS cluster holds stream groups, reporting `Deleting=True, reason: JetStreamDataRemains`; the annotation `cluster.nats.mikluko.io/force-delete` lets it proceed.
- The auth controller signs `NatsOperator`, `NatsSystemAccount` and `NatsAccount` JWTs, adopting the keys `keys` names and generating the rest into Secrets it owns, `<name>-<operator|systemaccount|account>-<identity|signing-1>`, and writes them to status; a generated Secret the object does not control reads Ready `False`, reason `SecretConflict`, and a generated identity Secret deleted after `status.publicKey` recorded it reads Ready `False`, reason `SeedLost`, and is not generated again; a `NatsAccount` whose public key a `NatsSystemAccount` or another `NatsAccount` under the same `NatsOperator` holds reads Ready `False`, reason `PublicKeyInUse`, and is not signed; a reference-form `NatsOperatorTrust` or `NatsAccountTrust` mirrors them in its own status.
- `NatsAccount.status.jwt` carries the account JWT, and `jwtTTL: 0s` signs one that never expires.
- `NatsAccount` imports are signed in with activation tokens for private exports, and only while a `NatsReferenceGrant` admits a cross-namespace one; `status.imports` and the `ReferencesResolved` condition report each.
- A `NatsAccount` import from a `NatsAccount` under a different `NatsOperator` is left out of the JWT, and the importer reports `ReferencesResolved` and `Ready` False with reason `ImportsUnresolved`.
- Signing keys marked `retiring` stay listed while every account is re-signed with another; the `NatsOperator` condition `RetiringKeysInUse` says when one can be removed.
- The auth controller signs `NatsUser`s: creds land in the Secret `credentials` names, or `<name>-creds`, under `user.creds`; a user with `publicKey` gets its JWT in `status.jwt` and no Secret, unless another `NatsUser` of the account holds that key, which reads Ready `False`, reason `PublicKeyInUse`; a key a user stops holding, by a changed `publicKey` or a creds Secret written afresh, is revoked in its account and listed in `status.replacedKeys` until the account JWT revokes it; `preset` signs the named permission set.
- `NatsUser.spec.accountRef` cannot change once set.
- A `NatsUser` whose `NatsReferenceGrant` is deleted is revoked in its account's JWT, and re-signed once a grant admits it again.
- Deleting a `NatsUser` holds it until its account's JWT revokes its key and, with `--system-connection` set, until every server holds that JWT and no connection of the user remains; then its creds Secret is removed.
- `--system-connection` on the auth controller names a `NatsConnection` whose creds are a system user holding the `auth-controller` preset; through it account JWTs are pushed to the servers' resolvers on every change and at half their `jwtTTL`, and `status.distribution` and the `Distributed` condition report how many servers hold the current one.
- Deleting a `NatsAccount` deletes it from the servers' resolvers, and again from every server that joins later; `NatsOperator.status.deletedAccounts` lists such accounts until their last JWT expires.
- `NatsAccount` and `NatsSystemAccount` `status.revocations` list the user keys the account revokes; a revocation is dropped once every signing key that may have issued a revoked JWT is removed from the account.
- A `NatsAccount` or `NatsSystemAccount` whose status has lost both its JWT and `status.revocations` takes its revocations back from the JWT the servers hold before it is signed again. While any server trusting the operator does not answer, one whose `status.distribution` records it distributed is not signed (Ready `False`, reason `RecoveringRevocations`); any other is signed with the condition `RevocationsUnrecovered` `True`, on the `NatsOperator` for its system account, until every server answers.
- The JetStream controller creates, updates and deletes the streams and consumers `NatsStream` and `NatsConsumer` declare, through their `NatsConnection`: it adopts existing ones under `adoptionPolicy`, goes Terminal on one it does not own, keeps or deletes them on resource deletion per `deletionPolicy`, and corrects drift every `--resync-period` (default 10m); status is read again every 15 seconds while the object's Raft group has no leader or a member that is not current.
- While a `NatsStream`'s stream moves to another NATS cluster, `status.transfer` reports the cluster it leaves and the one it moves to, when the move began, each new replica's `current` and `lag`, and how many of the stream's consumers have moved; `Synced` reads False, reason `Moving`, and status is read again every 5 seconds until the move ends.
- Deleting a `NatsStream`, `NatsConsumer`, `NatsKeyValue` or `NatsObjectStore` whose `NatsConnection` no longer exists, or no longer has a `NatsReferenceGrant` admitting it, removes the resource and leaves its server object, whatever its `deletionPolicy`.
- The JetStream controller manages the key-value buckets and object stores `NatsKeyValue` and `NatsObjectStore` declare, under the same policies, keeping them on resource deletion by default. `placement.preferred` on either kind goes Terminal.
- A stream, consumer, key-value bucket or object store the servers refuse as invalid goes Terminal; one they cannot place for want of online peers or room is retried.
- The JetStream controller runs `NatsSystemBalancer`: on system credentials it evens stream and consumer leaders, and with `moves.placement` stream copies, across the servers of the NATS cluster its `NatsConnection` reaches, over every account. It makes one move per `interval` (default 1m), none while that NATS cluster is not Settled, and none of a stream a `NatsClusterEvacuation` of that NATS cluster moves, a cluster of the same name in another NATS system aside; it moves leaders only for accounts carrying the `jetstream-stepdown` export, reporting the rest as `leader: Partial` in `status.capabilities`; it never moves a stream whose `placement.cluster` names another NATS cluster; and a second `NatsSystemBalancer` for the same NATS cluster goes `Ready=False` with reason `DuplicateBalancer`. `status.lastMove` and `status.pending` name each move's `account`, `stream` and, for a consumer's leader, `consumer`.
- The JetStream controller runs `NatsBalancer`: on the account's own `NatsConnection`, whose user needs to publish on `$JS.API.>` alone, it evens stream and consumer leaders, and with `moves.placement` stream copies, within each pool of the account's streams in the NATS cluster that connection reaches. A pool selects `NatsStream`, `NatsKeyValue` and `NatsObjectStore` resources in the balancer's namespace on the same connection by label; a stream several pools select is balanced in the first and reported as `Overlapping`; the rest of the account's streams, with or without a resource, form the pool `(default)`. `status.pools` reports each pool's streams and leader skew. It makes one move per `interval` (default 1m) after the time of `status.lastMove`, none while that NATS cluster is not Settled, none of a stream a `NatsClusterEvacuation` of that NATS cluster moves, a cluster of the same name in another NATS system aside, and none while a `NatsSystemBalancer` has a move pending on one of the account's streams (`Holding`, reason `YieldingToSystemBalancer`).
- The JetStream controller runs `NatsClusterEvacuation`: on system credentials it moves every stream, key-value bucket and object store in every account off `from.cluster` to servers carrying `to.serverTags`, with their consumers, a few at a time. It makes no move while a server of `from.cluster` carries every target tag (`TargetTagsInSource`); it never moves a stream whose resource declares `placement.cluster`, listing those that pin `from.cluster` and whose stream it holds in `status.pinned` and staying `Ready=False` with reason `PinnedObjects` while any remains; it lists moved streams that no resource owns and whose config still names `from.cluster` in `status.stalePlacement`; and deleting it before it is Ready cancels the moves `status.requested` lists.
>>>>>>> mikluko/octant/2218-tenancy

[Unreleased]: https://github.com/mikluko/nats-operator/commits/main
