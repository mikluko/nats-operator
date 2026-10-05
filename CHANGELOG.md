# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/2.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A `NatsOperator` has the condition `SigningKeyUntrusted`, True with the reason `UntrustedSigner` while a server does not list its active signing key in the NATS operator JWT it runs under, naming the key and how many servers do not list it, and False with the reason `SignerTrusted` otherwise.
- An importer of a private export of a `NatsAccount` takes `publicKey` in place of `kind` and `name`, for an account no `NatsAccount` describes; the auth controller mints its activation token into `status.exports[].importers[].activationToken`. A key it mints no token for leaves `Ready` False with the reason `ActivationsUnsigned`.
- A `NatsOperator` lists the revocations of the system account JWT it signs, each with its `issuers`, in `status.systemAccount.revocations`.
- The annotation `cluster.nats-operator.io/restart-all` on a `NatsCluster` restarts every server at once on a config that adds or removes `auth`.

### Changed

- **Breaking:** a server whose `VARZ` reports no NATS operator JWT, as one configured by `trusted_keys` does, or that does not answer `VARZ` is no longer counted in `status.distribution.current`, and the `NatsAccount` or `NatsSystemAccount` reads `Distributed` False with the reason `TrustUnknown` naming how many servers do not say; a system user of the `auth-controller` preset signed before 0.4.0 lacks the `VARZ` permission until its creds are reissued, and a `trusted_keys` server reads so until it runs under a NATS operator JWT.
- A `NatsOperator` reads `SigningKeyUntrusted` Unknown with the reason `TrustUnknown` while no server is known not to list its active signing key and a server does not say.

### Fixed

- Adding or removing `auth` on a running `NatsCluster` holds every server on the config it runs, with `Progressing` False and the reason `AuthChangeBlocked`, in place of a rollout that stalled at its first server.
- A server's startup probe is `/healthz?js-meta-only=true` in place of `/healthz`, and passes once its JetStream is current with a meta leader; the change restarts every server of every `NatsCluster` once, one at a time through the rollout gate.
- An `httpGet`, `tcpSocket` or `grpc` handler in a `podTemplate` probe or lifecycle hook that omits `port` keeps the rendered port, in place of port 0.
- The revocation of a deleted or denied `NatsUser` whose `status.publicKey` is the `publicKey` of its spec lists the account's identity key among its `issuers`, and is kept after every signing key it lists is rotated out.
- A revocation that a `NatsSystemAccount` takes from the JWT the servers hold lists the system account's identity key among its `issuers`, and is kept after its signing keys are rotated out.
- A `NatsCluster` with `jetstream`, no `jetstream.domain` and `gateway.remotes` naming another member creates no server whose data volume claim is older than the `NatsCluster`, and renders no change to the gateway remotes, the domain or `jetstream` of servers that lead a JetStream meta group of their own; it reads `Progressing` False with the reason `OwnMetaGroup`. Such a change to servers that cannot be observed waits, with `Progressing` False and the reason `ObservationFailed`.

## [0.4.0] - 2026-10-05

### Added

- `NatsAccount.spec.limits.jetstream.tiers` sets an account's JetStream limits by tier, `R1` to `R5` after the replica count of a stream, in place of limits for the account as a whole.
- `NatsAccount.spec.limits.jetstream`, and each of its tiers, takes `maxAckPending`, `memoryMaxStreamBytes`, `diskMaxStreamBytes` and `maxBytesRequired`.
- An import of a `NatsAccount` names the `NatsSystemAccount` of its `NatsOperator` under `accountRef`, taking `account-monitoring-services` or `account-monitoring-streams` with the account's own public key in the subject.
- An import of a `NatsAccount` names its exporter by `publicKey` in place of `accountRef`, with the export's `subject` and `type`, and for a private export `activation.secretKeyRef` selecting the activation token the exporter issued; `status.imports[].activation` then reads `Supplied`.
- An import of a `NatsAccount` takes `share` on a Service import and `allowTrace` on a Stream import.
- `NatsAccount.spec.adoption.droppedClaims` and `NatsSystemAccount.spec.adoption.droppedClaims`, `Refuse` when omitted or `Accept`, say whether the first signing of an account the servers already hold a JWT for goes ahead where it would drop claims that JWT carries.
- A signing key under `keys.signing` of a `NatsAccount` or a `NatsSystemAccount` takes `scope`, which makes it a scoped signing key: a `role`, and the `permissions`, `connectionTypes` and `limits` (`subscriptions`, `payload`) that the servers hold every user it signs to.
- `NatsUser.spec.role` signs the user with the account's scoped signing key of that role; it excludes `permissions`, `connectionTypes` and `preset`. A scoped signing key signs no other user and no activation token.

### Changed

- Each release's images and chart are signed with the certificate identity `https://github.com/mikluko/nats-operator/.github/workflows/release-roll.yaml@refs/tags/v<version>`, and their build provenance attestations name the signer workflow `release-roll.yaml` and the source ref `refs/tags/v<version>`; `SECURITY.md` has the commands for these and for releases 0.1.0 through 0.3.1.
- A `NatsAccount` importing from a `NatsAccount` that has no public key yet is not signed until it has one; meanwhile its `Ready` and `ReferencesResolved` conditions are False with the reason `ExporterPending`.
- The `auth-controller` user preset may publish `$SYS.REQ.SERVER.PING.VARZ`.
- A `NatsAccount` or `NatsSystemAccount` adopting an account made elsewhere is not signed while the JWT the servers hold carries a claim the JWT signed from spec would not, a claim no field expresses or one the spec omits alike; it has the condition `Ready` False with the reason `AdoptionDropsClaims` naming each such claim by its JWT field path, a `NatsSystemAccount`'s `NatsOperator` too, and the servers keep their JWT. A claim the spec sets to another value does not hold the signing. A JWT holding JetStream limits for a tier named other than `R1` to `R5` is never adopted; the reason is then `TierInexpressible`.

### Fixed

- A `NatsAccount` signed while a server does not answer for the JWT it holds is not pushed until every server has answered; meanwhile `RevocationsUnrecovered` is True and `Distributed` is False with the reason `Unreachable`. A server that misses three roster polls in a row is not waited for.
- A `NatsAccount` that adopts an account with no signing key keeps the revocations of the JWT the servers hold. A revocation read from the servers lists the account's identity key among its `issuers`, and is kept when the account's signing keys rotate.
- A `NatsSystemAccount` adopting a system account made elsewhere keeps the revocations of the JWT the servers hold, and every system account JWT carries the exports `account-monitoring-services` and `account-monitoring-streams` that `nsc` gives one. While the servers cannot be asked at its first signing, its `NatsOperator` has the condition `RevocationsUnrecovered` True and the system account JWT is not pushed.
- An account JWT signed by a NATS operator key that no server lists in the NATS operator JWT it runs under is not pushed, and `status.distribution` does not count a server that does not list the key; the `NatsAccount` or `NatsSystemAccount` then has the condition `Distributed` False with the reason `UntrustedSigner`.
- A `NatsAccount` that adopts an account revoking every user, with the key `*`, is signed with that revocation and records it in `status.revocations` under the `publicKey` `*`.
- Adding `auth` to a running `NatsCluster` rolls out one server at a time.

## [0.3.1] - 2026-10-04

### Changed

- The metrics exporter of a `NatsCluster`'s servers is prometheus-nats-exporter 0.20.2.

## [0.3.0] - 2026-10-04

### Changed

- **Breaking:** every API group moves from `nats.mikluko.io` to `nats-operator.io`: `nats-operator.io`, `auth.nats-operator.io`, `cluster.nats-operator.io` and `jetstream.nats-operator.io`. Finalizers, annotations, labels, JetStream ownership metadata and Lease names move with them. Objects and CRDs of the old groups are not served or migrated.

## [0.2.0] - 2026-10-04

### Added

- `NatsCluster.spec.auth.accountTrustRefs` names `NatsAccountTrust`s whose account JWTs every server preloads beside the system account's, on a `NatsCluster` that is not a leaf as on one that is.
- A `NatsCluster`'s `gateway.service` and `leafnodes.service` take `loadBalancerSourceRanges` and `loadBalancerClass`, set on the LoadBalancer Service they render; each is refused unless `type` is LoadBalancer, and `loadBalancerClass` is refused when it changes while `type` stays LoadBalancer.

## [0.1.1] - 2026-10-02

### Added

- The documentation site has a front page: what each controller does and the kinds it owns, the install command, and the list of stories.

### Fixed

- A documentation page's footer shows the date of the page's last change, where it read `0001-01-01`.
- The documentation site's theme menu shows its labels in full in Firefox.

## [0.1.0] - 2026-09-29

### Added

- Deleting the `NatsReferenceGrant` that admits a `NatsCluster`'s leaf remote to its `NatsConnection` drops that remote from the rendered leafnodes config and its credentials from `<name>-leaf-remotes`, the other remotes kept, and `LeafnodesConnected` reads False with the refusal naming the remote; restoring the grant renders it back.
- Deleting the `NatsReferenceGrant` that admits a `NatsAccount` to its `NatsOperator` deletes the account's JWT from the servers and empties its `status.jwt`, and its `NatsUser`s read `Ready` False, reason `AccountNotAdmitted`, and are not signed; restoring the grant signs both again.
- Licensed under Apache-2.0; the chart carries `artifacthub.io/license: Apache-2.0`.
- `SECURITY.md` states how to report a vulnerability through the repository's GitHub private vulnerability reporting, the acknowledgement time for a report, the supported versions, the trust boundaries between namespaces, and how to verify a release's signatures and provenance.
- Each release's controller images and chart are signed keylessly with cosign and carry a GitHub build provenance attestation.
- The cluster controller caches only the StatefulSets, ConfigMaps, Services, PersistentVolumeClaims, PodDisruptionBudgets and NetworkPolicies labelled `cluster.nats.mikluko.io/cluster`; every controller lists and watches only the metadata of Secrets and reads their data from the API server.
- A controller's `/readyz` passes once it has listed and watched everything it reconciles from, on every replica, elected or not.
- Every controller's OpenTelemetry resource carries its host name as `service.instance.id`, unless `OTEL_RESOURCE_ATTRIBUTES` sets one.
- Every controller logs its release version at startup and carries it as `service.version` of its OpenTelemetry resource.
- Every controller serves its Prometheus metrics over HTTPS, to a bearer token of a user allowed `get` on the non-resource URL `/metrics`; an allow is cached for five minutes and a denial for thirty seconds.
- Chart value `metrics.scraper.serviceAccount`, the `namespace/name` of a ServiceAccount granted `get` on `/metrics`.
- Chart value `metrics.prometheus.enabled` serves each controller's OpenTelemetry metrics on port `9464` over plain HTTP, through the metrics Service and ServiceMonitor.
- Chart value `metrics.tls.secretName` has every controller serve its metrics under the certificate in that Secret, through the flag `--metrics-cert-dir`, and each ServiceMonitor verify it against the Secret's `ca.crt` or `metrics.serviceMonitor.caSecret`.
- Chart value `metrics.serviceMonitor.authorization.credentials` has each ServiceMonitor scrape with the bearer token in a Secret key, as `authorization`, in place of `bearerTokenFile`.
- Chart values `networkPolicy.enabled` and `networkPolicy.from` render a NetworkPolicy over each controller's pods admitting its metrics ports `8080` and `9464` only from `networkPolicy.from`.
- Chart value `networkPolicy.egress` adds egress rules to each controller's NetworkPolicy, which then admits no other egress; it requires `networkPolicy.enabled`.
- `helm test` on the chart checks every enabled controller's `/readyz`.
- A failed `NatsCluster` reconcile reads `Progressing=False, reason: ReconcileFailed` and records a `ReconcileFailed` Warning event.
- The cluster controller leaves untouched any object of a name it renders that it does not control, and the `NatsCluster` reads `Ready=False, reason: ReconcileFailed` naming each one.
- Removing a `NatsCluster` server deletes its data volume claim only when the claim carries the server's labels, and its ConfigMap only when the `NatsCluster` controls it; the `NatsCluster` reads `Ready=False, reason: ReconcileFailed` naming each one left.
- `NatsCluster` `status.version` moves only once every server `replicas` plans reports the new version; a server being removed does not count.
- `NatsCluster` `status.removals` names each server being removed or replaced, its phase and since when; a replaced server is recreated only once its old volume claim is gone.
- CRDs for every kind at `v1beta1`, under `config/crd/`, in the API groups `nats.mikluko.io`, `cluster.nats.mikluko.io`, `auth.nats.mikluko.io` and `jetstream.nats.mikluko.io`.
- The API server refuses mutually exclusive fields set together, a `NatsCluster` version below 2.15.0 or a move of more than one minor, and changes nats-server would refuse to the immutable fields of JetStream objects.
- Helm chart `charts/nats-operator` installing the CRDs and any subset of the three controllers, each with its own ServiceAccount and a ClusterRole holding only the verbs it uses.
- The chart requires Kubernetes 1.33 or later.
- The chart refuses to render a controller with more than one replica while `leaderElection.enabled` is false.
- The chart sets each controller's `GOMEMLIMIT` to 90% of its memory limit, where one is set, unless `env` names it, and refuses a memory limit other than an integer with an optional `k`, `M`, `G`, `T`, `Ki`, `Mi`, `Gi` or `Ti` suffix.
- The chart names each controller's leader election lease `<release>-<API group>`.
- The chart refuses an `extraArgs` entry, global or per controller, setting `--leader-elect` or `--leader-election-id`.
- Each controller's leader election Role grants creating Leases, and getting, updating and patching only its own lease.
- The chart refuses to render a Service whose name is longer than 63 characters.
- The chart refuses a value key it does not know, checked against `values.schema.json`.
- Chart values `nodeSelector`, `annotations`, `podAnnotations` and `affinity`, globally and per controller.
- Chart values `tolerations`, `priorityClassName`, `topologySpreadConstraints`, `extraArgs` and `env`, globally and per controller.
- Chart values `metrics.service.enabled` and `metrics.serviceMonitor.enabled`, off by default, render a metrics Service and a prometheus-operator ServiceMonitor per controller; the ServiceMonitor skips certificate verification.
- Chart value `auth.systemConnection`, passed to the auth controller as `--system-connection`.
- A `NatsCluster` with a gateway without `tls` is `Ready` `False`, reason `GatewayWithoutTLS`, and nothing is rendered for it, unless the cluster controller runs with `--allow-gateway-without-tls`.
- Chart value `cluster.allowGatewayWithoutTLS`, off by default, passes `--allow-gateway-without-tls` to the cluster controller.
- Every controller's `--watch-namespaces` confines it to the namespaces named; chart value `watchNamespaces` passes it and grants each controller a Role in each of those namespaces, its ClusterRole keeping only `tokenreviews` and `subjectaccessreviews`.
- Chart values `cluster.image.digest`, `auth.image.digest` and `jetstream.image.digest` pin a controller's image by digest after its tag while that tag is the chart's `appVersion`; `tests.image.digest` pins the `helm test` image after its tag.
- Chart value `tests.image.digest` defaults to the digest of `busybox:1.37.0`.
- Each release's chart sets `cluster.image.digest`, `auth.image.digest` and `jetstream.image.digest` to the digests of the images released with it.
- `NatsCluster` `spec.image` takes `repository` and `digest`, and `spec.exporter.image` takes `repository`, `tag` and `digest`; a digest is rendered after the tag.
- A `NatsCluster`'s servers prefer distinct nodes, unless `spec.podTemplate` sets `affinity`.
- The exporter sidecar requests 10m CPU and 32Mi memory and is limited to 100m CPU and 128Mi memory; `NatsCluster` `spec.exporter.resources` replaces both.
- The exporter sidecar's default image is pinned by digest.
- A controller releases its leader-election lease as it shuts down.
- Each release publishes the three controller images for linux/amd64 and linux/arm64, the chart as an OCI artifact, and a GitHub release carrying the version's changelog entry.
- Documentation site at <https://mikluko.github.io/nats-operator/>, of the latest release: the stories, the design and the ADRs.
- Documentation page `/docs/install/`: installing the chart, its values, the controllers' flags and RBAC, upgrade and uninstall.
- Documentation page `/docs/reference/api/`: every kind, field and enum value of the four API groups.
- Documentation page `/docs/reference/nats-permissions/`: the nats-server subjects each controller requests and the presets that grant them.
- Documentation page `/docs/reference/telemetry/`: the controllers' OpenTelemetry configuration, instruments, spans and Kubernetes events.
- The auth controller exports `nats_operator.account.jwt_expiry`, when each `NatsAccount`'s current JWT expires.
- Each controller exports OpenTelemetry metrics and traces once the SDK's environment names an exporter or endpoint.
- The 0/1 gauges `nats_operator.condition` and `nats_operator.rollout.gate` carry no unit, and Prometheus reads them as `nats_operator_condition` and `nats_operator_rollout_gate`.
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
- A `NatsCluster` renders a NetworkPolicy admitting its route port only from its own pods, its monitoring port only from the cluster controller's namespace, and its metrics port from there and the peers `spec.exporter.from` names; `monitor.networkPolicy: false` renders none.
- `NatsCluster` `spec.exporter.from`, the NetworkPolicy peers admitted to the metrics port.
- A `NatsCluster`'s pods meet the restricted Pod Security Standard and mount no ServiceAccount token; `podTemplate` can still escalate what the pod runs.
- A change to the trusted NATS operator, `system_account` or the resolver restarts servers one at a time; a re-signed system account JWT reloads them.
- `NatsCluster` `tls` puts the client listener under TLS, from a Secret or cert-manager, and `status.endpoints.client` then reads `tls://`.
- `NatsCluster` `gateway` joins a supercluster, and status reports `GatewaysConnected`.
- Gateway TLS requires `ca.crt` in the certificate Secret and verifies peers both ways against it.
- `NatsCluster` `leafnodes` accepts leaf connections on port 7422, and is refused at apply without `auth`.
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
- A `NatsUser` whose `NatsReferenceGrant` is deleted is revoked and loses the creds Secret it owns; restoring the grant signs it under a fresh key.
- Deleting a `NatsUser` revokes it and, with `--system-connection` set, closes its connections before its creds Secret is removed, unless its account or the account's `NatsOperator` no longer exists.
- `--system-connection` on the auth controller pushes account JWTs to the servers, and `status.distribution` reports how many hold the current one.
- Deleting a `NatsAccount` deletes it from the servers' resolvers, also on servers that join later.
- `NatsAccount` and `NatsSystemAccount` `status.revocations` list the user keys the account revokes.
- An account whose status lost its JWT and revocations recovers them from the servers before it is signed again.
- The JetStream controller creates, adopts, updates and deletes the streams and consumers `NatsStream` and `NatsConsumer` declare, and corrects drift every `--resync-period`.
- `NatsStream` `status.transfer` reports a move to another NATS cluster while it runs.
- Deleting a JetStream resource whose `NatsConnection` is gone or no longer admitted leaves its server object.
- The JetStream controller manages the key-value buckets and object stores `NatsKeyValue` and `NatsObjectStore` declare.
- `NatsStream`, `NatsConsumer`, `NatsKeyValue` and `NatsObjectStore` `spec.connectionRef`, and `NatsConsumer` `spec.stream` and `spec.streamRef`, cannot change after creation.
- A `NatsObjectStore` whose stream `OBJ_<bucket>` exists without the object store's subjects or rollups goes Terminal, reason `NotABucket`, and the stream is left untouched.
- A JetStream object the servers refuse as invalid goes Terminal; one they cannot place is retried.
- `NatsSystemBalancer` evens leaders, and optionally copies, across the servers of one NATS cluster over every account.
- A `NatsSystemBalancer` with `moves.leader: false` probes no account's `jetstream-stepdown` export and leaves `status.capabilities.leader` unset.
- `NatsBalancer` evens leaders, and optionally copies, within pools of one account's streams.
- `NatsClusterEvacuation` moves every JetStream object whose resource sets no `placement.cluster` off one NATS cluster to servers carrying the target tags.
- `NatsClusterEvacuation` `spec.connectionRef`, `spec.from` and `spec.to` cannot change after creation.
- `NatsClusterEvacuation` `status.remaining` counts the streams still to leave the source NATS cluster.
- A `NatsStream` whose stream matches spec but whose transfer or consumers cannot be read reads `Ready` False, reason `ObserveFailed`, and is retried with backoff.
- The API server refuses a `NatsBalancer` or `NatsSystemBalancer` `interval` that is not positive.
- A `NatsAccount` or `NatsUser` whose `publicKey` another account or user under the same `NatsOperator` holds reads `Ready` False, reason `PublicKeyInUse`.
- A `NatsAccount` whose key is the identity of the `NatsSystemAccount` its `NatsOperator` references, as that account's status, `publicKey` or identity seed Secret gives it, reads `Ready` False, reason `PublicKeyInUse`.
- `NatsAccount` and `NatsSystemAccount` `status.distribution.lastPushTime` is set only by a push a server acknowledged.
- Generated seed Secrets are named `<name>-<operator|systemaccount|account>-<identity|signing-1>` and annotated `auth.nats.mikluko.io/generated-for`; one not annotated for the object reads `Ready` False, reason `SecretConflict`.
- Generated seed Secrets carry no owner reference: they stay when their object is deleted, and an object of the same kind and name applied again takes the same keys.
- A generated identity Secret lost after `status.publicKey` recorded its key reads `Ready` False, reason `SeedLost`, and no new identity is minted.
- A key a `NatsUser` stops holding is revoked in its account and listed in `status.replacedKeys` until the account JWT carries the revocation.
- A `NatsUser` whose `publicKey` changes to a key it is refused revokes the key it held.
- A `NatsAccount` no `NatsReferenceGrant` has yet admitted to its `NatsOperator` records no `status.publicKey`, and only the `NatsSystemAccount` a `NatsOperator` references holds a key against its accounts.
- Revocation recovery reads the servers' JWT only when every server of the roster answers.
- A server stays in the auth controller's roster until it misses three STATSZ polls in a row, counted in `status.distribution` and awaited by revocation recovery and user deletion.
- A `NatsAccount`, `NatsSystemAccount` or `NatsUser` reads `Distributed` False, reason `NoSystemConnection`, while the auth controller runs without `--system-connection`.
- An auth object whose reconcile fails reads `Ready` False, reason `ReconcileError`, and keeps `status.observedGeneration` at the generation last reconciled in full.
- `NatsClusterEvacuation` makes no move while any server of its NATS system is down or the meta group has no leader, reporting `Ready` and `Progressing` False with reason `ServersDown`.
- The `NatsSystemBalancer` and the `NatsBalancer`s of one NATS cluster move one at a time; while one's move is in flight the others read `Holding`, reason `MoveLeaseHeld`.
- A `NatsSystemBalancer` or `NatsBalancer` makes no move while any member of any group in its scope is not current, any server of its NATS cluster does not answer every page of JSZ, or the meta group has no leader.
- The `cluster-controller`, `jetstream-controller` and `auth-controller` presets subscribe only to their own inbox, `_INBOX.<preset>.>`; a JetStream controller account user whose `permissions` restrict subscriptions must allow `_INBOX.jetstream-controller.>`.
- The `readonly` preset subscribes only to its own inbox, `_INBOX.readonly.>`, which its client dials with.

[Unreleased]: https://github.com/mikluko/nats-operator/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/mikluko/nats-operator/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/mikluko/nats-operator/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/mikluko/nats-operator/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/mikluko/nats-operator/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/mikluko/nats-operator/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mikluko/nats-operator/releases/tag/v0.1.0
