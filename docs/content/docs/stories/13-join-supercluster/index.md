---
title: Joining a supercluster you did not deploy
weight: 13
params:
  e2e:
    waits:
      - {step: 0, wait: 2m, reason: "central's servers start"}
      - {step: 1, wait: 4m, reason: "west's servers start and connect central's gateway, and the meta group spanning both elects a leader"}
    substitutions:
      - files: [01-natsoperatortrust.yaml]
        reason: the JWTs of the NATS operator and system account the existing NATS cluster in e2e/00-central.yaml trusts, from internal/e2e/fixtures
        patchFile: e2e/natsoperatortrust.json
      - files: [01-natsaccounttrusts.yaml]
        kind: NatsAccountTrust
        name: orders
        reason: the key and JWT of the orders account e2e/00-central.yaml preloads, from internal/e2e/fixtures
        patchFile: e2e/natsaccounttrust-orders.json
      - files: [01-natsaccounttrusts.yaml]
        kind: NatsAccountTrust
        name: payments
        reason: the key and JWT of the payments account e2e/00-central.yaml preloads, from internal/e2e/fixtures
        patchFile: e2e/natsaccounttrust-payments.json
      - files: [01-west.yaml]
        reason: three servers share one kind node with central's, with one storage class
        patch:
          spec:
            resources: {requests: {cpu: 100m, memory: 256Mi}, limits: {memory: 256Mi}}
            jetstream: {volumeClaimTemplate: {spec: {storageClassName: standard, resources: {requests: {storage: 5Gi}}}}}
---

A supercluster already runs, and nobody runs these controllers for it: its servers were deployed by other means, its NATS operator is held elsewhere, and nothing pushes account JWTs to its servers. A platform team adds a NATS cluster of its own, `west`, as a new member, and an application team uses one of the supercluster's accounts on it.

## The existing supercluster

Its one member here is `central`, whose server config holds its system account and accounts as static preloads under a `MEMORY` resolver. Its gateway runs in the clear, with no gateway `authorization`, and `reject_unknown` admits no gateway its own list does not name, so whoever runs `central` adds `west` to that list before `west` can join; a `NatsCluster` does not edit a NATS cluster it did not deploy. The part of `central`'s config that concerns `west`:

```text
gateway {
  name: central
  port: 7222
  reject_unknown: true
  gateways: [
    {name: west, urls: ["nats://nats-west.example.net:7222"]}
  ]
}
operator: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...
system_account: ASYS...
resolver: MEMORY
resolver_preload: {
  ASYS...: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...
  AORDERS...: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...
  APAYMENTS...: eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...
}
```

## Trust roots and accounts

`west` boots from the same trust roots as `central`, copied into a literal `NatsOperatorTrust`.

{{< manifest "01-natsoperatortrust.yaml" >}}

Nothing pushes account JWTs to this supercluster, so `west` preloads them too, from literal `NatsAccountTrust`s holding the same JWTs `central` preloads.

{{< manifest "01-natsaccounttrusts.yaml" >}}

## The new member

`auth.accountTrustRefs` names the accounts every server preloads. The servers run a `Full` resolver, which stores each preload in its directory where it holds nothing newer for that account; on the JetStream volume an account whose reference is removed stays in that directory and is still served after the restart the removal causes.

Because `central`'s gateway runs in the clear, the cluster controller has to run with `--allow-gateway-without-tls`, which the chart passes with:

```yaml
cluster:
  allowGatewayWithoutTLS: true
```

{{< manifest "01-west.yaml" >}}

{{< manifest "01-status-natscluster.yaml" >}}

## A stream on the new member

The application team's connection holds a user of the `orders` account, which `west` knows only from its preload.

{{< manifest "02-natsconnection.yaml" >}}

{{< manifest "02-natsstream.yaml" >}}

{{< manifest "02-status-natsstream.yaml" >}}
