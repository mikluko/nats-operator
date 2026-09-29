---
title: Leaf nodes at the edge
weight: 10
params:
  e2e:
    waits:
      - {step: 1, wait: 4m, reason: "the hub and both leaves start before the leaf remotes connect"}
    clusters:
      - name: hub
        files: [e2e/00-hub.yaml, 01-hub.yaml]
      - name: edge
        files: [e2e/00-edge.yaml, 01-edge.yaml, 01-edge-operator.yaml, 01-status-natscluster-edge-site-1.yaml]
    substitutions:
      - files: [01-edge-operator.yaml]
        kind: NatsOperatorTrust
        reason: the JWTs of the NATS operator and system account e2e/00-hub.yaml adopts, from hack/e2e-fixtures
        patchFile: e2e/natsoperatortrust.json
      - files: [01-edge-operator.yaml]
        kind: NatsAccountTrust
        reason: the key and JWT of the telemetry account e2e/00-hub.yaml adopts, from hack/e2e-fixtures
        patchFile: e2e/natsaccounttrust.json
      - files: [01-hub.yaml, 01-edge.yaml, 01-edge-operator.yaml]
        kind: NatsCluster
        reason: a Kubernetes cluster is one kind node, on a host every kind cluster of the run shares
        patch: {spec: {resources: {requests: {cpu: 100m, memory: 192Mi}, limits: {memory: 192Mi}}}}
      - files: [01-hub.yaml]
        kind: NatsCluster
        reason: >-
          one kind node, on a host every kind cluster of the run shares, holds three servers, without JetStream and without
          the rest of story 9's supercluster; no cert-manager, so the leafnode listener runs without TLS
        patch: {spec: {replicas: 3, jetstream: null, gateway: null, leafnodes: {tls: null}}}
      - files: [01-edge.yaml]
        kind: NatsConnection
        name: hub
        reason: the hub's leafnode listener runs without TLS
        patch: {spec: {servers: ["nats://leaf.prod-east.acme.example:7422"]}}
      - files: [01-edge-operator.yaml]
        kind: NatsConnection
        name: hub-system
        reason: the hub's leafnode listener runs without TLS
        patch: {spec: {servers: ["nats://leaf.prod-east.acme.example:7422"]}}
      - files: [01-edge-operator.yaml]
        kind: NatsConnection
        name: hub-telemetry
        reason: the hub's leafnode listener runs without TLS
        patch: {spec: {servers: ["nats://leaf.prod-east.acme.example:7422"]}}
---

An edge site runs a small NATS cluster in its own Kubernetes cluster. It joins the production hub as a leaf: local clients publish telemetry that reaches the hub's `telemetry` account, and a local stream keeps accepting while the link is down. The leaf is not a supercluster member and has no auth plane of its own.

## The hub

The hub is story 9's `prod-east`, whose NatsCluster gains a leafnode listener, and the edge site gets a user in the account its traffic belongs to, usable only as a leaf.

{{< manifest "01-hub.yaml" >}}

## The leaf

A leaf is a NatsCluster that dials out through a NatsConnection, the same kind the JetStream controller uses. Its JetStream runs in a domain of its own. A NatsConnection in another namespace needs a NatsReferenceGrant there, and that grant hands the connection's credentials to the leaf's namespace, where the cluster controller copies them into the Secret `<name>-leaf-remotes`.

{{< manifest "01-edge.yaml" >}}

{{< manifest "01-status-natscluster-edge-site-1.yaml" >}}

## A leaf that enforces the hub's accounts

A second site trusts the hub's NATS operator, so its clients authenticate against the hub's accounts locally. It reads the same trust roots the supercluster members do, and resolves accounts over a second remote bound to the system account; without that remote it cannot fetch an account it has not cached.

{{< manifest "01-edge-operator.yaml" >}}
