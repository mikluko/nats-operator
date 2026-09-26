---
title: Leaf nodes at the edge
weight: 10
---

An edge site runs a small NATS cluster in its own Kubernetes cluster. It joins the production hub as a leaf: local clients publish telemetry that reaches the hub's `telemetry` account, and a local stream keeps accepting while the link is down. The leaf is not a supercluster member and has no auth plane of its own.

## The hub

The hub's NatsCluster gains a leafnode listener, and the edge site gets a user in the account its traffic belongs to, usable only as a leaf.

{{< manifest "hub.yaml" >}}

## The edge

A leaf is a NatsCluster that dials out through a NatsConnection, the same kind the JetStream controller uses. Its JetStream runs in a domain of its own.

{{< manifest "edge.yaml" >}}

{{< manifest "status-natscluster-edge.yaml" >}}

## An edge that enforces the hub's accounts

A second site trusts the hub's NATS operator, so its clients authenticate against the hub's accounts locally. It reads the same trust roots the supercluster members do, and resolves accounts over a second remote bound to the system account; without that remote it cannot fetch an account it has not cached.

{{< manifest "edge-operator.yaml" >}}
