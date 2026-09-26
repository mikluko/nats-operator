---
title: Balancing JetStream
weight: 7
---

Unevenly placed leaders and copies slow the whole NATS cluster, not only the account that owns them. The platform team balances across every account; an application team refines within pools of its own streams, so that hot streams spread among themselves instead of piling onto one server while the leader count still looks even. The two are layered: the account balancer yields to the system one, and both wait while the NATS cluster is not Settled.

## Across the NATS cluster

The system balancer runs on system credentials. Placement moves work for any account; leader moves work for accounts that grant them with an export preset.

{{< manifest "system.yaml" >}}

{{< manifest "account-export.yaml" >}}

Its status says what it can do, how uneven each server is, and the last move it made.

{{< manifest "status-natssystembalancer.yaml" >}}

## Within an account

The account balancer runs on the account's own connection and judges evenness per pool. Pools select streams, key-value buckets and object stores by label.

{{< manifest "account.yaml" >}}

When the system balancer has a move pending on one of its streams, it holds:

{{< manifest "status-natsbalancer.yaml" >}}
