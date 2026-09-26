---
title: Team self-service
weight: 4
---

The platform team owns `nats-system`: the NATS operator, the system account, every account and its limits. The payments team owns the `payments` namespace and wants to declare its own users and streams there, without reading anything in `nats-system`.

## What the platform team declares

The account, and a grant that lets users in `payments` attach to it. The grant sits in the namespace it opens up, so only someone who can write there can open it.

{{< manifest "platform.yaml" >}}

## What the payments team declares

Users that reference the account across namespaces, and everything JetStream in its own namespace. Creds Secrets land beside the users.

{{< manifest "team.yaml" >}}

## Without a grant

A user from a namespace no grant covers is refused, and because grants are checked on every reconcile, deleting one later revokes users it had admitted.

{{< manifest "status-natsuser-denied.yaml" >}}
