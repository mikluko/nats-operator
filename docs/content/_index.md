---
title: nats-operator
params:
  lead: nats-operator is three Kubernetes controllers that deploy NATS clusters, own their auth plane, and manage and balance JetStream. It is for platform engineers who run NATS on Kubernetes. You can install all three controllers or only the ones you need.
  actions:
    - name: Quickstart
      icon: rocket
      page: /docs/stories/01-quickstart
      primary: true
    - name: Install
      icon: download
      page: /docs/install
    - name: API reference
      icon: book
      page: /docs/reference/api
    - name: GitHub
      icon: github
      url: https://github.com/mikluko/nats-operator
  controllersHeading: Controllers
  controllers:
    - name: Cluster controller
      icon: network
      text: Deploys a NATS cluster from one `NatsCluster` and restarts it one server at a time. It also joins NATS clusters into a supercluster, or to a hub as leaves.
      kinds: [NatsCluster]
      story: /docs/stories/01-quickstart
    - name: Auth controller
      icon: key
      text: Creates the JWT auth plane, which is a NATS operator with its accounts and users. It distributes the auth plane to the servers and writes each user's credentials to a Secret.
      kinds: [NatsOperator, NatsSystemAccount, NatsAccount, NatsUser]
      story: /docs/stories/02-auth-plane
    - name: JetStream controller
      icon: layers
      text: Manages streams, consumers, key-value buckets and object stores, and balances JetStream leadership and placement. It works through a `NatsConnection`, so the NATS cluster can be one that the cluster controller did not deploy.
      kinds: [NatsStream, NatsConsumer, NatsKeyValue, NatsObjectStore, NatsBalancer, NatsSystemBalancer, NatsClusterEvacuation]
      story: /docs/stories/03-unmanaged
  areasHeading: Documentation
  areas:
    - page: /docs/install
      text: The chart's values, the controllers' flags, metrics and RBAC, and how to upgrade and uninstall.
    - page: /docs/stories
      text: One task per page, with the manifests to apply and the status to expect. The end-to-end harness applies the same manifests to kind clusters and waits for the same statuses.
    - page: /docs/reference/api
      text: Every kind, field and value of the four API groups.
    - page: /docs/reference/nats-permissions
      text: The nats-server subjects each controller requests, and the presets that grant them.
    - page: /docs/reference/telemetry
      text: The OpenTelemetry metrics and traces each controller exports, and the Kubernetes events the controllers record.
    - page: /docs/design/v1
      text: The scope, the architecture, the resource API and how each controller works.
    - page: /docs/adr
      text: The design decisions that are hard to reverse, one record each.
  storiesHeading: Stories
---

## Install

```sh
helm install nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
  --version <version> \
  --namespace nats-operator --create-namespace
```

Replace `<version>` with a release version. You need Kubernetes 1.33, Helm 3.14 and nats-server 2.15.0, or later.

The images and the chart are signed keylessly with cosign and carry a GitHub build provenance attestation.

[Install]({{< relref "docs/install" >}}) covers the values, RBAC, upgrade and uninstall. Then [the quickstart]({{< relref "docs/stories/01-quickstart" >}}) deploys a NATS cluster with JetStream.
