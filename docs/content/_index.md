---
title: nats-operator
params:
  hero:
    eyebrow: Kubernetes controllers for NATS
    headline: Run NATS on Kubernetes from manifests
    lead: Three controllers deploy NATS clusters, own their auth plane, and manage and balance JetStream. Install all three, or only the one you need.
    actions:
      - name: Quickstart
        icon: rocket
        page: /docs/stories/01-quickstart
        primary: true
      - name: Install
        icon: download
        page: /docs/install
      - name: GitHub
        icon: github
        url: https://github.com/mikluko/nats-operator
  controllersHeading: One controller per concern
  controllers:
    - name: Cluster controller
      icon: network
      text: Deploys a NATS cluster from one `NatsCluster`, rolls it one server at a time, and joins NATS clusters into a supercluster or to a hub as leaves.
      kinds: [NatsCluster]
      story: /docs/stories/01-quickstart
    - name: Auth controller
      icon: key
      text: Mints the JWT auth plane, a NATS operator with its accounts and users, distributes it to the servers, and writes each user's credentials to a Secret.
      kinds: [NatsOperator, NatsSystemAccount, NatsAccount, NatsUser]
      story: /docs/stories/02-auth-plane
    - name: JetStream controller
      icon: layers
      text: Manages streams, consumers, key-value buckets and object stores, and balances JetStream leadership and placement, through a `NatsConnection` to any NATS cluster.
      kinds: [NatsStream, NatsConsumer, NatsKeyValue, NatsObjectStore, NatsBalancer, NatsSystemBalancer, NatsClusterEvacuation]
      story: /docs/stories/03-unmanaged
  featuresHeading: What you get
  features:
    - name: Install only what you need
      icon: cube
      text: Each controller has its own switch in the chart and its own ServiceAccount, ClusterRole and Deployment.
    - name: Bring your own NATS
      icon: plug
      text: The JetStream controller works through a `NatsConnection`, so it manages JetStream on a NATS cluster it did not deploy.
    - name: Namespaced, with explicit grants
      icon: lock
      text: Every kind is namespaced. A reference crosses into another namespace only where a `NatsReferenceGrant` there admits it.
    - name: Superclusters and leaf nodes
      icon: globe
      text: Gateways join NATS clusters across Kubernetes clusters, and leaf nodes connect the edge to a hub.
    - name: Day-two operations as resources
      icon: sliders
      text: Balance stream leaders and placement, move a stream to another NATS cluster, or retire a NATS cluster by evacuating it.
    - name: Observable
      icon: wave-pulse
      text: The controllers export OpenTelemetry metrics, record Kubernetes events, and report progress in status conditions.
    - name: Signed releases
      icon: shield
      text: The images and the chart are signed keylessly with cosign and carry a GitHub build provenance attestation.
    - name: Tested as documented
      icon: badge-check
      text: The stories on this site are the end-to-end suite. The harness applies each story's manifests to kind clusters and waits for the statuses its pages show.
  storiesHeading: Learn it one story at a time
  storiesLead: Each story is a user's problem, the manifests that solve it, and the status to expect.
---

```sh
helm install nats-operator oci://ghcr.io/mikluko/nats-operator/charts/nats-operator \
  --version <version> \
  --namespace nats-operator --create-namespace
```

Needs Kubernetes 1.33, Helm 3.14 and nats-server 2.15.0, or later. [Install]({{< relref "docs/install" >}}) covers the values, RBAC, upgrade and uninstall.
