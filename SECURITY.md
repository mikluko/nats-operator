# Security

## Reporting a vulnerability

Report a vulnerability privately, through [GitHub's private vulnerability reporting](https://github.com/mikluko/nats-operator/security/advisories/new) on this repository.
Do not open a public issue.
You get an acknowledgement within seven days.

## Supported versions

Only the latest release is supported.
A security fix ships as a new release.

## Trust boundaries

Every kind is namespaced.
A reference crosses into another namespace only where a `NatsReferenceGrant` in that namespace allows it.
[Design section 7](docs/design/v1.md#7-tenancy) describes the tenancy model.

- A namespace granted `NatsAccount`s to a `NatsOperator` gets accounts signed under that NATS operator with the limits its `NatsAccount`s declare.
  It can also take any account key that no `NatsAccount` records yet.
- A namespace granted `NatsUser`s to a `NatsAccount` can claim and revoke any user key of that account, including keys issued outside the auth controller.
- A namespace granted a `NatsConnection` acts with that connection's credentials.
  For a leaf remote of the `NatsCluster` `<name>`, the namespace holds a copy of them in the Secret `<name>-leaf-remotes`.
  Withdrawing the grant does not revoke a copy already handed out; rotating the credentials does.
  Any hold on the `NatsCluster` keeps a withdrawn remote's copy in that Secret until the hold clears.
- Withdrawing a grant that allows a `NatsCluster` to reference a `NatsOperatorTrust` or a `NatsAccountTrust` holds the `NatsCluster` at its last render.
  Both kinds contain only public material.
- Anyone who can write a `NatsOperator`, `NatsAccount` or `NatsSystemAccount` can sign with any seed stored in a Secret in its namespace, without permission to read Secrets.
  `keys.identity` and `keys.signing` can refer to any Secret in that namespace.
- Anyone who can write a `NatsCluster` can run pods in its namespace with any privilege that the namespace allows.
  `spec.podTemplate` is merged over the rendered pod.
- Anyone who can write a `NatsCluster` in any watched namespace can have the cluster controller request a certificate from any ClusterIssuer, for any host that its `gateway` and `leafnodes` name.
  Gateway trust that rests on the certificate alone must come from an Issuer in the NATS cluster's own namespace, or from an issuer behind cert-manager's approver-policy.
- Each controller's ServiceAccount can get, list and watch every Secret, data included, in the namespaces the controller watches.
  Without `watchNamespaces`, that is every namespace.
  The auth and cluster controllers' ServiceAccounts can also create, update and delete those Secrets.
  The controllers watch the metadata of Secrets only, which limits what they cache and not what their RBAC permits.
- Anyone who can write a `NatsConnection` can have the JetStream controller dial any address in its `spec.servers`, and can read from its `Ready` condition whether that address answered.
  The JetStream controller dials from the release namespace, outside any egress policy of the `NatsConnection`'s namespace.
  The chart's `networkPolicy.egress` limits where the controllers connect.
- The chart's `watchNamespaces` confines every controller, and its RBAC, to the namespaces it lists.
  A Kubernetes cluster shared between tenants must be installed with it set.

## Verifying a release

The release workflow signs each release's three controller images and its chart keylessly with cosign, and gives each a GitHub build provenance attestation.
To verify them, you need [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) and the [GitHub CLI](https://cli.github.com/).
In the commands below, replace `<version>` with a release's version without the `v`, such as `0.1.1`.

The commands verify a release later than 0.3.1, which is built from its tag.
Releases 0.1.0 through 0.3.1 were built from `main` by another workflow file.
To verify one of them, see [Releases 0.1.0 through 0.3.1](#releases-010-through-031).

1. Verify the signatures of the images and the chart:

   ```sh
   for c in cluster-controller auth-controller jetstream-controller; do
     cosign verify "ghcr.io/mikluko/nats-operator/$c:<version>" \
       --certificate-identity "https://github.com/mikluko/nats-operator/.github/workflows/release-roll.yaml@refs/tags/v<version>" \
       --certificate-oidc-issuer https://token.actions.githubusercontent.com
   done

   cosign verify "ghcr.io/mikluko/nats-operator/charts/nats-operator:<version>" \
     --certificate-identity "https://github.com/mikluko/nats-operator/.github/workflows/release-roll.yaml@refs/tags/v<version>" \
     --certificate-oidc-issuer https://token.actions.githubusercontent.com
   ```

1. Verify the build provenance attestation of the cluster controller's image:

   ```sh
   gh attestation verify "oci://ghcr.io/mikluko/nats-operator/cluster-controller:<version>" \
     --repo mikluko/nats-operator \
     --signer-workflow mikluko/nats-operator/.github/workflows/release-roll.yaml \
     --source-ref "refs/tags/v<version>"
   ```

   To verify another, replace `cluster-controller` with `auth-controller`, `jetstream-controller` or `charts/nats-operator`.

### Releases 0.1.0 through 0.3.1

Run the same commands with these arguments changed:

- For `cosign verify`, the value of `--certificate-identity` is `https://github.com/mikluko/nats-operator/.github/workflows/release.yml@refs/heads/main`.
- For `gh attestation verify`, the value of `--signer-workflow` is `mikluko/nats-operator/.github/workflows/release.yml`, and the value of `--source-ref` is `refs/heads/main`.
