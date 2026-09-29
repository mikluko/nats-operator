# nats-operator

Three Kubernetes controllers that deploy NATS clusters, own their auth plane, and manage and balance JetStream. `CONTEXT.md` is the glossary and `docs/design/v1.md` is the design; both are read before anything is written, and "operator" unqualified is not used.

## Changelog

`CHANGELOG.md` is Keep a Changelog 2.0.0, newest version first. An entry states what is different for the reader and nothing else: no reason, no history, no account of the work. Those belong in the commit and the pull request. An entry that needs a *because* is cut back to the change, and if nothing is left it is not an entry.

    - `NatsCluster` renders one StatefulSet per server.                          entry
    - Switched to one StatefulSet per server, because a shared one could not     not an entry
      change its volume template.

## Releases

The changelog decides the version. Cutting one is renaming `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and opening a fresh `[Unreleased]` above it. `mikluko/action-changelog` validates the file on every pull request. Once ci passes on a push to `main`, the release workflow tags and publishes the newest entry neither tagged nor with its chart in the registry, and fails on one whose chart is in the registry but untagged; run by hand, it builds a due version without publishing. Nothing else sets a version.

Before the first release is announced, a human:

- runs `release` by hand on a branch whose `CHANGELOG.md` has the version cut, before cutting it on `main`; the run publishes nothing and proves the plan
- makes the ghcr packages `nats-operator/cluster-controller`, `nats-operator/auth-controller`, `nats-operator/jetstream-controller` and `nats-operator/charts/nats-operator` public; new packages of a personal account are private
- sets the repository's Pages source to GitHub Actions, before the release's docs job deploys
- turns on private vulnerability reporting, which `SECURITY.md` links to

## Fenced acts

These are performed only by an attended session, or by an unattended run the maintainer has released a ticket to; everything else is prepared and left for a human:

- merging into `main`
- cutting a version in `CHANGELOG.md`, and tagging a release
- publishing images, the Helm chart, or the documentation site
