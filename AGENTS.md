# nats-operator

Three Kubernetes controllers that deploy NATS clusters, own their auth plane, and manage and balance JetStream. `CONTEXT.md` is the glossary and `docs/design/v1.md` is the design; both are read before anything is written, and "operator" unqualified is not used.

## Changelog

`CHANGELOG.md` is Keep a Changelog 2.0.0, newest version first. An entry states what is different for the reader and nothing else: no reason, no history, no account of the work. Those belong in the commit and the pull request. An entry that needs a *because* is cut back to the change, and if nothing is left it is not an entry.

    - `NatsCluster` renders one StatefulSet per server.                          entry
    - Switched to one StatefulSet per server, because a shared one could not     not an entry
      change its volume template.

## Releases

The changelog decides the version. Cutting one is renaming `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and opening a fresh `[Unreleased]` above it; `mikluko/action-changelog` validates the file on every pull request, and the release workflow, once ci passes on a push to `main`, tags and publishes the newest entry that is neither tagged nor has its chart in the registry; run by hand on a ref whose newest version is neither, it builds without publishing. Nothing else sets a version. The first release leaves its ghcr packages private, the default for a personal account's new packages, until each is made public, and the documentation site needs the repository's Pages source set to GitHub Actions before its first deploy.

## Fenced acts

These are performed only by an attended session, or by an unattended run the maintainer has released a ticket to; everything else is prepared and left for a human:

- merging into `main`
- cutting a version in `CHANGELOG.md`, and tagging a release
- publishing images, the Helm chart, or the documentation site
