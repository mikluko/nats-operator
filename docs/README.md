# docs

The Hugo site published at <https://mikluko.github.io/nats-operator/>, on the [Hudocs](https://hudocs.com/) theme. The theme is a Hugo module pinned in `go.mod`, so a build needs Go on `PATH` to download it, and Hugo extended. `hugo.toml` mounts `design/` and `adr/` into the site's content under `content/docs/`, so each document has one source.

Build locally from this directory with `hugo server`, or `hugo` into `public/`. `go test ./hack -run TestSite` from the repository root builds the site and fails on a page with more than one `h1`, a redirect to a page the site lacks, a header that does not draw its logo icon, or a page served from a story's `e2e/` directory, and `TestSiteIconSet` fails when `assets/meteor-icons/icons.json` is not the pinned meteor-icons release's; `just site-check` checks its links and fragments with lychee.

`.github/workflows/docs.yml` builds the site on every pull request; the release workflow calls it to deploy the site at the commit each release tags. Run by hand, it keeps the build as the run's `github-pages` artifact and deploys nothing. The deployment needs one repository setting, made once: **Settings → Pages → Build and deployment → Source: GitHub Actions**.
