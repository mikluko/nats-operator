# docs

The Hugo site published at <https://mikluko.github.io/nats-operator/>, on the [Hudocs](https://hudocs.com/) theme. The theme is a Hugo module pinned in `go.mod`, so a build needs Go on `PATH` to download it, and Hugo extended. `hugo.toml` mounts `design/` and `adr/` into the site's content under `content/docs/`, so each document has one source.

[`STYLE.md`](STYLE.md) is the writing style for the site's pages.

Build locally from this directory with `hugo server`, or `hugo` into `public/`. `go test ./hack -run TestSite` from the repository root builds the site and fails on a page with more than one `h1`, a redirect to a page the site lacks, a header that does not draw its logo icon, a page served from a story's `e2e/` directory, a page with no Markdown rendition or no alternate link to it, a rendition that holds an unrendered shortcode, or a page under `docs/` that `llms.txt` does not link, and `TestSiteIconSet` fails when `assets/meteor-icons/icons.json` is not the pinned meteor-icons release's; `just site-check` checks its links and fragments with lychee.

Every page is also rendered as Markdown, `index.md` beside its `index.html`, and the home page as `llms.txt`, which links the Markdown of every page under `docs/`. A page's head names its renditions in `rel="alternate"` links.

`.github/workflows/docs.yml` builds the site on every pull request and deploys it on every push to `main`. Run by hand, it keeps the build as the run's `github-pages` artifact and deploys nothing. The deployment needs one repository setting, made once: **Settings → Pages → Build and deployment → Source: GitHub Actions**.
