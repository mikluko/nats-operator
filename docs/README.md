# docs

The Hugo site published at <https://mikluko.github.io/nats-operator/>. `hugo.toml` mounts `design/` and `adr/` into the site's content beside `content/`, so each document has one source.

Build locally from this directory with `hugo server`, or `hugo` into `public/`.

`.github/workflows/docs.yml` builds the site on every pull request and deploys it on every push to `main`. The deployment needs one repository setting, made once: **Settings → Pages → Build and deployment → Source: GitHub Actions**.
