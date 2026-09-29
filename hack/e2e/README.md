# hack/e2e

The end-to-end harness: it runs every story of the documentation site on kind clusters, applying each story's manifests and waiting for the status its status files show.

## Steps and expectations

`just e2e` runs the story bundles under `docs/content/docs/stories/` against Kubernetes clusters, `E2E_CLUSTERS` of them (see [Running the stories](#running-the-stories)). Every file name starts with a step number, and a step applies its manifests, deletes the objects its `NN-delete-*.yaml` files name, then waits until the live objects match its expectations:

- `NN-status-<kind>[-<qualifier>].yaml` is the `status` stanza `kubectl get -o yaml` would show for the object it names: the story's only object of that kind, or else the one the qualifier names.
- `NN-live-<kind>[-<qualifier>].yaml` is the whole object as `kubectl get -o yaml` shows it.

A live object matches when it holds every field the file states. A field the file states as `0`, `false`, `""` or `null` matches an object that lacks it, as the API server leaves such fields out, and so does a map or list holding nothing else. Conditions are matched by type, and only their status is compared; any other list must have the file's length, each item matched in turn. A value tagged `!any` is an example: it differs from run to run, and any value there matches. The page shows the value without its tag.

A step waits 90 seconds for its expectations, `E2E_WAIT` to change that for the run, and logs the fields still unmatched every 15 seconds; a story therefore fails within the sum of its steps' waits. A step fails at once, naming the cause, when:

- a container in the story's namespaces or the chart's `nats-operator` namespace, other than a Job's, waits as `CrashLoopBackOff`, `ImagePullBackOff`, `ErrImageNeverPull` or `InvalidImageName`;
- a Job there has failed;
- an object the step reads has `Terminal` True, or `Ready` False for a reason the controllers give a spec they will not act on until it is edited, such as `Rejected`, `UnsupportedSpec`, `GatewayWithoutTLS` or `DuplicateBalancer`; unless the step's own file expects that condition at that status.

A story's front matter may set `params.e2e.after`, the number of the story whose end state it starts from, `params.e2e.skip`, why the harness does not run it, and `params.e2e.waits`, a list of `{step, wait, reason}` giving a slow step a longer wait than the default, `wait` a Go duration such as `4m`. A story spanning Kubernetes clusters places each of its files with `params.e2e.clusters`, a list of `{name, files}`, the home cluster first; its files are applied to, deleted from and read in their own cluster, and the story is skipped when the run has fewer clusters.

Where a manifest cannot run on the harness's clusters as written, such as its resource requests or storage class, `params.e2e.substitutions` changes it for the run alone: a list of `{files, reason, patch}`, each patch a JSON merge patch applied to every document of the files it names, a status file's under its `status` key; `kind` and `name` narrow one to the manifests they match. The page still shows the file as written. Where a selected story's substitutions remove `spec.gateway.tls`, the chart is installed with `cluster.allowGatewayWithoutTLS=true`, so the cluster controller renders that gateway rather than refusing it.

What a story assumes already exists, such as a NATS cluster nobody here deployed, is stood up by files in its `e2e/` directory, named and run as the story's own files are; the site does not publish that directory. Those that hold keys are not committed: every run generates them afresh with `internal/e2e/fixtures` into `bin/e2e/fixtures/<story>/e2e/`, and the harness reads them as the story's own `e2e/` files. A substitution whose values are generated with them names a JSON file there as `patchFile`, in place of `patch`.

## Running the stories

`just e2e` runs them on kind clusters over rootful podman, each Kubernetes cluster one kind node, all of them on kind's podman network with MetalLB handing out LoadBalancer addresses on it:

- On Linux it runs in place, as root, with podman answering on `/run/podman/podman.sock` (`systemctl enable --now podman.socket`), and `helm` and `ko` on the path.
- On darwin only the builds run on the host: `ko` builds the controller images and `go` builds the harness for Linux, and both ship with the working tree into the Apple `container` machine `nats-operator-e2e`, where the harness runs. A machine that does not exist is created from `hack/machine.Containerfile`; `hack/e2e/machine.sh` installs podman and helm in it on every run.

Before a story runs, its namespaces are deleted and made again, one at a time. Their NatsClusters get the `cluster.nats.mikluko.io/force-delete` annotation and their JetStream resources `deletionPolicy: Retain` first, so neither waits on a server. A namespace that has had no Pod for a minute and still holds a finalizer the controllers add loses it, users first and NatsClusters last, and the log names each one released.

With `E2E_WATCH_NAMESPACES=true` the chart is installed with `watchNamespaces` listing the namespaces the stories run in, plus the namespace of the auth controller's system connection where the auth controller is installed, so the controllers reconcile under their Roles alone; each story's fresh namespaces get their Roles back from a `helm upgrade` before its first step.

The `e2e` workflow runs every story on two clusters nightly, and on three weekly, the one scheduled run in which story 9 does not skip.

The clusters stay for the next run; `just e2e-down` deletes them. `hack/e2e` lists the `E2E_*` variables it reads, such as `E2E_STORIES`, the story numbers to run.
