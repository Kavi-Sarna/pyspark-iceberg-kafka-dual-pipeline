# PR 4.1 — walkthrough

**Goal:** click one badge and get a running Spark cluster on GitHub's infrastructure, then hand someone a URL so they can watch a job run — with no server to keep alive and nothing for either of you to install.

**Why now:** PR 4 made the cluster real but local. Anyone you want to show it to would need a JDK, Docker and a clone. Codespaces removes all three, and this is the last natural point to add it before the project grows dependencies.

**Not in this PR:** publishing the image to GHCR, prebuilds, any change to the app or the cluster.

> **Verification status.** The devcontainer feature IDs, the base image, the Codespaces quotas, the port-visibility options, the forwarded-URL format and the `gh codespace` commands below were all checked against their registries or GitHub's current documentation. Nothing has been *run*: a devcontainer can only be exercised by creating a codespace, which needs the repository pushed.


## Step 0 — what a codespace actually is

A container running on GitHub's infrastructure, built from `.devcontainer/` in your repository, that you attach an editor to. Three properties make it the right tool here:

- **It is ephemeral by default.** It stops itself after 30 minutes idle, and a stopped codespace costs no compute. There is nothing to remember to shut down.
- **Ports are forwarded out.** A service listening on `8080` inside gets a URL on `app.github.dev`, and you choose who can reach it.
- **It is defined in the repo.** The environment is reviewed and versioned like everything else, rather than living in someone's setup notes.

What it is *not* is a deployment. When the codespace stops, the demo stops. That is the point.

### What it costs

|           | GitHub Free            | Pro  |
| --------- | ---------------------- | ---- |
| Compute   | 120 core-hours/month   | 180  |
| Storage   | 15 GB-month            | 20   |

Compute is billed at the **core count**, so one hour on a 2-core machine spends two core-hours. That makes the free tier roughly **60 wall-clock hours a month on 2 cores**, or 30 on 4. A stopped codespace spends no compute but still occupies storage.


## Step 1 — read the config before you run it

```json
  "features": {
    "ghcr.io/devcontainers/features/java:1": {
      "version": "21",
      "jdkDistro": "tem"
    },
    "ghcr.io/devcontainers/features/python:1": {
      "version": "3.12"
    },
    "ghcr.io/devcontainers/features/docker-in-docker:2": {}
  },
```

Three features, each earning its place:

- **Java 21.** Spark runs on the JVM even though this is a Python project. This is the same JDK requirement the `Makefile` enforces locally.
- **Python 3.12** only to bootstrap `uv`. The interpreter the project actually uses is the one `uv` manages, exactly as in the image.
- **docker-in-docker** so `make up` can run the Compose stack *inside* the codespace.

Verify a feature before adopting it rather than trusting a snippet. Each is an OCI artifact whose metadata lists its real options:

```bash
tok=$(curl -s "https://ghcr.io/token?scope=repository:devcontainers/features/java:pull" \
      | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
curl -s -H "Authorization: Bearer $tok" \
  -H "Accept: application/vnd.oci.image.manifest.v1+json" \
  https://ghcr.io/v2/devcontainers/features/java/manifests/1 \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["annotations"]["dev.containers.metadata"])'
```

*Verified:* `version` accepts `21`, and `jdkDistro` accepts `tem` for Temurin.

### The hook split is the one real decision

```json
  "onCreateCommand": ".devcontainer/on-create.sh",
  "postStartCommand": ".devcontainer/post-start.sh",
  "waitFor": "onCreateCommand",
```

The obvious thing is to put everything in `postCreateCommand`. It half-works, and the half that fails is invisible until your second visit.

**`postCreateCommand` runs once, at creation.** A codespace stops after 30 minutes idle. You resume it a week later, the container comes back, and the cluster does not — because nothing ran. You would be standing in front of someone debugging a dead stack.

So: the slow one-off build on **create**, the cluster on every **start**.

```bash
# on-create.sh — once
uv sync --frozen
make image

# post-start.sh — every start, including resume
make up
```

`post-start.sh` deliberately exits 0 even when `make up` fails:

```bash
if ! make up; then
  echo "WARNING: the cluster did not come up. The codespace is still usable." >&2
  exit 0
fi
```

A failing start hook can leave you with a codespace that will not open. A broken cluster should leave you an editor to fix it in.

### Ports

```json
  "forwardPorts": [8080, 8081, 18080, 4040],

  "portsAttributes": {
    "8080": { "label": "Spark master UI", "onAutoForward": "notify" },
    "8081": { "label": "Spark worker UI", "onAutoForward": "silent" },
    "18080": { "label": "Spark History Server", "onAutoForward": "silent" },
    "4040": { "label": "Spark driver UI (only while a job runs)", "onAutoForward": "silent" }
  },
```

Labels matter more than they look: the Ports panel is how you will find these under pressure, and `8081` vs `18080` is not self-explanatory six weeks later.

Note what is **not** here: any visibility setting. Ports default to private, and that is deliberate. See Step 4.


## Step 2 — first run

The repository has to exist on GitHub, because the codespace is built from it.

```bash
git init
# ... commit PRs 1-4, then this one
gh repo create dual-mode-pyspark-iceberg-pipeline --private --source=. --push
```

Then either click the badge in the README, or use the CLI:

```bash
gh codespace create \
  --repo Kavi-Sarna/dual-mode-pyspark-iceberg-pipeline \
  --branch main \
  --idle-timeout 30m \
  --retention-period 24h
```

Omit `--machine` and `gh` prompts with the machine types your account can actually use, which beats guessing an SKU name. To see them without creating anything:

```bash
gh api /repos/OWNER/REPO/codespaces/machines \
  --jq '.machines[] | "\(.name)  \(.display_name)"'
```

`--retention-period` is worth setting. It is how long a *stopped* codespace survives before GitHub deletes it, and it caps how long an idle demo keeps occupying your storage quota. The maximum is 30 days.

The badge URL is the documented deep link:

```markdown
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/OWNER/REPO)
```

Append `?quickstart=1` if you would rather it resume an existing codespace than always create a new one.

**The first create is slow** — several minutes. It pulls the devcontainer base, installs three features, downloads a standalone Python, resolves PySpark (a few hundred MB), then builds the image. A resume afterwards takes seconds. If you demo often, turn on **prebuilds** (Settings → Codespaces → Prebuilds), which bakes all of that ahead of time at the cost of Actions minutes and storage.

Watch it happen rather than staring at a spinner:

```bash
gh codespace list
gh codespace logs -c <codespace-name>
```


## Step 3 — confirm it actually came up

Once the editor loads, `post-start.sh` has already run `make up`. Check:

```bash
docker compose -f docker/compose.yaml ps
```

Expect `spark-master` and `spark-worker` healthy, `spark-history` running. Then open the Ports panel — master UI on `8080` should show **one alive worker, 1 core**.

If it did not come up, the hook told you so and exited 0 on purpose:

```bash
make up                                     # see the real error
docker compose -f docker/compose.yaml logs spark-master
```

Prove the whole path before you invite anyone:

```bash
make smoke
```

That brings up a stack, submits, asserts the job reported `spark://spark-master:7077`, asserts the master recorded a completed application, and tears down. If `make smoke` passes in the codespace, the demo will work.


## Step 4 — share the URL

This is the part worth getting right, because the default does not do what you want and fails quietly.

A forwarded port gets a URL shaped like:

```
https://CODESPACENAME-PORT.app.github.dev
```

for example `https://fluffy-space-guide-x5w9-8080.app.github.dev`.

**By default that URL is Private.** Sending it to someone gets them an authentication wall, not your Spark UI. There are three settings:

| Visibility               | Who can open it                             |
| ------------------------ | ------------------------------------------- |
| **Private** (default)    | Only you                                    |
| **Organization**         | Org members who have the URL                |
| **Public**               | Anyone with the URL, no authentication      |

To share with someone outside your org, the port has to be **Public**.

**In the editor:** open the Ports panel, right-click port `8080`, choose **Port Visibility → Public**, then copy the Local Address.

**From the CLI:**

```bash
gh codespace ports visibility 8080:public -c <codespace-name>
gh codespace ports -c <codespace-name>          # copy the URL
```

### Read this before you make a port public

The Spark master UI **has no authentication of any kind**. A public forwarded port is an open URL for as long as the codespace runs — anyone who has it, or guesses it, sees your cluster and can browse the job history. That is acceptable for a demo of an app with no data in it. It would not be acceptable once real data is in the stack.

So treat it as a per-demo action, not a setting:

```bash
gh codespace ports visibility 8080:private -c <codespace-name>   # when done
```

Two things that can surprise you: **organization owners can disable public port forwarding by policy**, in which case the option is simply absent — use Organization visibility instead, if your viewer is in the org. And the URL changes when you create a new codespace, so a link you sent last week is dead.

### Which port to actually share

- **`8080`, the master UI** — the good one. Shows the worker, and applications moving from Running to Completed while you submit.
- **`18080`, the History Server** — share this if you want them to look at a finished job in their own time. It persists after the app ends.
- **`4040`, the driver UI** — only exists *while a job is running*. It will 502 the rest of the time. Do not send this one as your demo link.


## Step 5 — run the demo

With the port public and the URL sent:

```bash
make up          # if not already running
make submit      # they watch it appear on 8080
```

A sequence that reads well to someone who has not seen Spark before:

1. Point them at `8080`. One worker, one core, no applications.
2. Run `make submit`. The app appears under **Running Applications**, then moves to **Completed Applications**. That is the driver asking the master for resources and the master handing them over.
3. Click into the completed application. Point out that the driver ran in its own container and the executor ran on the worker.
4. Open `18080` for the History Server view of the same run.
5. Optionally run `make smoke` and show that it asserts the *master* recorded the job — not merely that the submit exited zero.

The app moves no data yet, so there is nothing to show in a job DAG. That arrives with the join.


## Step 6 — end the demo and stop paying for it

```bash
gh codespace ports visibility 8080:private -c <name>   # close the public URL
gh codespace stop -c <name>                            # stop compute
gh codespace delete -c <name>                          # reclaim storage too
```

Stopping ends compute charges; the codespace still occupies storage against your 15 GB-month. Deleting reclaims it. Since everything here is reproducible from the repository, deleting costs you nothing but the next first-create wait.

It also stops itself after 30 minutes idle, so forgetting these commands is not expensive — just untidy.

```bash
gh codespace list    # what is still alive
```


## Troubleshooting

| Symptom                                       | Cause                                                                             |
| --------------------------------------------- | --------------------------------------------------------------------------------- |
| Badge opens a *creation* page every time      | Expected. Add `?quickstart=1` to resume instead.                                  |
| Shared URL shows a GitHub sign-in             | The port is still Private. Step 4.                                                |
| Public option missing from the menu           | Organization policy disables public forwarding. Use Organization visibility.      |
| Port shows `502 Bad Gateway`                  | Nothing is listening yet. `docker compose ps`, and remember `4040` only exists during a job. |
| Editor opens but no cluster                   | `post-start.sh` exits 0 by design. Run `make up` to see the real error.           |
| Cluster sluggish, containers OOM-killed       | 2-core/8 GB is the minimum. Recreate and pick a 4-core machine at the prompt.     |
| Codespace vanished                            | `--retention-period` elapsed after it stopped. Nothing is lost; recreate.         |
| Free hours gone faster than expected | Core-hours multiply by core count. A 4-core machine spends them twice as fast.             |


## Definition of done

- [ ] The badge creates a codespace and the editor opens
- [ ] `docker compose ps` shows master and worker healthy after the start hook
- [ ] `make smoke` passes inside the codespace
- [ ] Port `8080` set to Public produces a URL that opens in a private browser
      window with no sign-in
- [ ] `make submit` shows the application move to Completed on that URL
- [ ] Port set back to Private, and the codespace stopped or deleted
- [ ] A resumed codespace comes back with the cluster running, not dead

## Files in this PR

| File                                  |                                                           |
| ------------------------------------- | --------------------------------------------------------- |
| `.devcontainer/devcontainer.json`     | new — features, ports, the create/start hooks             |
| `.devcontainer/on-create.sh`          | new — one-off: `uv sync`, `make image`                    |
| `.devcontainer/post-start.sh`         | new — every start: `make up`, never fails the start       |
| `README.md`                           | badge, and the Codespaces section                         |
| `.dockerignore`                       | keep `.devcontainer/` out of the build context            |

No application code, no `Makefile` and no CI changes: this PR only *consumes* `make up`, which PR 4 provides.

Next: **PR 5** — explicit `StructType` schemas for shipments and deliveries, with a test that asserts them rather than trusting inference.
