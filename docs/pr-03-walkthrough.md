# PR 3 — walkthrough

**Goal:** ship the app as a container image built on the official Spark runtime, and make that image part of what "green" means.

**Why now:** PR 2 made the build reproducible on *your* machine. An image makes it reproducible on any machine — which is the precondition for a local Compose stack, and later for running on a cluster. Build it before there is any pipeline logic, so when the image breaks you are debugging one thing.

**Not in this PR:** Compose, `make smoke`, Iceberg jars, Kafka.

> **Verification status.** Everything in this document that does not require a Docker daemon has been run, and each claim below says which group it is in.
> The image build and `make scan` have not been run — Docker was unavailable on the machine this was written on. `make ci` deliberately still passes without a daemon; 
> 
> see Step 4.


## Step 0 — read the base image before you extend it

The single most useful thing you can do before writing a `FROM` line is read the Dockerfile of the thing you are inheriting. `apache/spark` is built in the open:

```bash
curl -s https://raw.githubusercontent.com/apache/spark-docker/master/4.0.4/scala2.13-java21-python3-ubuntu/Dockerfile
curl -s https://raw.githubusercontent.com/apache/spark-docker/master/4.0.4/scala2.13-java21-ubuntu/Dockerfile
curl -s https://raw.githubusercontent.com/apache/spark-docker/master/4.0.4/scala2.13-java21-ubuntu/entrypoint.sh
```

Five facts come out of those three files, and four of them change what you write:

| Fact                                                            | Consequence                                                      |
| --------------------------------------------------------------- | ---------------------------------------------------------------- |
| `FROM eclipse-temurin:21-jammy`, then `apt-get install python3` | Ubuntu 22.04's `python3` is **3.10**. Your project pins 3.12.    |
| `ARG spark_uid=185`, home directory `/nonexistent`              | Nothing can write to `$HOME` unless you point it somewhere real. |
| `ENV SPARK_HOME=/opt/spark`                                     | The image's Spark distribution, not the one inside the `pyspark` wheel, is what will actually run.                                             |
| `ENTRYPOINT ["/opt/entrypoint.sh"]`                             | That script dispatches `driver` and `executor` for Spark on Kubernetes. Overriding it works locally and breaks on a cluster.              |
| `USER spark` (named, not numeric)                               | You want the numeric form instead. See Step 5.                   |

The last three are not just in the Dockerfile source - they are in the published image's config, which is what actually applies. You can read that straight from the registry without pulling anything:

```bash
docker manifest inspect apache/spark:4.0.4-python3    # needs a daemon
# or, daemon-free, fetch the config blob from the registry API
```

*Verified* against the registry for `linux/arm64`: `User: "spark"`, `Entrypoint: ["/opt/entrypoint.sh"]`, `Cmd: null`, `WorkingDir: /opt/spark/work-dir`, `SPARK_HOME=/opt/spark`.

Do this before every base-image upgrade too. These are the things that change silently between tags.


## Step 1 — the Python version problem

The project pins `>=3.12,<3.13`. The base image has 3.10. Spark does not care what version you use, but it cares *very much* that the driver and every worker use the **same** one — a mismatch fails at the first action with `PYTHON_VERSION_MISMATCH`, which is the third of the three environment faults PR 2 was written to kill.

Three ways out:

1. **Relax the pin to 3.10.** Cheapest, and wrong: the image would then run a different interpreter from your laptop, so "it works in CI" stops being evidence.
2. **`apt-get install python3.12` in the runtime stage.** Jammy has no 3.12 package; you would be adding a third-party PPA to the runtime image to get one. More attack surface, more to break on a base bump.
3. **Have `uv` download a standalone 3.12 and carry it in.** `uv` already manages interpreters; this uses the tool you have rather than adding one.

Option 3, with one wrinkle that decides the whole file layout: a venv records the **absolute path** of its interpreter in `pyvenv.cfg`. So the interpreter has to live at the same absolute path in both stages. Pin the location explicitly with `UV_PYTHON_INSTALL_DIR=/python` and copy it across unchanged.

*Verified locally (no daemon needed):* set `UV_PYTHON_INSTALL_DIR`, run `uv python install 3.12`, then check `pyvenv.cfg`:

```bash
grep home .venv/pyvenv.cfg
# home = /python/cpython-3.12.12-.../bin
```

That absolute path is why `COPY --from=builder /python /python` is not optional and why the destination path must match the source.


## Step 2 — the builder stage

```dockerfile
ARG SPARK_IMAGE=apache/spark:4.0.4-python3

FROM ${SPARK_IMAGE} AS builder

USER root

COPY --from=ghcr.io/astral-sh/uv:0.10.6 /uv /usr/local/bin/uv

RUN set -eux; \
    if [ ! -s /etc/ssl/certs/ca-certificates.crt ]; then \
      apt-get update; \
      apt-get install -y --no-install-recommends ca-certificates; \
      rm -rf /var/lib/apt/lists/*; \
    fi

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_INSTALL_DIR=/python \
    UV_PYTHON_PREFERENCE=only-managed

RUN uv python install 3.12

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --no-editable

COPY README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable
```

Line by line, the parts that are decisions rather than boilerplate:

**The builder and the runtime share a base.** The obvious builder is something small like `debian:bookworm-slim`, and that is what this PR had first. Using the *runtime* image instead is better on three counts, and the reasoning generalises:

- **One base to track.** Two bases means two CVE feeds, two upgrade cadences and two things that can drift. A scan report that mixes them tells you less than one that does not.
- **No extra download.** The Spark image has to be pulled anyway — it is the runtime base. A second distro is pure addition.
- **Matching glibc.** The venv is built where it will run. Building on Debian 12 (glibc 2.36) and running on Ubuntu 22.04 (glibc 2.35) is the wrong direction: it happens to work because the standalone interpreter and every wheel here target an older glibc, but nothing enforces that.

`ARG SPARK_IMAGE` at the top names it once so the two `FROM` lines cannot drift apart.

**`COPY --from=ghcr.io/astral-sh/uv:0.10.6 /uv`** rather than `FROM ghcr.io/astral-sh/uv:...-python3.12-bookworm-slim`. The tag I reached for first does not exist — the version-qualified python variants are not published. Worth checking before you build, because a 404 on a base image is a slow, confusing failure:

```bash
curl -s "https://ghcr.io/token?scope=repository:astral-sh/uv:pull" | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])' > /tmp/t
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $(cat /tmp/t)" \
  -H 'Accept: application/vnd.oci.image.index.v1+json' \
  https://ghcr.io/v2/astral-sh/uv/manifests/0.10.6
# 200
```

*Verified:* `0.10.6` exists and its filesystem has `/uv` and `/uvx` at the root.

**`ca-certificates`, conditionally.** `uv python install` fetches the interpreter over HTTPS, and without a CA bundle the build fails on a TLS error that reads like a network outage. The Spark image most likely already has one, so the guard installs it only when it is missing rather than paying for an `apt-get update` on every build.

**`UV_COMPILE_BYTECODE=1`.** Writes `.pyc` at build time. Otherwise the first run of each container pays the compile cost, and — since the venv is owned by root and the process runs as 185 — cannot cache the result, so it pays it *every* time.

**`UV_PYTHON_PREFERENCE=only-managed`.** This matters more now that the builder is the Spark image: there *is* a `python3` on `PATH`, and it is 3.10. Without this, uv would happily satisfy the request with the wrong interpreter.

**`UV_LINK_MODE=copy`.** uv hardlinks from its cache by default. The cache here is a BuildKit cache mount, a different filesystem from the image layer, so hardlinking fails. `copy` is the correct mode inside a container build.

**Two `uv sync` calls, dependencies first.** This is the whole point of ordering. `COPY pyproject.toml uv.lock` then sync dependencies produces a layer that is invalidated only when the lockfile changes. Editing `main.py` invalidates only the layers below it. Put `COPY src` first and every one-character edit re-downloads several hundred MB of PySpark.

**`--no-editable`.** By default `uv sync` installs your project in editable mode — a `.pth` file pointing back at `/app/src`. That works only if `/app/src` also exists at runtime. `--no-editable` copies the package into `site-packages` instead, so the venv is self-contained and the runtime stage needs nothing but `/app/.venv`.

*Verified locally:*

```bash
UV_PROJECT_ENVIRONMENT=/tmp/v uv sync --frozen --no-dev --no-editable
ls /tmp/v/lib/python3.12/site-packages/ | grep pipeline
# pipeline
# pipeline-0.1.0.dist-info

cd /tmp && /tmp/v/bin/python -m pipeline.main
# INFO pipeline: spark   4.0.4
# INFO pipeline: python  3.12.12
# INFO pipeline: master  local[*]
```

Run from `/tmp`, with the source tree nowhere in sight. That is the shape of what goes into the image.


## Step 3 — the runtime stage

```dockerfile
FROM ${SPARK_IMAGE}

USER root

COPY --from=builder /python /python
COPY --from=builder /app/.venv /app/.venv

RUN set -eux; \
    v="$(/app/.venv/bin/python -c 'import pyspark; print(pyspark.__version__)')"; \
    echo "wheel=${v}  distribution=$(ls /opt/spark/jars/spark-core_*.jar)"; \
    ls /opt/spark/jars/spark-core_*-"${v}".jar > /dev/null

ENV PATH="/app/.venv/bin:${PATH}" \
    PYSPARK_PYTHON=/app/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python \
    HOME=/tmp

USER 185
WORKDIR /app

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["/app/.venv/bin/python", "-m", "pipeline.main"]
```

### Two Sparks in one image

This is the subtle one. The `pyspark` wheel bundles a complete Spark distribution — jars and all — and the base image has another at `/opt/spark`. Which one runs?

`pyspark` resolves its Spark home by checking `SPARK_HOME` first and only then falling back to the copy inside the wheel. The base image sets `SPARK_HOME=/opt/spark`. So **the image's distribution wins**, and the wheel contributes only the Python API.

*Verified accidentally, and it is the clearest possible demonstration.* Running the standalone venv from Step 2 in a shell that still had an old `SPARK_HOME` exported produced this:

```
/usr/local/src/spark-3.4.0-bin-hadoop3/bin/spark-class: line 71: ...
pyspark.errors.exceptions.base.PySparkRuntimeError: [JAVA_GATEWAY_EXITED]
```

A `pyspark==4.0.4` wheel reaching for a Spark **3.4.0** launcher, purely because `SPARK_HOME` said so. Same mechanism, wrong target. (This is the PR 2 trap; the `Makefile` `unexport SPARK_HOME` exists precisely to prevent it locally.)

In the image the mechanism is what we want — `/opt/spark` is the distribution that carries the Kubernetes scheduler backend, which the wheel's copy would also have but which we want to be explicit about. The hazard is **skew**: bump `pyspark` in `pyproject.toml`, forget the base image tag, and you get a 4.1 Python API driving 4.0.4 jars. That fails at runtime with a stack trace mentioning neither file.

So the build asserts it. `spark-core_2.13-4.0.4.jar` is Maven naming and is stable, which makes it a safer thing to grep than a `RELEASE` file's prose:

```bash
ls .venv/lib/python3.12/site-packages/pyspark/jars/ | grep '^spark-core'
# spark-core_2.13-4.0.4.jar
```

*Verified* against the wheel locally; the same convention holds for the distribution in the image. A guard that fails the build beats a guard in a comment.

### `PYSPARK_PYTHON`

Spark launches Python workers with whatever `PYSPARK_PYTHON` names, defaulting to `python3` on `PATH`. Here that resolves to the image's 3.10 and you get `PYTHON_VERSION_MISMATCH` at the first action. Pinning both driver and worker to the venv's interpreter is the fix — the same two lines the `Makefile` exports locally, for the same reason.

### `HOME=/tmp`

uid 185's home is `/nonexistent`. Anything reading `$HOME` — pip's cache, Ivy's jar cache once you add Iceberg — needs somewhere writable.

Note the limit: this fixes the **environment variable**, not the JVM's `user.home`, which the JVM reads from the passwd entry. That is why `spark-shell` in this image still fails to save JLine history with `-e HOME=/tmp` and needs `--driver-java-options "-Duser.home=/tmp"` instead. *Verified* in Ground School F1, on the same image.


## Step 4 — wire the image into the contract

An image that CI does not build is an image that is already broken. Add it to `ci/run.sh`, not to the workflow YAML:

```bash
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker build -f docker/Dockerfile -t pipeline:local .
elif [[ "${REQUIRE_DOCKER:-0}" == "1" ]]; then
  echo "ERROR: REQUIRE_DOCKER=1 but no Docker daemon is reachable." >&2
  exit 1
else
  echo
  echo "WARNING: skipped 'docker build' — no Docker daemon reachable."
  echo "         CI runs this step. Start Docker and re-run to check it here."
  echo
fi
```

and in `.github/workflows/ci.yml`:

```yaml
      - run: ./ci/run.sh
        env:
          REQUIRE_DOCKER: '1'
```

This is the only conditional step in the contract, and the conditionality is deliberate. Two bad alternatives:

- **Always require Docker.** Then `make lint` on a laptop without the daemon running is a hard failure, for a reason unrelated to what you changed.
- **Always skip when absent.** Then a broken Dockerfile passes CI on a runner that happens to lack a daemon, which is the failure mode this step exists to prevent.

The `REQUIRE_DOCKER` switch gives you strict where strictness is free and degraded-with-a-warning where it is not. Silence would be the real mistake: a skipped check must announce itself.

*Verified:* `make ci` with no daemon running exits 0 and prints the warning.

```
Success: no issues found in 5 source files
..                                          [100%]
2 passed in 3.15s

WARNING: skipped 'docker build' — no Docker daemon reachable.
         CI runs this step. Start Docker and re-run to check it here.
```


## Step 5 — entrypoint, user, and `.dockerignore`

### Keep the base entrypoint

The obvious move is `ENTRYPOINT ["/app/.venv/bin/python", "-m", "pipeline.main"]`.
It works — and it quietly breaks the day you run this on Kubernetes.

`/opt/entrypoint.sh` is a dispatcher:

```bash
case "$1" in
  driver)   ... exec ... "$SPARK_HOME/bin/spark-submit" --deploy-mode client "$@" ;;
  executor) ... exec ... org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend ... ;;
  *)        exec "$@" ;;
esac
```

Spark on Kubernetes starts executor pods by running this image with `executor` as the first argument. Replace the entrypoint and that dispatch is gone. You would not find out until the first cluster submit, long after the change.

Keeping `ENTRYPOINT` and putting our command in `CMD` costs nothing: `docker run pipeline:local` falls into the `*)` branch and `exec`s the interpreter, while `driver` and `executor` keep working. Declare the `ENTRYPOINT` explicitly even though it is inherited, so a base-image change cannot silently move it.

### `USER 185`, not `USER spark`

Kubernetes cannot determine whether a *named* user is root. A pod with `runAsNonRoot: true` and a named `USER` fails to start with `CreateContainerConfigError`. The numeric form is checkable, so use it.

### `.dockerignore`

The build context is uploaded to the daemon before anything runs. Without this file, `.venv` alone sends several hundred megabytes on every build — and worse, `COPY src ./src` would be fine but any broader `COPY` would drag a macOS-built venv into a Linux image, shadowing the interpreter the build just installed.

```
.venv/
__pycache__/
*.py[cod]

.git/
.github/
.pytest_cache/
.mypy_cache/
.ruff_cache/

docs/

spark-warehouse/
metastore_db/
derby.log
```

Rule of thumb: if a file is not read by a `COPY` in the Dockerfile, it should be in `.dockerignore`.


## Step 6 — where the vulnerabilities actually are

Scan any image built from a general-purpose distro and you will get a list. The useful question is not "how many" but "how many *ship*, and how many can I actually fix".

**Only the runtime stage ships.** In a multi-stage build the final image is the last `FROM` plus whatever you explicitly `COPY --from`. Here that is exactly two paths — `/python` and `/app/.venv` — and neither contains a distro package. The builder's package manager, its apt lists and its uv cache are all discarded. So a CVE in the builder is build-time exposure only: it matters if your build is a supply-chain target, and it is not part of what you deploy.

That is the first thing to check when a scanner points at a builder base: `docker history` and the `COPY --from` lines tell you whether the finding is even in the artifact.

**The runtime base is the one that counts, and you do not get to choose it.**
Every published `apache/spark:4.0.4-*` tag is Ubuntu-based, via `eclipse-temurin:21-jammy` — Ubuntu 22.04, plus a full JDK, `wget`, `gnupg2`, `krb5-user`, `procps` and more. It will always carry more findings than a slim builder. Confirm the constraint rather than assuming it:

```bash
curl -s "https://registry.hub.docker.com/v2/repositories/apache/spark/tags?page_size=100&name=4.0.4" \
  | python3 -c 'import json,sys;[print(t["name"]) for t in json.load(sys.stdin)["results"]]'
```

*Verified:* sixteen tags, every one of them `-ubuntu`. The genuine alternatives are to build Spark on your own base from `apache/spark-docker`, or to accept the base and track it. For this project, track it.

**`--ignore-unfixed` is the setting that makes a scan actionable.** A large share of findings on any Debian or Ubuntu base have no fixed version available — the distro's security team has assessed them as minor and will not backport. Those cannot be resolved by upgrading, only by changing base or accepting them. Showing them next to fixable ones trains you to ignore the whole report.

The scan lives in `ci/scan.sh`, for the same reason the checks live in `ci/run.sh`: the workflow should call a script the repo owns, so you can run the identical thing locally.

```bash
docker run --rm -v "${PWD}/.trivy:/t" "${TRIVY_IMAGE}" image \
  --cache-dir /t/db \
  --input /t/image.tar \
  --scanners vuln \
  --severity "${SEVERITY}" \
  --ignore-unfixed \
  --exit-code "${SCAN_EXIT_CODE}" \
  | tee .trivy/report.txt
```

Four deliberate choices:

- **Trivy runs as a container, and gets the image as a tar.** The common recipe mounts `/var/run/docker.sock` into the scanner. That grants the container root-equivalent control of your host daemon, a steep price for reading image layers. `docker save` plus `--input` avoids it.
- **The database is cached in `.trivy/db`.** It is large and trivy refreshes it on a timer; re-downloading per run makes the target something you stop using.
- **`--exit-code` is a variable.** Trivy exits 0 even when it finds things — *"By default, Trivy exits with code 0 even when security issues are detected"* — so a scan without this flag can never fail anything. `make scan` leaves it at 0 because locally you want to read the list; the nightly sets 1.
- **`tee`** keeps a report file for the workflow to upload as an artifact.

### Why this is not in `ci/run.sh`

Every other check in the contract is a function of your commit. Ruff, mypy, pytest and the build give the same answer today and next month for the same input. A vulnerability scan does not: its answer depends on a feed that changes daily. Put it in `ci/run.sh` and a PR that passed yesterday fails today on an unchanged diff — and almost never for a reason the author can act on, because the findings come from Ubuntu 22.04 and the JDK inside a base image this repo does not build. The only remedies are waiting for Apache to publish a rebuilt Spark image, changing base, or an ignore entry. The first can take weeks.

A check people cannot act on is a check people learn to bypass. So it runs on a schedule against `main` instead, where a new finding is what it actually is: news about the world, not a defect in someone's diff.

```yaml
on:
  schedule:
    - cron: '23 19 * * *' # 19:23 UTC, off the hour to avoid the queue spike
  workflow_dispatch:
```

Three things to know about scheduled workflows:

- **A schedule only fires from the default branch.** The file has to be merged to `main` before it runs once.
- **GitHub disables scheduled workflows in a repository with no activity for 60 days.** On a project you touch rarely, the scan quietly stops.
- **Off the hour.** `0 * * * *` is the most contended minute on the platform and scheduled runs are queued, not guaranteed, so an on-the-hour job drifts more.

`workflow_dispatch` is there so you can trigger it by hand instead of waiting a day to find out whether it works.

The nightly sets `SCAN_EXIT_CODE=1`, so a finding fails the run and you get the standard failed-workflow notification. The report uploads with `if: always()`, because the run you most want to read is the one that failed.

If you later want a PR-time gate as well, gate on CRITICAL only and leave HIGH reporting — and add a `.trivyignore` with expiry dates first, so there is somewhere for an accepted finding to go.


## Step 7 — the Makefile targets

```makefile
image: ## Build the container image
	docker build -f docker/Dockerfile -t pipeline:local .

image-run: image ## Run the app inside the image
	docker run --rm pipeline:local
```

`image-run` depends on `image` so you cannot accidentally run a stale tag — the classic way to spend twenty minutes debugging a fix you never rebuilt. `scan` depends on `image` for the same reason.


## Step 8 — check it

**Not yet verified.** Run these once your daemon is up; they are the acceptance criteria for this PR.

```bash
make image
```

Expect the builder stage to download a CPython 3.12 and resolve the lockfile, then the guard line to print something like
`wheel=4.0.4  distribution=/opt/spark/jars/spark-core_2.13-4.0.4.jar`.

```bash
make image-run
```

Expect the same three lines `make run` prints:

```
INFO pipeline: spark   4.0.4
INFO pipeline: python  3.12.12
INFO pipeline: master  local[*]
```

Then confirm the things the image is *supposed* to guarantee:

```bash
# the venv's 3.12, not the image's 3.10
docker run --rm pipeline:local /app/.venv/bin/python -V
docker run --rm --entrypoint python3 pipeline:local -V     # 3.10.x — the base image's

# non-root
docker run --rm pipeline:local id -u                        # 185

# the entrypoint still dispatches
docker run --rm pipeline:local driver --help | head -3

# layer caching actually works: touch a source file, rebuild, watch the
# dependency layer come from cache
touch src/pipeline/main.py && make image

# what actually ships, and what of it is fixable
make scan
```

And confirm the guard fails when it should — temporarily change the base tag to `apache/spark:4.0.3-python3` and check that `make image` fails on the `spark-core_*-4.0.4.jar` line rather than succeeding into a skewed image.


## Definition of done

- [ ] `make image` builds
- [ ] `make image-run` prints `spark 4.0.4` and `python 3.12.12`
- [ ] `ci/run.sh` asserts uid 185 and `/opt/entrypoint.sh`, and both pass
- [ ] `driver` still dispatches: `docker run --rm pipeline:local driver --help`
- [ ] A deliberate version skew fails the build, not the run
- [ ] `make ci` passes with the daemon up, and passes with a warning without it
- [ ] `.dockerignore` keeps `.venv` out of the context
- [ ] `make scan` reports only fixable findings, and they all come from the runtime base rather than anything this repo added
- [ ] `README.md` documents `make image` / `make image-run` / `make scan`

## Files in this PR

| File                          |                                                                  |
| ----------------------------- | ---------------------------------------------------------------- |
| `docker/Dockerfile`           | new — two-stage build                                            |
| `.dockerignore`               | new                                                              |
| `ci/run.sh`                   | modified — conditional `docker build`, `REQUIRE_DOCKER` gate     |
| `.github/workflows/ci.yml`    | modified — `REQUIRE_DOCKER: '1'`                                 |
| `Makefile`                    | modified — `image`, `image-run`, `scan`                          |
| `ci/scan.sh`                  | new — image scan, severity and exit code by env                  |
| `.github/workflows/scan.yml`  | new — nightly schedule plus manual trigger                       |
| `README.md`                   | modified — status, stack, Container section, layout              |
| `.gitignore`                  | modified — `.trivy/` scan workspace                              |
| `docs/pr-03-walkthrough.md`   | new — this file                                                  |


Next: **PR 4** — Docker Compose brings up the image alongside its dependencies, and `make smoke` runs the app end to end against an empty stack. That is the target that turns "main is always runnable" from a rule into a check.