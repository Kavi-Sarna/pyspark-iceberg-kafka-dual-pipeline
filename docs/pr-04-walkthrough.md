# PR 4 — walkthrough

**Goal:** a real Spark cluster on your laptop, the app submitted to it, and a `make smoke` that proves the whole path works against an empty stack.

**Why now:** this is the last PR before any business logic. Everything so far has run in `local[*]`, which is a single JVM pretending to be a cluster. The first time you submit to a master is the first time you find out whether your image, your Python and your submit arguments actually agree.

**Not in this PR:** MinIO, the Iceberg REST catalog, schemas, the join.

> **Verification status.** The `build_session` bug in Step 1 was found and fixed against a real `spark-submit`, and that fix is verified. Everything requiring a Docker daemon — the Compose stack, `make submit`, `make smoke` — has not been run. Docker was unavailable on the machine this was written on.


## Step 0 — what a standalone cluster actually is

Four roles, and conflating them is the source of most confusion:

| Role            | What it is                                                                                                            |
| --------------- | --------------------------------------------------------------------------------------------------------------------- |
| **Master**      | A scheduler. Hands out resources. Does not execute your code.                                                         |
| **Worker**      | A daemon on a machine that offers cores and memory, and launches executors.                                           |
| **Driver**      | *Your program.* In PySpark, a Python process paired with a JVM. It builds the plan and asks the master for resources. |
| **Executor**    | A JVM the worker starts, which runs tasks and forks Python processes for Python code.                                 |

Two consequences that matter immediately:

- **The driver is not inside the cluster.** In client mode it runs wherever you launched it, and executors connect *back* to it. That is why an address the executors can reach is not optional.
- **Executors run your Python.** Not the master's, not your laptop's. Whatever `PYSPARK_PYTHON` names inside the executor's container has to exist and has to have your dependencies. This is the entire reason PR 3 exists.


## Step 1 — the bug this PR exposes

PR 1 wrote this, with a comment claiming it protected cluster submits:

```python
builder = SparkSession.builder.appName(app_name)
if not SparkConf().contains("spark.master"):
    builder = builder.master("local[*]")
```

Before wiring up any cluster, test the claim:

```bash
spark-submit --master "local[1]" src/pipeline/main.py
```

```
INFO pipeline: master  local[*]
```

**It reports `local[*]`, not `local[1]`.** The guard never fires. Here is why:

```bash
python -c "
from pyspark import SparkConf
c = SparkConf()
print('_jconf is None :', c._jconf is None)
print('contains(spark.master):', c.contains('spark.master'))
"
# _jconf is None : True
# contains(spark.master): False
```

`SparkConf` is a thin wrapper over a JVM object. Construct one before a JVM exists and it silently falls back to an empty Python dict, so `contains()` answers `False` no matter what you were submitted with. `.master("local[*]")` is then applied as an explicit builder option, which outranks the defaults spark-submit loaded.

This is the worst class of bug: it never errors. You submit to a sixteen-node cluster, the job succeeds, and it ran entirely on the submitting machine.

**The signal that does work.** spark-submit starts the JVM *first*, then hands the Python process its py4j gateway port:

```bash
spark-submit --master "local[1]" probe.py
# ['PYSPARK_DRIVER_PYTHON', 'PYSPARK_GATEWAY_PORT', 'PYSPARK_GATEWAY_SECRET',
#  'PYSPARK_PYTHON', 'SPARK_CONF_DIR', 'SPARK_ENV_LOADED', 'SPARK_HOME', ...]

python probe.py
# ['PYSPARK_DRIVER_PYTHON', 'PYSPARK_PYTHON', 'SPARK_LOCAL_IP']
```

`PYSPARK_GATEWAY_PORT` is present under spark-submit and absent otherwise. It is exactly the condition we care about — a launcher has already started the JVM and chosen the master.

```python
def launcher_set_master() -> bool:
    """True when spark-submit has already chosen the master.

    spark-submit starts the JVM first and hands the Python process its py4j
    gateway port. SparkConf cannot be asked instead: with no JVM yet it falls
    back to an empty dict, so it reports no master even under spark-submit.
    """
    return "PYSPARK_GATEWAY_PORT" in os.environ


def build_session(app_name: str = "pipeline") -> SparkSession:
    """Build a session, defaulting to local[*] only when nothing else decides."""
    builder = SparkSession.builder.appName(app_name)
    if not launcher_set_master():
        builder = builder.master("local[*]")
    return builder.getOrCreate()
```

*Verified:*

```
spark-submit --master local[1] → master  local[1]     (was local[*])
python -m pipeline.main        → master  local[*]     (unchanged)
```

The predicate is a separate function so it is unit-testable without a JVM:

```python
@pytest.mark.parametrize(
    ("env", "expected"),
    [({}, False), ({"PYSPARK_GATEWAY_PORT": "35123"}, True)],
)
def test_launcher_set_master(monkeypatch, env, expected):
    monkeypatch.delenv("PYSPARK_GATEWAY_PORT", raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    assert launcher_set_master() is expected
```

Take the general lesson: a comment asserting that code handles a case is not evidence. The first time a path runs is the first time you know.


## Step 2 — the Compose stack

```yaml
x-image: &image
  image: pipeline:local
  build:
    context: ..
    dockerfile: docker/Dockerfile
```

**Every service runs the image this repo builds**, not a stock `apache/spark`.
That is the single most important line in the file. If the master ran a stock image and only the driver ran yours, a dependency missing from the image would surface as a task failure on an executor, which is a far worse place to debug it than a failed build.

```yaml
  spark-master:
    <<: *image
    command:
      - /opt/spark/bin/spark-class
      - org.apache.spark.deploy.master.Master
```

**`spark-class`, not `sbin/start-master.sh`.** The `start-*.sh` scripts daemonize: they fork the JVM and return. In a container that means PID 1 exits immediately and Docker declares the service dead. `spark-class` runs in the foreground, which is what a container supervisor expects.

This also depends on PR 3's decision to keep the base `ENTRYPOINT`. These commands reach the JVM through `/opt/entrypoint.sh`'s `*)` branch, which does `exec "$@"`. Had we replaced the entrypoint with the Python interpreter, none of these services could start.

### Healthchecks are how you avoid a race

```yaml
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8080"]
      interval: 3s
      retries: 30
      start_period: 5s
```

`wget`, not `curl` — the Spark base image installs `wget` and not `curl`. Check before you write a healthcheck; one that references a missing binary fails forever and reads like the service is broken.

With `depends_on: condition: service_healthy` on the worker, and `--wait` on `compose up`, the submit cannot race a master that is still binding its port. `depends_on` *without* a condition only waits for the container to start, which is not the same thing and is a classic source of flaky local stacks.


## Step 3 — two problems with submitting

```yaml
  submit:
    <<: *image
    profiles: ["submit"]
```

`profiles` keeps a one-shot job out of `compose up`. Without it the submit container starts alongside the cluster, races the master and exits.

### The driver has to be reachable

```
--conf spark.driver.host="$$(hostname -i)"
```

Executors dial the driver back. The driver must advertise an address they can resolve, and its own container IP on the project network always works.

Why not the service name? `docker compose run` does not add the service name as a network alias, so `submit` would not resolve. `$$` is Compose's escape for a literal `$`, so bash — not Compose — expands the command.

### spark-submit takes a file, not a module

PR 3 installed the package with `--no-editable`, so there is no `/app/src`. The code lives in `site-packages`, at a path containing the Python version. Rather than hardcode that:

```
"$$(/app/.venv/bin/python -c 'import pipeline.main as m; print(m.__file__)')"
```

Ask the installed package where it is. This survives a Python version bump and a layout change, and it is worth knowing as a general trick for containerized PySpark.


## Step 4 — event logs and the volume ownership trap

The master UI forgets an application shortly after it finishes. The History Server is what lets you look at a completed job, and it reads event logs the *driver* writes:

```
--conf spark.eventLog.enabled=true
--conf spark.eventLog.dir=/tmp/spark-events
```

with a named volume shared between `submit` and `spark-history`.

Here is the trap. A named volume mounted at a path that **does not exist in the image** is created by Docker as root-owned `755`. The container runs as uid 185. Every write fails, and the error surfaces as a Spark exception about the event log directory rather than as a permissions message.

A named volume mounted at a path that **does** exist in the image inherits that directory's ownership. So the fix belongs in the Dockerfile:

```dockerfile
RUN mkdir -p /tmp/spark-events && chown 185:185 /tmp/spark-events
```

Note this is a *runtime-image* change made by the PR that introduced event logging, which is right: the image is the thing that has to be correct.

Keep `eventLog.enabled` on the submit command rather than baked into the image. Where logs go is a deployment decision, not a property of the artifact.


## Step 5 — the smoke test, and why it asserts more than exit zero

```bash
make smoke
```

`ci/smoke.sh` brings up the stack, submits, checks, and tears down. The interesting part is what it checks.

**Exit zero is not enough.** The bug in Step 1 exits zero. A job that quietly falls back to `local[*]` succeeds, prints happy logs and proves nothing. So:

```bash
if ! grep -q "master  ${MASTER_URL}" "${log}"; then
  echo "ERROR: expected 'master  ${MASTER_URL}' in the output." >&2
  echo "       The job ran, but not on the cluster." >&2
  exit 1
fi
```

**And the driver's word is not enough either.** The driver reporting success is self-assessment. Ask the master:

```python
state = json.load(urllib.request.urlopen("http://localhost:8080/json/", timeout=10))
done = state.get("completedapps", [])
names = [a.get("name") for a in done]
print(f"master reports {len(done)} completed application(s): {names}")
if not any(n == "pipeline-smoke" for n in names):
    sys.exit("ERROR: master has no completed application named 'pipeline-smoke'.")
```

That is why the submit passes `--name pipeline-smoke`: so the assertion can identify its own application rather than any application.

Two details in the script worth copying:

- **`cleanup` runs on `EXIT` and once up front.** A stack left behind by a failed run would otherwise satisfy the next run's healthchecks and mask a genuine failure.
- **`--wait` on `compose up`** rather than a `sleep`. A sleep is either too short and flaky or too long and slow, and it is always both on someone else's machine.

### Why smoke belongs in `ci/run.sh` and the scan does not

`ci/run.sh` gained one line:

```bash
  ./ci/smoke.sh
```

The distinction is determinism. The smoke test is a function of your commit: the same code gives the same answer today and next year. The vulnerability scan is a function of a feed that changes daily, so it runs nightly instead. Both need Docker, both sit behind the same gate — but only one can gate a PR without failing unchanged diffs.

It costs CI a couple of minutes. It buys the project's central rule: *`main` is always runnable*, checked rather than asserted.


## Step 6 — the Makefile targets

```makefile
up:     ## Start the local Spark cluster
down:   ## Stop the cluster and drop its volumes
submit: up  ## Submit the app to the running cluster
smoke:  ## Bring up a cluster, submit, assert, tear down
```

`submit` depends on `up`, so submitting to a cluster you forgot to start is not a failure mode. `smoke` does not, because it owns the full lifecycle including teardown.

`down` passes `--volumes`, which drops the event logs too. That is right for a local stack: a smoke run should leave nothing behind for the next one to trip over.


## Step 7 — check it

**Not yet verified.** These are the acceptance criteria.

```bash
make smoke
```

Expect the stack to come up, the submit to print the three familiar lines with `master  spark://spark-master:7077`, then `SMOKE OK`.

Then look at it with your own eyes, which is the point of a local cluster:

```bash
make up
open http://localhost:8080      # master: one alive worker, 1 core
open http://localhost:8081      # worker
make submit
open http://localhost:8080      # the app, now under Completed Applications
open http://localhost:18080     # history server: the finished application
make down
```

Confirm the things the stack is supposed to guarantee:

```bash
# the executors ran your image's Python, not the base image's 3.10
docker compose -f docker/compose.yaml run --rm submit 2>&1 | grep "python "

# the event log actually got written
make up && make submit
docker compose -f docker/compose.yaml exec spark-history ls -l /tmp/spark-events
```

And confirm the Step 1 fix holds where it matters — temporarily force `builder.master("local[*]")` unconditionally and check that `make smoke` **fails** on the master assertion rather than passing.


## Definition of done

- [ ] `make up` brings up master, worker and history, all healthy
- [ ] `make submit` reports `master  spark://spark-master:7077`
- [ ] The application appears under Completed Applications at `localhost:8080`
- [ ] The History Server at `localhost:18080` shows the finished application
- [ ] `make smoke` passes, and fails when the master assertion is broken
- [ ] `make ci` runs the smoke test and is green
- [ ] `make down` leaves no containers or volumes behind

## Files in this PR

| File                          |                                                                    |
| ----------------------------- | ------------------------------------------------------------------ |
| `docker/compose.yaml`         | new — master, worker, history, submit                              |
| `ci/smoke.sh`                 | new — end-to-end check with real assertions                        |
| `docs/pr-04-walkthrough.md`   | new — this file                                                    |
| `src/pipeline/main.py`        | fixed — `launcher_set_master`, replacing a guard that never fired  |
| `tests/unit/test_main.py`     | tests for the fix, both branches                                   |
| `docker/Dockerfile`           | event-log directory owned by uid 185                               |
| `ci/run.sh`                   | runs the smoke test                                                |
| `Makefile`                    | `up`, `down`, `submit`, `smoke`                                    |
| `README.md`                   | status, local stack, layout                                        |

Next: **PR 5** — explicit `StructType` schemas for shipments and deliveries, with a test that asserts them rather than trusting inference.