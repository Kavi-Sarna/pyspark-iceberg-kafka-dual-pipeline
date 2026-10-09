# PR 14 — walkthrough

**Goal:** one artifact that runs either mode, with everything it needs to know read once, from one place, into frozen dataclasses.

**Why now:** both runners work. What is left is that tuning them means editing code: `LATENESS`, the join window and the trigger were module constants, and the rest of the configuration was env lookups scattered across five modules.

**Not in this PR:** Kubernetes, metrics, the operational drills.


## Step 0 — three layers, in order

```
1. the packaged conf/app.toml      ships inside the image
2. --config or PIPELINE_CONFIG     a file for this deployment
3. environment variables           per-instance values, and every secret
```

Each overrides the one before. The merge is one level deep, which is all this file has, and sections not mentioned in an override survive intact — a test pins that, because the obvious implementation replaces whole sections and silently drops the rest.

### Where the file lives, and why not where you would expect

The roadmap said `conf/app.toml` at the repository root. It is at `src/pipeline/conf/app.toml` instead, for the same reason the Avro schemas are inside the package: the image installs the project with `--no-editable`, so only `site-packages` ships and a directory at the repository root is simply not there.

*Verified* against a non-editable install:

```bash
UV_PROJECT_ENVIRONMENT=/tmp/v uv sync --frozen --no-dev --no-editable
ls /tmp/v/lib/python3.12/site-packages/pipeline/conf/
# app.toml
```

An external file is still perfectly possible — that is what layer two is for.


## Step 1 — the file holds shape, the environment holds secrets

```toml
[catalog]
name = "lake"                            # ICEBERG_CATALOG
type = "rest"                            # ICEBERG_CATALOG_TYPE
uri = "http://iceberg-rest:8181"         # ICEBERG_URI
s3_endpoint = "http://minio:9000"        # S3_ENDPOINT
region = "us-east-1"                     # AWS_REGION
# access_key / secret_key come only from AWS_ACCESS_KEY_ID and
# AWS_SECRET_ACCESS_KEY.
```

This file is committed, so it cannot hold a credential. That is not a style preference — a committed secret is the single most common way they leak, and a config loader that *can* read one from a file will eventually be given one. `_catalog` reads keys only from the environment, and a test asserts that a config built with no environment has no credentials in it.

Naming the environment variable beside each value is deliberate too. The question anyone has in front of a config file is "how do I override this here", and answering it in the file is cheaper than a README section that drifts.


## Step 2 — the two absent keys are the switches

`warehouse` and `bootstrap_servers` have **no default**, and that is load-bearing.

With defaults, every subsystem would always be configured, and `make run` would start contacting a catalog that is not there. Their absence is what keeps the gate from PR 7 working:

```python
def _catalog(section, e) -> CatalogConfig | None:
    """None when no warehouse is configured, so a bare run stays offline."""
    warehouse = e.get("ICEBERG_WAREHOUSE", section.get("warehouse"))
    if not warehouse:
        return None
```

*Verified:*

```
INFO pipeline: master  local[*]
INFO pipeline: catalog none
```

### The default that did leak, and how it was caught

The first version kept `s3_endpoint` in the file unconditionally. That is correct for the Compose stack and wrong for `make local`, where the warehouse is a directory: the catalog would be handed `S3FileIO` and an endpoint, and would try to reach MinIO to read a path on disk.

The fix keys off the warehouse rather than the catalog type, because the question is about storage:

```python
    # Only object-store warehouses get S3FileIO. A directory warehouse that
    # inherited the file's s3_endpoint would try to reach MinIO to read a
    # local path.
    s3_endpoint = None
    if warehouse.startswith("s3"):
        s3_endpoint = e.get("S3_ENDPOINT", section.get("s3_endpoint"))
```

Two tests hold it: a directory warehouse has no `.s3.` keys at all, an object store warehouse has five.


## Step 3 — one duration, two spellings

Spark wants a watermark as the string `"10 minutes"`. `JoinSpec` wants a `timedelta`. Writing the number in both places is how they drift apart, so the file holds one value and the loader produces both:

```python
def parse_duration(text: str) -> timedelta:
    """Accept the same spelling Spark uses for watermarks, so one value serves both.

    Spark wants "10 minutes" as a string; JoinSpec wants a timedelta. Writing
    the number twice is how the two drift apart.
    """
```

It is strict on purpose. `"soon"`, `"10"` and `"10 fortnights"` all raise rather than falling back to a default, because a silently-ignored watermark is a bug you find in production. Five parametrized cases each way.


## Step 4 — what stopped being code

```python
def spec_for(settings: StreamSettings) -> JoinSpec:
    return JoinSpec(
        keys=(JOIN_KEY,),
        left_time="dispatch_time",
        right_time="delivered_time",
        max_lag=settings.max_lag,
    )
```

`stream.SPEC`, `stream.LATENESS` and `stream.TRIGGER` are gone. The runners now take `Tables` and `StreamSettings`, and the table names came out of two modules that each had their own copy.

The payoff is immediate:

```bash
PIPELINE_CONFIG=tight.toml make watermark-demo
#   lateness=1 minute max_lag=0:02:00 trigger=10 seconds
```

Narrowing the window to watch rows expire sooner used to be an edit, a rebuild and a mental note to put it back.

Note what did **not** move: `batch.SPEC` still has `max_lag=None` as a constant, because that is not tuning. A batch join has no time bound by definition, and making it configurable would invite someone to set one.


## Step 5 — check it

```bash
make test               # 80 unit tests
make local              # unchanged: 3 tables, 3 shipments, 4 deliveries, 4 rows
make run                # unchanged: catalog none
make watermark-demo
```

Then prove the layering:

```bash
cat > /tmp/tight.toml <<'TOML'
[stream]
lateness = "1 minute"
max_lag = "2 minutes"
TOML
PIPELINE_CONFIG=/tmp/tight.toml make watermark-demo   # a narrower window
ICEBERG_CATALOG=other make run                        # env beats file
```

And prove it fails loudly:

```bash
printf '[stream]\nmax_lag = "soon"\n' > /tmp/bad.toml
PIPELINE_CONFIG=/tmp/bad.toml make run
# ValueError: cannot read 'soon' as a duration, e.g. '10 minutes'
```


## Definition of done

- [ ] `make local` and `make run` behave exactly as before
- [ ] An override file changes the stream window without touching code
- [ ] The environment beats both files
- [ ] A directory warehouse gets no S3 options
- [ ] No credential can be read from a file
- [ ] An unreadable duration raises rather than defaulting


## Files in this PR

| File                                                                          |                                                                   |
| ----------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `src/pipeline/conf/app.toml`                                                  | new — the packaged defaults                                       |
| `src/pipeline/config.py`                                                      | new — `AppConfig`, `Tables`, `StreamSettings`, `parse_duration`   |
| `tests/unit/test_config.py`                                                   | new — seventeen tests                                             |
| `src/pipeline/runner/stream.py`                                               | settings instead of constants; `spec_for()`                       |
| `src/pipeline/runner/batch.py`                                                | takes `Tables`                                                    |
| `src/pipeline/main.py`                                                        | `--config`; loads once and passes down                            |
| `src/pipeline/bootstrap.py`, `seed.py`, `producer.py`, `watermark_demo.py`    | read `AppConfig`                                                  |
| `src/pipeline/session.py`, `streams.py`                                       | `from_env` removed; loading lives in one place                    |
| `tests/`                                                                      | the moved config tests and the new fixtures                       |

Next: **PR 15** — observability: metrics, structured logs, and something to look at when a run goes wrong.
