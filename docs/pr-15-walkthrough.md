# PR 15 — walkthrough

**Goal:** event logs the History Server can read, and a bound on how much disk a long-running query may consume.

**Why now:** a batch job writes an event log and finishes. A streaming query runs for weeks. Until something deletes old parts, the log grows for as long as the query lives.

**Not in this PR:** metrics, Kubernetes.


## Step 0 — two things the plan got slightly wrong

The plan said *turn on rolling event logs — a streaming query writes forever otherwise*. Both halves needed checking, and both moved.

**Rolling is already the default in Spark 4.** Two runs differing only in an explicit setting:

```
rolling unset  ->  ./eventlog_v2_local-.../events_1_local-....zstd
rolling=false  ->  ./local-....zstd
rolling=true   ->  ./eventlog_v2_local-.../events_1_local-....zstd
```

A single `.zstd` file appears only when rolling is explicitly switched **off**. So there is nothing to turn on.

**And rolling alone bounds nothing.** From Spark's monitoring documentation, `spark.history.fs.eventLog.rolling.maxFilesToRetain` defaults to `Int.MaxValue` — *"By default, all event log files will be retained."*

Which means the plan's conclusion was right and its mechanism was wrong. Rolling splits one growing file into many files; it does not delete any of them. A streaming query still writes forever, just tidily. **Retention is the setting that bounds disk**, and it is on the History Server rather than the job:

```yaml
      SPARK_HISTORY_OPTS: >-
        -Dspark.history.fs.logDirectory=/tmp/spark-events
        -Dspark.history.fs.eventLog.rolling.maxFilesToRetain=20
        -Dspark.history.fs.eventLog.rolling.compaction.score.threshold=0.7
        -Dspark.history.fs.cleaner.enabled=true
        -Dspark.history.fs.cleaner.maxAge=7d
```

Three different brakes, worth separating:

|                               |                                                               |
| ----------------------------- | ------------------------------------------------------------- |
| `maxFilesToRetain`            | how many *parts of one application's log* stay uncompacted    |
| compaction `score.threshold`  | when rewriting older parts is worth the IO                    |
| `cleaner`                     | how long *whole finished applications* are kept               |

Only the last one existed before this PR, and it was off.


## Step 1 — one directory, named once

```python
    def spark_options(self) -> dict[str, str]:
        """Driver-side settings. The retention ones are read by the History
        Server, not by the job, but they belong with the rest."""
```

The job writes to `spark.eventLog.dir`; the History Server reads `spark.history.fs.logDirectory`. Two settings, one place, and if they disagree the UI is simply empty with nothing in any log to say why. Both come from one value in `app.toml` and a test asserts they stay equal.

Splitting `spark_options()` from `history_options()` keeps that honest: the two consumers are different processes, but the value they share is declared once.


## Step 2 — a default that broke a bare run

The first version shipped `enabled = true`. That looked harmless and was not:

```
java.io.FileNotFoundException: File file:/private/tmp/spark-events does not exist
```

**Spark refuses to start if the event log directory is missing.** It does not create it, and it does not warn and carry on. So a packaged default of `true` makes `make run` depend on a path nobody created — exactly the kind of dependency the configuration gates exist to avoid.

```toml
# Off by default. Spark refuses to start if the directory does not exist, so
# a packaged default of true would make a bare run depend on a path nobody
# created. The Compose stack sets EVENT_LOG_ENABLED=true, where the volume is.
enabled = false                          # EVENT_LOG_ENABLED
```

The Compose services set it, because that is where the volume is mounted. Verified both ways: a bare `make run` on a machine with no `/tmp/spark-events` starts cleanly, and `EVENT_LOG_ENABLED=true make run` produces the rolled directory.

Worth generalising: **a default that works only on the machine you wrote it on is not a default.** The question to ask of every one is "what does this assume exists".


## Step 3 — where the logs live

The plan said event logs on MinIO. They are on the shared volume, and the reason is worth recording with numbers.

Writing to `s3a://` needs `hadoop-aws`, and `hadoop-aws` on Hadoop 3.4 resolves `software.amazon.awssdk:bundle` transitively:

```
hadoop-aws 3.4.1                       0MB
awssdk bundle 2.29.52                611MB
```

611MB, downloaded per container that writes a log, for a local demo. The individual modules are a different story —

```
awssdk s3             4MB
awssdk sts            0MB
awssdk apache-client  0MB
```

— so the light path is `hadoop-aws` plus the explicit modules with `spark.jars.excludes=software.amazon.awssdk:bundle`. That is the configuration to reach for when the driver and the History Server are separate pods and no volume is shared between them, which is where this is heading. It is not worth the setup while both run on one Docker network with a volume between them.

The directory is a single config value, so moving it later is one line:

```bash
EVENT_LOG_DIR=s3a://warehouse/spark-events
```


## Step 4 — check it

```bash
make test                      # 79, no Docker
rm -rf /tmp/spark-events && make run      # starts clean with no directory
mkdir -p /tmp/spark-events
EVENT_LOG_ENABLED=true make run
find /tmp/spark-events
```

Expect `eventlog_v2_<appid>/` containing `appstatus_<appid>` and `events_1_<appid>.zstd` — a directory, compressed, one part so far.

On the Compose stack:

```bash
make up && make demo
open http://localhost:18080
```

The History Server should list the applications. Then watch a long query roll:

```bash
make stream AWAIT_SECONDS=300
docker compose -f docker/compose.yaml exec spark-history \
  ls -la /tmp/spark-events/
```

`events_2_`, `events_3_` and so on appear as the query runs. With `maxFilesToRetain=20` the count stops climbing; without it, it would not.


## Definition of done

- [ ] A bare `make run` works with no event log directory present
- [ ] `EVENT_LOG_ENABLED=true` produces a rolled, compressed directory
- [ ] The History Server lists applications at `localhost:18080`
- [ ] Job and History Server agree on the directory, from one value
- [ ] Part files stop accumulating past the retention limit


## Files in this PR

| File                          |                                                                       |
| ----------------------------- | --------------------------------------------------------------------- |
| `src/pipeline/conf/app.toml`  | an `[events]` section                                                 |
| `src/pipeline/config.py`      | `EventLog`, with `spark_options()` and `history_options()`            |
| `src/pipeline/session.py`     | `build_session` applies event options                                 |
| `src/pipeline/main.py`        | passes them                                                           |
| `docker/compose.yaml`         | History Server retention and compaction; `EVENT_LOG_ENABLED` on the jobs; the duplicated per-job confs removed |
| `tests/unit/test_config.py`   | six more tests                                                        |
| `README.md`                   | status                                                                |

Next: **PR 16** — a `StreamingQueryListener` pushing batch duration, input rate and state-store rows to StatsD, with Prometheus and Grafana as the stand-in.
