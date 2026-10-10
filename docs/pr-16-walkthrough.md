# PR 16 — walkthrough

**Goal:** the numbers that tell you whether a streaming query is healthy — trigger duration, state store size, rows dropped by the watermark, and Kafka lag — reported to StatsD and drawn in Grafana.

**Why now:** the query works. PR 13 proved the join's semantics with a test. None of that tells you, at three in the morning, whether the thing is keeping up, whether its state store is growing without bound, or whether it is quietly discarding late rows.

**Not in this PR:** Kubernetes, alerting rules, operational drills.


## Step 0 — the metric that is not where you would look for it

The obvious first move with a lagging consumer is to ask Kafka:

```bash
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups
```

Run that against our stack while the streaming job is working and you get nothing useful. There is no group holding our offsets.

This is not a bug and it is not a configuration mistake. Spark's Kafka source does not use Kafka's consumer group machinery to track position. It records the offsets it has processed in its **checkpoint**, because that is the only way the offsets can be committed in the same atomic step as the output — which is what makes the query restartable. An offset committed to Kafka and an output written to Iceberg are two commits, and two commits can disagree. One checkpoint cannot disagree with itself.

The consequence is the whole reason this PR exists:

> Every standard Kafka lag tool reports nothing for a Spark query. The position exists only inside the checkpoint, and the only thing that reads it out for you is the query's own progress report.

So lag is not something we fetch. It is something we derive, from a callback.


## Step 1 — what Spark hands you on every trigger

`StreamingQueryListener` has three methods, and `onQueryProgress` fires once per trigger with a `StreamingQueryProgress`. Its `.json` is the honest interface — the Python object wraps some of it, but not all of it, and the JSON has everything. Trimmed to what we use:

```json
{
  "batchId": 7,
  "numInputRows": 20,
  "inputRowsPerSecond": 11.38,
  "processedRowsPerSecond": 50.63,
  "durationMs": {
    "addBatch": 218,
    "getBatch": 0,
    "latestOffset": 0,
    "queryPlanning": 11,
    "walCommit": 88,
    "commitOffsets": 75,
    "triggerExecution": 395
  },
  "stateOperators": [
    {
      "numRowsTotal": 3,
      "numRowsUpdated": 1,
      "numRowsRemoved": 2,
      "memoryUsedBytes": 1048,
      "numRowsDroppedByWatermark": 4
    }
  ],
  "sources": [
    {
      "description": "KafkaV2[Subscribe[ops.shipments]]",
      "startOffset":  {"ops.shipments": {"0": 10, "1": 0}},
      "endOffset":    {"ops.shipments": {"0": 20, "1": 5}},
      "latestOffset": {"ops.shipments": {"0": 25, "1": 5}},
      "numInputRows": 15
    }
  ]
}
```

Four things in there are worth more than the rest.

**`durationMs.triggerExecution`** is the whole trigger. Compare it with the trigger interval: if a query triggers every 10 seconds and `triggerExecution` is consistently above 10,000, every trigger starts late and the lag only grows. The other phases say *where* the time went — `addBatch` is the actual work, `walCommit` and `commitOffsets` are the checkpoint, `latestOffset` is asking Kafka how far it has got.

**`stateOperators[].numRowsTotal`** is how many rows the join is holding because their partner may still arrive. This is the number PR 13's watermark experiment was about, now visible continuously rather than in a test. A line that only ever climbs is a watermark that is not advancing.

**`stateOperators[].numRowsDroppedByWatermark`** is the one nobody expects. When a row arrives with an event time older than the watermark, Spark discards it. No exception, no warning, nothing in the logs. The row was yours and now it is gone. This counter is the only place that is reported, which makes it the single most important number in the list.

**`sources[].latestOffset` minus `sources[].endOffset`** is lag: where the topic ends now, minus where this batch finished. Per partition, which matters, because one hot partition behind by a million while the other two sit at zero averages out to something that looks fine.


## Step 2 — make the interesting part a pure function

The listener itself has nothing in it worth testing — it receives an event, calls a function, sends the results. Everything that can actually be *wrong* is in the translation from that JSON into named numbers. So that is a separate function with no Spark in it at all:

```python
def metrics_from(progress: Mapping[str, Any]) -> dict[str, float]:
```

A dict in, a dict out. The entire unit test file is a recorded progress event and some assertions, and it runs in milliseconds with no JVM.

This is the same instinct as `core/` holding the join: the part with the decisions in it should not need the heavy machinery to exercise.

One decision that lives inside it: a query with no stateful operator must produce **no** state metrics, not zeros. `stateOperators` is an empty list for a stateless query, and the loop over it simply does not run. A flat zero would be a claim — "the join is holding nothing" — and that is a different statement from "there is no join here".


## Step 3 — lag, by subtraction, with one trap

```python
def kafka_lag(source: Mapping[str, Any]) -> dict[str, int]:
    end, latest = source.get("endOffset"), source.get("latestOffset")
    if not isinstance(end, dict) or not isinstance(latest, dict):
        return {}
    ...
```

The type check is not defensive clutter. **Not every source reports offsets as a map.** The rate source and the file source report plain integers, and the file source reports `null` for `startOffset` on its first batch. Feeding those to code that expects `{topic: {partition: offset}}` would raise inside a listener callback, on the driver, on every trigger — and a listener that raises is a listener Spark may stop calling. Returning nothing is correct: those sources have no lag to report.

Two smaller decisions sit in the same function. The subtraction is clamped at zero, because an unclean leader election can truncate a broker's log and leave the head behind a point we have already read — and a negative *records behind* is not something a dashboard can mean. And the loop runs over `latestOffset`, so a partition that appears in `endOffset` but not in `latestOffset` is simply absent for that trigger rather than reported as zero. Zero would be a claim that it has caught up, and we do not know that. Same reasoning as the empty `stateOperators` list: a missing series is honest, a fabricated zero is not.

The resulting metric name is `lag.<topic>.<partition>`, and that is where the trap is. Our topics are `ops.shipments` and `ops.deliveries`. StatsD's separator is a dot, and so is the one inside the topic name, so `pipeline.stream.lag.ops.shipments.0` is six segments and only three of them are ours. Step 6 is where that gets untangled.


## Step 4 — a metrics client that cannot hurt the query

The StatsD client is about thirty lines rather than a dependency. The protocol is one line per metric — `name:value|g` for a gauge — and writing it keeps visible what would otherwise be a library call. Four details in it are not decoration, and three of them came out of review rather than out of the first draft.

```python
self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
self._socket.setblocking(False)
```

**UDP, non-blocking.** The listener callback runs on the driver, in the query's own progress thread. A TCP metrics client that blocks on connect to a collector that has gone away would stall that thread. Observability that can take down the thing it observes is worse than no observability.

**And `setblocking(False)` does not do as much as it looks like it does.**
It governs the send syscall. It does not govern the *name lookup* that `sendto()` performs when you hand it `("statsd", 9125)` — that resolution is a separate synchronous call that ignores the socket's mode entirely. With a hostname and a slow resolver, the "non-blocking" client blocks once per metric per trigger, on exactly the thread the non-blocking was for. So the address is resolved once, at construction:

```python
self._address = self._resolve()  # getaddrinfo, once
...
self._socket.sendto(line.encode(), self._address)
```

That fixes the address for the life of the process, which is right for a sidecar and wrong for a collector that moves. It is a trade, made knowingly.

```python
except OSError:
    log.debug("dropped metric: %s", line, exc_info=True)
```

**Errors are swallowed deliberately.** UDP is fire-and-forget in theory, but sending to a host that is not listening still raises `ConnectionRefusedError` on Linux once an ICMP rejection comes back. Losing a metric is acceptable. Killing a query because its metrics sink is down is not.

**And the number has to survive being formatted.** The first version used `f"{value:g}"`, which is the obvious choice and quietly wrong:

```
1048576.0   ->  1.04858e+06
50331648.0  ->  5.03316e+07
```

`%g` gives six significant digits and switches to exponential notation past a million. statsd-exporter parses that without complaining, so nothing fails — you simply get `1048580` back for a value of `1048576`. A megabyte of state store, or a partition a few million records behind, is not an exotic reading; it is precisely the region where the number starts to matter. Formatting with explicit precision costs one line.

The same line also drops non-finite values, which is not hypothetical: Spark reports `inputRowsPerSecond` as `NaN` on a first trigger, Python's `json.loads` accepts a bare `NaN` quite happily, and `nan` is not a number statsd-exporter can parse.

Every number here is a gauge — a sample of a current value, not a counter that only goes up — including `numRowsDroppedByWatermark`, which is per-trigger rather than cumulative.


## Step 5 — register it on the session, and before the query starts

```python
spark.streams.addListener(MetricsListener(Statsd(app.metrics)))
query = stream.run(...)
```

Two things about those two lines.

**On the session, not the query.** `spark.streams` is session-wide, so one listener sees every query the session runs. Registering per-query would mean remembering to do it at every start site.

**Before `start()`.** A listener added afterwards misses whatever has already fired. For a 10-second trigger that is a trigger or two, which is exactly the window in which startup problems show up.

And the listener runs on the **driver only**. Executors never see progress events. This is why `METRICS_ENABLED` is set on the `stream` service in Compose and nowhere else: that container is the driver.

Like the event log in PR 15, metrics are **off** in the packaged defaults. A bare `make run` on a laptop should not be sending UDP to a hostname that only resolves inside a Compose network.

One more thing inside the callback: it is wrapped.

```python
try:
    progress = json.loads(event.progress.json)
    ...
except Exception:
    log.warning("could not report metrics for this trigger", exc_info=True)
```

Spark's listener bus does catch what a listener throws, so an unwrapped version would not kill the query. But it would report the failure as a JVM error on every single trigger, which is a strange way for a metrics path to fail when the same module goes to the trouble of swallowing a dropped datagram at `debug`. One boundary, chosen on purpose, and a test that holds it.


## Step 6 — dots become labels

Prometheus has no dots in metric names, so something must translate. Without a mapping file, statsd-exporter does the dumb thing and swaps dots for underscores:

```
pipeline.stream.lag.ops.shipments.0  ->  pipeline_stream_lag_ops_shipments_0
pipeline.stream.lag.ops.shipments.1  ->  pipeline_stream_lag_ops_shipments_1
```

Those are two unrelated metrics as far as Prometheus is concerned. You cannot sum them, graph them as one series, or write a single alert across them. Every new partition invents a new metric name.

The mapping pulls the varying parts out into labels:

```yaml
- match: "pipeline.stream.lag.*.*.*"
  name: "pipeline_stream_kafka_lag_records"
  labels:
    topic: "$1.$2"
    partition: "$3"
```

Each `*` matches exactly one dot-separated segment, so the topic has to be reassembled from the first two. That is the trap from step 3, paid off in one line. Now there is one metric with a `topic` and a `partition` label, and `max(pipeline_stream_kafka_lag_records)` is a sentence you can write.

One more line in that file matters:

```yaml
defaults:
  ttl: 30s
```

A StatsD gauge holds its last value forever. Stop the query and the lag graph shows a confident flat line at whatever it was when the process died — the most dangerous kind of wrong, because it looks like a healthy steady state. Three missed triggers and the series simply ends, which is the honest answer.


## Step 7 — check it

```bash
make up                       # now also starts statsd, prometheus, grafana
make produce SCENARIO=matched
make stream AWAIT_SECONDS=120
```

While it runs:

```bash
# The raw translation, before Prometheus is involved. Fastest way to tell
# whether metrics are leaving the driver at all.
curl -s localhost:9102/metrics | grep pipeline_stream
```

Expect `pipeline_stream_duration_milliseconds{phase="triggerExecution"}`, a `pipeline_stream_state_rows`, and a `pipeline_stream_kafka_lag_records` per partition.

Grafana takes port 3000 and Prometheus 9090, which are conventional and also commonly already in use on a development machine. If `make up` fails with *port is already allocated*, that is what happened; change the host side of the mapping in `docker/compose.yaml`.

Then open **http://localhost:3000/d/pipeline-stream**. No login: the Grafana container runs with anonymous admin access, which is fine for a demo on localhost and is exactly why none of these ports should ever be exposed further.

The experiment worth running is PR 13's, watched live:

```bash
make produce SCENARIO=unmatched   # a shipment with no delivery
```

*State store rows* steps up and stays up — the join is holding the row because a delivery could still arrive. Then:

```bash
make produce SCENARIO=advance     # push event time forward on both topics
```

and it drops back, with *evicted this trigger* spiking as the watermark finally passes the held row. That is the entire watermark story in two panels.

### What each panel is for

| Panel                         | The failure it shows                                                                              |
| ----------------------------- | ------------------------------------------------------------------------------------------------- |
| Trigger duration by phase     | `triggerExecution` above the trigger interval: the query can never catch up. The phase breakdown says whether it is the work or the checkpoint.           |
| Kafka lag by partition        | A single partition climbing while the others are flat — skew, not throughput.                     |
| State store rows              | A line that only climbs: the watermark is not advancing, and memory is the next thing to go.      |
| State store memory            | Why the driver's heap looks the way it does.                                                      |
| Rows dropped by watermark     | Any non-zero value is data you have lost and would otherwise never hear about.                    |
| Throughput                    | Processed below arriving, sustained, is the shape of a query that will not recover on its own.    |


## Step 8 — testing a callback

Two layers, because they fail differently.

**Unit** (`tests/unit/test_metrics.py`) — a recorded progress event and assertions about the numbers that come out. No Spark, no socket. Several of these exist because review found the bug first: the large-value formatting, the `NaN`, the clamp, and the wrapped callback each have a test now, which is the only thing that stops them coming back.

**Integration** (`tests/integration/test_metrics_listener.py`) — a real query against a rate source, a real UDP socket bound to a free port, and an assertion that datagrams actually arrive. This covers the three things the unit tests structurally cannot:

- whether Spark ever calls the listener at all
- whether the progress JSON really has the keys we read out of it
- whether a datagram actually leaves the process

Each of those is a way for metrics to be silently absent, and the first symptom of any of them is an empty dashboard at the worst possible moment.


## Definition of done

- [ ] A bare `make run` sends no UDP anywhere
- [ ] `make up` brings up statsd-exporter, Prometheus and Grafana
- [ ] `curl localhost:9102/metrics` shows `pipeline_stream_*` while streaming
- [ ] Lag appears per partition with a `topic` label, not as six metric names
- [ ] The Grafana dashboard loads with no login and no datasource to pick
- [ ] `SCENARIO=unmatched` then `SCENARIO=advance` moves the state store panel
- [ ] Stopping the query ends the series rather than freezing it
- [ ] A state store above a megabyte reports its real size, not `1.04858e+06`
- [ ] A broken progress event logs a warning and the query keeps running


## Files in this PR

| File                                              |                                                                           |
| ------------------------------------------------- | ------------------------------------------------------------------------- |
| `src/pipeline/metrics.py`                         | the listener, the StatsD client, and the pure translation                 |
| `src/pipeline/conf/app.toml`                      | a `[metrics]` section, off by default                                     |
| `src/pipeline/config.py`                          | `AppConfig` carries a `StatsdConfig`                                      |
| `src/pipeline/main.py`                            | registers the listener on the session before the query starts             |
| `docker/compose.yaml`                             | statsd-exporter, Prometheus and Grafana; `METRICS_ENABLED` on the driver  |
| `docker/metrics/statsd-mapping.yaml`              | dotted names become metrics with labels                                   |
| `docker/metrics/prometheus.yaml`                  | scrape config                                                             |
| `docker/metrics/grafana-datasource.yaml`          | the datasource, provisioned not clicked                                   |
| `docker/metrics/grafana-dashboards.yaml`          | the dashboard provider                                                    |
| `docker/metrics/dashboards/pipeline.json`         | six panels, each tied to a failure                                        |
| `tests/unit/test_metrics.py`                      | twenty tests over a recorded progress event                               |
| `tests/integration/test_metrics_listener.py`      | a real query, a real socket                                               |
| `tests/unit/test_config.py`                       | four more, for the metrics section                                        |
| `Makefile`                                        | `make up` prints the Grafana and Prometheus URLs                          |
| `README.md`                                       | status, layout and the new URLs                                           |

Next: **PR 17** — running it on Kubernetes.
