# Running the demo

How to get the streaming join doing real work, in a browser, in front of
someone. Roughly fifteen minutes end to end, most of it the first build.

Everything below works the same on a laptop with Docker. Codespaces is
covered because it needs no local setup and produces a URL you can share.

---

## 1. Pick the machine

From the repository: **Code → Codespaces → New with options**, then choose
**8-core** for a demo.

The devcontainer declares a 4-core minimum and that is genuinely a minimum.
The stack is eighteen containers — Spark master, worker and History Server,
MinIO, the Iceberg REST catalog, Kafka, Schema Registry, statsd-exporter,
Prometheus, Grafana — and a Spark driver with two executors on top. On two
cores everything starts and then nothing gets enough CPU to be worth
watching, which is worse than failing, because nothing looks broken.

Codespaces bills core-hours at the core count, and GitHub Free includes 120
a month. That is 60 hours on a 2-core machine, 30 on a 4-core, and **15 on an
8-core** — so stop the codespace when the demo is over.

## 2. Wait for the first build

Creation runs `on-create.sh`: install `uv`, resolve the locked dependencies,
build the image. Several minutes, once. Every later start runs `post-start.sh`
instead, which only brings the cluster up.

When the terminal prints `Cluster is up`, the Ports panel has eight forwarded
ports with labels.

## 3. Start the query, then the stream

Two terminals.

```bash
# terminal 1 — the streaming join, running for fifteen minutes
make stream AWAIT_SECONDS=900
```

Wait for this line before going on. It means the topics and tables exist and
the query is subscribed:

```
stream  ops.shipments + ops.deliveries -> lake.ops.shipment_deliveries
```

```bash
# terminal 2 — 190,000 records across the two topics
make load SHIPMENTS=100000
```

That takes about 95 seconds at the default 2,000 records/second, and prints
progress every 20,000.

Nothing is lost if you start them the other way round: the Kafka source reads
from `earliest`, so a query started afterwards still sees everything.

## 4. What to watch, in order

**Grafana, port 3000**, dashboard *Streaming join*. Refresh is 5s and the
trigger is 10s, so give it two triggers before deciding a panel is broken.

| Panel | What it does during the run |
|---|---|
| Kafka lag by partition | Spikes as the load starts, then drains. Six series, three partitions on each of two topics |
| Trigger duration by phase | `triggerExecution` should stay under 10s. Above it, the query is falling behind and lag stops draining |
| State store rows | Climbs while the join holds shipments waiting for a delivery, then plateaus as the watermark starts releasing them as fast as new ones arrive |
| Rows dropped by watermark | Goes **non-zero**. About 2% of deliveries are generated deliberately late — a panel that can only ever read zero teaches nothing about the panel |
| Throughput | Arriving and processed should track each other. Processed below arriving, sustained, is the shape of a query that will not catch up |

**Spark driver UI, port 4040.** The Structured Streaming tab is the one worth
showing: input rate, process rate, batch duration, and the operator graph for
the stream-stream join. The Executors tab shows where the work is.

**Spark master UI, port 8080** shows the application and its executors.
**MinIO console, port 9001** (`pipeline-local` / `pipeline-local-secret`) is
where the Iceberg table physically lands: `warehouse/ops/shipment_deliveries`
fills with Parquet and metadata as triggers commit.

## 5. The numbers to quote

The generated stream is deterministic, so these are the same every run:

```
189,944 records     100,001 shipments + 89,943 deliveries
 10,058 unmatched   shipments that never get a delivery at all
  1,733 late        ~1.9% of deliveries, too far behind to join
2 hours             of event time, compressed into ~95 seconds of wall clock
```

Every shipment is emitted exactly once, so the output table ends at
**100,001 rows**: **88,210** matched, and **11,791** null-padded — the 10,058
with no delivery, plus the 1,733 whose delivery arrived too late to join.

Those null-padded rows are the point of the whole project. A left join on two
unbounded streams cannot know that a delivery will never arrive, so it holds
the shipment until event time proves no match is still possible, and only
then emits it with nulls.

**The last two records are why that works.** A watermark only advances when
records arrive, so when a bounded load stops, the watermark freezes where the
data stopped and the newest shipments stay in the state store forever. The
generator closes with a matched pair dated far enough ahead to drag the
watermark past the whole window — on *both* topics, because the watermark is
the minimum across inputs. Without it, about a third of the unmatched
shipments are never released, and the count below quietly disagrees with the
dashboard. Turn it off with `--no-flush` to see that happen.

Afterwards, count what landed:

```bash
docker compose -f docker/compose.yaml run --rm batch \
  /app/.venv/bin/python -c "
from pipeline.config import AppConfig
from pipeline.session import build_session
app = AppConfig.load()
spark = build_session('count', catalog=app.catalog)
spark.sql('SELECT count(*) total, count(delivery_id) matched FROM lake.ops.shipment_deliveries').show()
"
```

## 6. Showing someone

In the **Ports** panel, right-click a port → **Port Visibility** → **Public**.
That gives a `*.app.github.dev` URL anyone can open.

Make public only what you are presenting, and only for as long as the demo
runs. **Nothing in this stack has authentication** — Grafana is configured
with anonymous admin access precisely because it is a throwaway local
cluster, and the Spark UIs never had any. A public port is an open URL for
as long as the codespace is running.

Stop the codespace afterwards: **Codespaces → ⋯ → Stop codespace**. Stopping
also ends the billing.

## 7. Turning the knobs

```bash
make load SHIPMENTS=1000000 RATE=20000      # ten times the volume, ten times the rate
make load RATE=0                            # as fast as the broker will take it
make load MATCH_RATE=0.5                    # half the shipments never match
make load LATE_RATE=0.25                    # a quarter of deliveries arrive too late
make load WINDOW="20 minutes"               # the same records over less event time
```

`MATCH_RATE` and `LATE_RATE` are the two that change what the dashboard
*means* rather than how busy it is. Dropping `MATCH_RATE` to 0.5 makes the
state store panel climb much higher, because half of everything is waiting
for a partner that is never coming. Raising `LATE_RATE` drives the dropped
rows panel, which is the one worth having an answer for before anyone asks.

`WINDOW` is the subtle one: it controls how fast event time moves per record.
A narrower window with the same record count means the watermark advances
more slowly, so state is held longer.

## 8. When it does not work

| Symptom | Cause |
|---|---|
| Grafana panels empty, no errors | Give it two triggers. Then `curl -s localhost:9102/metrics \| grep pipeline_stream` — nothing there means the driver is not sending |
| `triggerExecution` climbing past the trigger interval | Not enough CPU. An 8-core machine, or `make load RATE=500` |
| Lag climbing and never draining | Same cause. The query is behind and will stay behind |
| Port 4040 shows nothing | It only exists while a driver runs. Start `make stream` first |
| `make load` exits instantly with a Kafka error | The broker is not ready. `make up` and wait for the healthcheck |
| Codespace feels frozen during creation | The image build. Watch the Codespaces creation log |

## 9. Teardown

```bash
make down          # stop the containers and drop the volumes
```

Then stop the codespace itself, or it idles for 30 minutes and stops on its
own — still billing until it does.
