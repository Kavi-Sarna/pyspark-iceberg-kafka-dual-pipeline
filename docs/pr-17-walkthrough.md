# PR 17 — walkthrough

**Goal:** the same image, run by Kubernetes instead of by Compose. The streaming join as a `SparkApplication`, the batch join as a `ScheduledSparkApplication`, on a cluster made of containers.

**Why now:** everything before this ran where a shell could set it up. A cluster is where the assumptions that a shell was quietly satisfying come due: who the process is, what it can create, where its state lives, and which of its dependencies it brought with it.

**Not in this PR:** the metrics stack on Kubernetes, an ingress, a real registry, autoscaling the cluster itself.


## Step 0 — what the operator is for

You can run Spark on Kubernetes without an operator. `spark-submit --master k8s://...` works, and the driver pod it creates goes on to create executor pods by itself. The operator does not replace that; it wraps it.

What it adds is that the application becomes an *object*. `kubectl get sparkapplication` has a status. A failed driver can be restarted by policy rather than by you. And `ScheduledSparkApplication` exists, which is the difference between a batch job that runs and a batch job that runs every ten minutes without a cron container holding a kubeconfig.

The operator watches for these custom resources, and for each one it builds and runs a `spark-submit`. Everything below is really about what that generated submit command needs to contain.


## Step 1 — the cluster is containers

`kind` runs Kubernetes nodes as Docker containers. No cloud account, no VM, and `make kind-down` leaves nothing behind.

```yaml
nodes:
  - role: control-plane
    image: kindest/node:v1.35.8@sha256:07b2536e...
```

One node, pinned by digest like everything else in this repo. The single node is not just a convenience — step 6 leans on it, and says so.

The operator installs from its Helm chart, pinned to the same version as the operator image:

```bash
helm upgrade --install spark-operator spark-operator/spark-operator \
  --version 2.5.2 \
  --namespace spark-operator --create-namespace \
  --set "spark.jobNamespaces={pipeline}"
```

`spark.jobNamespaces` is worth a moment. Left unset, the operator watches every namespace in the cluster and needs cluster-wide permissions to do it. Naming the one namespace it should watch is both less privilege and less surprise.


## Step 2 — the executors run your image, not a Spark image

This is the sentence from the roadmap that sounds obvious and is not: *the operator needs an image containing your dependencies — the executors don't have your venv.*

On a standalone cluster the workers are long-lived processes that already exist; you submit code to them. On Kubernetes there are no workers. The driver creates executor pods, and **every one of them runs the image you named in the custom resource**. If your Python dependency is missing from that image, it is missing from every executor, and the failure arrives as a `ModuleNotFoundError` inside a task rather than at submit time.

Three things in the image make this work, and all three were put there in earlier PRs for exactly this moment:

```dockerfile
ENTRYPOINT ["/opt/entrypoint.sh"]
```

The base image's entrypoint dispatches on `driver` and `executor`, which is how Spark on Kubernetes tells a pod what to be. PR 3 kept it and left a comment saying replacing it "would work here and break executor pods on a cluster". This is the cluster.

```dockerfile
ENV PYSPARK_PYTHON=/app/.venv/bin/python
```

Without it the executor launches the base image's Python 3.10 and fails with `PYTHON_VERSION_MISMATCH`. The manifests state it again as `spark.pyspark.python`, because an image environment variable is not the same thing as a Spark setting and only one of them survives into the executor pod spec.

```yaml
mainApplicationFile: local:///app/.venv/lib/python3.12/site-packages/pipeline/main.py
```

`local://` means *already inside the image*. Nothing is uploaded from the machine running `kubectl`. And the path is a consequence of `uv sync --no-editable` back in PR 3: an editable install would have left a `.pth` file pointing at `/app/src`, which the runtime stage never copies.


## Step 3 — the jars, and the trap PR 14 set

Here is the one that would have cost an afternoon.

`session.py` asks for the Iceberg and Kafka jars through the session builder:

```python
builder.config("spark.jars.packages", PACKAGES)
```

PR 14 established that **this is silently ignored under `spark-submit`**.
PySpark starts the JVM at `getOrCreate()`, and by then the classpath is already fixed; the setting is accepted and does nothing. Compose works around it by passing `--packages` on the submit command line.

The operator runs `spark-submit`. So the builder's packages are ignored here too, and the only place they can be stated is the custom resource:

```yaml
deps:
  packages:
    - org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0
    - org.apache.iceberg:iceberg-aws-bundle:1.11.0
    - org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4
    - org.apache.spark:spark-avro_2.13:4.0.4
```

`deps.packages` becomes `--packages`, which is read in time.

Which creates a new problem: those coordinates now exist in two places, and nothing links them. Bump Iceberg in `session.py` and the cluster keeps running the old one until something fails strangely. So that is a test:

```python
def test_the_streaming_job_asks_for_the_jars_the_code_expects(stream):
    wanted = set(PACKAGES.split(",")) | set(STREAM_PACKAGES.split(","))
    assert set(stream["spec"]["deps"]["packages"]) == wanted
```

The batch job deliberately asks for less — Iceberg only. It never reads Kafka, and every scheduled run would otherwise download two connectors it does not call.

One honest caveat: `--packages` resolves from Maven Central on **every** driver and executor start. For anything that restarts often, the right answer is to bake the jars into the image. This keeps the image identical to the one Compose runs, which is worth more here than the startup seconds.


## Step 4 — the driver needs an identity

In cluster mode the driver creates and deletes executor pods. A pod's default ServiceAccount cannot do that, so the application comes up and then fails at the first executor request.

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete", "deletecollection"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["create", "get", "list", "watch", "delete"]
```

A `Role`, not a `ClusterRole`, and written out verb by verb. The chart will happily create a service account for you; doing it here means what the driver can reach is visible in the repository instead of inherited from a chart's defaults.

The `services` rule is not obvious: Spark creates a headless service so executors can reach the driver's block manager. The `configmaps` rule is how the driver's resolved configuration gets to the executors.


## Step 5 — the configuration did not change, and that is the point

The Services are named `minio`, `iceberg-rest`, `kafka` and `schema-registry`. Those are the Compose service names, which are also the hostnames already sitting in the packaged `app.toml`.

In-namespace DNS resolves a bare Service name, so **nothing about the job's configuration changes between Compose and Kubernetes**. Same image, same defaults, same environment variable names. That is the return on PR 14 having pushed configuration out of the code, and it is cheap enough to assert:

```python
def test_the_service_names_are_the_hostnames_the_config_already_has():
    ...
    host = url.split("//", 1)[1].split(":")[0]
    assert host in services, f"{host} has no Service"
```

Credentials go the other way. They are a `Secret`, pulled in with `envFrom`, never written into the application spec and never into `app.toml`. The values committed here unlock a cluster that lives on one laptop and is destroyed by `make kind-down`; what matters is the shape, which is the same one a real deployment uses with a different source behind it.


## Step 6 — the checkpoint, and the thing that only works in kind

A streaming query's checkpoint has to outlive the driver pod. That is what a checkpoint is for.

It is a `PersistentVolumeClaim` rather than `s3://` for a reason the Compose file already recorded: Structured Streaming checkpoints rely on rename being atomic, and object stores have no rename.

```yaml
volumeMounts:
  - { name: checkpoint, mountPath: /var/lib/pipeline/checkpoints }
```

on **both** the driver and the executors, because executors write the state store. And there is the catch:

> A `ReadWriteOnce` PVC can be mounted by many pods **on one node**. Our cluster has one node, so driver and executors always land on it and this works. On any real cluster it does not, and the second executor to be scheduled elsewhere fails to mount.

The real answers are a `ReadWriteMany` volume, or a checkpoint on durable storage that supports what Spark needs. Both are out of scope here; what is in scope is not pretending the single-node version generalizes.

One more detail that bites late rather than early:

```yaml
podSecurityContext:
  runAsNonRoot: true
  runAsUser: 185
  runAsGroup: 185
  fsGroup: 185
```

The image runs as uid 185, and a provisioned volume's ownership is not guaranteed to match. `fsGroup` asks the kubelet to make the volume group-writable; the cost of omitting it is not a startup error but a permission failure on the first trigger, from somewhere inside the state store. The Dockerfile hit the same thing with `/tmp/spark-events` and solved it with `chown`; a volume the cluster creates needs the Kubernetes version.

Worth being precise about what that buys here. kind's `local-path` provisioner hands back a world-writable directory, so this particular cluster would have worked without it. `fsGroup` is right for the general case rather than load-bearing for this one, and saying otherwise would be telling you something I have not run.

`runAsNonRoot: true` is the stronger half of that block. `runAsUser: 185` is a request; `runAsNonRoot` is an assertion the kubelet checks before starting the pod. If the image were ever rebased onto one that dropped its `USER 185`, the first would quietly run as root and the second refuses outright.


## Step 7 — streaming and batch want opposite things

Same code, same image, same entry point. Nearly every operational setting is the opposite.

|                       | streaming             | batch             |
| --------------------- | --------------------- | ----------------- |
| `restartPolicy`       | `Always`              | `Never`           |
| dynamic allocation    | off                   | on                |
| executors             | fixed at 2            | 1 to 4            |
| checkpoint volume     | mounted               | none              |
| schedule              | runs continuously     | `*/10 * * * *`    |

**Restart.** A streaming query is meant to be running, so a dead driver is an event to recover from. A failed batch run already has its next attempt on the schedule; restarting it in place would race that attempt and hide the failure from the run history.

**Scaling.** Executors in the streaming job hold state store partitions for the join. Removing one forces its state to be reloaded elsewhere, and a continuous query has no idle period during which that is free. Batch is where scaling belongs — and on Kubernetes it comes with a condition:

```yaml
dynamicAllocation:
  enabled: true
  shuffleTrackingEnabled: true
  shuffleTrackingTimeout: 60
```

Those are CRD fields rather than `sparkConf` entries, and the distinction matters: the operator translates them into `--conf spark.dynamicAllocation.*` itself. The first draft also set `spark.dynamicAllocation.shuffleTracking.enabled` in `sparkConf`, which is not wrong so much as an invitation for the two to disagree later. Reading the operator's submission code settles it, and shows that `shuffleTrackingEnabled` defaults to **true** whenever dynamic allocation is on — so the explicit `true` here is documentation, not a switch.

Kubernetes has no external shuffle service. On YARN an executor could hand its shuffle files to a node-level daemon and exit; here, an executor that leaves takes its shuffle output with it, and any stage still needing that output recomputes. Shuffle tracking is what keeps such an executor alive until nothing needs it. Dynamic allocation without it is not a tuning choice, it is a correctness-adjacent footgun.

**Overlap.** `concurrencyPolicy: Forbid`, because two drivers writing the same Iceberg table race for the same commit.


## Step 8 — two bugs that nothing would have caught

Writing these manifests produced exactly one real defect, and it is worth the space because the class of bug is common and silent.

```yaml
- { name: KAFKA_PROCESS_ROLES, value: broker,controller }
```

That is not what it looks like. Inside a flow mapping, the comma is the mapping's own separator, so YAML reads `value: broker` and then a second key named `controller` with no value:

```python
{"name": "KAFKA_PROCESS_ROLES", "value": "broker", "controller": None}
```

The broker would have started with half its roles. Nothing in the file looks wrong, the YAML is valid, and the eventual failure says nothing about commas.

Two layers now stand between that and a cluster.

**Offline, in the test suite** (`tests/unit/test_manifests.py`): every `env` entry must have exactly a `name` and exactly one of `value` or `valueFrom`, every value must be a string, and no manifest may contain a `null` anywhere. The stray `controller: None` fails all three.

**Against the published schemas** (`make k8s-validate`): the strict Kubernetes schemas reject the unexpected property outright. This one needs the network, so it is a separate target rather than part of `ci/run.sh` — an offline machine should not fail a run for a reason unrelated to the commit.

The schema layer carries one subtlety. A CRD uses a *structural* schema, which **prunes** unknown fields rather than rejecting them. Misspell a field in a `SparkApplication` and the API server accepts the object, drops the field, and the setting you wrote simply never applies. So the validator walks the document against the CRD's properties itself and reports anything the schema does not know:

```
30-stream.yaml  SparkApplication/pipeline-stream  ok
40-batch.yaml   ScheduledSparkApplication/pipeline-batch  ok

20 resources, 0 failed
```

### The second bug, which all of that passed straight through

The first draft of the streaming manifest said:

```yaml
arguments:
  - --mode
  - stream
  - --checkpoint
  - /var/lib/pipeline/checkpoints/stream
```

`pipeline.main` has no `--checkpoint` flag. It had one, in PR 12. PR 14 moved the checkpoint into `app.toml` with everything else and removed it, and argparse is unforgiving:

```
pipeline: error: unrecognized arguments: --checkpoint /var/lib/pipeline/...
```

Exit code 2, before a `SparkSession` exists. Under `restartPolicy: Always` that is not a failure, it is a crash loop — forever, with a message that never appears unless you go and read a driver pod's logs.

Every layer of validation passed it. The CRD requires `arguments` to be a list of strings and it is a list of strings. The schemas have no idea what the program on the other end accepts. Nor did the nineteen manifest tests, one of which *looked* like it covered this:

```python
checkpoint = args[args.index("--checkpoint") + 1]
mounts = [m["mountPath"] for m in spec["driver"]["volumeMounts"]]
assert any(checkpoint.startswith(m) for m in mounts)
```

That checks the YAML against itself. It is a tautology wearing the clothes of an invariant, and it gave confidence rather than coverage.

The real test is three lines, and needs neither cluster nor network:

```python
def test_the_arguments_are_ones_the_entry_point_accepts(fixture, request):
    spec = ...
    parse_args(spec.get("arguments", []))
```

**A manifest can only be checked against the program it launches.**

And then the part that makes this more than a PR-17 story. `docker/compose.yaml` passes its own command line, and it was carrying the same dead flag:

```yaml
--mode stream
--checkpoint /var/lib/pipeline/checkpoints/stream
```

`make stream` had been broken since PR 14. Three PRs shipped over the top of it, because nothing ever compared a hand-written command line with the parser that receives it. The equivalent test now does, for every Compose service that passes `--mode`.

The checkpoint path itself was never the problem: `[stream].checkpoint` in `app.toml` already says `/var/lib/pipeline/checkpoints/stream`, which is the mounted path. The argument was not just wrong, it was redundant.


## Step 9 — run it

Every `k8s-*` and `kind-*` target that touches a cluster checks the context first:

```
ERROR: kubectl context is 'arn:aws:eks:...:cluster/prod-data-eks',
       expected 'kind-pipeline'.
```

That is not a hypothetical. It is what the guard printed the first time it ran on the machine these manifests were written on. `kubectl` keeps one current context for everything, these manifests carry demo credentials, and two of the targets delete things. kind names its context deterministically, so there is no reason to find out afterwards.

```bash
make k8s-validate    # no cluster needed
make kind-up         # cluster, operator, image loaded
make k8s-up          # data stack, then topics, subjects and tables
make k8s-stream      # the streaming join
make k8s-batch       # the scheduled batch job
```

Then:

```bash
kubectl -n pipeline get sparkapplication -w
make k8s-logs        # follow the driver
make k8s-ui          # Spark UI on localhost:4040
```

The thing worth watching is the executor pods appearing *after* the driver — created by it, not by the operator:

```bash
kubectl -n pipeline get pods -w
```

Produce into the in-cluster broker with a one-off pod, or port-forward `svc/kafka` and run the existing producer against `localhost:9092`.

Teardown:

```bash
make k8s-down        # the applications
make kind-down       # the whole cluster
```


## Definition of done

- [ ] `make k8s-validate` passes with no cluster present
- [ ] `make kind-up` leaves the operator running and the image loaded
- [ ] `make k8s-up` completes the bootstrap Job
- [ ] The streaming driver reaches `RUNNING` and creates two executor pods
- [ ] The driver writes a checkpoint to the PVC rather than failing on uid 185
- [ ] Deleting the driver pod brings back a driver that resumes from it
- [ ] A scheduled batch run appears within ten minutes and completes
- [ ] `make kind-down` leaves nothing behind


## Files in this PR

| File                              |                                                                                                                   |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `k8s/kind.yaml`                   | the cluster: one node, node image pinned by digest                                                                |
| `k8s/00-namespace.yaml`           | namespace, the driver's ServiceAccount and Role, credentials, the checkpoint volume                               |
| `k8s/10-stack.yaml`               | MinIO, the Iceberg REST catalog, Kafka and Schema Registry, named so the existing config finds them               |
| `k8s/20-bootstrap.yaml`           | a plain Job for topics, subjects and tables: no Spark work, so no driver                                          |
| `k8s/30-stream.yaml`              | the streaming join as a `SparkApplication`                                                                        |
| `k8s/40-batch.yaml`               | the batch join as a `ScheduledSparkApplication`                                                                   |
| `ci/k8s.sh`                       | validation against the published Kubernetes and operator schemas, including fields a CRD would silently prune     |
| `tests/unit/test_manifests.py`    | twenty-six offline checks, including the one that compares arguments with the parser                              |
| `Makefile`                        | the `kind-*` and `k8s-*` targets, each guarded on the kubectl context                                             |
| `docker/compose.yaml`             | the same dead flag, removed                                                                                       |
| `pyproject.toml`                  | pyyaml in the dev group                                                                                           |
| `README.md`                       | status, layout and the Kubernetes section                                                                         |

Next: **PR 18** — operational drills. Kill the driver mid-stream and watch it resume from the checkpoint; delete the checkpoint and reason about what comes out twice; change the query and watch a restart refuse the old state.
