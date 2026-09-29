# PR 5 — walkthrough

**Goal:** declare the shipment and delivery events, and the shape the join
produces, as explicit `StructType`s — then assert all three rather than trusting
them.

**Why now:** the infrastructure is finished. Everything from here is domain
logic, and the first piece of domain logic is agreeing what the data *is*.
Schemas come before the join because the join's correctness is a statement about
schemas.

**Not in this PR:** the join itself, any I/O, Iceberg, Kafka.

> **Verification status.** Everything here ran. This is the first PR with no
> Docker dependency at all, which is the point: a pure core is testable without
> a cluster, and `make test` proves it in four seconds.

---

## Step 0 — the domain

Two events and one question.

| Event | Fields |
|---|---|
| **shipment** | `shipment_id`, `carrier`, `dispatch_time` |
| **delivery** | `delivery_id`, `shipment_id`, `delivered_time` |

A shipment is dispatched. A delivery event may follow. The question the app
answers is **which shipments never arrived** — which is a LEFT JOIN of shipments
against deliveries, keeping the rows with nothing on the right.

That framing decides the schema work. The rows that matter most are the ones
with nulls in them, so nullability is not a detail here — it is the feature.

---

## Step 1 — why declare rather than infer

`spark.read.json(path)` will happily work out a schema for you. Three reasons
not to let it.

**It throws on empty input.** Verified:

```python
spark.createDataFrame([])
# PySparkValueError: [CANNOT_INFER_EMPTY_SCHEMA] Can not infer schema from an empty dataset.
```

This project's central rule is that `main` is always runnable, and `make smoke`
runs it against an *empty* stack. An inferring job cannot satisfy that: it fails
precisely when there is nothing to process, which is the normal state of a
freshly deployed pipeline.

**It costs a pass over the data.** Inference reads the input to decide the
types, before the job that reads the input to do the work.

**It makes the schema a function of whatever arrived.** Declared, the schema is
a contract you can be held to and a reviewer can read. Inferred, it is an
observation about yesterday's data, and it can change between runs without
anyone touching the code.

> A note on rigour, since this project keeps claiming things and then checking
> them. An earlier draft of this file also said inference "silently guesses
> wrong types on a partial sample". Testing it with a JSON file of 200 integer
> values followed by one string, read with `samplingRatio 0.1`, Spark inferred
> `string` and lost no rows. The claim did not survive, so it is not here. The
> three reasons above did survive.

---

## Step 2 — the declarations

```python
JOIN_KEY = "shipment_id"

SHIPMENT = StructType(
    [
        StructField("shipment_id", StringType(), nullable=False),
        StructField("carrier", StringType(), nullable=False),
        StructField("dispatch_time", TimestampType(), nullable=False),
    ]
)

DELIVERY = StructType(
    [
        StructField("delivery_id", StringType(), nullable=False),
        StructField("shipment_id", StringType(), nullable=False),
        StructField("delivered_time", TimestampType(), nullable=False),
    ]
)
```

Every field non-null, because a shipment without a dispatch time or a delivery
without a shipment id is not a degraded record — it is not one of these events
at all. `JOIN_KEY` is a constant rather than a string literal repeated in four
places, so a rename is one edit.

Note where this lives: `src/pipeline/core/`. The package docstring is one line —
*"Pure domain logic. No session, no I/O, no configuration."* — and it is a
constraint, not a description. Nothing under `core/` may import an adapter.

---

## Step 3 — nullability is not decoration

Here is the part worth slowing down for.

`nullable=False` is not a comment. Spark enforces it:

```python
spark.createDataFrame([("shp-2", None, datetime(2026, 9, 13, 10, 0))], SHIPMENT).collect()
# PySparkValueError: [FIELD_NOT_NULLABLE_WITH_NAME] field carrier:
#   This field is not nullable, but got None.
```

*Verified.* It rejects the row rather than coercing it, which is what you want
from a contract.

Now the consequence that decides the third schema. Build both sides with every
field declared non-null, put one perfectly matched row in each, and left join
them:

```
LEFT JOIN output nullability:
  shipment_id      string     nullable=False
  carrier          string     nullable=False
  dispatch_time    timestamp  nullable=False
  delivery_id      string     nullable=True     <-- was declared False
  delivered_time   timestamp  nullable=True     <-- was declared False
```

The right-hand fields came back **nullable**, even though `DELIVERY` declares
them otherwise and the one row present matched perfectly.

This is not about the data. It is about the join type. The same two frames,
inner joined:

```
inner join, for contrast:
  shipment_id      nullable=False
  carrier          nullable=False
  dispatch_time    nullable=False
  delivery_id      nullable=False
  delivered_time   nullable=False
```

*Both verified.* A left join must be able to emit a null-padded row, so the
planner relaxes nullability on the right side whether or not any such row
exists. An inner join can make no such row, so it does not.

Which is why the output schema says so explicitly:

```python
# Right-side fields are nullable here despite DELIVERY declaring them otherwise.
# A left join null-pads unmatched shipments, which is the row this app exists to
# produce.
JOINED = StructType(
    [
        StructField("shipment_id", StringType(), nullable=False),
        StructField("carrier", StringType(), nullable=False),
        StructField("dispatch_time", TimestampType(), nullable=False),
        StructField("delivery_id", StringType(), nullable=True),
        StructField("delivered_time", TimestampType(), nullable=True),
    ]
)
```

Get this wrong in the other direction — declare `delivery_id` non-null on the
output — and you have written a schema that forbids the exact rows the
application exists to produce. It would pass review, because it looks tidy.

---

## Step 4 — asserting a schema properly

A test that checks column names passes while a `timestamp` silently becomes a
`string`. Compare all three properties:

```python
Field = tuple[str, str, bool]


def fields(schema: StructType) -> list[Field]:
    return [(f.name, f.dataType.simpleString(), f.nullable) for f in schema.fields]
```

Then a parametrized case per schema, with the expected shape written out in
full. The `JOINED` case, for instance:

```python
[
    ("shipment_id", "string", False),
    ("carrier", "string", False),
    ("dispatch_time", "timestamp", False),
    ("delivery_id", "string", True),
    ("delivered_time", "timestamp", True),
]
```

Comparing whole lists rather than asserting field by field means a *missing* or
*extra* column fails too, which per-field assertions miss.

### Testing a declaration against reality

`JOINED` is a claim about what Spark does. The test checks it against Spark:

```python
def test_left_join_produces_the_declared_output_schema(spark: SparkSession) -> None:
    shipments = spark.createDataFrame(
        [("shp-1", "carrier-a", datetime(2026, 9, 13, 10, 0))], SHIPMENT
    )
    deliveries = spark.createDataFrame([("dlv-1", "shp-1", datetime(2026, 9, 13, 10, 2))], DELIVERY)

    joined = shipments.join(deliveries, on=JOIN_KEY, how="left")

    assert fields(joined.schema) == fields(JOINED)
```

Yes, this performs a join in the PR before the join exists. That is deliberate,
and it is not PR 6's work: it uses a raw DataFrame join as an *oracle* to prove
the declaration is correct. PR 6 then implements `left_join()` and has a schema
it can be measured against, rather than one it defines by accident.

---

## Step 5 — one session for the whole suite

```python
@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-tests")
        # Default is 200, so every shuffle would schedule 200 near-empty tasks.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()
```

Three choices:

- **`scope="session"`.** JVM startup dominates. Paying it once is the difference
  between seconds and minutes as the suite grows.
- **`spark.sql.shuffle.partitions = 1`.** The default is 200. On a three-row
  DataFrame that schedules 200 tasks to produce 197 empty partitions, and the
  scheduling is the entire runtime. This single setting is the biggest lever on
  local Spark test speed.
- **`spark.ui.enabled = false`.** Nothing needs a web UI per test, and it binds
  a port.

One thing that looked like a hazard and is not: `tests/unit/test_main.py` builds
and stops its *own* sessions, and a shared fixture would break if
`getOrCreate()` handed back a stopped one. Checked directly — after `stop()`,
`getOrCreate()` returns a new, working session rather than the dead one. So
there is no ordering dependency between the test files.

---

## Step 6 — check it

```bash
make test
```

```
12 passed in 4.60s
```

No Docker, no cluster, no network. That is the signal the roadmap asks for at
this phase: if the core needed any of those, it would not be pure.

Confirm the tests actually bite, rather than trusting green:

```bash
# flip JOINED's delivery_id to nullable=False and re-run
make test     # test_left_join_produces_the_declared_output_schema must fail

# change dispatch_time to StringType and re-run
make test     # test_schema_is_exactly_as_declared[shipment] must fail
```

A schema test that cannot fail is worse than none, because it reads like
coverage.

---

## Definition of done

- [ ] `SHIPMENT`, `DELIVERY` and `JOINED` declared in `src/pipeline/core/schemas.py`
- [ ] Every field's name, type **and** nullability asserted
- [ ] `JOINED` verified against a real left join, not just declared
- [ ] The left-versus-inner nullability difference captured as a test
- [ ] `make test` passes with no Docker running
- [ ] Breaking a declaration makes a test fail

## Files in this PR

| File | |
|---|---|
| `src/pipeline/core/__init__.py` | new — the purity constraint, in one line |
| `src/pipeline/core/schemas.py` | new — `SHIPMENT`, `DELIVERY`, `JOINED`, `JOIN_KEY` |
| `tests/conftest.py` | new — session-scoped SparkSession fixture |
| `tests/unit/test_schemas.py` | new — five tests |
| `README.md` | layout, status, and a design-rule claim corrected |

Next: **PR 6** — `core/join.py`, with cases for the matched row, the unmatched
shipment, the duplicate delivery that multiplies rows, and the empty input.
