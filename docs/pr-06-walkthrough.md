# PR 6 — walkthrough

**Goal:** `left_join(shipments, deliveries, spec) -> DataFrame` — the whole point of the application, as a pure function, with tests for what a LEFT JOIN guarantees and what it conspicuously does not.

**Why now:** the schemas exist and the infrastructure is done. This is the last PR before I/O, and it is the one piece that must be right: every later PR is a different way of feeding this function.

**Not in this PR:** Iceberg, Kafka, runners, watermarks, any I/O whatsoever.

> **Verification status.** Everything ran. Twenty-one tests, no Docker, 7.8 seconds. Two things broke while building this and are written up where they happened: a mypy failure on `Column + timedelta`, and a test-ordering bug in the fixture from PR 5 that this PR exposed.


## Step 0 — what a LEFT JOIN actually promises

It is worth being precise, because three of the four behaviours surprise people.

|                                   |                                                                                           |
| --------------------------------- | ----------------------------------------------------------------------------------------- |
| **Every left row survives**       | A shipment with no delivery is kept, padded with nulls. This is the guarantee.            |
| **Right-only rows vanish**        | A delivery whose `shipment_id` matches no shipment is dropped entirely.                   |
| **Row count is not preserved**    | One row per *matching right row*. A shipment with two deliveries becomes two rows.        |
| **Nullability changes**           | Right-hand columns become nullable regardless of how they were declared — PR 5's finding. |

Only the first is what most people mean by "left join". The third is how volume silently multiplies in production.

The test fixture is the dataset from Ground School F3, chosen because one set of rows exercises all four at once:

```python
shipments:  shp-1 (10:00)   shp-2 (10:05)    shp-3 (10:10)
deliveries: dlv-1 -> shp-1  dlv-2 -> shp-3   dlv-3 -> shp-3   dlv-4 -> shp-9
```

`shp-1` matches once, `shp-2` never, `shp-3` twice, and `dlv-4` points at a shipment nobody has heard of.


## Step 1 — the spec, so the core has no branches

```python
@dataclass(frozen=True)
class JoinSpec:
    keys: tuple[str, ...]
    left_time: str
    right_time: str
    # A delivery counts only if it lands within this window after dispatch.
    # None joins on the keys alone.
    max_lag: timedelta | None = None
```

The architecture note in the roadmap is emphatic about this: a stream-stream left outer join fails at analysis time unless both sides carry a watermark *and* the join condition is time-bounded, while batch has no such requirement. The tempting fix is `if is_streaming:` inside the core. Don't — pass the difference in as data and the core stays a pure function of its arguments.

`frozen=True` because a spec that a caller can mutate mid-pipeline is a bug waiting to happen, and a frozen dataclass is hashable, which will matter when specs get cached or compared.

`max_lag` is not speculative streaming scaffolding: it is meaningful in batch too, it is used here, and it is tested. The streaming PR then supplies a different value rather than changing the interface.


## Step 2 — the join, and the duplicate column nobody mentions

```python
def left_join(shipments: DataFrame, deliveries: DataFrame, spec: JoinSpec) -> DataFrame:
    """Every shipment, with its delivery where one matched and nulls where none did."""
    joined = shipments.join(deliveries, _condition(shipments, deliveries, spec), "left")

    # A condition built from columns emits the key twice. Select the key from
    # the left so it is never null, and name the output columns explicitly
    # rather than relying on join order.
    carried = [c for c in deliveries.columns if c not in spec.keys]
    return joined.select(
        *(shipments[c] for c in shipments.columns),
        *(deliveries[c] for c in carried),
    )
```

Here is the trap. `df.join(other, on="shipment_id", how="left")` — the *string* form — tells Spark the key is the same on both sides, and it emits one merged column. But a time bound means the condition has to be a `Column` expression, and the moment you do that:

```python
j = s.join(d, s["shipment_id"] == d["shipment_id"], "left")
j.columns
# ['shipment_id', 'carrier', 'dispatch_time', 'delivery_id', 'shipment_id', 'delivered_time']
```

*Verified.* **Two `shipment_id` columns.** Everything still "works" until something selects that name and gets an ambiguity error, or writes the frame out and produces a table with a duplicate field.

So the function selects explicitly. Taking the key from `shipments` also matters for a left join specifically: the right-hand copy is null for unmatched rows, and selecting that one would null out the very identifier you need.

The explicit select produces exactly the declared shape:

```
shipment_id      string     nullable=False
carrier          string     nullable=False
dispatch_time    timestamp  nullable=False
delivery_id      string     nullable=True
delivered_time   timestamp  nullable=True
```

which is `JOINED` from PR 5, asserted by a test rather than eyeballed.


## Step 3 — the condition, and the silent inner join

```python
def _condition(shipments: DataFrame, deliveries: DataFrame, spec: JoinSpec) -> Column:
    on: Column = reduce(operator.and_, (shipments[key] == deliveries[key] for key in spec.keys))
    if spec.max_lag is None:
        return on

    # Inside the join condition, never as a filter afterwards: a null-padded row
    # fails any comparison, so a post-filter would drop the unmatched shipments
    # this job exists to find.
    dispatched = shipments[spec.left_time]
    delivered = deliveries[spec.right_time]
    # F.lit rather than a bare timedelta: the operand must be a Column.
    within: Column = (delivered >= dispatched) & (delivered <= dispatched + F.lit(spec.max_lag))
    return on & within
```

**This is the most important comment in the codebase so far.**

A time bound feels like a filter, and the natural thing to write is:

```python
left_join(...).filter(F.col("delivered_time") > F.col("dispatch_time"))
```

That turns your left join into an inner join, silently. For `shp-2` there is no delivery, so `delivered_time` is `NULL`; `NULL > anything` evaluates to `NULL`, not `True`; and the row is discarded. You have just deleted the only rows the application exists to produce, and nothing errors.

It has its own test, asserting the wrong behaviour explicitly so the difference is visible:

```python
def test_a_post_filter_would_turn_this_into_an_inner_join(
    shipments: DataFrame, deliveries: DataFrame
) -> None:
    """Why max_lag lives in the condition rather than in a later .filter().

    For an unmatched shipment the right-hand column is NULL, and NULL > anything
    is NULL rather than true, so the null-padded row is discarded. That deletes
    exactly the rows this job exists to produce.
    """
    joined = left_join(shipments, deliveries, SPEC)
    post_filtered = joined.filter(F.col("delivered_time") > F.col("dispatch_time"))

    assert "shp-2" in [r[0] for r in rows(joined)]
    assert "shp-2" not in [r[0] for r in rows(post_filtered)]
```

Inside the condition it behaves correctly: a delivery outside the window simply does not match, and the shipment is still emitted, null-padded. That test exists too, and it is the one that proves the distinction is real rather than theoretical.

### `F.lit` around the timedelta

`dispatched + spec.max_lag` runs fine — Spark renders it as `PT2H` — but mypy rejects it:

```
error: Unsupported operand types for + ("Column" and "timedelta")  [operator]
```

The runtime supports it and the type stubs do not. Rather than silencing the checker, `F.lit(spec.max_lag)` makes it a genuine `Column + Column`. That is the better fix anyway: it is the operation Spark actually performs, spelled out.


## Step 4 — the ordering bug PR 5 left behind

PR 5's fixture was `scope="session"`. Adding `tests/unit/test_join.py` broke two tests in `test_schemas.py`:

```
AttributeError: 'NoneType' object has no attribute 'sc'
```

Nothing in `test_schemas.py` changed. What changed is that `test_join` sorts before `test_main`, which sorts before `test_schemas`. So:

1. `test_join` requests the fixture → a session is created and cached.
2. `test_main` tests `main()`, which calls `spark.stop()` on the **same** session, because Spark allows one context per JVM.
3. `test_schemas` gets the cached fixture value — a dead object.

PR 5 considered this hazard and cleared it on the wrong evidence. The check was "does `getOrCreate()` after `stop()` return a working session?" — it does. But a session-scoped fixture is not calling `getOrCreate()` again; it is handing back the reference it captured the first time. Verified properly this time:

```python
s.stop()
SparkSession.getActiveSession()  # None
s.createDataFrame([(1,)], "x int")  # AttributeError
```

The fix is to stop caching:

```python
@pytest.fixture
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session
```

Function-scoped, but `getOrCreate()` is nearly free when a session is live and builds a fresh one when the last was stopped. It keeps the speed benefit without the staleness. A session-scoped autouse finalizer stops whatever is active at the end so the JVM exits cleanly.

`spark.sql.shuffle.partitions = 1` stays, and it is still the single biggest lever on local Spark test speed: the default of 200 schedules 200 tasks to produce 197 empty partitions on a three-row frame, and the scheduling *is* the runtime.

*Verified* across four file permutations, because "it passes" was exactly the evidence that misled PR 5:

```
main, schemas, join  -> 21 passed
schemas, main, join  -> 21 passed
join, main, schemas  -> 21 passed
schemas, join, main  -> 21 passed
```

The general lesson: a shared fixture and a test that disposes of a global are incompatible, and the bug is invisible until an unrelated file changes the alphabet.


## Step 5 — the tests

Nine, and each one is a claim that could be false.

| Test                                                                  | Claim                                                     |
| --------------------------------------------------------------------- | --------------------------------------------------------- |
| `test_output_matches_the_declared_schema`                             | The implementation honours PR 5's `JOINED`.               |
| `test_the_four_behaviours`                                            | Matched, null-padded, multiplied, and right-only-dropped, in one assertion. |
| `test_every_shipment_survives`                                        | The guarantee: one distinct key out per key in.           |
| `test_row_count_is_not_preserved_when_the_right_side_repeats`         | Three shipments in, four rows out.                        |
| `test_unmatched_shipments_are_the_answer`                             | The business question returns `shp-2`.                    |
| `test_empty_deliveries_still_emits_every_shipment`                    | Empty right side, three null-padded rows.                 |
| `test_empty_shipments_emits_nothing`                                  | Empty left side, zero rows, schema intact.                |
| `test_max_lag_excludes_a_late_delivery_without_dropping_the_shipment` | A bound narrows matches; it never deletes rows.           |
| `test_a_post_filter_would_turn_this_into_an_inner_join`               | The trap, asserted.                                       |

The two empty cases matter more than they look. `main` must be runnable against an empty stack, and `spark.createDataFrame([], SHIPMENT)` only works *because* PR 5 declared the schema — inference throws on an empty dataset. The design rule and the test are the same decision, two PRs apart.


## Step 6 — check it

```bash
make test
```

```
21 passed in 7.76s
```

Confirm the tests bite:

```bash
# change "left" to "inner" in left_join and re-run
make test   # test_the_four_behaviours and test_every_shipment_survives must fail

# select the key from `deliveries` instead of `shipments`
make test   # test_output_matches_the_declared_schema must fail on nullability

# move the max_lag bound out of the condition into a .filter()
make test   # test_max_lag_excludes... must fail: shp-2 disappears
```


## Definition of done

- [ ] `left_join` is pure: no session built, nothing read or written
- [ ] Output schema equals `JOINED`, asserted
- [ ] All four LEFT JOIN behaviours covered by tests
- [ ] Both empty-input cases covered
- [ ] The post-filter trap asserted, not just commented
- [ ] Tests pass in any file order, with no Docker

## Files in this PR

| File                              |                                                           |
| --------------------------------- | --------------------------------------------------------- |
| `src/pipeline/core/join.py`       | new — `JoinSpec` and `left_join`                          |
| `tests/unit/test_join.py`         | new — nine tests                                          |
| `tests/conftest.py`               | fixture no longer session-scoped; fixes an ordering bug   |
| `README.md`                       | status, layout                                            |

Nothing imports `core/` outside tests yet. The join lands inert and is wired in by the first runner.

Next: **PR 7** — Compose gains MinIO and the Iceberg REST catalog, and the session builder learns about catalogs.
