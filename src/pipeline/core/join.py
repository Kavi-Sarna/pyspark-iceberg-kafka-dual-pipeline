"""The join. DataFrame in, DataFrame out, and nothing else.

No SparkSession is built here, nothing is read or written, and no configuration
is looked up. That is what makes it testable with two in-memory frames.
"""

from __future__ import annotations

import operator
from dataclasses import dataclass
from datetime import timedelta
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class JoinSpec:
    """How to join, expressed as data.

    Batch and streaming need different join conditions. Passing the difference
    in keeps the branch out of the core.
    """

    keys: tuple[str, ...]
    left_time: str
    right_time: str
    # A delivery counts only if it lands within this window after dispatch.
    # None joins on the keys alone.
    max_lag: timedelta | None = None


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
