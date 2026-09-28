"""Declared schemas for the two input events and the join output.

Nothing here is inferred. Inference throws on empty input, costs a pass over the
data, and makes the schema a function of whatever happened to arrive rather than
a contract the job can be held to.
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

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
