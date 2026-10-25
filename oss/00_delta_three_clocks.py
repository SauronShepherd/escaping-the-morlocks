"""
Lab: event time, Delta commit time, and query time are different clocks.

Prerequisite:
- A Spark session configured with a Delta Lake version compatible with your Spark and Scala versions.
- Run only against the temporary path below.

This script expects the variable spark to be an already configured SparkSession.
"""

import tempfile
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

table_path = str(Path(tempfile.mkdtemp(prefix="morlocks-delta-")) / "events")

initial = [
    (1, "Ada", "2026-10-20T08:15:00Z"),
    (2, "Babbage", "2026-10-20T08:30:00Z"),
]
schema = "id LONG, name STRING, event_time STRING"

(
    spark.createDataFrame(initial, schema)
    .withColumn("event_time", F.to_timestamp("event_time"))
    .write.format("delta")
    .mode("overwrite")
    .save(table_path)
)

baseline_version = (
    DeltaTable.forPath(spark, table_path)
    .history()
    .agg(F.max("version").alias("version"))
    .first()["version"]
)

late = [(999, "Larry Latecomer", "2026-10-19T09:15:00Z")]
(
    spark.createDataFrame(late, schema)
    .withColumn("event_time", F.to_timestamp("event_time"))
    .write.format("delta")
    .mode("append")
    .save(table_path)
)

history = DeltaTable.forPath(spark, table_path).history()
late_event_time = (
    spark.read.format("delta")
    .load(table_path)
    .where(F.col("id") == 999)
    .select("event_time")
    .first()["event_time"]
)
first_commit = (
    history.where(F.col("operation") == "WRITE")
    .orderBy(F.col("version").desc())
    .select("version", "timestamp")
    .first()
)

before = (
    spark.read.format("delta")
    .option("versionAsOf", baseline_version)
    .load(table_path)
    .where(F.col("id") == 999)
    .count()
)
now = (
    spark.read.format("delta")
    .load(table_path)
    .where(F.col("id") == 999)
    .count()
)

assert before == 0
assert now == 1
assert late_event_time < first_commit["timestamp"]

print("Table path:", table_path)
print("Baseline Delta version:", baseline_version)
print("First later WRITE:", first_commit["version"], first_commit["timestamp"])
print("Event time:", late_event_time)
print("Absent from baseline (expected):", before == 0)
print("Present after append:", now == 1)

history.select("version", "timestamp", "operation").orderBy("version").show(truncate=False)
