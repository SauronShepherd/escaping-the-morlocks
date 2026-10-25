"""Exercise the open-source Delta UniForm -> Hudi preview path locally.

This no-catalog path is intentionally small: one local Spark process, one
temporary Delta table, and Hudi reads directly from the generated table path.
"""

from __future__ import annotations

import os
import re
import tempfile
import time
from pathlib import Path


DELTA_VERSION = "3.3.2"
UNIFORM_PACKAGE = f"io.delta:delta-hudi_2.12:{DELTA_VERSION}"
HUDI_RUNTIME = "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
WAIT_SECONDS = 90


def build_spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder
        .appName("EscapingTheMorlocks-OSS-UniForm-Hudi")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.ivySettings", (Path(__file__).resolve().parents[1] / "ivysettings.xml").as_uri())
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.excludes", "org.xerial.snappy:snappy-java,com.github.luben:zstd-jni")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=[UNIFORM_PACKAGE, HUDI_RUNTIME],
    ).getOrCreate()


def delta_version(spark: object, path: str) -> int:
    from delta.tables import DeltaTable

    return int(DeltaTable.forPath(spark, path).history().selectExpr("max(version) AS version").first()["version"])


def read_hudi(spark: object, path: str):
    return (
        spark.read.format("hudi")
        .option("hoodie.metadata.enable", "true")
        .load(path)
    )


def hudi_timeline_instants(table_dir: Path) -> list[str]:
    timeline_dir = table_dir / ".hoodie"
    if not timeline_dir.is_dir():
        return []
    instants = set()
    for entry in timeline_dir.iterdir():
        if not entry.is_file() or "inflight" in entry.name.lower():
            continue
        match = re.match(r"^(\d{14,17})", entry.name)
        is_completed_commit = "__COMPLETED__" in entry.name and any(
            action in entry.name for action in ("__commit__", "__deltacommit__", "__replacecommit__")
        )
        is_legacy_commit = entry.suffix in {".commit", ".deltacommit", ".replacecommit"}
        if match and (is_completed_commit or is_legacy_commit):
            instants.add(match.group(1))
    return sorted(instants)


def wait_for_hudi_rows(spark: object, path: str, table_dir: Path, expected_rows: int):
    deadline = time.monotonic() + WAIT_SECONDS
    last_error = "Hudi timeline is not ready yet"
    while time.monotonic() < deadline:
        try:
            frame = read_hudi(spark, path)
            count = frame.count()
            instants = hudi_timeline_instants(table_dir)
            if count == expected_rows and len(instants) >= expected_rows:
                return frame
            last_error = f"Hudi exposed {count} rows and {len(instants)} completed instants"
        except Exception as error:
            last_error = str(error).splitlines()[0]
        time.sleep(2)
    raise TimeoutError(f"UniForm Hudi did not catch up within {WAIT_SECONDS}s: {last_error}")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="morlocks-uniform-hudi-") as temporary_root:
        table_dir = (Path(temporary_root) / "morlock_events").resolve()
        table_path = table_dir.as_uri()
        spark = build_spark()
        try:
            first = spark.createDataFrame(
                [(1, "2026-10-23 12:00:01", "first commit")],
                "id long, event_time string, payload string",
            ).selectExpr("id", "to_timestamp(event_time) AS event_time", "payload")

            (
                first.write.format("delta")
                .option("delta.columnMapping.mode", "name")
                .option("delta.universalFormat.enabledFormats", "hudi")
                .mode("overwrite")
                .save(table_path)
            )
            first_delta_version = delta_version(spark, table_path)
            first_hudi = wait_for_hudi_rows(spark, table_path, table_dir, 1)
            first_hudi_instant = hudi_timeline_instants(table_dir)[0]
            print(f"First Delta version {first_delta_version} is readable at Hudi instant {first_hudi_instant}.")

            second = spark.createDataFrame(
                [(2, "2026-10-23 12:00:02", "second commit")],
                "id long, event_time string, payload string",
            ).selectExpr("id", "to_timestamp(event_time) AS event_time", "payload")
            second.write.format("delta").mode("append").save(table_path)
            latest_delta_version = delta_version(spark, table_path)
            latest_hudi = wait_for_hudi_rows(spark, table_path, table_dir, 2)
            latest_hudi_instant = hudi_timeline_instants(table_dir)[-1]

            delta_past_rows = (
                spark.read.format("delta")
                .option("versionAsOf", first_delta_version)
                .load(table_path)
                .count()
            )
            hudi_past_rows = (
                spark.read.format("hudi")
                .option("hoodie.metadata.enable", "true")
                .option("as.of.instant", first_hudi_instant)
                .load(table_path)
                .count()
            )
            hudi_latest_rows = latest_hudi.count()

            print("\nThe coordinates are different; the past is still readable:")
            print(f"  Delta version:       {first_delta_version} -> {latest_delta_version}")
            print(f"  Hudi instant:        {first_hudi_instant} -> {latest_hudi_instant}")
            print(f"  Delta historical rows: {delta_past_rows}")
            print(f"  Hudi historical rows:  {hudi_past_rows}")
            print(f"  Hudi latest rows:      {hudi_latest_rows}")

            if (delta_past_rows, hudi_past_rows, hudi_latest_rows) != (1, 1, 2):
                raise AssertionError("Expected historical row counts (1, 1, 2); compare the coordinates above.")
            print("\nPASS: OSS Delta UniForm generated a Hudi view and both formats traveled to the past.")
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
