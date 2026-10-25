// Lab: write one disposable Hudi Copy-on-Write table, then create Delta and Iceberg metadata views.
//
// Run with the Apache XTable 0.4.0-incubating Spark runtime on Spark 3.4 or 3.5.
// The runtime marks format-engine dependencies as provided: install a compatible Hudi,
// Iceberg and Delta classpath for your Spark/Scala build. Pin those exact versions.
// Do not use this sample to sync MOR log files or Delta/Iceberg deletion vectors.

import java.util.Arrays

import org.apache.xtable.spark.{TableSyncSpec, XTableSyncService}
import org.apache.spark.sql.functions._

val tableName = "morlock_xtable_cow"
val basePath = "/tmp/morlocks/xtable-cow" // replace with a disposable path on your storage

val hudiOptions = Map(
  "hoodie.table.name" -> tableName,
  "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
  "hoodie.datasource.write.operation" -> "upsert",
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.precombine.field" -> "event_time",
  "hoodie.datasource.write.partitionpath.field" -> "region",
  "hoodie.datasource.write.hive_style_partitioning" -> "true"
)

import spark.implicits._

val first = Seq(
  (1L, "Ada", "2026-10-20T08:15:00Z", "eu"),
  (2L, "Babbage", "2026-10-20T08:30:00Z", "us")
).toDF("id", "name", "event_time", "region")
  .withColumn("event_time", to_timestamp($"event_time"))

first.write.format("hudi").options(hudiOptions).mode("overwrite").save(basePath)

val update = Seq(
  (2L, "Babbage Lovelace", "2026-10-21T09:00:00Z", "us")
).toDF("id", "name", "event_time", "region")
  .withColumn("event_time", to_timestamp($"event_time"))

update.write.format("hudi").options(hudiOptions).mode("append").save(basePath)

// Capture the source timeline coordinate before syncing.
spark.read.format("hudi").load(basePath)
  .select("_hoodie_commit_time", "id", "name")
  .orderBy("_hoodie_commit_time", "id")
  .show(false)

new XTableSyncService().sync(
  TableSyncSpec.builder()
    .key(tableName)
    .basePath(basePath)
    .sourceFormat("HUDI")
    .targets(Arrays.asList("ICEBERG", "DELTA"))
    .build(),
  spark.sparkContext.hadoopConfiguration
)

// Read each target with its own compatible connector and catalog.
// Record the target-native coordinate separately from the Hudi instant.
