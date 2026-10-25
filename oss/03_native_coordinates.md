# Native coordinate and retention cards

Use these snippets in an isolated table setup with the matching format connector.

## Delta Lake

~~~python
past = (
    spark.read.format("delta")
    .option("versionAsOf", 17)
    .load(table_path)
)
~~~

Record the Delta table identity and version. Keep the log-history window and deleted-file retention window consistent with the promised recovery period. VACUUM can remove data files needed by older versions.

## Apache Iceberg

~~~sql
SELECT * FROM prod.analytics.events.history;
SELECT * FROM prod.analytics.events.snapshots;
SELECT * FROM prod.analytics.events VERSION AS OF 874223019844012345;
~~~

Record the Iceberg table identity and snapshot ID. Snapshot expiration changes which historical states remain queryable; review branches and tags before maintenance.

## Apache Hudi

~~~sql
SELECT * FROM hudi_events
TIMESTAMP AS OF '2026-10-21 14:05:00.000';
~~~

Record the Hudi table identity and instant. For Merge-on-Read tables, compare snapshot and read-optimized modes after a delta commit. Cleaner policy controls old file slices.

## Cross-format evidence ledger

| Field | Example |
|---|---|
| Source table identity | s3://warehouse/events |
| Source format coordinate | Hudi instant 20261021140500000 |
| Target reader coordinate | Iceberg snapshot 874223019844012345 |
| Translator and version | Apache XTable 0.4.0-incubating |
| Completion time | UTC timestamp from the sync job |
| Logical row count | Count after applying format semantics |
| Schema fingerprint | Stable hash of normalized schema JSON |
| Logical checksum | Stable hash of sorted canonical rows |

Do not compare format IDs as if they shared a counter. Store the relationship you actually observed.
