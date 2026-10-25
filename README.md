# Escaping the Morlocks — Table Time Travel Labs

A reproducible companion to **Escaping The Morlocks**. These labs investigate the difference between event time, commit time, and query time, then compare native histories with metadata translation.

The project follows the hands-on, evidence-first style of [nothing-breaks-like-a-chart](https://github.com/SauronShepherd/nothing-breaks-like-a-chart) and [spark-clustas-demise](https://github.com/SauronShepherd/spark-clustas-demise): small experiments, explicit claims, runnable code, observed output, and conclusions. The notebooks keep Apache Spark and table formats at the center; Databricks is a separate optional path.

## Start here: OSS Spark, no Docker

The fastest self-contained experiment is the OSS Delta UniForm → Hudi preview lab. It uses one local Spark process, a temporary directory, and no catalog service. Run from this repository root in WSL2 Ubuntu, Linux, or macOS:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python oss/04_uniform_hudi_local.py
```

Requires Python 3.10+, Java 17, and internet access on first run so Spark can download pinned Maven artifacts. The script creates and deletes only its own random temporary directory.

Expected conclusion: the first Delta version is readable at a separately numbered Hudi instant; both can read the first state, while Hudi latest sees both rows. Expected counts: `1, 1, 2`. This exercises the OSS Hudi UniForm preview path, not production certification.

## Lab map

| Lab | Runtime | What it demonstrates |
|---|---|---|
| `oss/04_uniform_hudi_local.py` | OSS Spark, Delta 3.3.2, Hudi 0.15; no Docker/catalog | UniForm Hudi preview, async metadata visibility, separate Delta/Hudi coordinates |
| `oss/00_delta_three_clocks.py` | Existing configured Spark + Delta | Event time is data; Delta version/commit time is table history |
| `oss/01_xtable_spark_runtime.scala` | Spark 3.4/3.5 with compatible table-format libraries | Hudi COW source translated to Iceberg and Delta metadata with Apache XTable |
| `oss/02_delta_uniform.md` | OSS Delta/Spark | UniForm prerequisites and why Iceberg needs a real Hive Metastore |
| `oss/03_native_coordinates.md` | Matching native connector/catalog | Native coordinates and retention evidence for Delta, Iceberg, Hudi |
| `databricks/00_setup_and_evidence.ipynb` | Databricks + Unity Catalog | Disposable Delta table and a format-neutral evidence ledger |
| `databricks/01_three_clocks.ipynb` | Databricks + Unity Catalog | Proves a late event first appears in a later Delta version |

The two Databricks notebooks require an actual Databricks compute session with the necessary Unity Catalog permissions. They are syntax-checked here, but are not represented as executed locally. Run them in order in the dedicated `morlocks_time_travel_lab` schema. The setup notebook drops only its two named lab tables inside that schema.

## What the translators do—and do not do

- **Apache XTable** synchronizes supported table metadata between formats. It is a separate OSS Spark runtime and requires a carefully matched Spark/Scala/Hudi/Iceberg/Delta classpath. The sample is Hudi Copy-on-Write and does not certify MOR log or deletion-vector equivalence.
- **Delta UniForm** keeps Delta as the writer and generates secondary format metadata. OSS Delta documents Iceberg and Hudi; the OSS Hudi route is preview. Iceberg access requires a properly initialized Hive Metastore. This is not the Databricks Unity Catalog path.
- Translated histories have distinct native IDs. Never equate Delta versions, Iceberg snapshot IDs, or Hudi instants by number.
- A generated view is not a second independent writer or a backup. Retention of both metadata and underlying data files bounds the historical window.

## Conclusions

1. Event timestamps do not tell you when a row entered table history.
2. Save a native coordinate together with a table identity, format, and observed target coordinate.
3. UniForm is useful when Delta remains the write authority and consumers need generated Iceberg/Hudi read metadata; monitor conversion lag.
4. XTable is useful when source ownership or translation direction varies; operate it as a metadata-sync service and validate each reader.
5. Native history semantics and retention policies differ across Delta, Iceberg, and Hudi.
6. A snapshot or log entry cannot recover files that cleanup removed. Independent backups remain necessary.
7. A successful count comparison is only a first check; compare schema fingerprints and stable logical checksums before asserting equivalence.

## Safety

All destructive operations are scoped to disposable lab tables/paths. Never copy cleanup steps to production without reviewing table identity, retention, reader compatibility, backup coverage, and the exact files affected.

## References

- [Delta UniForm](https://docs.delta.io/delta-uniform/)
- [Delta batch reads and time travel](https://docs.delta.io/delta-batch/)
- [Apache Iceberg Spark queries](https://iceberg.apache.org/docs/latest/spark-queries/)
- [Apache Hudi 0.15 Spark support](https://hudi.apache.org/docs/0.15.0/quick-start-guide/)
- [Apache Hudi 0.15 batch reads](https://hudi.apache.org/docs/0.15.0/reading_tables_batch_reads/)
- [Apache XTable Spark runtime](https://xtable.apache.org/docs/how-to-spark-runtime/)
- [nothing-breaks-like-a-chart](https://github.com/SauronShepherd/nothing-breaks-like-a-chart)
- [spark-clustas-demise](https://github.com/SauronShepherd/spark-clustas-demise)
