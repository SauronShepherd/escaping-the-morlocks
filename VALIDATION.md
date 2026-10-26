# Validation report — Escaping the Morlocks labs

Date: 2026-10-26
Environment for completed Spark runs: WSL2 Ubuntu, Python 3.12.3, Java 21, Spark 3.5.9, local[2].

## Five requested notebooks

| Notebook | Status | Evidence / limitation |
|---|---|---|
| 01 Delta | **PASS** | Executed all code cells. Counts 2 → 3 → 2; SHA-256 digests recorded. Larry's event_time precedes the version-1 commit timestamp but Larry is absent from v0 and visible in v1. |
| 02 Iceberg | **PASS** | Executed all code cells. Three distinct snapshot IDs (opaque values), same counts and same three logical digests as Delta; snapshot-id reads returned the expected states. |
| 03 Hudi | **PASS** | Executed all code cells on Hudi 0.15.0 / Spark 3.5.9. Counts 2 → 3 → 2; three matching digests. A first attempt using max row commit-time missed the delete instant, so the notebook now reads completed timeline actions; the rerun passed. |
| 04 XTable | **PASS** | Executed on WSL2 with Java 17, Spark 3.4.2, Hudi 0.14.0, Iceberg 1.9.2, XTable 0.4.0. Two Hudi → Iceberg synchronization runs produced three rows at each reader and the same canonical SHA-256 (`444eb9509b0b5bd5d284478d5948d09ba9f2b1434b8b590008ea798560e12c80`). Captured source Hudi instants and target Iceberg snapshot IDs. Hudi 0.15.0 is incompatible with this XTable runtime (observed NoSuchMethodError), so the notebook pins the release-matched stack. |
| 05 UniForm | **DIAGNOSTIC FAILURE — VALUE MISMATCH** | Executed the revised notebook through Delta v0/v1 writes and Hudi conversion with two completed Hudi instants. Historical/latest counts match (1/2 on each side), but canonical SHA-256 values do not: Delta v0 `a121a0bc…`, Hudi first instant `38199e6a…`; Delta latest `eada23ac…`, Hudi latest `075d3057…`. The first notebook run raised at the checksum assertion. The notebook now prints reader rows and reports the mismatch instead of falsely passing; that exact edited reporting cell has syntax-checked but has not been rerun. Earlier embedded Derby/Hive Iceberg registration failed with a metastore lock error; this no-catalog notebook does not claim Iceberg UniForm coverage. |

All five notebook files parse as valid notebook JSON; all Python code cells pass Python AST parsing.

## What the native tests establish

The shared test shows that event time is business data, not commit order. A late-arriving row belongs to the later committed table state even though its event timestamp is older. It also shows that the formats use different history maps: Delta log versions, Iceberg snapshot IDs, and Hudi instants must be stored independently.

The three native labs use Delta 3.3.2, Iceberg 1.9.2, and Hudi 0.15.0 on Spark 3.5.9. They do not test production retention policies, MOR Hudi, schema evolution, delete vectors, or distributed catalogs.

## Interoperability constraints observed

- UniForm keeps Delta as the writer and publishes generated Hudi/Iceberg views; conversion can lag and the generated views are read-only. The OSS Hudi preview exposes matching row counts in this sample, but our stronger notebook value checksums differ; treat data equivalence as unresolved.
- UniForm Iceberg needs Hive Metastore support plus the required Delta protocol and column-mapping settings. Iceberg UniForm interop is not verified in this local environment. An earlier Derby-embedded HMS attempt failed registration with a metastore lock error.
- Apache XTable is a general metadata synchronization project, but its 0.4.0 published Spark runtime cannot be treated as compatible with every engine version just because Spark itself starts. The current notebook pins a compatible Hudi/Spark pair and was runtime-tested on the release-matched stack.
- XTable synchronizes metadata; UniForm generates Delta-owned secondary metadata. Neither provides a cross-format atomic transaction or independent backup.
- XTable feature limits include COW/read-optimized views; Hudi MOR logs and Delta/Iceberg delete vectors are not represented by those synced views. Verify supported capabilities and retention for the exact release and reader.

## Previous optional Databricks notebooks

The two notebooks in databricks/ are separate optional material. Earlier evidence ran their Spark/Delta Python code in OSS Spark with a shim; it was not a live Databricks Runtime or Unity Catalog run.

## References checked

- Delta time travel: https://docs.delta.io/delta-batch/
- UniForm requirements and behavior: https://docs.delta.io/delta-uniform/
- Iceberg Spark snapshots: https://iceberg.apache.org/docs/1.9.0/spark-queries/
- Hudi Spark 3.5 support: https://hudi.apache.org/docs/0.15.0/quick-start-guide/
- XTable Spark Runtime compatibility: https://xtable.apache.org/docs/how-to-spark-runtime/
- XTable feature limitations: https://xtable.apache.org/docs/features-and-limitations/
- XTable 0.4.0 Hudi linkage issue: https://github.com/apache/incubator-xtable/issues/941

This is local reproducibility evidence, not production certification.
