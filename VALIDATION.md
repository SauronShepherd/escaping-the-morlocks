# Validation report — Escaping the Morlocks labs

Date: 2026-10-25

## Runtime evidence collected in WSL2 Ubuntu

Environment: Ubuntu under WSL2, Python 3.12.3, Java 21.0.12, PySpark 3.5.9, Delta Lake 3.3.2. Spark ran locally in `local[2]`; Maven dependencies resolved through the repository's `ivysettings.xml` using Maven Central.

- `databricks/00_setup_and_evidence.ipynb`: all Python code cells executed successfully in a local OSS Spark session with a `display()` shim. The setup created the named `morlocks_time_travel_lab` schema and its two lab tables. It did not require Databricks APIs for the Spark/Delta logic.
- `databricks/01_three_clocks.ipynb`: all Python code cells executed in the same OSS Spark session. Baseline Delta version 0 did not contain late customer 999; version 1 did. Its August 10 event timestamp preceded the October 25 commit timestamp and query time. The evidence ledger captured the result.
- `oss/00_delta_three_clocks.py`: executed on OSS Spark/Delta. Baseline version 0 had no late row; version 1 contained it. The printed history showed two WRITE commits.
- `oss/04_uniform_hudi_local.py`: executed on OSS Spark 3.5.9 + Delta 3.3.2 + Hudi 0.15.0. Delta version `0 -> 1` and Hudi instant `20261025213939654 -> 20261025214054011` were distinct coordinates. Delta historical rows = 1, Hudi historical rows = 1, Hudi latest rows = 2. The script ended with `PASS`.
- Both Databricks notebooks parse as notebook JSON, and all Python code cells compile. They have no saved outputs; the successful execution above was an external local run, not a Databricks notebook attachment/session.
- Both OSS Python scripts pass Python AST parsing.

## Interactive article review

- Desktop book QA confirmed the cover and page-turn controls, quote spread, hidden cover bookmarks, and ten-section index. Iceberg, Hudi, XTable and UniForm bookmarks reached the intended sections in the earlier pass.
- QA found and fixed a Delta bookmark spread-normalization bug shared by marker/deep-link/resize navigation. The corrected Delta bookmark still needs a fresh browser retest.
- The complete Astro build remains blocked by unrelated pre-existing draft validation errors in `draft-metric-views-are-not-metrics-with-a-view.md` (tag count and image slug) and `from-dusk-till-dawn-the-naughty-cluster.md` (invalid reading time). A clean final browser/mobile pass should follow when the dev server/build issues are resolved.

## Not yet executed

- `oss/01_xtable_spark_runtime.scala`: still requires a compatible XTable, Spark/Scala, Hudi, Iceberg and Delta classpath. It is not claimed as runtime-tested.
- `oss/02_delta_uniform.md` and `oss/03_native_coordinates.md` are procedure/reference notes, not executable scripts.
- The two notebooks ran against local OSS Spark + Delta, not a live Databricks Runtime or Unity Catalog. Databricks-specific catalog permissions and service behavior remain unverified.
- This run is a local reproducibility check, not production certification. In particular, the UniForm Hudi route is a preview path; validate supported versions and behavior before production use.

## Evidence-backed conclusions

1. Event timestamps are business data; Delta versions, Iceberg snapshot IDs and Hudi instants identify native table-history states.
2. UniForm is a Delta-owned write path that generates secondary metadata. Its OSS Hudi route is preview; the Iceberg path needs a properly initialized Hive Metastore.
3. XTable synchronizes supported metadata between formats. It is neither a fourth native table format nor a cross-format transaction coordinator.
4. Translated format coordinates must be recorded independently; they are not matching counters.
5. Historical queries require readable metadata and retained data files. Neither mechanism substitutes for independent backups.


