# Validation report — Escaping the Morlocks labs

Date: 2026-10-25

## Checks completed

- Both Databricks notebooks parse as notebook JSON; all Python code cells compile; both notebooks have no saved execution outputs.
- The setup notebook uses the explicitly named `morlocks_time_travel_lab` schema and only drops its two named lab tables. It does not drop the schema.
- Both OSS Python scripts pass Python AST parsing.
- The README describes how to run the OSS path, the Databricks-only path, version boundaries, safety, and experiment conclusions.
- Browser QA of the book confirmed: desktop title and cover render; the left leaf and format bookmarks are hidden on the cover; turning forward shows the song quote on the right; turning back restores the cover; the chapter index exposes ten sections; and Iceberg, Hudi, XTable and UniForm bookmarks reached their corresponding headings without page errors.
- Browser QA found a real Delta bookmark bug: its heading could land on the left leaf while the reader rounded to the preceding spread. A shared page normalization fix now covers bookmarks, deep links and resize. **The corrected Delta jump still needs a fresh browser retest.**

## Runtime checks still outstanding

- The complete Astro build is blocked by content validation errors in unrelated drafts: `draft-metric-views-are-not-metrics-with-a-view.md` (tag count and image slug) and `from-dusk-till-dawn-the-naughty-cluster.md` (invalid reading time). The dev server initially served the interactive book at HTTP 200, but stopped responding after the navigation change; final browser QA and mobile viewport checks remain outstanding.
- OSS Spark execution is not yet certified. WSL2 has Python 3.12 and Java 21 but lacks `python3-venv`; apt stalled. Windows Python has PySpark, while installing Delta Spark triggered a lengthy PySpark wheel build that was stopped before completion.
- The Databricks notebooks need a live Databricks runtime and Unity Catalog permissions. They are syntax-checked, not executed.
- XTable's Scala exercise needs a compatible Spark 3.4/3.5, Scala, Hudi, Iceberg, Delta and XTable classpath. No such prepared runtime was available, so it is not executed.

## Conclusions supported by the material

1. Event timestamps are business data; Delta versions, Iceberg snapshot IDs and Hudi instants identify native table-history states.
2. UniForm is a Delta-owned write path that generates secondary metadata. Its OSS Hudi route is explicitly marked preview; the Iceberg path needs a properly initialized Hive Metastore.
3. XTable synchronizes supported metadata between formats. It is neither a fourth native table format nor a cross-format transaction coordinator.
4. Translated format coordinates must be recorded independently; they are not matching counters.
5. Historical queries require both readable metadata and retained data files. Neither mechanism substitutes for independent backups.

## To finish runtime certification

Use Python 3.10+ and Java 17 with the pinned requirements in a clean venv, then run `python oss/04_uniform_hudi_local.py`. Run the Databricks notebooks in order only on an isolated Unity Catalog-enabled workspace. Run the XTable Scala lab with the compatibility matrix from Apache XTable 0.4.0. Record actual command output and exact runtime versions before claiming those labs passed.
