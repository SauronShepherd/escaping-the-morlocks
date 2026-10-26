# Escaping the Morlocks — Table Time Travel Labs

Five runnable OSS notebooks accompany Escaping the Morlocks. The first three replay the same incident in Delta Lake, Apache Iceberg, and Apache Hudi. The final two investigate interoperability: XTable metadata synchronization and Delta UniForm generated views.

The labs follow the evidence-first style of [nothing-breaks-like-a-chart](https://github.com/SauronShepherd/nothing-breaks-like-a-chart) and [spark-clustas-demise](https://github.com/SauronShepherd/spark-clustas-demise). Databricks is discussed in the article and optional notebooks, but is not required for these OSS experiments.

## Run locally — no Docker

Use WSL2 Ubuntu, Linux, or macOS with Python 3.10+ and Java 17 or 21. Create a virtual environment, run `python -m pip install -r requirements.txt`, then `python -m pip install jupyter` and start `jupyter lab`. Open and run one notebook at a time from a fresh kernel. The notebooks start local Spark, create disposable temporary tables, and download connector artifacts from Maven Central on first run. No services or catalog containers are needed for native labs.

For XTable, use Java 17 and create a second Spark 3.4 compatible Python environment: `python3 -m venv .venv-xtable`, activate it, run `pip install -r requirements-xtable.txt`, then register it with `python -m ipykernel install --user --name morlocks-xtable --display-name "Python (Morlocks XTable)"`. In Jupyter, select that kernel for notebook 04. The notebook downloads the matched Hudi 0.14.0, Iceberg 1.9.2, and XTable 0.4.0 jars on first run. UniForm notebook 05 tests the path-based OSS Hudi preview without a catalog. Iceberg UniForm has additional catalog requirements and is documented as a limitation, not claimed as tested here.

## The five labs

| Notebook | Experiment |
|---|---|
| `notebooks/01_delta_time_travel.ipynb` | Delta log versions 0/1/2; event time vs commit time; versionAsOf |
| `notebooks/02_iceberg_time_travel.ipynb` | Iceberg snapshots; snapshot IDs; snapshot-id reads |
| `notebooks/03_hudi_time_travel.ipynb` | Hudi COW instants; as.of.instant; completed timeline |
| `notebooks/04_xtable_interoperability.ipynb` | Hudi COW source synchronized to an Iceberg reader; matching XTable 0.4/Hudi 0.14 compatibility stack |
| `notebooks/05_uniform_interoperability.ipynb` | Delta writer with generated Hudi preview view; historical-read and checksum diagnostic; Iceberg catalog caveat |

The shared native scenario has three states: Ada + Babbage (2 rows); upsert Babbage and append Larry with an older event_time (3); delete Ada (2). Expected counts are **2 → 3 → 2**. The notebooks compute canonical row checksums as well as counts.

## Reading the results

- Delta versions, Iceberg snapshot IDs, and Hudi instants are native coordinates. Record them independently; they are not matching counters.
- XTable synchronizes supported metadata. It is not atomic cross-format commit. A source may be ahead of a translated target until sync completes.
- UniForm keeps Delta as writer and generates read-only metadata views. Conversion can lag. OSS Hudi UniForm support in the pinned Delta release is preview; Iceberg requires supported protocol/column mapping and Hive Metastore.
- A snapshot/log without retained data files cannot reconstruct the past. History is not a backup. Never run cleanup against production based on these disposable labs.

## Other material

- `databricks/` contains optional Databricks-oriented notebooks; use them only in a configured Databricks workspace.
- `oss/` contains smaller scripts and reference notes, including a standalone no-catalog UniForm-to-Hudi preview script; the notebook adds stricter value checks.
- `VALIDATION.md` records which notebook runtimes have actually passed and the conclusions supported by observed outputs.

## Core references

- [Delta batch reads and time travel](https://docs.delta.io/delta-batch/)
- [Delta UniForm](https://docs.delta.io/delta-uniform/)
- [Iceberg Spark queries](https://iceberg.apache.org/docs/1.9.0/spark-queries/)
- [Hudi 0.15 quick start](https://hudi.apache.org/docs/0.15.0/quick-start-guide/)
- [Hudi batch reads](https://hudi.apache.org/docs/0.15.0/reading_tables_batch_reads/)
- [Apache XTable Spark Runtime](https://xtable.apache.org/docs/how-to-spark-runtime/)\n