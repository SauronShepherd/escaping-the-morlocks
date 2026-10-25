# OSS Delta UniForm: the no-Docker Hudi preview lab

This tiny experiment runs outside Databricks with open-source Apache Spark. It exercises **UniForm Hudi**, which the OSS Delta docs currently mark as preview, because Hudi readers can open the generated table directly by path. It needs no catalog service. The standard UniForm Iceberg route is covered separately below: Delta's OSS docs require a Hive Metastore catalog for that route, so an embedded Spark metastore is not a valid shortcut.

The versions are pinned for this exercise: PySpark 3.5.9, Delta Lake 3.3.2, and Apache Hudi 0.15.0. Hudi 0.15 supports Spark 3.5.x. Use Python 3.10+ and Java 17, plus internet access for the first run's Maven downloads. Check them with <code>python3 --version</code> and <code>java -version</code>. On Ubuntu or WSL, install missing prerequisites with:

~~~bash
sudo apt update
sudo apt install -y openjdk-17-jre-headless python3-venv
~~~

The script creates a random temporary directory and removes only that directory when it exits.

## 1. Install and run

Extract the companion lab pack and open a terminal in the extracted <code>escaping-the-morlocks</code> folder, where its top-level <code>README.md</code> lives. On macOS or Linux, run:

~~~bash
python3 -m venv .venv-morlocks
.venv-morlocks/bin/python -m pip install "pyspark==3.5.9" "delta-spark==3.3.2"
.venv-morlocks/bin/python oss/04_uniform_hudi_local.py
~~~

If you are running from a clone of the full article repository instead, use <code>labs/escaping-the-morlocks/oss/04_uniform_hudi_local.py</code> as the final command path.

On Windows, use Ubuntu in WSL2 and run the same shell commands there. If WSL is not installed, run <code>wsl --install -d Ubuntu</code> from an Administrator PowerShell and complete Ubuntu's first-run setup. This lab needs no Docker setup. If Ubuntu says the <code>venv</code> module is missing, install its small system package with <code>sudo apt install python3-venv</code>.

Spark downloads the pinned <code>delta-hudi</code> and Hudi Spark runtime JARs from Maven on first launch. No separate Spark installation is required.

## 2. What the script does

The script writes two small Delta commits with column mapping and UniForm Hudi enabled. It waits until the Hudi path can see each expected state, then performs three reads:

- Delta at the first Delta version.
- Hudi at the first Hudi instant.
- Hudi at the latest Hudi instant.

The expected row counts are <code>1, 1, 2</code>. The output prints Delta versions and Hudi instants as separate native coordinates. It does not pretend that their numbers match.

## 3. Iceberg UniForm needs a real catalog

To test OSS UniForm Iceberg, configure Spark to use a properly initialized Hive Metastore, include compatible <code>delta-iceberg</code> and Iceberg runtime JARs, and read through an Iceberg catalog connected to that same metastore. A local embedded Spark catalog does not meet this requirement, so the Hudi shortcut does not apply to the Iceberg route. The exact requirements are in the [Delta Lake UniForm guide](https://docs.delta.io/delta-uniform/) and [Iceberg Spark catalog configuration](https://iceberg.apache.org/docs/latest/spark-configuration/).

## 4. Safety notes

- This is a preview feature exercise on a disposable local table. Do not use it as a production compatibility certification.
- UniForm's Hudi and Iceberg views are read-only. Keep all writes on the Delta side.
- Enabling column mapping changes the Delta table contract; use a new lab table.
- A local Spark/Hudi success does not prove another engine, catalog service, cloud store, or retention policy works.
