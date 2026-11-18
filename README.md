# lake-dags

Apache Airflow 3.2.2 DAGs for the OneLiteFeather data lake.

## Layout

- `dags/`: DAG modules, the Airflow DAG folder.
- `dags/lake/`: helper package with config loading, SQL generation, SparkApplication manifests and the Kubernetes runner.
- `dags/lake/spark/`: PySpark job script.
- `dags/config/`: YAML configuration, for example `maintenance.yaml`.
- `tests/`: pytest suite. It needs no cluster and no network.

## How DAGs reach Airflow

Airflow uses git-sync to pull branch `main`, subPath `dags`, into the Airflow DAG folder. Merging to `main` is the deployment.

## Nightly Iceberg maintenance

The `iceberg_maintenance` DAG runs at 03:00 UTC every day in the `data` namespace. It creates one
`SparkApplication` per table listed in `dags/config/maintenance.yaml`, waits for it to finish and
deletes it again. Per table it optionally deletes days older than `retention_days`, then compacts
data files, expires old snapshots and removes orphan files. Tables that do not exist yet are skipped.

## Running tests

Fast suite (no Airflow needed):

```sh
uv sync
uv run ruff check .
uv run pytest -m "not airflow"
```

Full suite, including the DAG import test. Install the Airflow extra with the constraints file used
in CI (see `.github/workflows/test.yml`):

```sh
uv sync --extra airflow
uv run ruff check .
uv run pytest
```
