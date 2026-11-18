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

## Running tests

```sh
uv sync
uv run ruff check .
uv run pytest
```
