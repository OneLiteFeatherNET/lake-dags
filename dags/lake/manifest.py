"""Kubernetes objects for one maintenance run, as plain dicts. No kubernetes client import."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from lake.config import SparkSettings

NAMESPACE = "data"
SPARK_API_VERSION = "spark.apache.org/v1"
IVY_DIR = "/tmp/.ivy2"
JOB_MOUNT_PATH = "/opt/lake/job"
JOB_FILE_NAME = "maintenance_job.py"
JOB_FILE_PATH = f"{JOB_MOUNT_PATH}/{JOB_FILE_NAME}"
JOB_SOURCE_PATH = Path(__file__).resolve().parent / "spark" / JOB_FILE_NAME
DRIVER_SERVICE_ACCOUNT = "spark"  # chart workloadResources.serviceAccount.name
OAUTH_SECRET = "lake-maintenance-oauth"
S3_SECRET = "polaris-corvus"
POLARIS_URI = "http://polaris.data.svc:8181/api/catalog"
POLARIS_WAREHOUSE = "corvus"
OAUTH_SERVER_URI = (
    "https://login.microsoftonline.com/1a14dfb5-0eac-41bf-94cb-195c2e387520/oauth2/v2.0/token"
)
OAUTH_SCOPE = "api://9e2548f4-5e10-440d-9444-cad813a20c87/.default"
S3_ENDPOINT = "http://rook-ceph-rgw-feather-s3.rook-ceph-fr01.svc"
S3_REGION = "us-east-1"
SPARK_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
SPARK_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"
JOB_VOLUME = "maintenance-job"
DRIVER_CONTAINER = "spark-driver"
EXECUTOR_CONTAINER = "spark-executor"
MEMORY_OVERHEAD = "1g"
_INVALID_LABEL_CHARS = re.compile(r"[^a-z0-9-]+")
_MAX_LABEL = 63
_SPARK_SIZE = re.compile(r"([1-9][0-9]*)([mg])")


def read_job_source(path: Path = JOB_SOURCE_PATH) -> str:
    return path.read_text(encoding="utf-8")


def pod_memory(heap: str) -> str:
    """Pod request and limit for a Spark heap: the heap plus MEMORY_OVERHEAD, in Gi or Mi."""
    total = _to_mib(heap) + _to_mib(MEMORY_OVERHEAD)
    return f"{total // 1024}Gi" if total % 1024 == 0 else f"{total}Mi"


def _to_mib(size: str) -> int:
    match = _SPARK_SIZE.fullmatch(size) if isinstance(size, str) else None
    if match is None:
        raise ValueError(f"heap must be a Spark size such as '3g' or '512m', got {size!r}")
    amount, unit = match.groups()
    return int(amount) * (1024 if unit == "g" else 1)


def dns_label(value: str, max_len: int = _MAX_LABEL) -> str:
    label = _INVALID_LABEL_CHARS.sub("-", value.lower()).strip("-")
    return label[:max_len].rstrip("-")


def job_name(table_name: str, run_suffix: str) -> str:
    """Unique per run: 'lake-maint-<table>-<suffix>', at most 63 characters."""
    suffix = dns_label(run_suffix)
    base = dns_label(f"lake-maint-{table_name}", _MAX_LABEL - len(suffix) - 1)
    return f"{base}-{suffix}"


def build_configmap(
    name: str, namespace: str, job_source: str, labels: dict[str, str]
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "namespace": namespace, "labels": dict(labels)},
        "data": {JOB_FILE_NAME: job_source},
    }


def build_spark_application(
    *,
    name: str,
    namespace: str,
    catalog: str,
    settings: SparkSettings,
    config_map_name: str,
    table_fqn: str,
    statements_json: str,
    labels: dict[str, str],
) -> dict[str, Any]:
    return {
        "apiVersion": SPARK_API_VERSION,
        "kind": "SparkApplication",
        "metadata": {"name": name, "namespace": namespace, "labels": dict(labels)},
        "spec": {
            "runtimeVersions": {"sparkVersion": settings.spark_version},
            "pyFiles": f"local://{JOB_FILE_PATH}",
            "driverArgs": [catalog, table_fqn, statements_json],
            "sparkConf": _spark_conf(catalog, settings),
            "driverSpec": {
                "podTemplateSpec": _pod_template(
                    settings,
                    config_map_name,
                    DRIVER_CONTAINER,
                    memory=settings.driver_memory,
                    with_oauth=True,
                )
            },
            "executorSpec": {
                "podTemplateSpec": _pod_template(
                    settings,
                    config_map_name,
                    EXECUTOR_CONTAINER,
                    memory=settings.executor_memory,
                    with_oauth=False,
                )
            },
            "applicationTolerations": {
                # Retries happen on the Airflow side, so the operator never restarts the app.
                "restartConfig": {"restartPolicy": "Never"},
                # Release pods once the app ends; the runner deletes the object anyway.
                "resourceRetainPolicy": "Never",
                # One executor is enough for a maintenance job.
                "instanceConfig": {"initExecutors": 1, "minExecutors": 1, "maxExecutors": 1},
            },
        },
    }


def _spark_conf(catalog: str, settings: SparkSettings) -> dict[str, str]:
    prefix = f"spark.sql.catalog.{catalog}"
    return {
        # The Apache operator takes the image only from Spark conf; the v1 CRD has no image field.
        "spark.kubernetes.container.image": settings.image,
        "spark.kubernetes.authenticate.driver.serviceAccountName": DRIVER_SERVICE_ACCOUNT,
        "spark.kubernetes.driver.podTemplateContainerName": DRIVER_CONTAINER,
        "spark.kubernetes.executor.podTemplateContainerName": EXECUTOR_CONTAINER,
        "spark.driver.memory": settings.driver_memory,
        "spark.executor.memory": settings.executor_memory,
        "spark.driver.memoryOverhead": MEMORY_OVERHEAD,
        "spark.executor.memoryOverhead": MEMORY_OVERHEAD,
        "spark.jars.packages": f"{settings.iceberg_runtime},{settings.iceberg_aws_bundle}",
        # The image runs as user spark with HOME=/nonexistent; Ivy would put its cache there and
        # spark-submit fails with FileNotFoundException before the job starts.
        "spark.jars.ivy": IVY_DIR,
        "spark.sql.extensions": SPARK_EXTENSIONS,
        prefix: SPARK_CATALOG_CLASS,
        f"{prefix}.type": "rest",
        f"{prefix}.uri": POLARIS_URI,
        f"{prefix}.warehouse": POLARIS_WAREHOUSE,
        f"{prefix}.oauth2-server-uri": OAUTH_SERVER_URI,
        f"{prefix}.scope": OAUTH_SCOPE,
        f"{prefix}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{prefix}.s3.endpoint": S3_ENDPOINT,
        f"{prefix}.s3.path-style-access": "true",
        f"{prefix}.client.region": S3_REGION,
        "spark.sql.session.timeZone": "UTC",
    }


def _pod_template(
    settings: SparkSettings,
    config_map_name: str,
    container_name: str,
    *,
    memory: str,
    with_oauth: bool,
) -> dict[str, Any]:
    env = [
        _secret_env("AWS_ACCESS_KEY_ID", S3_SECRET, "AWS_ACCESS_KEY_ID"),
        _secret_env("AWS_SECRET_ACCESS_KEY", S3_SECRET, "AWS_SECRET_ACCESS_KEY"),
    ]
    if with_oauth:
        env = [
            _secret_env("CLIENT_ID", OAUTH_SECRET, "client_id"),
            _secret_env("CLIENT_SECRET", OAUTH_SECRET, "client_secret"),
            *env,
        ]
    resources = {"cpu": "1", "memory": pod_memory(memory)}
    spec: dict[str, Any] = {
        "containers": [
            {
                "name": container_name,
                "image": settings.image,
                "env": env,
                "volumeMounts": [
                    {"name": JOB_VOLUME, "mountPath": JOB_MOUNT_PATH, "readOnly": True}
                ],
                "resources": {"requests": dict(resources), "limits": dict(resources)},
            }
        ],
        "volumes": [{"name": JOB_VOLUME, "configMap": {"name": config_map_name}}],
    }
    return {"spec": spec}


def _secret_env(env_name: str, secret: str, key: str) -> dict[str, Any]:
    return {"name": env_name, "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}}
