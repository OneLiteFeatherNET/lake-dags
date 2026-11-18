"""Thin Airflow/Kubernetes adapter, the only module importing Airflow or kubernetes. No logic."""

from __future__ import annotations

from typing import Any

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes.client import CoreV1Api, CustomObjectsApi

SPARK_GROUP = "spark.apache.org"
SPARK_VERSION = "v1"
SPARK_PLURAL = "sparkapplications"


class KubeSparkClient:
    def __init__(self) -> None:
        api_client = KubernetesHook(conn_id=None, in_cluster=True).get_conn()
        self._core = CoreV1Api(api_client)
        self._custom = CustomObjectsApi(api_client)

    def create_configmap(self, namespace: str, body: dict[str, Any]) -> None:
        self._core.create_namespaced_config_map(namespace=namespace, body=body)

    def delete_configmap(self, namespace: str, name: str) -> None:
        self._core.delete_namespaced_config_map(name=name, namespace=namespace)

    def create_spark_application(self, namespace: str, body: dict[str, Any]) -> None:
        self._custom.create_namespaced_custom_object(
            group=SPARK_GROUP,
            version=SPARK_VERSION,
            namespace=namespace,
            plural=SPARK_PLURAL,
            body=body,
        )

    def get_spark_application(self, namespace: str, name: str) -> dict[str, Any]:
        return self._custom.get_namespaced_custom_object(
            group=SPARK_GROUP,
            version=SPARK_VERSION,
            namespace=namespace,
            plural=SPARK_PLURAL,
            name=name,
        )

    def delete_spark_application(self, namespace: str, name: str) -> None:
        self._custom.delete_namespaced_custom_object(
            group=SPARK_GROUP,
            version=SPARK_VERSION,
            namespace=namespace,
            plural=SPARK_PLURAL,
            name=name,
        )
