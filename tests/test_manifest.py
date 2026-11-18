import json

from lake.config import load_config
from lake.manifest import (
    JOB_FILE_NAME,
    NAMESPACE,
    build_configmap,
    build_spark_application,
    job_name,
)

LABELS = {"app.kubernetes.io/part-of": "lake-maintenance"}
NAME = "lake-maint-capture-raw-20261118030000"


def _app(statements=("CALL corvus.system.rewrite_data_files('capture.raw')",)):
    config = load_config()
    return build_spark_application(
        name=NAME,
        namespace=NAMESPACE,
        catalog=config.catalog,
        settings=config.spark,
        config_map_name=NAME,
        table_fqn="capture.raw",
        statements_json=json.dumps(list(statements)),
        labels=LABELS,
    )


def _driver_env(app):
    return {
        e["name"]: e
        for e in app["spec"]["driverSpec"]["podTemplateSpec"]["spec"]["containers"][0]["env"]
    }


def test_spark_application_uses_v1_api_in_namespace_data():
    app = _app()

    assert app["apiVersion"] == "spark.apache.org/v1"
    assert app["kind"] == "SparkApplication"
    assert app["metadata"]["namespace"] == "data"
    assert app["metadata"]["name"] == NAME


def test_spark_version_and_image_come_from_config():
    app = _app()
    image = load_config().spark.image

    assert app["spec"]["runtimeVersions"]["sparkVersion"] == "4.1.4"
    assert app["spec"]["driverSpec"]["podTemplateSpec"]["spec"]["containers"][0]["image"] == image
    assert app["spec"]["executorSpec"]["podTemplateSpec"]["spec"]["containers"][0]["image"] == image
    assert image.endswith(":4.1.4-scala2.13-java17-python3-ubuntu")


def test_packages_string_contains_both_maven_coordinates():
    packages = _app()["spec"]["sparkConf"]["spark.jars.packages"]

    assert "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0" in packages.split(",")
    assert "org.apache.iceberg:iceberg-aws-bundle:1.12.0" in packages.split(",")


def test_catalog_conf_points_at_polaris_without_credentials():
    conf = _app()["spec"]["sparkConf"]

    assert conf["spark.sql.catalog.corvus.uri"] == "http://polaris.data.svc:8181/api/catalog"
    assert conf["spark.sql.catalog.corvus.warehouse"] == "corvus"
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert not any("credential" in key or "secret" in key.lower() for key in conf)


def test_driver_gets_oauth_and_s3_secret_refs():
    env = _driver_env(_app())

    assert env["CLIENT_ID"]["valueFrom"]["secretKeyRef"] == {
        "name": "lake-maintenance-oauth",
        "key": "client_id",
    }
    assert env["CLIENT_SECRET"]["valueFrom"]["secretKeyRef"] == {
        "name": "lake-maintenance-oauth",
        "key": "client_secret",
    }
    assert env["AWS_ACCESS_KEY_ID"]["valueFrom"]["secretKeyRef"] == {
        "name": "polaris-corvus",
        "key": "AWS_ACCESS_KEY_ID",
    }
    assert env["AWS_SECRET_ACCESS_KEY"]["valueFrom"]["secretKeyRef"] == {
        "name": "polaris-corvus",
        "key": "AWS_SECRET_ACCESS_KEY",
    }


def test_secret_env_entries_never_carry_literal_values():
    app = _app()

    for pod in (app["spec"]["driverSpec"], app["spec"]["executorSpec"]):
        for container in pod["podTemplateSpec"]["spec"]["containers"]:
            for entry in container["env"]:
                assert set(entry) == {"name", "valueFrom"}


def test_executor_gets_s3_secret_but_no_oauth_secret():
    env = {
        e["name"]
        for e in _app()["spec"]["executorSpec"]["podTemplateSpec"]["spec"]["containers"][0]["env"]
    }

    assert env == {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}


def test_configmap_is_mounted_read_only_at_job_path_for_driver_and_executor():
    app = _app()

    for pod in (app["spec"]["driverSpec"], app["spec"]["executorSpec"]):
        container = pod["podTemplateSpec"]["spec"]["containers"][0]
        mounts = [m for m in container["volumeMounts"] if m["mountPath"] == "/opt/lake/job"]
        assert mounts and mounts[0]["readOnly"] is True
        volume = [
            v for v in pod["podTemplateSpec"]["spec"]["volumes"] if v["name"] == mounts[0]["name"]
        ][0]
        assert volume["configMap"]["name"] == NAME


def test_driver_runs_as_spark_service_account_with_resources():
    driver = _app()["spec"]["driverSpec"]["podTemplateSpec"]["spec"]

    assert driver["serviceAccountName"] == "spark"
    assert driver["containers"][0]["resources"]["limits"] == {"cpu": "1", "memory": "2Gi"}


def test_py_file_points_at_mounted_job_and_args_are_catalog_table_statements():
    statements = ["CALL corvus.system.rewrite_data_files('capture.raw')"]
    spec = _app(statements)["spec"]

    assert spec["pyFiles"] == f"/opt/lake/job/{JOB_FILE_NAME}"
    assert spec["driverArgs"] == ["corvus", "capture.raw", json.dumps(statements)]


def test_single_executor_instance_is_configured():
    instance_config = _app()["spec"]["applicationTolerations"]["instanceConfig"]

    assert instance_config == {"initExecutors": 1, "minExecutors": 1, "maxExecutors": 1}


def test_configmap_contains_job_source_under_expected_key():
    configmap = build_configmap(NAME, NAMESPACE, "print('hi')\n", LABELS)

    assert configmap["kind"] == "ConfigMap"
    assert configmap["metadata"]["namespace"] == "data"
    assert configmap["data"][JOB_FILE_NAME] == "print('hi')\n"


def test_job_name_is_a_dns_label_of_at_most_63_characters():
    name = job_name("capture.a_very_long_table_name_that_goes_on_and_on_and_on", "20261118030000")

    assert len(name) <= 63
    assert name.endswith("-20261118030000")
    assert name == name.lower()
    assert "." not in name and "_" not in name


def test_job_name_is_built_from_table_and_run_suffix():
    assert job_name("capture.raw", "20261118030000") == "lake-maint-capture-raw-20261118030000"
