import importlib.util
from pathlib import Path

import pytest

if importlib.util.find_spec("airflow") is None:
    pytest.skip("apache-airflow is not installed", allow_module_level=True)

pytestmark = pytest.mark.airflow

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def dag_bag(tmp_path_factory):
    # Airflow reads AIRFLOW_HOME on import, so it is pointed at a temp dir before the first import.
    airflow_home = tmp_path_factory.mktemp("airflow_home")
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("AIRFLOW_HOME", str(airflow_home))
        patch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
        from airflow.models import DagBag

        yield DagBag(dag_folder=str(REPO_ROOT / "dags"))


def test_dag_folder_imports_without_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_iceberg_maintenance_dag_is_registered(dag_bag):
    assert "iceberg_maintenance" in dag_bag.dags


def test_iceberg_maintenance_has_one_task_per_configured_table(dag_bag):
    dag = dag_bag.dags["iceberg_maintenance"]

    task_ids = {task.task_id for task in dag.tasks}
    assert task_ids == {"maintain_capture_raw", "maintain_capture_events"}


def test_iceberg_maintenance_runs_nightly_at_0300_utc(dag_bag):
    dag = dag_bag.dags["iceberg_maintenance"]

    assert dag.timetable.expression == "0 3 * * *"
    assert str(dag.timezone) == "UTC"


def test_iceberg_maintenance_run_settings(dag_bag):
    dag = dag_bag.dags["iceberg_maintenance"]

    assert dag.catchup is False
    assert dag.max_active_runs == 1
