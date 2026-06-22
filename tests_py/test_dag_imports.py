import importlib.util
from pathlib import Path
import pytest

def _load():
    spec = importlib.util.spec_from_file_location(
        "certify_metrics_dag", "airflow/dags/certify_metrics_dag.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def test_dag_structure():
    try:
        mod = _load()
    except ImportError as e:
        if "airflow" in str(e):
            pytest.skip("airflow not installed")
        raise
    dag = mod.dag
    task_ids = {t.task_id for t in dag.tasks}
    assert {"dbt_build", "dbt_test", "certify", "pack", "alert_on_failure"} <= task_ids
    certify = dag.get_task("certify")
    assert "dbt_test" in {t.task_id for t in certify.upstream_list}
