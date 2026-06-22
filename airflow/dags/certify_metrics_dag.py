"""Daily certification pipeline: build -> test -> certify -> pack, with failure alerting.

Demonstrates pipeline decomposition, scheduling, and failure-recovery routing.
Not executed in CI; the same steps run directly there (see .github/workflows/ci.yml).
"""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "analytics-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="certify_metrics",
    description="Build models and certify metrics against golden references daily.",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["metrics", "certification"],
) as dag:
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --profiles-dir . --select staging intermediate marts reference certification",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --profiles-dir .",
    )
    certify = BashOperator(
        task_id="certify",
        bash_command="python -m metrics_cli.cli certify",
    )
    pack = BashOperator(
        task_id="pack",
        bash_command="python -m metrics_cli.cli pack",
    )

    def _alert(**_):
        # In production: page the metric owner from certification_registry.md.
        raise RuntimeError("Certification failed; see certification_registry.md")

    alert_on_failure = PythonOperator(
        task_id="alert_on_failure",
        python_callable=_alert,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    dbt_build >> dbt_test >> certify >> pack
    certify >> alert_on_failure
