from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

GCP_PROJECT = "{{ var.value.gcp_project_id }}"
GCP_REGION = "europe-west2"

with DAG(
    dag_id="music_transform",
    description="dbt run and test — triggered by music_pipeline once all loads complete",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["music", "gcp", "dbt", "monthly"],
) as dag:

    run_dbt = CloudRunExecuteJobOperator(
        task_id="run_dbt",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="dbt-runner",
        overrides={"container_overrides": [{"args": ["run"]}]},
    )

    test_dbt = CloudRunExecuteJobOperator(
        task_id="test_dbt",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="dbt-runner",
        overrides={"container_overrides": [{"args": ["test"]}]},
    )

    run_dbt >> test_dbt
