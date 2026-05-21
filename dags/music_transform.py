from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

import config

with DAG(
    dag_id=config.DAG_TRANSFORM,
    description="dbt run and test — triggered by music_pipeline once all loads complete",
    default_args=config.DEFAULT_TASK_ARGS,
    on_failure_callback=config.on_pipeline_failure,
    schedule=None,
    start_date=config.START_DATE,
    catchup=False,
    tags=["music", "gcp", "dbt", "monthly"],
) as dag:

    run_dbt = CloudRunExecuteJobOperator(
        task_id="run_dbt",
        project_id=config.GCP_PROJECT,
        region=config.GCP_REGION,
        job_name=config.JOB_DBT_RUNNER,
        gcp_conn_id=config.GCP_CONN_ID,
        overrides={"container_overrides": [{"args": ["run"]}]},
    )

    test_dbt = CloudRunExecuteJobOperator(
        task_id="test_dbt",
        project_id=config.GCP_PROJECT,
        region=config.GCP_REGION,
        job_name=config.JOB_DBT_RUNNER,
        gcp_conn_id=config.GCP_CONN_ID,
        overrides={"container_overrides": [{"args": ["test"]}]},
    )

    run_dbt >> test_dbt
