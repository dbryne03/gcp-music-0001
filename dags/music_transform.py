from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.sensors.external_task import ExternalTaskSensor

GCP_PROJECT = "{{ var.value.gcp_project_id }}"
GCP_REGION = "europe-west2"

with DAG(
    dag_id="music_transform",
    description="dbt — run and test all models once all source loads have completed",
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["music", "gcp", "dbt", "monthly"],
) as dag:

    # Gate on all three source load tasks succeeding in the same execution interval
    wait_lastfm = ExternalTaskSensor(
        task_id="wait_lastfm_loaded",
        external_dag_id="lastfm_pipeline",
        external_task_id="load_lastfm",
        mode="reschedule",
        timeout=60 * 60 * 6,
    )

    wait_musicbrainz = ExternalTaskSensor(
        task_id="wait_musicbrainz_loaded",
        external_dag_id="musicbrainz_pipeline",
        external_task_id="load_musicbrainz",
        mode="reschedule",
        timeout=60 * 60 * 6,
    )

    wait_spotify = ExternalTaskSensor(
        task_id="wait_spotify_loaded",
        external_dag_id="spotify_pipeline",
        external_task_id="load_spotify",
        mode="reschedule",
        timeout=60 * 60 * 6,
    )

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

    [wait_lastfm, wait_musicbrainz, wait_spotify] >> run_dbt >> test_dbt
