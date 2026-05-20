from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

GCP_PROJECT = "{{ var.value.gcp_project_id }}"
GCP_REGION = "europe-west2"
GCS_BUCKET = "{{ var.value.gcs_bucket_raw }}"

with DAG(
    dag_id="music_pipeline",
    description="Monthly music intelligence pipeline — extract, stage, transform, report",
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["music", "gcp", "monthly"],
) as dag:

    extract_lastfm = CloudRunExecuteJobOperator(
        task_id="extract_lastfm",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="lastfm-producer",
    )

    extract_musicbrainz = CloudRunExecuteJobOperator(
        task_id="extract_musicbrainz",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="musicbrainz-extractor",
    )

    extract_spotify = CloudRunExecuteJobOperator(
        task_id="extract_spotify",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="spotify-extractor",
    )

    wait_for_musicbrainz = GCSObjectExistenceSensor(
        task_id="wait_for_musicbrainz",
        bucket=GCS_BUCKET,
        object="raw/batch/musicbrainz/mb_dump.json",
    )

    wait_for_spotify = GCSObjectExistenceSensor(
        task_id="wait_for_spotify",
        bucket=GCS_BUCKET,
        object="raw/batch/spotify/spotify_tracks.parquet",
    )

    # TODO: BigQuery load tasks (GCS → raw tables)
    # TODO: dbt run Cloud Run Job
    # TODO: dbt test Cloud Run Job
    # TODO: completion notification

    [extract_lastfm, extract_musicbrainz] >> wait_for_musicbrainz
    extract_spotify >> wait_for_spotify
