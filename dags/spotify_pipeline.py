from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCP_PROJECT = "{{ var.value.gcp_project_id }}"
GCP_REGION = "europe-west2"
GCS_BUCKET = "{{ var.value.gcs_bucket_raw }}"

with DAG(
    dag_id="spotify_pipeline",
    description="Spotify — download HuggingFace dataset, stage to GCS, load to BigQuery",
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["music", "gcp", "spotify", "monthly"],
) as dag:

    extract = CloudRunExecuteJobOperator(
        task_id="extract_spotify",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="spotify-extractor",
    )

    wait = GCSObjectExistenceSensor(
        task_id="wait_for_spotify",
        bucket=GCS_BUCKET,
        object="raw/batch/spotify/spotify_tracks.parquet",
    )

    load = GCSToBigQueryOperator(
        task_id="load_spotify",
        bucket=GCS_BUCKET,
        source_objects=["raw/batch/spotify/spotify_tracks.parquet"],
        destination_project_dataset_table="{{ var.value.gcp_project_id }}.raw.spotify",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    extract >> wait >> load
