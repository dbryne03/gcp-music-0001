from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import config

with DAG(
    dag_id=config.DAG_SPOTIFY,
    description="Spotify — download HuggingFace dataset, stage to GCS, load to BigQuery",
    schedule=None,
    start_date=config.START_DATE,
    catchup=False,
    tags=["music", "gcp", "spotify", "monthly"],
) as dag:

    extract = CloudRunExecuteJobOperator(
        task_id="extract_spotify",
        project_id=config.GCP_PROJECT,
        region=config.GCP_REGION,
        job_name=config.JOB_SPOTIFY,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    wait = GCSObjectExistenceSensor(
        task_id="wait_for_spotify",
        bucket=config.GCS_BUCKET,
        object=config.GCS_SPOTIFY_BLOB,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    load = GCSToBigQueryOperator(
        task_id="load_spotify",
        bucket=config.GCS_BUCKET,
        source_objects=[config.GCS_SPOTIFY_BLOB],
        destination_project_dataset_table=config.BQ_SPOTIFY,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=config.load_schema("spotify"),
        gcp_conn_id=config.GCP_CONN_ID,
    )

    extract >> wait >> load
