from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import config

with DAG(
    dag_id=config.DAG_MUSICBRAINZ,
    description="MusicBrainz — download artist dump, stage to GCS, load to BigQuery",
    schedule=None,
    start_date=config.START_DATE,
    catchup=False,
    tags=["music", "gcp", "musicbrainz", "monthly"],
) as dag:

    extract = CloudRunExecuteJobOperator(
        task_id="extract_musicbrainz",
        project_id=config.GCP_PROJECT,
        region=config.GCP_REGION,
        job_name=config.JOB_MUSICBRAINZ,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    wait = GCSObjectExistenceSensor(
        task_id="wait_for_musicbrainz",
        bucket=config.GCS_BUCKET,
        object=config.GCS_MB_BLOB,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    load = GCSToBigQueryOperator(
        task_id="load_musicbrainz",
        bucket=config.GCS_BUCKET,
        source_objects=[config.GCS_MB_BLOB],
        destination_project_dataset_table=config.BQ_MB_DUMP,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=config.SCHEMA_MB_DUMP,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    extract >> wait >> load
