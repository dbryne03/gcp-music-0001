from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import config

_LASTFM_BLOB = (
    f"{config.GCS_LASTFM_PREFIX}/"
    "{{ (data_interval_start - macros.timedelta(days=data_interval_start.weekday())).strftime('%Y-%m-%d') }}"
    ".ndjson"
)

with DAG(
    dag_id=config.DAG_LASTFM,
    description="Last.fm — produce to Kafka, consume to GCS, load to BigQuery",
    default_args=config.DEFAULT_TASK_ARGS,
    on_failure_callback=config.on_pipeline_failure,
    schedule=None,
    start_date=config.START_DATE,
    catchup=False,
    tags=["music", "gcp", "lastfm", "monthly"],
) as dag:

    extract = CloudRunExecuteJobOperator(
        task_id="extract_lastfm",
        project_id=config.GCP_PROJECT,
        region=config.GCP_REGION,
        job_name=config.JOB_LASTFM_PRODUCER,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    consume = CloudRunExecuteJobOperator(
        task_id="consume_lastfm",
        project_id=config.GCP_PROJECT,
        region=config.GCP_REGION,
        job_name=config.JOB_LASTFM_CONSUMER,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    wait = GCSObjectExistenceSensor(
        task_id="wait_for_lastfm",
        bucket=config.GCS_BUCKET,
        object=_LASTFM_BLOB,
    )

    load = GCSToBigQueryOperator(
        task_id="load_lastfm",
        bucket=config.GCS_BUCKET,
        source_objects=[f"{config.GCS_LASTFM_PREFIX}/*.ndjson"],
        destination_project_dataset_table=config.BQ_LASTFM,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=config.SCHEMA_LASTFM,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    extract >> consume >> wait >> load
