import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCP_PROJECT = "{{ var.value.gcp_project_id }}"
GCP_REGION = "europe-west2"
GCS_BUCKET = "{{ var.value.gcs_bucket_raw }}"

_SCHEMAS = Path(__file__).parent.parent / "infra" / "schemas"


def _schema(name: str) -> list:
    return json.loads((_SCHEMAS / f"{name}.json").read_text())


# chart_week is always the Monday of the execution week
_LASTFM_BLOB = (
    "raw/api/lastfm/"
    "{{ (data_interval_start - macros.timedelta(days=data_interval_start.weekday())).strftime('%Y-%m-%d') }}"
    ".ndjson"
)

with DAG(
    dag_id="lastfm_pipeline",
    description="Last.fm — produce to Kafka, consume to GCS, load to BigQuery",
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["music", "gcp", "lastfm", "monthly"],
) as dag:

    extract = CloudRunExecuteJobOperator(
        task_id="extract_lastfm",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="lastfm-producer",
    )

    consume = CloudRunExecuteJobOperator(
        task_id="consume_lastfm",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="lastfm-consumer",
    )

    wait = GCSObjectExistenceSensor(
        task_id="wait_for_lastfm",
        bucket=GCS_BUCKET,
        object=_LASTFM_BLOB,
    )

    load = GCSToBigQueryOperator(
        task_id="load_lastfm",
        bucket=GCS_BUCKET,
        source_objects=["raw/api/lastfm/*.ndjson"],
        destination_project_dataset_table="{{ var.value.gcp_project_id }}.raw.lastfm",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=_schema("lastfm"),
    )

    extract >> consume >> wait >> load
