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


with DAG(
    dag_id="musicbrainz_pipeline",
    description="MusicBrainz — download artist dump, stage to GCS, load to BigQuery",
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["music", "gcp", "musicbrainz", "monthly"],
) as dag:

    extract = CloudRunExecuteJobOperator(
        task_id="extract_musicbrainz",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="musicbrainz-extractor",
    )

    wait = GCSObjectExistenceSensor(
        task_id="wait_for_musicbrainz",
        bucket=GCS_BUCKET,
        object="raw/batch/musicbrainz/mb_artists.ndjson",
    )

    load = GCSToBigQueryOperator(
        task_id="load_musicbrainz",
        bucket=GCS_BUCKET,
        source_objects=["raw/batch/musicbrainz/mb_artists.ndjson"],
        destination_project_dataset_table="{{ var.value.gcp_project_id }}.raw.mb_dump",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=_schema("mb_dump"),
    )

    extract >> wait >> load
