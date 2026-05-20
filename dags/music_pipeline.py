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


LASTFM_SCHEMA = _schema("lastfm")
MB_DUMP_SCHEMA = _schema("mb_dump")

with DAG(
    dag_id="music_pipeline",
    description="Monthly music intelligence pipeline — extract, stage, transform, report",
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["music", "gcp", "monthly"],
) as dag:

    # ── Extract ───────────────────────────────────────────────────────────────

    extract_lastfm = CloudRunExecuteJobOperator(
        task_id="extract_lastfm",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="lastfm-producer",
    )

    consume_lastfm = CloudRunExecuteJobOperator(
        task_id="consume_lastfm",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        job_name="lastfm-consumer",
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

    # ── Stage sensors ─────────────────────────────────────────────────────────

    # chart_week is always the Monday of the execution week
    _lastfm_blob = (
        "raw/api/lastfm/"
        "{{ (data_interval_start - macros.timedelta(days=data_interval_start.weekday())).strftime('%Y-%m-%d') }}"
        ".ndjson"
    )

    wait_for_lastfm = GCSObjectExistenceSensor(
        task_id="wait_for_lastfm",
        bucket=GCS_BUCKET,
        object=_lastfm_blob,
    )

    wait_for_musicbrainz = GCSObjectExistenceSensor(
        task_id="wait_for_musicbrainz",
        bucket=GCS_BUCKET,
        object="raw/batch/musicbrainz/mb_artists.ndjson",
    )

    wait_for_spotify = GCSObjectExistenceSensor(
        task_id="wait_for_spotify",
        bucket=GCS_BUCKET,
        object="raw/batch/spotify/spotify_tracks.parquet",
    )

    # ── BigQuery load ─────────────────────────────────────────────────────────

    load_lastfm = GCSToBigQueryOperator(
        task_id="load_lastfm",
        bucket=GCS_BUCKET,
        source_objects=["raw/api/lastfm/*.ndjson"],
        destination_project_dataset_table="{{ var.value.gcp_project_id }}.raw.lastfm",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=LASTFM_SCHEMA,
    )

    load_musicbrainz = GCSToBigQueryOperator(
        task_id="load_musicbrainz",
        bucket=GCS_BUCKET,
        source_objects=["raw/batch/musicbrainz/mb_artists.ndjson"],
        destination_project_dataset_table="{{ var.value.gcp_project_id }}.raw.mb_dump",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=MB_DUMP_SCHEMA,
    )

    load_spotify = GCSToBigQueryOperator(
        task_id="load_spotify",
        bucket=GCS_BUCKET,
        source_objects=["raw/batch/spotify/spotify_tracks.parquet"],
        destination_project_dataset_table="{{ var.value.gcp_project_id }}.raw.spotify",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # ── Transform ─────────────────────────────────────────────────────────────

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

    # ── Dependencies ──────────────────────────────────────────────────────────

    extract_lastfm >> consume_lastfm >> wait_for_lastfm >> load_lastfm
    extract_musicbrainz >> wait_for_musicbrainz >> load_musicbrainz
    extract_spotify >> wait_for_spotify >> load_spotify

    [load_lastfm, load_musicbrainz, load_spotify] >> run_dbt >> test_dbt
