"""
Shared configuration for all music pipeline DAGs.

Airflow variables — set these in Astronomer: Admin → Variables
  gcp_project_id : portfolio-hub-2026
  gcp_region     : europe-west2
  gcs_bucket_raw : portfolio-hub-2026-music-raw

Airflow connection — set in Astronomer: Admin → Connections
  music-airflow-sa : Google Cloud connection using music-airflow-sa SA key
"""
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMAS = Path(__file__).parent / "schemas"


def load_schema(name: str) -> list:
    """Load a BigQuery table schema from dags/schemas/{name}.json."""
    return json.loads((_SCHEMAS / f"{name}.json").read_text())


# ── Airflow variables (Jinja — resolved at task execution time) ───────────────
GCP_PROJECT = "{{ var.value.gcp_project_id }}"
GCP_REGION  = "{{ var.value.gcp_region }}"
GCS_BUCKET  = "{{ var.value.gcs_bucket_raw }}"

# ── GCP connection ────────────────────────────────────────────────────────────
GCP_CONN_ID = "music-airflow-sa"

# ── BigQuery destination tables ───────────────────────────────────────────────
BQ_LASTFM  = "{{ var.value.gcp_project_id }}.raw.lastfm"
BQ_MB_DUMP = "{{ var.value.gcp_project_id }}.raw.mb_dump"
BQ_SPOTIFY = "{{ var.value.gcp_project_id }}.raw.spotify"

# ── GCS paths (plain strings — used in f-strings at parse time) ───────────────
GCS_LASTFM_PREFIX = "raw/api/lastfm"
GCS_MB_BLOB       = "raw/batch/musicbrainz/mb_artists.ndjson"
GCS_SPOTIFY_BLOB  = "raw/batch/spotify/spotify_tracks.parquet"

# ── BigQuery schemas ──────────────────────────────────────────────────────────
# Loaded from dags/schemas/*.json — single source of truth shared with
# infra/provision/bigquery.sh for table creation via the bq CLI.

SCHEMA_LASTFM  = load_schema("lastfm")
SCHEMA_MB_DUMP = load_schema("mb_dump")
SCHEMA_SPOTIFY = load_schema("spotify")

# ── Cloud Run Job names ───────────────────────────────────────────────────────
JOB_LASTFM_PRODUCER = "lastfm-producer"
JOB_LASTFM_CONSUMER = "lastfm-consumer"
JOB_MUSICBRAINZ     = "musicbrainz-extractor"
JOB_SPOTIFY         = "spotify-extractor"
JOB_DBT_RUNNER      = "dbt-runner"

# ── Sub-DAG identifiers ───────────────────────────────────────────────────────
DAG_LASTFM      = "lastfm_pipeline"
DAG_MUSICBRAINZ = "musicbrainz_pipeline"
DAG_SPOTIFY     = "spotify_pipeline"
DAG_TRANSFORM   = "music_transform"

# ── Pipeline schedule ─────────────────────────────────────────────────────────
SCHEDULE              = "0 0 1 * *"
START_DATE            = datetime(2026, 1, 1)
TRIGGER_POKE_INTERVAL = 60

# ── Task resilience defaults ──────────────────────────────────────────────────
# Applied via default_args on each source DAG.
# execution_timeout is set above the Cloud Run task-timeout (3600 s) to allow
# for API and polling overhead without masking genuinely stuck jobs.
DEFAULT_TASK_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}


# ── Failure callback ──────────────────────────────────────────────────────────
def on_pipeline_failure(context: dict) -> None:
    """Log a structured failure alert when any task in a source pipeline fails.

    Called by Airflow after all retries are exhausted. Extend this function
    to emit Slack or email notifications when an alerting integration is added.

    Args:
        context: Airflow task context dictionary provided by the scheduler.
    """
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    exc      = context.get("exception")

    logger.error(
        "PIPELINE FAILURE — dag=%s task=%s run=%s exception=%s",
        dag_id, task_id, run_id, exc,
    )
    # TODO: add Slack / email alert here
