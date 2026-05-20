"""
Shared configuration for all music pipeline DAGs.

Airflow variables — set these in Astronomer: Admin → Variables
  gcp_project_id : portfolio-hub-2026
  gcp_region     : europe-west2
  gcs_bucket_raw : portfolio-hub-2026-music-raw

Airflow connection — set in Astronomer: Admin → Connections
  music-airflow-sa : Google Cloud connection using music-airflow-sa SA key
"""
from datetime import datetime

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
# Inlined to avoid a filesystem dependency at DAG parse time.
# infra/schemas/*.json holds the same definitions for gcloud/bq CLI use.

SCHEMA_LASTFM = [
    {"name": "artist_mbid",  "type": "STRING",    "mode": "NULLABLE"},
    {"name": "artist_name",  "type": "STRING",    "mode": "REQUIRED"},
    {"name": "chart_week",   "type": "STRING",    "mode": "REQUIRED"},
    {"name": "rank",         "type": "INTEGER",   "mode": "REQUIRED"},
    {"name": "listeners",    "type": "INTEGER",   "mode": "REQUIRED"},
    {"name": "playcount",    "type": "INTEGER",   "mode": "REQUIRED"},
    {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

SCHEMA_MB_DUMP = [
    {"name": "id",           "type": "STRING",    "mode": "REQUIRED"},
    {"name": "name",         "type": "STRING",    "mode": "REQUIRED"},
    {"name": "sort_name",    "type": "STRING",    "mode": "REQUIRED"},
    {"name": "type",         "type": "STRING",    "mode": "NULLABLE"},
    {"name": "country",      "type": "STRING",    "mode": "NULLABLE"},
    {"name": "begin_date",   "type": "STRING",    "mode": "NULLABLE"},
    {"name": "end_date",     "type": "STRING",    "mode": "NULLABLE"},
    {"name": "ended",        "type": "BOOLEAN",   "mode": "NULLABLE"},
    {"name": "genres",       "type": "STRING",    "mode": "REPEATED"},
    {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
]

SCHEMA_SPOTIFY = [
    {"name": "track_id",         "type": "STRING",    "mode": "REQUIRED"},
    {"name": "artists",          "type": "STRING",    "mode": "NULLABLE"},
    {"name": "album_name",       "type": "STRING",    "mode": "NULLABLE"},
    {"name": "track_name",       "type": "STRING",    "mode": "NULLABLE"},
    {"name": "popularity",       "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "duration_ms",      "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "explicit",         "type": "BOOLEAN",   "mode": "NULLABLE"},
    {"name": "danceability",     "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "energy",           "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "key",              "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "loudness",         "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "mode",             "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "speechiness",      "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "acousticness",     "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "instrumentalness", "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "liveness",         "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "valence",          "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "tempo",            "type": "FLOAT",     "mode": "NULLABLE"},
    {"name": "time_signature",   "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "track_genre",      "type": "STRING",    "mode": "NULLABLE"},
    {"name": "_ingested_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
]

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
