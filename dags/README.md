# dags

Airflow DAG definitions for the `gcp-music-0001` music pipeline, running on Astronomer Cloud (Airflow 3.2.1).

---

## Structure

```
dags/
  config.py               Shared constants — Airflow variables, job names, schema loaders, alerting
  music_pipeline.py       Orchestrator — the only scheduled DAG; triggers all sub-pipelines
  lastfm_pipeline.py      Last.fm extract → consume → wait → load to BigQuery
  musicbrainz_pipeline.py MusicBrainz extract → wait → load to BigQuery
  spotify_pipeline.py     Spotify extract → wait → load to BigQuery
  music_transform.py      dbt source freshness → dbt run → dbt test
  schemas/                BigQuery raw table schema definitions (JSON)
```

## Orchestration model

`music_pipeline` is the single scheduled DAG (`0 0 1 * *`). All other DAGs have `schedule=None` and are triggered exclusively by `TriggerDagRunOperator` with `wait_for_completion=True`. This prevents unintentional runs and means a failure in one source pipeline does not block the others.

```
music_pipeline (scheduled)
  ├── trigger_lastfm        ──► lastfm_pipeline
  ├── trigger_musicbrainz   ──► musicbrainz_pipeline
  ├── trigger_spotify       ──► spotify_pipeline
  └── trigger_transform     ──► music_transform (after all three sources)
```

## Configuration — `config.py`

All DAG-level constants live in `config.py`. Airflow variables are referenced via Jinja templates (`{{ var.value.* }}`) and resolved at task execution time, not at parse time.

| Constant | Description |
|:---|:---|
| `GCP_PROJECT`, `GCP_REGION`, `GCS_BUCKET` | Jinja variable references |
| `GCP_CONN_ID` | Airflow connection ID for all GCP operators |
| `BQ_LASTFM`, `BQ_MB_DUMP`, `BQ_SPOTIFY` | Fully-qualified BigQuery table targets |
| `GCS_LASTFM_PREFIX`, `GCS_MB_BLOB`, `GCS_SPOTIFY_BLOB` | GCS paths for sensors and loaders |
| `SCHEMA_LASTFM`, `SCHEMA_MB_DUMP`, `SCHEMA_SPOTIFY` | BigQuery schema lists loaded from `schemas/*.json` |
| `JOB_*` | Cloud Run Job names |
| `DAG_*` | DAG ID strings (single source of truth for trigger targets) |
| `SCHEDULE`, `START_DATE`, `TRIGGER_POKE_INTERVAL` | Pipeline timing |
| `DEFAULT_TASK_ARGS` | `retries=2`, `retry_delay=2m`, `execution_timeout=2h` applied to all tasks |
| `ALERT_EMAIL` | Failure alert destination |
| `on_pipeline_failure` | Failure callback — structured log + email via SMTP |

## Airflow variables

Set these in Astronomer → Admin → Variables before the first run:

| Variable | Value |
|:---|:---|
| `gcp_project_id` | `portfolio-hub-2026` |
| `gcp_region` | `europe-west2` |
| `gcs_bucket_raw` | `portfolio-hub-2026-music-raw` |

## Airflow connections

Set in Astronomer → Admin → Connections:

| Connection ID | Type | Notes |
|:---|:---|:---|
| `music-airflow-sa` | Google Cloud | Service account key for `music-airflow-sa`; used by all Cloud Run and BigQuery operators |
| `google_cloud_default` | Google Cloud | Uses Workload Identity impersonation via the Astronomer deployment SA → `music-airflow-sa`; used by `GCSObjectExistenceSensor` |

## Schemas

BigQuery raw table schemas live in `schemas/` as JSON files. They are the single source of truth shared between the DAG layer (`GCSToBigQueryOperator(schema_fields=...)`) and `infra/provision/bigquery.sh` (table creation via `bq` CLI). See [schemas/README.md](schemas/README.md).
