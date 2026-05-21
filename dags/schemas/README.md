# dags/schemas

BigQuery raw table schema definitions, shared between the Airflow DAG layer and the infrastructure provisioning scripts.

---

## Files

| File | Table | Description |
|:---|:---|:---|
| `lastfm.json` | `raw.lastfm` | Last.fm weekly chart records produced by `lastfm-consumer` |
| `mb_dump.json` | `raw.mb_dump` | MusicBrainz artist records from the monthly JSON dump |
| `spotify.json` | `raw.spotify` | Spotify tracks from the HuggingFace Parquet dataset |

## Single source of truth

Schema files are consumed in two places:

1. **Airflow DAGs** — `config.py` calls `load_schema(name)` which reads the JSON and passes it to `GCSToBigQueryOperator(schema_fields=...)`. This controls how GCS data is typed when loaded into BigQuery.

2. **Infrastructure** — `infra/provision/bigquery.sh` references these files when creating tables via the `bq mk --schema` CLI flag.

Updating a schema file automatically applies to both the DAG loader and the next infra run — no duplication required.

## Schema format

Each file is a JSON array of BigQuery field descriptors:

```json
[
  { "name": "field_name", "type": "STRING", "mode": "NULLABLE" },
  { "name": "record_count", "type": "INTEGER", "mode": "REQUIRED" }
]
```

Supported types: `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`, `TIMESTAMP`, `DATE`, `RECORD`.  
Supported modes: `NULLABLE` (default), `REQUIRED`, `REPEATED`.
