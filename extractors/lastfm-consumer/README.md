# lastfm-consumer

Cloud Run Job — drains the `lastfm.charts` Kafka topic and stages chart records to GCS as NDJSON.

---

## Behaviour

1. Subscribes to the `lastfm.charts` topic starting from the earliest uncommitted offset
2. Polls until 6 consecutive empty polls (30 s of silence), signalling the topic is fully drained
3. Routes malformed messages (JSON parse or UTF-8 decode failures) to a timestamped dead-letter NDJSON under `raw/api/lastfm/dead-letter/`
4. Stamps a single `_ingested_at` UTC timestamp across all records in the batch
5. Writes valid records to `raw/api/lastfm/{chart_week}.ndjson` — the blob name is derived from the `chart_week` field so the `GCSObjectExistenceSensor` in the DAG can target it without dynamic path resolution
6. Commits Kafka offsets **only after a successful GCS write** — a GCS failure leaves offsets uncommitted so the job can be re-run to replay the batch

## Output paths

| Path | Content |
|:---|:---|
| `raw/api/lastfm/{YYYY-MM-DD}.ndjson` | Valid chart records for the given chart week |
| `raw/api/lastfm/dead-letter/{YYYYMMDDTHHMMSSZ}.ndjson` | Raw bytes of malformed messages (best-effort inspection) |

## Environment variables

| Variable | Source | Description |
|:---|:---|:---|
| `KAFKA_BOOTSTRAP_SERVERS` | Secret Manager | Confluent Cloud bootstrap server |
| `KAFKA_API_KEY` | Secret Manager | Confluent Cloud API key |
| `KAFKA_API_SECRET` | Secret Manager | Confluent Cloud API secret |
| `KAFKA_TOPIC_LASTFM` | Cloud Run env | Kafka topic name (`lastfm.charts`) |
| `GCS_BUCKET_RAW` | Cloud Run env | GCS bucket name (without `gs://` prefix) |

## Files

| File | Description |
|:---|:---|
| `main.py` | Consumer entry point — `_consumer`, `drain`, `stage_dead_letters`, `stage_to_gcs`, `main` |
| `test_main.py` | Unit tests — drain logic, ingestion timestamp, dead-letter routing, GCS blob naming |
| `Dockerfile` | Non-root Python 3.12 slim image |
| `requirements.txt` | `confluent-kafka`, `google-cloud-storage` |
| `pytest.ini` | pytest configuration |

## Running tests

```bash
pip install -r requirements.txt
pytest
```
