import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from google.cloud import storage

logger = logging.getLogger(__name__)

GROUP_ID = "lastfm-consumer"
GCS_BLOB_PREFIX = "raw/api/lastfm"
POLL_TIMEOUT = 5.0   # seconds per poll
MAX_EMPTY_POLLS = 6  # 30 s of silence signals topic is drained


def _consumer() -> Consumer:
    """Build a Confluent Cloud consumer configured for the music pipeline.

    Credentials are read from environment variables injected by Secret Manager
    at Cloud Run Job startup. Offsets are not committed automatically — the
    caller is responsible for committing after a successful GCS write.

    Returns:
        A configured confluent_kafka Consumer ready to subscribe.
    """
    return Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def drain(consumer: Consumer, topic: str) -> list[dict]:
    """Read all available messages from a Kafka topic and return them as records.

    Polls until MAX_EMPTY_POLLS consecutive empty polls are received, which
    signals the topic is caught up. PARTITION_EOF is treated as an empty poll
    rather than an error. A single _ingested_at timestamp is stamped across
    all records in the batch so they share a consistent ingestion time.

    Args:
        consumer: A subscribed-ready confluent_kafka Consumer instance.
        topic: Kafka topic name to subscribe to and drain.

    Returns:
        List of parsed message payloads with _ingested_at added. Empty list
        if no messages were available.

    Raises:
        RuntimeError: If a non-EOF Kafka error is received during polling.
    """
    consumer.subscribe([topic])
    records, empty_polls = [], 0
    ingested_at = datetime.now(timezone.utc).isoformat()

    while empty_polls < MAX_EMPTY_POLLS:
        msg = consumer.poll(POLL_TIMEOUT)

        if msg is None:
            empty_polls += 1
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                empty_polls += 1
            else:
                raise RuntimeError(f"Kafka error: {msg.error()}")
            continue

        empty_polls = 0
        try:
            record = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Skipping malformed message at offset %s: %s", msg.offset(), exc)
            continue
        record["_ingested_at"] = ingested_at
        records.append(record)

    logger.info("Drained %d records from %s", len(records), topic)
    return records


def stage_to_gcs(records: list[dict], bucket: str) -> str:
    """Write a batch of chart records to GCS as NDJSON.

    The output path is derived from the chart_week field of the first record,
    producing a predictable blob name that the downstream Airflow
    GCSObjectExistenceSensor can target without dynamic path resolution.

    Args:
        records: Non-empty list of chart record dicts, each containing a
            chart_week field in YYYY-MM-DD format.
        bucket: GCS bucket name (without gs:// prefix).

    Returns:
        The full blob name written, e.g. raw/api/lastfm/2026-05-19.ndjson.
    """
    chart_week = records[0]["chart_week"]
    blob_name = f"{GCS_BLOB_PREFIX}/{chart_week}.ndjson"
    ndjson = "\n".join(json.dumps(r) for r in records)

    client = storage.Client()
    client.bucket(bucket).blob(blob_name).upload_from_string(
        ndjson, content_type="application/x-ndjson"
    )
    logger.info("Staged to gs://%s/%s", bucket, blob_name)
    return blob_name


def main() -> None:
    """Entry point for the Last.fm Kafka consumer Cloud Run Job.

    Drains the Last.fm charts topic, writes records to GCS, and commits
    offsets only after a successful write. If the topic is empty the job
    exits cleanly without writing anything.
    """
    bucket = os.environ["GCS_BUCKET_RAW"]
    topic = os.environ["KAFKA_TOPIC_LASTFM"]

    consumer = _consumer()
    try:
        records = drain(consumer, topic)

        if not records:
            logger.warning("No records consumed — nothing to stage")
            return

        stage_to_gcs(records, bucket)
        consumer.commit()
        logger.info("Offsets committed")
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
