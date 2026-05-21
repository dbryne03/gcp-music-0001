import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from google.cloud import storage

logger = logging.getLogger(__name__)

GROUP_ID = "lastfm-consumer"
GCS_BLOB_PREFIX = "raw/api/lastfm"
GCS_DEAD_LETTER_PREFIX = "raw/api/lastfm/dead-letter"
POLL_TIMEOUT = 5.0   # seconds per poll
MAX_EMPTY_POLLS = 6  # 30 s of silence signals topic is drained


def _consumer() -> Consumer:
    """Build a Confluent Cloud consumer configured for the music pipeline.

    Credentials are read from environment variables injected by Secret Manager
    at Cloud Run Job startup. Offsets are not committed automatically — the
    caller is responsible for committing after a successful GCS write.

    Returns:
        A configured confluent_kafka Consumer ready to subscribe.

    Raises:
        KeyError: If any of ``KAFKA_BOOTSTRAP_SERVERS``, ``KAFKA_API_KEY``, or
            ``KAFKA_API_SECRET`` are absent from the environment.
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


def drain(consumer: Consumer, topic: str) -> tuple[list[dict], list[bytes]]:
    """Read all available messages from a Kafka topic and return them as records.

    Polls until MAX_EMPTY_POLLS consecutive empty polls are received, which
    signals the topic is caught up. PARTITION_EOF is treated as an empty poll
    rather than an error. A single _ingested_at timestamp is stamped across
    all records in the batch so they share a consistent ingestion time.

    Args:
        consumer: A subscribed-ready confluent_kafka Consumer instance.
        topic: Kafka topic name to subscribe to and drain.

    Returns:
        Tuple of ``(records, dead_letters)``. ``records`` is a list of parsed
        dicts with ``_ingested_at`` stamped. ``dead_letters`` is a list of raw
        ``bytes`` values for malformed messages, staged separately to GCS.

    Raises:
        RuntimeError: If a non-EOF Kafka error is received during polling.
    """
    consumer.subscribe([topic])
    records, dead_letters, empty_polls = [], [], 0
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
            logger.warning("Malformed message at offset %s — routing to dead-letter: %s",
                           msg.offset(), exc)
            dead_letters.append(msg.value())
            continue
        record["_ingested_at"] = ingested_at
        records.append(record)

    logger.info("Drained %d records from %s (%d dead-lettered)",
                len(records), topic, len(dead_letters))
    return records, dead_letters


def stage_dead_letters(dead_letters: list[bytes], bucket: str) -> None:
    """Write malformed Kafka messages to a GCS dead-letter path for inspection.

    A timestamped NDJSON blob is written under ``raw/api/lastfm/dead-letter/``
    so that malformed messages can be inspected and replayed without blocking
    the main ingestion path. Does nothing if ``dead_letters`` is empty.

    Args:
        dead_letters: Raw message bytes that failed JSON parsing or UTF-8 decoding.
        bucket: GCS bucket name (without ``gs://`` prefix).
    """
    if not dead_letters:
        return
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    blob_name = f"{GCS_DEAD_LETTER_PREFIX}/{timestamp}.ndjson"
    payload = b"\n".join(dead_letters)
    client = storage.Client()
    client.bucket(bucket).blob(blob_name).upload_from_string(
        payload, content_type="application/x-ndjson"
    )
    logger.warning("Staged %d dead-letter messages to gs://%s/%s",
                   len(dead_letters), bucket, blob_name)


def stage_to_gcs(records: list[dict], bucket: str) -> str:
    """Write a batch of chart records to GCS as NDJSON.

    The output path is derived from the ``chart_week`` field of the first record,
    producing a predictable blob name that the downstream Airflow
    ``GCSObjectExistenceSensor`` can target without dynamic path resolution.

    Args:
        records: Non-empty list of chart record dicts, each containing a
            ``chart_week`` field in ``YYYY-MM-DD`` format.
        bucket: GCS bucket name (without ``gs://`` prefix).

    Returns:
        Full blob name written, e.g. ``raw/api/lastfm/2026-05-19.ndjson``.
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

    Drains the Last.fm charts topic, routes malformed messages to the GCS
    dead-letter path, writes valid records to GCS as NDJSON, and commits
    offsets only after a successful write. If the topic is empty the job exits
    cleanly without writing anything. The consumer is always closed, even on
    failure, to release the consumer group membership.
    """
    bucket = os.environ["GCS_BUCKET_RAW"]
    topic = os.environ["KAFKA_TOPIC_LASTFM"]

    consumer = _consumer()
    try:
        records, dead_letters = drain(consumer, topic)

        stage_dead_letters(dead_letters, bucket)

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
