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
        record = json.loads(msg.value().decode("utf-8"))
        record["_ingested_at"] = ingested_at
        records.append(record)

    logger.info("Drained %d records from %s", len(records), topic)
    return records


def stage_to_gcs(records: list[dict], bucket: str) -> str:
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
