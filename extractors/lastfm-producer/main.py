import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Generator

import requests
from confluent_kafka import Producer
from pydantic import BaseModel

logger = logging.getLogger(__name__)

LASTFM_API = "https://ws.audioscrobbler.com/2.0/"
PAGE_LIMIT = 50
REQUEST_INTERVAL = 0.2  # 5 requests/second


class ArtistChart(BaseModel):
    artist_mbid: str | None  # empty string from API normalised to None
    artist_name: str
    chart_week: str           # ISO date of the Monday that opened this chart week
    rank: int
    listeners: int
    playcount: int


def _chart_week() -> str:
    """Return the ISO date of the Monday that opened the current chart week.

    Returns:
        Date string in YYYY-MM-DD format, always a Monday.
    """
    today = datetime.now(timezone.utc)
    monday = today - timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")


def fetch_charts(api_key: str) -> Generator[ArtistChart, None, None]:
    """Paginate the Last.fm chart.getTopArtists endpoint and yield chart records.

    Iterates all pages at PAGE_LIMIT artists per page. A 0.2 s delay is
    applied between pages to stay within the Last.fm rate limit of 5 req/s.
    Artist MBIDs returned as empty strings are normalised to None. Global
    rank is calculated continuously across pages.

    Args:
        api_key: Last.fm API key used to authenticate each request.

    Yields:
        ArtistChart records in descending chart rank order.

    Raises:
        requests.HTTPError: If any API request returns a non-2xx status.
    """
    week = _chart_week()
    page, total_pages = 1, None

    while total_pages is None or page <= total_pages:
        resp = requests.get(
            LASTFM_API,
            params={
                "method": "chart.getTopArtists",
                "api_key": api_key,
                "format": "json",
                "page": page,
                "limit": PAGE_LIMIT,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()["artists"]
        total_pages = int(data["@attr"]["totalPages"])

        for i, artist in enumerate(data["artist"]):
            yield ArtistChart(
                artist_mbid=artist.get("mbid") or None,
                artist_name=artist["name"],
                chart_week=week,
                rank=(page - 1) * PAGE_LIMIT + i + 1,
                listeners=int(artist["listeners"]),
                playcount=int(artist["playcount"]),
            )

        page += 1
        if page <= total_pages:
            time.sleep(REQUEST_INTERVAL)


def _kafka_producer() -> Producer:
    """Build a Confluent Cloud producer configured for the music pipeline.

    Credentials are read from environment variables injected by Secret Manager
    at Cloud Run Job startup.

    Returns:
        A configured confluent_kafka Producer ready to produce messages.
    """
    return Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
    })


def publish_to_kafka(records: list[ArtistChart], topic: str) -> None:
    """Serialise and produce ArtistChart records to a Kafka topic.

    Each record is serialised to JSON and produced asynchronously. Delivery
    errors are collected via the on_delivery callback and raised as a single
    RuntimeError after flush so no partial failures are silently swallowed.

    Args:
        records: List of ArtistChart records to publish.
        topic: Kafka topic name to produce to.

    Raises:
        RuntimeError: If one or more messages fail delivery after flush.
    """
    errors: list[str] = []

    def on_delivery(err, _msg):
        if err:
            errors.append(str(err))

    producer = _kafka_producer()
    try:
        for record in records:
            producer.produce(
                topic,
                value=record.model_dump_json().encode(),
                on_delivery=on_delivery,
            )
        producer.flush()
        if errors:
            raise RuntimeError(f"{len(errors)} messages failed delivery: {errors[:3]}")
        logger.info("Published %d records to topic %s", len(records), topic)
    finally:
        producer.close()


def main() -> None:
    """Entry point for the Last.fm producer Cloud Run Job.

    Fetches all pages of the current chart.getTopArtists chart and publishes
    every record to the configured Kafka topic for downstream consumption.
    """
    api_key = os.environ["LASTFM_API_KEY"]
    topic = os.environ["KAFKA_TOPIC_LASTFM"]

    records = list(fetch_charts(api_key=api_key))
    logger.info("Fetched %d chart records", len(records))

    publish_to_kafka(records=records, topic=topic)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
