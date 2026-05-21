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


def fetch_charts(api_key: str) -> Generator[list[ArtistChart], None, None]:
    """Paginate the Last.fm chart.getTopArtists endpoint and yield one page at a time.

    Yields pages rather than individual records so the caller can publish each
    page to Kafka immediately. If a timeout occurs on page N, pages 1..N-1 are
    already in Kafka; only page N onward is lost on a retry.

    Args:
        api_key: Last.fm API key used to authenticate each request.

    Yields:
        List of ArtistChart records for each page, in chart rank order.

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

        yield [
            ArtistChart(
                artist_mbid=artist.get("mbid") or None,
                artist_name=artist["name"],
                chart_week=week,
                rank=(page - 1) * PAGE_LIMIT + i + 1,
                listeners=int(artist["listeners"]),
                playcount=int(artist["playcount"]),
            )
            for i, artist in enumerate(data["artist"])
        ]

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


def _publish_page(producer: Producer, records: list[ArtistChart], topic: str) -> None:
    """Serialise and produce one page of ArtistChart records to Kafka.

    Delivery errors are collected via the on_delivery callback and raised
    after flush so no failures are silently swallowed.

    Args:
        producer: Confluent Cloud producer instance (caller manages lifecycle).
        records: Page of ArtistChart records to publish.
        topic: Kafka topic name to produce to.

    Raises:
        RuntimeError: If one or more messages fail delivery after flush.
    """
    errors: list[str] = []

    def on_delivery(err, _msg):
        if err:
            errors.append(str(err))

    for record in records:
        producer.produce(
            topic,
            value=record.model_dump_json().encode(),
            on_delivery=on_delivery,
        )
    producer.flush()

    if errors:
        raise RuntimeError(f"{len(errors)} messages failed delivery: {errors[:3]}")


def main() -> None:
    """Entry point for the Last.fm producer Cloud Run Job.

    Fetches chart pages and publishes each page to Kafka immediately before
    fetching the next. A timeout on page N means pages 1..N-1 are already
    in Kafka; only page N onward is lost on a Cloud Run retry.
    """
    api_key = os.environ["LASTFM_API_KEY"]
    topic = os.environ["KAFKA_TOPIC_LASTFM"]

    producer = _kafka_producer()
    try:
        total = 0
        for page_num, page_records in enumerate(fetch_charts(api_key=api_key), start=1):
            _publish_page(producer, page_records, topic)
            total += len(page_records)
            logger.info("Page %d: published %d records (running total: %d)",
                        page_num, len(page_records), total)
        logger.info("All pages published — %d records total to topic %s", total, topic)
    finally:
        producer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
