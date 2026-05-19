import os
import logging
from pydantic import BaseModel
from typing import Generator

logger = logging.getLogger(__name__)


class ArtistChart(BaseModel):
    artist_mbid: str
    artist_name: str
    chart_week: str
    rank: int
    listeners: int
    playcount: int


def fetch_charts(api_key: str, limit: int = 50) -> Generator[ArtistChart, None, None]:
    # TODO: paginate Last.fm chart.getTopArtists
    # https://www.last.fm/api/show/chart.getTopArtists
    raise NotImplementedError


def publish_to_kafka(records: list[ArtistChart], topic: str) -> None:
    # TODO: produce to Confluent Cloud topic
    raise NotImplementedError


def main() -> None:
    api_key = os.environ["LASTFM_API_KEY"]
    topic = os.environ["KAFKA_TOPIC_LASTFM"]

    records = list(fetch_charts(api_key=api_key))
    logger.info("Fetched %d chart records", len(records))

    publish_to_kafka(records=records, topic=topic)
    logger.info("Published to Kafka topic: %s", topic)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
