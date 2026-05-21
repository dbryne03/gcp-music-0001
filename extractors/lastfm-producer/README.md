# lastfm-producer

Cloud Run Job — paginates the Last.fm `chart.getTopArtists` REST API and publishes chart records to a Confluent Cloud Kafka topic.

---

## Behaviour

1. Determines the current chart week (Monday of the current UTC week)
2. Paginates `chart.getTopArtists` at 50 artists/page, respecting the 5 req/s rate limit (0.2 s between pages)
3. Validates each record as an `ArtistChart` Pydantic model — empty MBIDs from the API are normalised to `None`
4. **Publishes each page immediately** to the `lastfm.charts` Kafka topic before fetching the next page
5. Raises `RuntimeError` if any message fails delivery after `flush()` — no silent failures

Publishing per-page (not after collecting all pages) means a Cloud Run retry on page N only replays from page N; pages 1..N-1 are already in Kafka.

## Output schema

Each Kafka message is a JSON-serialised `ArtistChart`:

| Field | Type | Notes |
|:---|:---|:---|
| `artist_mbid` | `str \| null` | MusicBrainz ID; `null` when the API returns empty string |
| `artist_name` | `str` | Artist display name |
| `chart_week` | `str` | `YYYY-MM-DD` of the Monday that opened this chart week |
| `rank` | `int` | Chart position (1-based, sequential across all pages) |
| `listeners` | `int` | Unique listeners in the chart period |
| `playcount` | `int` | Total plays in the chart period |

## Environment variables

| Variable | Source | Description |
|:---|:---|:---|
| `LASTFM_API_KEY` | Secret Manager | Last.fm API key |
| `KAFKA_BOOTSTRAP_SERVERS` | Secret Manager | Confluent Cloud bootstrap server |
| `KAFKA_API_KEY` | Secret Manager | Confluent Cloud API key |
| `KAFKA_API_SECRET` | Secret Manager | Confluent Cloud API secret |
| `KAFKA_TOPIC_LASTFM` | Cloud Run env | Kafka topic name (`lastfm.charts`) |

## Files

| File | Description |
|:---|:---|
| `main.py` | Extractor entry point — `ArtistChart` model, `fetch_charts`, `_publish_page`, `main` |
| `test_main.py` | Unit tests — chart week logic, pagination, MBID normalisation, delivery failure |
| `Dockerfile` | Non-root Python 3.12 slim image |
| `requirements.txt` | `requests`, `confluent-kafka`, `pydantic` |
| `pytest.ini` | pytest configuration |

## Running tests

```bash
pip install -r requirements.txt
pytest
```
