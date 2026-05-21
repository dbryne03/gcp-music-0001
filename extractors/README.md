# extractors

Cloud Run Job source code for the `gcp-music-0001` ingestion layer. Each extractor is an independent Python service that runs to completion, scales to zero, and is containerised in its own Docker image.

---

## Services

| Directory | Cloud Run Job | Source → Destination |
|:---|:---|:---|
| [`lastfm-producer/`](lastfm-producer/) | `lastfm-producer` | Last.fm REST API → Confluent Cloud Kafka topic |
| [`lastfm-consumer/`](lastfm-consumer/) | `lastfm-consumer` | Kafka topic → GCS NDJSON |
| [`musicbrainz/`](musicbrainz/) | `musicbrainz-extractor` | MusicBrainz JSON dump → GCS NDJSON |
| [`spotify/`](spotify/) | `spotify-extractor` | HuggingFace Parquet → GCS Parquet |

## Common patterns

**Non-root containers** — all images run as UID 1000. No process inside a container runs as root.

**Environment-injected secrets** — credentials are never baked into images. GCP Secret Manager injects them as environment variables at Cloud Run Job startup.

**Data quality gates** — each extractor validates output volume before staging to GCS. A partial extraction raises `ValueError` and fails the job, triggering Airflow retry and alerting rather than silently loading corrupt data.

**Ingestion timestamp** — every extractor stamps a `_ingested_at` UTC field on output records so the raw layer carries a consistent ingestion time independent of GCS object metadata.

## Building images

All images are built by GitHub Actions (`deploy.yml`) and pushed to Artifact Registry:

```
europe-west2-docker.pkg.dev/portfolio-hub-2026/music-pipeline/<service>:<sha>
```

To build locally:

```bash
docker build -t lastfm-producer extractors/lastfm-producer/
docker build -t lastfm-consumer extractors/lastfm-consumer/
docker build -t musicbrainz-extractor extractors/musicbrainz/
docker build -t spotify-extractor extractors/spotify/
```

## Running tests

Each extractor has its own `pytest` suite in `test_main.py`:

```bash
cd extractors/lastfm-producer && pip install -r requirements.txt && pytest
cd extractors/lastfm-consumer && pip install -r requirements.txt && pytest
cd extractors/musicbrainz     && pip install -r requirements.txt && pytest
cd extractors/spotify         && pip install -r requirements.txt && pytest
```
