# gcp-music-0001

Monthly music intelligence pipeline on Google Cloud Platform.

**Sources:** Last.fm API · MusicBrainz JSON dump · Spotify Parquet dataset  
**Stack:** Cloud Run Jobs · Kafka (Confluent Cloud) · GCS · BigQuery · dbt Core · Astronomer Cloud · Looker Studio · Pulumi (TypeScript)

## Structure

```
extractors/
  lastfm/        Cloud Run Job — Last.fm REST API → Kafka → GCS
  musicbrainz/   Cloud Run Job — MusicBrainz JSON dump → GCS
  spotify/       Cloud Run Job — Spotify Parquet dataset → GCS
dbt/
  models/
    staging/     Source conforming, one model per source
    intermediate/ Artist resolution, track enrichment
    mart/        dim_artist, dim_track, fact_chart_position
dags/            Astronomer Cloud Airflow DAGs
infra/           Pulumi TypeScript — GCP infrastructure
.github/
  workflows/     CI/CD
```

## Setup

```bash
cp .env.example .env
# populate secrets

# Pulumi
cd infra && npm install && pulumi up

# dbt
cd dbt && pip install dbt-bigquery && dbt deps && dbt debug

# Extractors (local)
cd extractors/lastfm && pip install -r requirements.txt
```

## Environment Variables

See `.env.example` for required configuration.
