# gcp-music-0001

Monthly music intelligence pipeline on Google Cloud Platform.

[![Status](https://img.shields.io/badge/Status-In%20Development-orange?style=flat-square)]()
[![Portfolio](https://img.shields.io/badge/Portfolio-Google%20Cloud%20Platform%20%230001-3b7d5c?style=flat-square)](https://davidbryneadedeji.com/docs/projects/gcp0001)
[![Looker Studio](https://img.shields.io/badge/Live-Looker%20Studio%20Dashboard-4285F4?style=flat-square&logo=googleanalytics&logoColor=white)](#)
[![Sheets](https://img.shields.io/badge/Live-Google%20Sheets%20Report-34A853?style=flat-square&logo=googlesheets&logoColor=white)](#)

---

Ingests chart, artist, and track data from three external sources on the first of each month, unifies them in BigQuery via dbt, and surfaces insights through Looker Studio and Google Sheets. Infrastructure runs entirely on GCP, provisioned via idempotent gcloud CLI scripts managed by GitHub Actions.

A full Technical Design Document is available in [TDD.md](TDD.md).

## Stack

| Layer | Technology |
|:---|:---|
| Extraction | Cloud Run Jobs, Python 3.12, Pydantic |
| Messaging | Apache Kafka (Confluent Cloud) |
| Storage | Google Cloud Storage |
| Warehousing | Google BigQuery |
| Transformation | dbt Core |
| Orchestration | Astronomer Cloud (managed Airflow 3.2.1) |
| Reporting | Looker Studio, Google Sheets |
| IaC | gcloud CLI (Shell) |
| CI/CD | GitHub Actions |
| Secrets | GCP Secret Manager |
| Region | `europe-west2` (London) |

## Structure

```
extractors/              Cloud Run Job source code
  lastfm-producer/       Last.fm REST API → Kafka topic
  lastfm-consumer/       Kafka topic → GCS (NDJSON)
  musicbrainz/           MusicBrainz JSON dump → GCS (NDJSON)
  spotify/               HuggingFace Parquet dataset → GCS
dbt/                     Transformation layer
  models/
    staging/             Source conforming — one model per source
    intermediate/        Artist resolution, track enrichment
    mart/                dim_artist, dim_track, fact_chart_position
  Dockerfile             dbt-runner Cloud Run Job image
  profiles.yml           Production BigQuery connection
dags/                    Airflow DAG definitions
  config.py              Shared constants — Airflow variables, job names, schemas
  music_pipeline.py      Orchestrator — monthly entry point (scheduled)
  lastfm_pipeline.py     Extract → load (triggered by orchestrator)
  musicbrainz_pipeline.py Extract → load (triggered by orchestrator)
  spotify_pipeline.py    Extract → load (triggered by orchestrator)
  music_transform.py     Source freshness → dbt run → dbt test
  schemas/               BigQuery raw table schema definitions
infra/                   GCP infrastructure scripts
  config.env             Shared configuration — project, region, resource names
  lifecycle.json         GCS raw data retention policy (90 days)
  provision/             Idempotent GCP resource provisioning scripts
  deploy/                Cloud Run Job create/update scripts
.github/
  workflows/             CI/CD — validate → infra → deploy
Dockerfile               Astro Runtime base image for Airflow environment
requirements.txt         Airflow provider packages
packages.txt             OS packages for Astro Runtime
.astro/config.yaml       Astro project identifier
```

## CI/CD

Three workflow files chain via `workflow_run`. PRs trigger only `validate.yml`.

```
Validate ──(main, on success)──► Infrastructure ──(on success)──► Deploy
```

| Workflow | Triggers | Jobs |
|:---|:---|:---|
| `validate.yml` | Every PR and push | DAG syntax, dbt parse, pytest ×4, Docker build ×5 |
| `infra.yml` | After Validate on `main` | GCP provisioning (APIs, storage, BigQuery, registry, secrets, IAM) |
| `deploy.yml` | After Infrastructure on `main` | Docker push ×5, Cloud Run Job update, Astronomer deploy |

Authentication uses Workload Identity Federation (OIDC) — no long-lived JSON keys.

## Running locally

```bash
# Authenticate
gcloud auth login
gcloud config set project portfolio-hub-2026

# Provision GCP resources
bash infra/provision/apis.sh
bash infra/provision/storage.sh
bash infra/provision/bigquery.sh
bash infra/provision/registry.sh
bash infra/provision/secrets.sh
bash infra/provision/iam.sh

# Deploy Cloud Run Jobs (after pushing images)
DEPLOY_SHA=<commit-sha> bash infra/deploy/jobs.sh
```

See [infra/README.md](infra/README.md) for full details including manual-only scripts.

## Data sources

| Source | Format | Volume |
|:---|:---|:---|
| Last.fm `chart.getTopArtists` | REST API (paginated) | ~50 artists/page |
| MusicBrainz artist dump | `artist.tar.xz` batch download | ~2M records, 2 GB compressed |
| Spotify tracks dataset | HuggingFace Parquet | ~114k tracks, 13.6 MB |
