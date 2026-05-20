# gcp-music-0001

Monthly music intelligence pipeline on Google Cloud Platform.

[![Status](https://img.shields.io/badge/Status-In%20Development-orange?style=flat-square)]()
[![Portfolio](https://img.shields.io/badge/Portfolio-Google%20Cloud%20Platform%20%230001-3b7d5c?style=flat-square)](https://davidbryneadedeji.com/docs/projects/gcp0001)
[![Looker Studio](https://img.shields.io/badge/Live-Looker%20Studio%20Dashboard-4285F4?style=flat-square&logo=googleanalytics&logoColor=white)](#)
[![Sheets](https://img.shields.io/badge/Live-Google%20Sheets%20Report-34A853?style=flat-square&logo=googlesheets&logoColor=white)](#)

---

Infrastructure is deployed and runs entirely on Google Cloud Platform, provisioned via an idempotent gcloud CLI bootstrap script. Source code is version-controlled here; live outputs are accessible via the badges above.

A Technical Design Document is available in [`TDD.md`](TDD.md).

## Stack

| Layer | Technology |
|:---|:---|
| Extraction | Cloud Run Jobs, Python 3.12, Pydantic |
| Messaging | Apache Kafka (Confluent Cloud) |
| Storage | Google Cloud Storage |
| Warehousing | Google BigQuery |
| Transformation | dbt Core |
| Orchestration | Astronomer Cloud (managed Airflow) |
| Reporting | Looker Studio, Google Sheets |
| IaC | gcloud CLI (Shell) |
| CI/CD | GitHub Actions |
| Secrets | GCP Secret Manager |

## Structure

```
extractors/
  lastfm-producer/   Cloud Run Job — Last.fm REST API → Kafka topic
  lastfm-consumer/   Cloud Run Job — Kafka topic → GCS
  musicbrainz/       Cloud Run Job — MusicBrainz JSON dump → GCS
  spotify/           Cloud Run Job — HuggingFace Parquet dataset → GCS
dbt/
  models/
    staging/         Source conforming, one model per source
    intermediate/    Artist resolution, track enrichment
    mart/            dim_artist, dim_track, fact_chart_position
  Dockerfile         dbt-runner Cloud Run Job image
  profiles.yml       Production BigQuery connection (oauth, europe-west2)
dags/
  config.py               Shared constants — variables, job names, schemas
  music_pipeline.py       Orchestrator — monthly entry point (scheduled)
  lastfm_pipeline.py      Extract → load (triggered by orchestrator)
  musicbrainz_pipeline.py Extract → load (triggered by orchestrator)
  spotify_pipeline.py     Extract → load (triggered by orchestrator)
  music_transform.py      dbt run → dbt test (triggered by orchestrator)
  schemas/                BigQuery raw table schema definitions
infra/
  config.env         Shared infrastructure variables
  lifecycle.json     GCS raw data retention policy (90 days)
  provision/         Idempotent GCP resource provisioning scripts
  deploy/            Cloud Run Job create/update scripts
.astro/
  config.yaml        Astro project identifier (required by astro deploy CLI)
Dockerfile           Astro Runtime base image for Airflow environment
packages.txt         OS packages for Astro Runtime (empty — none required)
requirements.txt     Airflow provider packages installed into Astronomer image
.github/
  workflows/         CI/CD — validate, infra, deploy
```
