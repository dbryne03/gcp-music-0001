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
  schemas/           Raw BigQuery table schema definitions (JSON)
dags/
  music_pipeline.py       Orchestrator — monthly entry point
  lastfm_pipeline.py      Extract → load (triggered by orchestrator)
  musicbrainz_pipeline.py Extract → load (triggered by orchestrator)
  spotify_pipeline.py     Extract → load (triggered by orchestrator)
  music_transform.py      dbt run → dbt test (triggered by orchestrator)
infra/
  bootstrap.sh       Idempotent gcloud resource provisioning
  schemas/           BigQuery raw table schema definitions
.github/
  workflows/         CI/CD
```
