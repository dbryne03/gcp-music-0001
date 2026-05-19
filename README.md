# gcp-music-0001

Monthly music intelligence pipeline on Google Cloud Platform.

[![Portfolio](https://img.shields.io/badge/Portfolio-Google%20Cloud%20Platform%20%230001-3b7d5c?style=flat-square)](https://davidbryneadedeji.com/docs/projects/gcp)
[![Looker Studio](https://img.shields.io/badge/Live-Looker%20Studio%20Dashboard-4285F4?style=flat-square&logo=googleanalytics&logoColor=white)](#)
[![Sheets](https://img.shields.io/badge/Live-Google%20Sheets%20Report-34A853?style=flat-square&logo=googlesheets&logoColor=white)](#)

---

Infrastructure is deployed and runs entirely on Google Cloud Platform, provisioned via Pulumi. Source code is version-controlled here; live outputs are accessible via the badges above.

A Technical Design Document is available on request at [davidbryneadedeji.com](https://davidbryneadedeji.com).

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
| IaC | Pulumi (TypeScript) |
| CI/CD | GitHub Actions |

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
dags/            Astronomer Cloud Airflow DAG
infra/           Pulumi TypeScript — GCP infrastructure
.github/
  workflows/     CI/CD
```
