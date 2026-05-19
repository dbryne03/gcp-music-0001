# gcp-music-0001

Monthly music intelligence pipeline on Google Cloud Platform.

**Live output:** [Looker Studio Dashboard](#) · [Google Sheets Report](#)  
**Portfolio:** [davidbryneadedeji.com/docs/projects/gcp](https://davidbryneadedeji.com/docs/projects/gcp)

---

This project is fully cloud-hosted. There is no local development environment — all infrastructure runs on GCP and is provisioned via Pulumi. The code in this repository is the artefact.

Recruiters and hiring managers can review the source code here and the live outputs via the links above. A Technical Design Document is available on request at [davidbryneadedeji.com](https://davidbryneadedeji.com).

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
