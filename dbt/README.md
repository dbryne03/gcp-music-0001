# dbt

dbt Core transformation layer for `gcp-music-0001`. Transforms raw BigQuery tables into a clean dimensional model consumed by Looker Studio and Google Sheets.

---

## Structure

```
dbt/
  dbt_project.yml        Project configuration — name, version, model materialisation defaults
  profiles.yml           BigQuery connection profile (oauth, europe-west2)
  packages.yml           dbt package dependencies (dbt-utils)
  requirements.txt       Python dependencies for the dbt-runner Cloud Run Job
  Dockerfile             dbt-runner Cloud Run Job image
  models/
    sources.yml          Source declarations with freshness thresholds
    staging/             Source conforming — one model per raw table
    intermediate/        Artist resolution and track enrichment
    mart/                Dimensional models consumed by reporting
```

## Model layers

| Layer | Materialisation | Purpose |
|:---|:---|:---|
| Staging | View | Cast types, rename fields, add surrogate keys — one model per source |
| Intermediate | Ephemeral | Artist cross-source resolution and track enrichment — no BigQuery objects created |
| Mart | Table | `dim_artist`, `dim_track`, `fact_chart_position` — reporting targets |

See [models/README.md](models/README.md) for model-level documentation.

## Source freshness

`sources.yml` declares freshness thresholds on all raw tables using the `_ingested_at` column. The `music_transform` DAG runs `dbt source freshness` before `dbt run` — if any source is stale, the run is blocked and an alert is sent.

## Running locally

```bash
cd dbt
pip install -r requirements.txt
dbt deps                          # install dbt-utils
dbt debug                         # verify connection
dbt source freshness              # check raw table recency
dbt run                           # build all models
dbt test                          # run all tests
```

A local `profiles.yml` with your GCP credentials is required. The production profile uses the `music-cloudrun-sa` service account via Cloud Run's attached identity.

## dbt-runner Cloud Run Job

The `dbt-runner` job accepts a dbt command as its container args override:

| Airflow task | Container args | Purpose |
|:---|:---|:---|
| `check_source_freshness` | `["source", "freshness"]` | Validate raw table recency |
| `run_dbt` | `["run"]` | Build all mart and staging models |
| `test_dbt` | `["test"]` | Run all schema and data tests |

Environment variables required by the job:

| Variable | Value |
|:---|:---|
| `DBT_PROFILES_DIR` | `/dbt` |
| `GCP_PROJECT_ID` | `portfolio-hub-2026` |
