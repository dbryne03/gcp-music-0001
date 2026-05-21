# infra

GCP infrastructure for `gcp-music-0001`. All scripts are idempotent and
managed by GitHub Actions — see [`.github/workflows/`](../.github/workflows/)
for execution order.

---

## Naming convention

Scripts prefixed with `_` are **manual-only** — they are never called by CI.
This follows the convention used in shell tooling and some DevOps codebases
where `_` marks a file as a supporting operation that requires human intent
to run (one-off setup, sensitive privilege operations, etc.).

Automated scripts (no prefix) must be safe to run on every push.
Manual scripts (`_` prefix) require owner-level credentials and are run once
or on demand from a local terminal.

---

## Structure

```
infra/
  config.env            Shared configuration — project, region, resource names
  lifecycle.json        GCS object lifecycle policy (raw/ → delete after 90 days)
  provision/
    apis.sh             Enable required GCP APIs                         [auto]
    storage.sh          GCS bucket + lifecycle rules                     [auto]
    bigquery.sh         BigQuery datasets and raw tables                 [auto]
    registry.sh         Artifact Registry repository                     [auto]
    secrets.sh          Secret Manager secret placeholders               [auto]
    iam.sh              Service accounts and IAM bindings                [auto]
    _wif.sh             Workload Identity Federation setup               [manual]
  deploy/
    jobs.sh             Cloud Run Jobs — create on first run, update image on re-run
```

BigQuery table schemas live in `../dags/schemas/` — shared with the DAG layer.

---

## Configuration

All scripts source `config.env` at startup. To change the project or any
resource name, update `config.env` — no changes to individual scripts needed.

| Key | Value |
|:---|:---|
| `PROJECT` | `portfolio-hub-2026` |
| `REGION` | `europe-west2` |
| `BUCKET` | `portfolio-hub-2026-music-raw` |
| `REGISTRY` | `europe-west2-docker.pkg.dev/portfolio-hub-2026/music-pipeline` |
| `CLOUDRUN_SA` | `music-cloudrun-sa@portfolio-hub-2026.iam.gserviceaccount.com` |
| `AIRFLOW_SA` | `music-airflow-sa@portfolio-hub-2026.iam.gserviceaccount.com` |
| `GITHUB_SA` | `github-actions-sa@portfolio-hub-2026.iam.gserviceaccount.com` |
| `ASTRO_SA` | `astro-continuous-horizon-8866@astro-us-central1-0001.iam.gserviceaccount.com` |

---

## Provision vs Deploy

**`provision/`** scripts create static GCP infrastructure that exists
independently of any container image. They run before images are built.

**`deploy/`** scripts deploy application workloads that reference specific
container images. They run after images are pushed to Artifact Registry.

---

## Running manually

All provision scripts can be run individually from the repo root:

```bash
# Authenticate first
gcloud auth login
gcloud config set project portfolio-hub-2026

bash infra/provision/apis.sh
bash infra/provision/storage.sh
bash infra/provision/bigquery.sh
bash infra/provision/registry.sh
bash infra/provision/secrets.sh
bash infra/provision/iam.sh
```

For `deploy/jobs.sh`, pass the image tag via `DEPLOY_SHA`:

```bash
DEPLOY_SHA=<commit-sha> bash infra/deploy/jobs.sh
# or omit DEPLOY_SHA to use :latest
bash infra/deploy/jobs.sh
```

---

## Required GitHub secret

| Secret | Description |
|:---|:---|
| `SERVICE_ACCOUNT` | JSON key for `github-actions-sa` |

The `github-actions-sa` requires the following project-level roles:
`serviceusage.serviceUsageAdmin`, `storage.admin`, `bigquery.admin`,
`artifactregistry.admin`, `secretmanager.admin`, `iam.serviceAccountAdmin`,
`resourcemanager.projectIamAdmin`, `run.admin`.

---

## Secret values

Secrets are created as empty placeholders by `provision/secrets.sh`. Values
must be added manually via the GCP console or `gcloud`:

```bash
echo -n "<value>" | gcloud secrets versions add <secret-name> \
  --project=portfolio-hub-2026 --data-file=-
```

| Secret | Source |
|:---|:---|
| `lastfm-api-key` | last.fm/api — register an application |
| `kafka-bootstrap-servers` | Confluent Cloud cluster settings |
| `kafka-api-key` | Confluent Cloud API key |
| `kafka-api-secret` | Confluent Cloud API secret |
