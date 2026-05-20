#!/usr/bin/env bash
# Idempotent GCP resource bootstrap for gcp-music-0001.
# GitHub Actions SA required roles: serviceusage.serviceUsageAdmin,
# storage.admin, bigquery.admin, artifactregistry.admin,
# secretmanager.admin, iam.serviceAccountAdmin,
# resourcemanager.projectIamAdmin, run.admin.
set -euo pipefail

PROJECT="portfolio-hub-2026"
REGION="europe-west2"
BUCKET="${PROJECT}-music-raw"
CLOUDRUN_SA="music-cloudrun-sa@${PROJECT}.iam.gserviceaccount.com"
AIRFLOW_SA="music-airflow-sa@${PROJECT}.iam.gserviceaccount.com"

# ── APIs ──────────────────────────────────────────────────────────────────────

echo "=== APIs ==="

gcloud services enable \
    cloudresourcemanager.googleapis.com \
    iam.googleapis.com \
    storage.googleapis.com \
    bigquery.googleapis.com \
    artifactregistry.googleapis.com \
    secretmanager.googleapis.com \
    run.googleapis.com \
    --project="${PROJECT}"

echo "  [ok]  all required APIs enabled"

# ── Storage ───────────────────────────────────────────────────────────────────

echo "=== Storage ==="

if gcloud storage buckets describe "gs://${BUCKET}" --project="${PROJECT}" &>/dev/null 2>&1; then
    echo "  [exists]  gs://${BUCKET}"
else
    echo "  [create]  gs://${BUCKET}"
    gcloud storage buckets create "gs://${BUCKET}" \
        --project="${PROJECT}" \
        --location="${REGION}" \
        --uniform-bucket-level-access
fi

# ── BigQuery ──────────────────────────────────────────────────────────────────

echo "=== BigQuery ==="

if bq show --project_id="${PROJECT}" "${PROJECT}:raw" &>/dev/null 2>&1; then
    echo "  [exists]  dataset raw"
else
    echo "  [create]  dataset raw"
    bq mk --dataset --location="${REGION}" \
        --description="Raw landing dataset — one table per source" \
        "${PROJECT}:raw"
fi

if bq show --project_id="${PROJECT}" "${PROJECT}:music" &>/dev/null 2>&1; then
    echo "  [exists]  dataset music"
else
    echo "  [create]  dataset music"
    bq mk --dataset --location="${REGION}" \
        --description="dbt mart layer — dimensional models" \
        "${PROJECT}:music"
fi

# ── Artifact Registry ─────────────────────────────────────────────────────────

echo "=== Artifact Registry ==="

if gcloud artifacts repositories describe music-pipeline \
        --project="${PROJECT}" --location="${REGION}" &>/dev/null 2>&1; then
    echo "  [exists]  music-pipeline"
else
    echo "  [create]  music-pipeline"
    gcloud artifacts repositories create music-pipeline \
        --repository-format=docker \
        --location="${REGION}" \
        --project="${PROJECT}" \
        --description="Music pipeline container images"
fi

# ── Secret Manager ────────────────────────────────────────────────────────────

echo "=== Secret Manager ==="

for SECRET in lastfm-api-key kafka-bootstrap-servers kafka-api-key kafka-api-secret; do
    if gcloud secrets describe "${SECRET}" --project="${PROJECT}" &>/dev/null 2>&1; then
        echo "  [exists]  ${SECRET}"
    else
        echo "  [create]  ${SECRET}"
        gcloud secrets create "${SECRET}" \
            --project="${PROJECT}" \
            --replication-policy=automatic
    fi
done

# ── Service Accounts ──────────────────────────────────────────────────────────

echo "=== Service Accounts ==="

if gcloud iam service-accounts describe "${CLOUDRUN_SA}" \
        --project="${PROJECT}" &>/dev/null 2>&1; then
    echo "  [exists]  music-cloudrun-sa"
else
    echo "  [create]  music-cloudrun-sa"
    gcloud iam service-accounts create music-cloudrun-sa \
        --project="${PROJECT}" \
        --display-name="Music Pipeline — Cloud Run Jobs"
fi

if gcloud iam service-accounts describe "${AIRFLOW_SA}" \
        --project="${PROJECT}" &>/dev/null 2>&1; then
    echo "  [exists]  music-airflow-sa"
else
    echo "  [create]  music-airflow-sa"
    gcloud iam service-accounts create music-airflow-sa \
        --project="${PROJECT}" \
        --display-name="Music Pipeline — Airflow / Astronomer Cloud"
fi

# ── IAM ───────────────────────────────────────────────────────────────────────

echo "=== IAM ==="

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --project="${PROJECT}" \
    --member="serviceAccount:${CLOUDRUN_SA}" \
    --role="roles/storage.objectAdmin"
echo "  [ok]  cloudrun-sa → storage.objectAdmin on ${BUCKET}"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${CLOUDRUN_SA}" \
    --role="roles/bigquery.dataEditor" \
    --condition=None
echo "  [ok]  cloudrun-sa → bigquery.dataEditor"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${CLOUDRUN_SA}" \
    --role="roles/bigquery.jobUser" \
    --condition=None
echo "  [ok]  cloudrun-sa → bigquery.jobUser"

for SECRET in lastfm-api-key kafka-bootstrap-servers kafka-api-key kafka-api-secret; do
    gcloud secrets add-iam-policy-binding "${SECRET}" \
        --project="${PROJECT}" \
        --member="serviceAccount:${CLOUDRUN_SA}" \
        --role="roles/secretmanager.secretAccessor"
done
echo "  [ok]  cloudrun-sa → secretmanager.secretAccessor on all secrets"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/run.invoker" \
    --condition=None
echo "  [ok]  airflow-sa → run.invoker"

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --project="${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/storage.objectViewer"
echo "  [ok]  airflow-sa → storage.objectViewer on ${BUCKET}"

# ── Cloud Run Jobs ────────────────────────────────────────────────────────────
# TODO: lastfm-producer, lastfm-consumer, musicbrainz-extractor,
#       spotify-extractor, dbt-runner

echo ""
echo "Bootstrap complete."
