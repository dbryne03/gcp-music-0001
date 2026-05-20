#!/usr/bin/env bash
# Creates service accounts and applies all IAM bindings.
# add-iam-policy-binding calls are idempotent — no pre-check needed.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../vars.sh"

echo "=== Service Accounts ==="

for SA_NAME in music-cloudrun-sa music-airflow-sa; do
    SA="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
    if gcloud iam service-accounts describe "${SA}" --project="${PROJECT}" &>/dev/null 2>&1; then
        echo "  [exists]  ${SA_NAME}"
    else
        echo "  [create]  ${SA_NAME}"
        case "${SA_NAME}" in
            music-cloudrun-sa) DISPLAY="Music Pipeline — Cloud Run Jobs" ;;
            music-airflow-sa)  DISPLAY="Music Pipeline — Airflow / Astronomer Cloud" ;;
        esac
        gcloud iam service-accounts create "${SA_NAME}" \
            --project="${PROJECT}" \
            --display-name="${DISPLAY}"
    fi
done

echo "=== IAM ==="

# cloudrun-sa — GCS
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --project="${PROJECT}" \
    --member="serviceAccount:${CLOUDRUN_SA}" \
    --role="roles/storage.objectAdmin"
echo "  [ok]  cloudrun-sa → storage.objectAdmin on ${BUCKET}"

# cloudrun-sa — BigQuery
gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${CLOUDRUN_SA}" \
    --role="roles/bigquery.dataEditor" --condition=None
echo "  [ok]  cloudrun-sa → bigquery.dataEditor"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${CLOUDRUN_SA}" \
    --role="roles/bigquery.jobUser" --condition=None
echo "  [ok]  cloudrun-sa → bigquery.jobUser"

# cloudrun-sa — Secret Manager
for SECRET in lastfm-api-key kafka-bootstrap-servers kafka-api-key kafka-api-secret; do
    gcloud secrets add-iam-policy-binding "${SECRET}" \
        --project="${PROJECT}" \
        --member="serviceAccount:${CLOUDRUN_SA}" \
        --role="roles/secretmanager.secretAccessor"
done
echo "  [ok]  cloudrun-sa → secretmanager.secretAccessor on all secrets"

# airflow-sa
gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/run.invoker" --condition=None
echo "  [ok]  airflow-sa → run.invoker"

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --project="${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/storage.objectViewer"
echo "  [ok]  airflow-sa → storage.objectViewer on ${BUCKET}"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/bigquery.jobUser" --condition=None
echo "  [ok]  airflow-sa → bigquery.jobUser"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/bigquery.dataEditor" --condition=None
echo "  [ok]  airflow-sa → bigquery.dataEditor"

# github-actions-sa — actAs on cloudrun-sa for job creation
gcloud iam service-accounts add-iam-policy-binding "${CLOUDRUN_SA}" \
    --member="serviceAccount:${GITHUB_SA}" \
    --role="roles/iam.serviceAccountUser" \
    --project="${PROJECT}"
echo "  [ok]  github-actions-sa → serviceAccountUser on music-cloudrun-sa"
