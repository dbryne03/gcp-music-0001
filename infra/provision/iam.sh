#!/usr/bin/env bash
# Creates service accounts and applies resource-scoped IAM bindings.
# Requires: storage.admin, secretmanager.admin, iam.serviceAccountAdmin,
#   run.admin (for SA creation), artifactregistry.admin.
# Project-level bindings for pipeline SAs live in _project_iam.sh (manual).
# add-iam-policy-binding calls are idempotent — no pre-check needed.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/../config.env"; set +a

echo "=== Service Accounts ==="

declare -A SA_DISPLAY=(
    ["${CLOUDRUN_SA}"]="Music Pipeline — Cloud Run Jobs"
    ["${AIRFLOW_SA}"]="Music Pipeline — Airflow / Astronomer Cloud"
)

for SA_EMAIL in "${!SA_DISPLAY[@]}"; do
    SA_NAME="${SA_EMAIL%%@*}"
    if gcloud iam service-accounts describe "${SA_EMAIL}" --project="${PROJECT}" &>/dev/null; then
        echo "  [exists]  ${SA_NAME}"
    else
        echo "  [create]  ${SA_NAME}"
        gcloud iam service-accounts create "${SA_NAME}" \
            --project="${PROJECT}" \
            --display-name="${SA_DISPLAY[$SA_EMAIL]}"
    fi
done

echo "=== IAM ==="

# cloudrun-sa — GCS (bucket-scoped)
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --project="${PROJECT}" \
    --member="serviceAccount:${CLOUDRUN_SA}" \
    --role="roles/storage.objectAdmin"
echo "  [ok]  cloudrun-sa → storage.objectAdmin on ${BUCKET}"

# cloudrun-sa — Secret Manager (secret-scoped)
for SECRET in lastfm-api-key kafka-bootstrap-servers kafka-api-key kafka-api-secret; do
    gcloud secrets add-iam-policy-binding "${SECRET}" \
        --project="${PROJECT}" \
        --member="serviceAccount:${CLOUDRUN_SA}" \
        --role="roles/secretmanager.secretAccessor"
done
echo "  [ok]  cloudrun-sa → secretmanager.secretAccessor on all secrets"

# airflow-sa — GCS (bucket-scoped)
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
    --project="${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/storage.objectViewer"
echo "  [ok]  airflow-sa → storage.objectViewer on ${BUCKET}"

# github-actions-sa — actAs on cloudrun-sa (SA-scoped)
gcloud iam service-accounts add-iam-policy-binding "${CLOUDRUN_SA}" \
    --member="serviceAccount:${GITHUB_SA}" \
    --role="roles/iam.serviceAccountUser" \
    --project="${PROJECT}"
echo "  [ok]  github-actions-sa → serviceAccountUser on music-cloudrun-sa"

# Astronomer workload identity — SA-scoped impersonation
gcloud iam service-accounts add-iam-policy-binding "${AIRFLOW_SA}" \
    --member="serviceAccount:${ASTRO_SA}" \
    --role="roles/iam.serviceAccountTokenCreator" \
    --project="${PROJECT}"
echo "  [ok]  astro-sa → serviceAccountTokenCreator on music-airflow-sa"
