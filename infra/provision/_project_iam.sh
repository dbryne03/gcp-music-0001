#!/usr/bin/env bash
# Applies project-level IAM bindings for pipeline service accounts.
#
# MANUAL USE ONLY — requires resourcemanager.projectIamAdmin which
# github-actions-sa does not hold (a service account managing project-wide
# IAM policy is a high blast-radius privilege that must stay with humans).
#
# Run once after service accounts are created, or when roles change:
#   bash infra/provision/_project_iam.sh
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/../config.env"; set +a

echo "=== Project-level IAM ==="

# cloudrun-sa — BigQuery (read data, run jobs)
gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${CLOUDRUN_SA}" \
    --role="roles/bigquery.dataEditor" --condition=None
echo "  [ok]  cloudrun-sa → bigquery.dataEditor"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${CLOUDRUN_SA}" \
    --role="roles/bigquery.jobUser" --condition=None
echo "  [ok]  cloudrun-sa → bigquery.jobUser"

# airflow-sa — Cloud Run (trigger jobs, check operation status)
gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/run.invoker" --condition=None
echo "  [ok]  airflow-sa → run.invoker"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/run.developer" --condition=None
echo "  [ok]  airflow-sa → run.developer"

# airflow-sa — BigQuery (run load jobs, write to tables)
gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/bigquery.jobUser" --condition=None
echo "  [ok]  airflow-sa → bigquery.jobUser"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${AIRFLOW_SA}" \
    --role="roles/bigquery.dataEditor" --condition=None
echo "  [ok]  airflow-sa → bigquery.dataEditor"
