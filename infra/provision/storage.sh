#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/../config.env"; set +a

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

echo "=== GCS Lifecycle ==="

gcloud storage buckets update "gs://${BUCKET}" \
    --lifecycle-file="${SCRIPT_DIR}/../lifecycle.json" \
    --project="${PROJECT}"
echo "  [ok]  lifecycle rules applied to ${BUCKET}"
