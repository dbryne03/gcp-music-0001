#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../vars.sh"

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
