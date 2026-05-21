#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/../config.env"; set +a

echo "=== Secret Manager ==="

for SECRET in lastfm-api-key kafka-bootstrap-servers kafka-api-key kafka-api-secret; do
    if gcloud secrets describe "${SECRET}" --project="${PROJECT}" &>/dev/null; then
        echo "  [exists]  ${SECRET}"
    else
        echo "  [create]  ${SECRET}"
        gcloud secrets create "${SECRET}" \
            --project="${PROJECT}" \
            --replication-policy=automatic
    fi
done
