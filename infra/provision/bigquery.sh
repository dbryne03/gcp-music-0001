#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/../config.env"; set +a

SCHEMAS_DIR="${SCRIPT_DIR}/../schemas"

echo "=== BigQuery ==="

for DATASET in raw music; do
    if bq show --project_id="${PROJECT}" "${PROJECT}:${DATASET}" &>/dev/null 2>&1; then
        echo "  [exists]  dataset ${DATASET}"
    else
        echo "  [create]  dataset ${DATASET}"
        bq mk --dataset --location="${REGION}" "${PROJECT}:${DATASET}"
    fi
done

echo "=== Raw Tables ==="

declare -A RAW_TABLES=(
    ["lastfm"]="${SCHEMAS_DIR}/lastfm.json"
    ["mb_dump"]="${SCHEMAS_DIR}/mb_dump.json"
    ["spotify"]="${SCHEMAS_DIR}/spotify.json"
)

for TABLE in "${!RAW_TABLES[@]}"; do
    if bq show --project_id="${PROJECT}" "${PROJECT}:raw.${TABLE}" &>/dev/null 2>&1; then
        echo "  [exists]  raw.${TABLE}"
    else
        echo "  [create]  raw.${TABLE}"
        bq mk --table \
            --project_id="${PROJECT}" \
            "${PROJECT}:raw.${TABLE}" \
            "${RAW_TABLES[$TABLE]}"
    fi
done
