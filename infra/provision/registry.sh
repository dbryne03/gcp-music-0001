#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/../config.env"; set +a

echo "=== Artifact Registry ==="

if gcloud artifacts repositories describe music-pipeline \
        --project="${PROJECT}" --location="${REGION}" &>/dev/null; then
    echo "  [exists]  music-pipeline"
else
    echo "  [create]  music-pipeline"
    gcloud artifacts repositories create music-pipeline \
        --repository-format=docker \
        --location="${REGION}" \
        --project="${PROJECT}" \
        --description="Music pipeline container images"
fi
