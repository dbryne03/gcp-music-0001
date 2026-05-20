#!/usr/bin/env bash
# Deploys Cloud Run Jobs for gcp-music-0001.
# Creates jobs on first run; updates the image on subsequent runs.
# Requires DEPLOY_SHA env var (set by CI to the triggering commit SHA).
# Falls back to 'latest' when run manually without DEPLOY_SHA set.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../vars.sh"

TAG="${DEPLOY_SHA:-latest}"

echo "=== Cloud Run Jobs (tag: ${TAG}) ==="

declare -A JOBS=(
    ["lastfm-producer"]="${REGISTRY}/lastfm-producer:${TAG}"
    ["lastfm-consumer"]="${REGISTRY}/lastfm-consumer:${TAG}"
    ["musicbrainz-extractor"]="${REGISTRY}/musicbrainz-extractor:${TAG}"
    ["spotify-extractor"]="${REGISTRY}/spotify-extractor:${TAG}"
    ["dbt-runner"]="${REGISTRY}/dbt-runner:${TAG}"
)

for JOB_NAME in "${!JOBS[@]}"; do
    IMAGE="${JOBS[$JOB_NAME]}"
    if gcloud run jobs describe "${JOB_NAME}" \
            --project="${PROJECT}" --region="${REGION}" &>/dev/null 2>&1; then
        echo "  [update]  ${JOB_NAME}"
        gcloud run jobs update "${JOB_NAME}" \
            --image="${IMAGE}" \
            --region="${REGION}" \
            --project="${PROJECT}"
    else
        echo "  [create]  ${JOB_NAME}"
        gcloud run jobs create "${JOB_NAME}" \
            --image="${IMAGE}" \
            --service-account="${CLOUDRUN_SA}" \
            --max-retries=1 \
            --task-timeout=3600 \
            --region="${REGION}" \
            --project="${PROJECT}"
    fi
done

echo ""
echo "Cloud Run Jobs deployed."
