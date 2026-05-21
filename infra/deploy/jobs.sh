#!/usr/bin/env bash
# Deploys Cloud Run Jobs for gcp-music-0001.
# Creates jobs on first run; updates the image on subsequent runs.
# Requires DEPLOY_SHA env var (set by CI to the triggering commit SHA).
# Falls back to 'latest' when run manually without DEPLOY_SHA set.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/../config.env"; set +a

TAG="${DEPLOY_SHA:-latest}"

if [[ "${TAG}" != "latest" ]] && ! [[ "${TAG}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "ERROR: DEPLOY_SHA must be a 40-character lowercase hex SHA or unset (defaults to latest)"
    exit 1
fi

echo "=== Cloud Run Jobs (tag: ${TAG}) ==="

# Per-job resource settings — format: "cpu:memory"
declare -A JOB_RESOURCES=(
    ["lastfm-producer"]="1:512Mi"
    ["lastfm-consumer"]="1:512Mi"
    ["musicbrainz-extractor"]="2:4Gi"
    ["spotify-extractor"]="1:1Gi"
    ["dbt-runner"]="2:2Gi"
)

declare -A JOB_IMAGES=(
    ["lastfm-producer"]="${REGISTRY}/lastfm-producer"
    ["lastfm-consumer"]="${REGISTRY}/lastfm-consumer"
    ["musicbrainz-extractor"]="${REGISTRY}/musicbrainz-extractor"
    ["spotify-extractor"]="${REGISTRY}/spotify-extractor"
    ["dbt-runner"]="${REGISTRY}/dbt-runner"
)

for JOB_NAME in "${!JOB_IMAGES[@]}"; do
    BASE_IMAGE="${JOB_IMAGES[$JOB_NAME]}"
    RESOURCES="${JOB_RESOURCES[$JOB_NAME]}"
    CPU="${RESOURCES%%:*}"
    MEMORY="${RESOURCES##*:}"

    if gcloud run jobs describe "${JOB_NAME}" \
            --project="${PROJECT}" --region="${REGION}" &>/dev/null; then
        # Update: always use :latest — ensures the job runs the most recently
        # built image without failing when only some images were rebuilt this run.
        echo "  [update]  ${JOB_NAME} (${CPU} vCPU, ${MEMORY})"
        gcloud run jobs update "${JOB_NAME}" \
            --image="${BASE_IMAGE}:latest" \
            --cpu="${CPU}" \
            --memory="${MEMORY}" \
            --region="${REGION}" \
            --project="${PROJECT}"
    else
        # Create: pin to the exact SHA for reproducibility at creation time.
        echo "  [create]  ${JOB_NAME} (${CPU} vCPU, ${MEMORY})"
        gcloud run jobs create "${JOB_NAME}" \
            --image="${BASE_IMAGE}:${TAG}" \
            --service-account="${CLOUDRUN_SA}" \
            --cpu="${CPU}" \
            --memory="${MEMORY}" \
            --max-retries=1 \
            --task-timeout=3600 \
            --region="${REGION}" \
            --project="${PROJECT}"
    fi
done

# ── Environment variables and secrets ─────────────────────────────────────────

echo "=== Cloud Run Job config ==="

KAFKA_SECRETS="KAFKA_BOOTSTRAP_SERVERS=kafka-bootstrap-servers:latest,KAFKA_API_KEY=kafka-api-key:latest,KAFKA_API_SECRET=kafka-api-secret:latest"

gcloud run jobs update musicbrainz-extractor \
    --set-env-vars="GCS_BUCKET_RAW=${BUCKET}" \
    --region="${REGION}" --project="${PROJECT}"
echo "  [ok]  musicbrainz-extractor env"

gcloud run jobs update spotify-extractor \
    --set-env-vars="GCS_BUCKET_RAW=${BUCKET}" \
    --region="${REGION}" --project="${PROJECT}"
echo "  [ok]  spotify-extractor env"

gcloud run jobs update lastfm-producer \
    --set-env-vars="GCS_BUCKET_RAW=${BUCKET},KAFKA_TOPIC_LASTFM=lastfm.charts" \
    --set-secrets="LASTFM_API_KEY=lastfm-api-key:latest,${KAFKA_SECRETS}" \
    --region="${REGION}" --project="${PROJECT}"
echo "  [ok]  lastfm-producer env + secrets"

gcloud run jobs update lastfm-consumer \
    --set-env-vars="GCS_BUCKET_RAW=${BUCKET},KAFKA_TOPIC_LASTFM=lastfm.charts" \
    --set-secrets="${KAFKA_SECRETS}" \
    --region="${REGION}" --project="${PROJECT}"
echo "  [ok]  lastfm-consumer env + secrets"

gcloud run jobs update dbt-runner \
    --set-env-vars="DBT_PROFILES_DIR=/dbt,GCP_PROJECT_ID=${PROJECT}" \
    --region="${REGION}" --project="${PROJECT}"
echo "  [ok]  dbt-runner env"

echo ""
echo "Cloud Run Jobs deployed."
