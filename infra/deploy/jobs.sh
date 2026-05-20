#!/usr/bin/env bash
# Deploys Cloud Run Jobs for gcp-music-0001.
# Creates jobs on first run; updates the image on subsequent runs.
# Requires DEPLOY_SHA env var (set by CI to the triggering commit SHA).
# Falls back to 'latest' when run manually without DEPLOY_SHA set.
set -euo pipefail
set -a; source "$(dirname "${BASH_SOURCE[0]}")/../config.env"; set +a

TAG="${DEPLOY_SHA:-latest}"

if [[ "${TAG}" != "latest" ]] && ! [[ "${TAG}" =~ ^[0-9a-f]{40}$ ]]; then
    echo "ERROR: DEPLOY_SHA must be a 40-character lowercase hex SHA or unset (defaults to latest)"
    exit 1
fi

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

# ── Environment variables and secrets ─────────────────────────────────────────
# Applied after create/update so they are set on first run and idempotent on
# subsequent runs regardless of whether the job was just created or existed.

echo "=== Cloud Run Job config ==="

KAFKA_SECRETS="KAFKA_BOOTSTRAP_SERVERS=kafka-bootstrap-servers:latest,KAFKA_API_KEY=kafka-api-key:latest,KAFKA_API_SECRET=kafka-api-secret:latest"

# musicbrainz-extractor — GCS bucket only
gcloud run jobs update musicbrainz-extractor \
    --set-env-vars="GCS_BUCKET_RAW=${BUCKET}" \
    --region="${REGION}" --project="${PROJECT}"
echo "  [ok]  musicbrainz-extractor env"

# spotify-extractor — GCS bucket only
gcloud run jobs update spotify-extractor \
    --set-env-vars="GCS_BUCKET_RAW=${BUCKET}" \
    --region="${REGION}" --project="${PROJECT}"
echo "  [ok]  spotify-extractor env"

# lastfm-producer — GCS bucket, Kafka topic, and all Kafka + Last.fm secrets
gcloud run jobs update lastfm-producer \
    --set-env-vars="GCS_BUCKET_RAW=${BUCKET},KAFKA_TOPIC_LASTFM=lastfm.charts" \
    --set-secrets="LASTFM_API_KEY=lastfm-api-key:latest,${KAFKA_SECRETS}" \
    --region="${REGION}" --project="${PROJECT}"
echo "  [ok]  lastfm-producer env + secrets"

# lastfm-consumer — GCS bucket, Kafka topic, and Kafka secrets
gcloud run jobs update lastfm-consumer \
    --set-env-vars="GCS_BUCKET_RAW=${BUCKET},KAFKA_TOPIC_LASTFM=lastfm.charts" \
    --set-secrets="${KAFKA_SECRETS}" \
    --region="${REGION}" --project="${PROJECT}"
echo "  [ok]  lastfm-consumer env + secrets"

echo ""
echo "Cloud Run Jobs deployed."
