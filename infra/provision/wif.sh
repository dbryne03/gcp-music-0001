#!/usr/bin/env bash
# Configures Workload Identity Federation for GitHub Actions.
# Eliminates the long-lived SERVICE_ACCOUNT JSON key.
# Run manually once with owner-level credentials before the first
# CI run that uses OIDC auth. Subsequent runs are idempotent.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/../config.env"; set +a

REPO="dbryne03/gcp-music-0001"
MEMBER="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${WIF_POOL}/attribute.repository/${REPO}"

echo "=== Workload Identity Federation ==="

# Workload Identity Pool
if gcloud iam workload-identity-pools describe "${WIF_POOL}" \
        --project="${PROJECT}" --location="global" &>/dev/null 2>&1; then
    echo "  [exists]  pool ${WIF_POOL}"
else
    echo "  [create]  pool ${WIF_POOL}"
    gcloud iam workload-identity-pools create "${WIF_POOL}" \
        --project="${PROJECT}" \
        --location="global" \
        --display-name="GitHub Actions"
fi

# OIDC Provider
if gcloud iam workload-identity-pools providers describe "${WIF_PROVIDER}" \
        --project="${PROJECT}" \
        --location="global" \
        --workload-identity-pool="${WIF_POOL}" &>/dev/null 2>&1; then
    echo "  [exists]  provider ${WIF_PROVIDER}"
else
    echo "  [create]  provider ${WIF_PROVIDER}"
    gcloud iam workload-identity-pools providers create-oidc "${WIF_PROVIDER}" \
        --project="${PROJECT}" \
        --location="global" \
        --workload-identity-pool="${WIF_POOL}" \
        --display-name="GitHub Actions OIDC" \
        --issuer-uri="https://token.actions.githubusercontent.com" \
        --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.actor=assertion.actor" \
        --attribute-condition="assertion.repository=='${REPO}'"
fi

# Bind github-actions-sa — only this repo can impersonate it
gcloud iam service-accounts add-iam-policy-binding "${GITHUB_SA}" \
    --project="${PROJECT}" \
    --role="roles/iam.workloadIdentityUser" \
    --member="${MEMBER}"
echo "  [ok]  github-actions-sa → workloadIdentityUser for ${REPO}"
