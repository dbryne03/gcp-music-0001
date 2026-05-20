# Shared variables sourced by all infra scripts.
# Not executable — source only: source "$(dirname "${BASH_SOURCE[0]}")/vars.sh"

PROJECT="portfolio-hub-2026"
REGION="europe-west2"
BUCKET="${PROJECT}-music-raw"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT}/music-pipeline"
CLOUDRUN_SA="music-cloudrun-sa@${PROJECT}.iam.gserviceaccount.com"
AIRFLOW_SA="music-airflow-sa@${PROJECT}.iam.gserviceaccount.com"
GITHUB_SA="github-actions-sa@${PROJECT}.iam.gserviceaccount.com"
