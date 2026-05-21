# infra/deploy

Cloud Run Job deployment scripts. These reference container images that must already exist in Artifact Registry — they run after images are built and pushed by `deploy.yml`.

---

## Scripts

| Script | `[auto/manual]` | Description |
|:---|:---|:---|
| `jobs.sh` | auto | Create Cloud Run Jobs on first run; update the container image on subsequent runs |

## `jobs.sh` behaviour

**First run (create):** Creates the Cloud Run Job with the exact image SHA (`DEPLOY_SHA`), service account, CPU, memory, max retries, and task timeout. Pins to the SHA for reproducibility at creation time.

**Subsequent runs (update):** Updates to `:latest` — ensures the job always runs the most recently built image without failing when only some images were rebuilt in a given CI run.

The `DEPLOY_SHA` variable is validated as a 40-character lowercase hex string before use as an image tag. If unset (manual run), it defaults to `latest`.

## Job configuration

| Job | CPU | Memory | Max retries | Task timeout |
|:---|:---|:---|:---|:---|
| `lastfm-producer` | 1 | 512 Mi | 1 | 3600 s |
| `lastfm-consumer` | 1 | 512 Mi | 1 | 3600 s |
| `musicbrainz-extractor` | 2 | 4 Gi | 1 | 3600 s |
| `spotify-extractor` | 1 | 1 Gi | 1 | 3600 s |
| `dbt-runner` | 2 | 2 Gi | 1 | 3600 s |

## Running manually

```bash
# Deploy with a specific SHA
DEPLOY_SHA=<40-char-sha> bash infra/deploy/jobs.sh

# Deploy using :latest (manual / local only)
bash infra/deploy/jobs.sh
```

Authenticate first:

```bash
gcloud auth login
gcloud config set project portfolio-hub-2026
```
