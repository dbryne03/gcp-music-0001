# .github/workflows

GitHub Actions CI/CD workflows for `gcp-music-0001`. Three focused workflow files chain via `workflow_run` — each fires when the previous completes successfully on `main`. Pull requests trigger only `validate.yml`.

---

## Chain

```
Validate ──(main, on success)──► Infrastructure ──(on success)──► Deploy
```

## `validate.yml`

Runs on every pull request and every push to `main`. No GCP authentication required.

| Job | Description |
|:---|:---|
| `dags` | `py_compile` on all DAG files — catches import and syntax errors before Astronomer sync |
| `dbt` | Writes a CI dbt profile, runs `dbt deps` and `dbt parse` |
| `extractors` (matrix ×4) | `pip install` with cached dependencies + `pytest` for each extractor |
| `docker` (matrix ×5) | Docker build with GHA layer cache; no push (validates `Dockerfile` only) |

Path filters ensure only the affected jobs re-run on each push. Manual runs execute all jobs regardless of changed files.

## `infra.yml`

Triggered when `validate.yml` completes successfully on `main`. Runs each provision script as a discrete named step.

| Step | Script |
|:---|:---|
| Enable APIs | `infra/provision/apis.sh` |
| Provision storage | `infra/provision/storage.sh` |
| Provision BigQuery | `infra/provision/bigquery.sh` |
| Provision registry | `infra/provision/registry.sh` |
| Provision secrets | `infra/provision/secrets.sh` |
| Provision IAM | `infra/provision/iam.sh` |

`cancel-in-progress: false` — a partial infra run completing is safer than a newer run cancelling mid-script and leaving GCP in an inconsistent state.

## `deploy.yml`

Triggered when `infra.yml` completes successfully on `main`. Accepts an optional `sha` input for targeted redeploys without a new commit.

| Job | Description |
|:---|:---|
| `images` | Build and push all 5 Docker images to Artifact Registry; tagged with commit SHA and `:latest` |
| `workloads` | Update Cloud Run Jobs (gates on `images` succeeding) |
| `deploy-astronomer` | Deploy updated Airflow image and DAGs to Astronomer Cloud |

## Authentication

All workflows that touch GCP use **Workload Identity Federation** (OIDC):

```yaml
- uses: google-github-actions/auth@v2
  with:
    workload_identity_provider: ${{ env.WIF_PROVIDER }}
    service_account: ${{ env.GITHUB_SA }}
```

No long-lived JSON keys are stored in GitHub Secrets. The WIF pool and provider are configured in `infra/provision/_wif.sh` (manual-only).

## Permissions

- Global: `permissions: contents: read`
- Jobs that push to GCP elevate to `id-token: write` (required for OIDC token exchange)
- Jobs that push Docker images elevate to `packages: write`

## Manual runs

Selecting "Run workflow" in the GitHub Actions UI sets `github.event_name == 'workflow_dispatch'`. In this mode:

- All path filters are bypassed — every job in every workflow runs
- Run names are prefixed with `Manual #<run-number>` for easy identification in the Actions history
- The full chain (`validate → infra → deploy`) executes in the correct order
