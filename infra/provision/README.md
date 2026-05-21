# infra/provision

Idempotent GCP resource provisioning scripts. These create static infrastructure that exists independently of any container image. They run on every merge to `main` via `infra.yml`.

---

## Scripts

All scripts source `../config.env` at startup. Running them multiple times is safe — existing resources are left unchanged.

| Script | `[auto/manual]` | Description |
|:---|:---|:---|
| `apis.sh` | auto | Enable all required GCP APIs |
| `storage.sh` | auto | Create GCS bucket with uniform bucket-level access; apply lifecycle policy from `../lifecycle.json` |
| `bigquery.sh` | auto | Create `raw` and `music` datasets; create raw tables (`lastfm`, `mb_dump`, `spotify`) using schemas from `../../dags/schemas/` |
| `registry.sh` | auto | Create Artifact Registry Docker repository (`music-pipeline`, `europe-west2`) |
| `secrets.sh` | auto | Create Secret Manager secret placeholders (no values — added manually) |
| `iam.sh` | auto | Create `music-cloudrun-sa` and `music-airflow-sa`; apply resource-scoped IAM bindings |
| `_project_iam.sh` | **manual** | Project-level IAM bindings for pipeline SAs (`bigquery.dataEditor`, `bigquery.jobUser`, `run.invoker`, `run.developer`) |
| `_wif.sh` | **manual** | Workload Identity Federation pool and provider setup for GitHub Actions OIDC |

## Script naming convention

Scripts prefixed with `_` are **manual-only** and must never be called by CI. They require owner-level credentials and are run from a local terminal. The `_` prefix is the convention: if a script file starts with `_`, it is not a valid CI target.

Automated scripts (no prefix) must be safe to run on every push with the `github-actions-sa` service account.

## IAM model

`iam.sh` applies **resource-scoped** bindings only — no `gcloud projects add-iam-policy-binding` calls:

| SA | Scope | Roles |
|:---|:---|:---|
| `music-cloudrun-sa` | GCS bucket | `roles/storage.objectAdmin` |
| `music-cloudrun-sa` | Each Secret Manager secret | `roles/secretmanager.secretAccessor` |
| `music-airflow-sa` | GCS bucket | `roles/storage.objectViewer` |
| `github-actions-sa` | `music-cloudrun-sa` (SA-level) | `roles/iam.serviceAccountUser` |
| Astronomer SA | `music-airflow-sa` (SA-level) | `roles/iam.serviceAccountTokenCreator` |

Project-level bindings (`bigquery.dataEditor`, `run.invoker`, etc.) are in `_project_iam.sh`.

## Adding secret values

`secrets.sh` creates empty placeholder secrets. Values are added manually:

```bash
echo -n "<value>" | gcloud secrets versions add <secret-name> \
  --project=portfolio-hub-2026 --data-file=-
```

| Secret | Source |
|:---|:---|
| `lastfm-api-key` | last.fm/api — register an application |
| `kafka-bootstrap-servers` | Confluent Cloud cluster settings |
| `kafka-api-key` | Confluent Cloud API key |
| `kafka-api-secret` | Confluent Cloud API secret |
