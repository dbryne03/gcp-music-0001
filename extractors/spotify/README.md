# spotify-extractor

Cloud Run Job — downloads the Spotify tracks dataset from HuggingFace, cleans it, and stages it to GCS as a Parquet file.

---

## Behaviour

1. Downloads the auto-generated Parquet export from the `maharshipandya/spotify-tracks-dataset` HuggingFace dataset (`refs/convert/parquet` revision) — targets the Parquet export directly, avoiding a local CSV → Parquet conversion step
2. Drops the serialised DataFrame index column (`Unnamed: 0`) that is an artefact of the dataset's CSV origin
3. Stamps a `_ingested_at` UTC timestamp column
4. Validates that at least 100,000 rows are present before staging — fewer signals a corrupt or incomplete download
5. Uploads the prepared Parquet to `raw/batch/spotify/spotify_tracks.parquet` (overwrites previous run)

`hf_hub_download` caches the file locally so repeat runs in the same environment skip the network fetch.

## Output path

`raw/batch/spotify/spotify_tracks.parquet`

## Environment variables

| Variable | Source | Description |
|:---|:---|:---|
| `GCS_BUCKET_RAW` | Cloud Run env | GCS bucket name (without `gs://` prefix) |

## Files

| File | Description |
|:---|:---|
| `main.py` | Extractor entry point — `download_parquet`, `prepare`, `stage_to_gcs`, `main` |
| `test_main.py` | Unit tests — index column removal, `_ingested_at` stamping, row count preservation, GCS upload |
| `Dockerfile` | Non-root Python 3.12 slim image |
| `requirements.txt` | `pandas`, `pyarrow`, `huggingface-hub`, `google-cloud-storage` |
| `pytest.ini` | pytest configuration |

## Running tests

```bash
pip install -r requirements.txt
pytest
```
