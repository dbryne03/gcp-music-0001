# musicbrainz-extractor

Cloud Run Job — downloads the latest MusicBrainz artist JSON dump, verifies its integrity, filters it to pipeline fields, and stages the result to GCS as NDJSON.

---

## Behaviour

1. Fetches the latest dump version identifier from the MusicBrainz `LATEST` file
2. Downloads `artist.tar.xz` in 8 MB chunks, computing SHA256 incrementally (single read, no extra memory)
3. Verifies the checksum against `SHA256SUMS` — raises `ValueError` on mismatch
4. Stream-extracts the `artist` member from the XZ tarball; filters each record to the nine fields the pipeline needs
5. Normalises hyphenated MusicBrainz keys to snake_case, flattens `life-span` to `begin_date`/`end_date`/`ended`, and reduces `genres` to a plain list of name strings
6. Validates that at least 1,000,000 records were extracted before staging — fewer signals a partial extraction
7. Uploads the filtered NDJSON to `raw/batch/musicbrainz/mb_artists.ndjson` (overwrites previous run)

## Output schema

| Field | Type | Notes |
|:---|:---|:---|
| `id` | `str` | MusicBrainz artist MBID |
| `name` | `str` | Artist display name |
| `sort_name` | `str` | Sortable name (e.g. "Bowie, David") |
| `type` | `str \| null` | `Person`, `Group`, `Orchestra`, etc. |
| `country` | `str \| null` | ISO 3166-1 alpha-2 country code |
| `begin_date` | `str \| null` | Birth/formation date (partial dates allowed) |
| `end_date` | `str \| null` | Death/dissolution date |
| `ended` | `bool \| null` | Whether the artist career has ended |
| `genres` | `list[str]` | Genre name strings |
| `_ingested_at` | `str` | UTC ISO 8601 timestamp stamped at extraction time |

## Environment variables

| Variable | Source | Description |
|:---|:---|:---|
| `GCS_BUCKET_RAW` | Cloud Run env | GCS bucket name (without `gs://` prefix) |

## Resource requirements

Requires **2 vCPU / 4 Gi memory** — the XZ tarball decompression holds the full archive in memory during extraction.

## Files

| File | Description |
|:---|:---|
| `main.py` | Extractor entry point — `get_latest_version`, `expected_sha256`, `download`, `extract_and_filter`, `stage_to_gcs`, `main` |
| `test_main.py` | Unit tests — field normalisation, life-span mapping, genre flattening, SHA256 parsing, GCS upload |
| `Dockerfile` | Non-root Python 3.12 slim image |
| `requirements.txt` | `requests`, `google-cloud-storage` |
| `pytest.ini` | pytest configuration |

## Running tests

```bash
pip install -r requirements.txt
pytest
```
