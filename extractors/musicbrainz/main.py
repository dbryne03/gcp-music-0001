import hashlib
import io
import json
import logging
import os
import tarfile
import tempfile
from pathlib import Path

import requests
from google.cloud import storage

logger = logging.getLogger(__name__)

BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/json-dumps"
ARCHIVE_NAME = "artist.tar.xz"
GCS_BLOB = "raw/batch/musicbrainz/mb_artists.ndjson"


def get_latest_version() -> str:
    resp = requests.get(f"{BASE_URL}/LATEST", timeout=30)
    resp.raise_for_status()
    return resp.text.strip()


def expected_sha256(version: str) -> str:
    resp = requests.get(f"{BASE_URL}/{version}/SHA256SUMS", timeout=30)
    resp.raise_for_status()
    for line in resp.text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == ARCHIVE_NAME:
            return parts[0]
    raise ValueError(f"{ARCHIVE_NAME} not found in SHA256SUMS")


def download(version: str, dest: Path) -> str:
    url = f"{BASE_URL}/{version}/{ARCHIVE_NAME}"
    logger.info("Downloading %s", url)
    sha256 = hashlib.sha256()
    with requests.get(url, stream=True, timeout=3600) as resp:
        resp.raise_for_status()
        with dest.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
                sha256.update(chunk)
    return sha256.hexdigest()


def extract_and_filter(archive: Path, out: Path) -> int:
    """Stream-extract artist NDJSON, keeping only pipeline-relevant fields."""
    count = 0
    with tarfile.open(archive, "r:xz") as tar:
        member = next(
            m for m in tar.getmembers()
            if m.isfile() and m.name.split("/")[-1] == "artist"
        )
        with tar.extractfile(member) as src, out.open("w") as dst:
            for raw_line in src:
                rec = json.loads(raw_line)
                span = rec.get("life-span") or {}
                dst.write(json.dumps({
                    "id":         rec.get("id"),
                    "name":       rec.get("name"),
                    "sort_name":  rec.get("sort-name"),
                    "type":       rec.get("type"),
                    "country":    rec.get("country"),
                    "begin_date": span.get("begin"),
                    "end_date":   span.get("end"),
                    "ended":      span.get("ended"),
                    "genres":     [g["name"] for g in rec.get("genres") or []],
                }) + "\n")
                count += 1
    logger.info("Extracted %d artist records", count)
    return count


def stage_to_gcs(local: Path, bucket: str) -> None:
    client = storage.Client()
    client.bucket(bucket).blob(GCS_BLOB).upload_from_filename(str(local))
    logger.info("Staged to gs://%s/%s", bucket, GCS_BLOB)


def main() -> None:
    bucket = os.environ["GCS_BUCKET_RAW"]

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        version = get_latest_version()
        logger.info("Latest dump: %s", version)

        archive = tmp / ARCHIVE_NAME
        actual = download(version, archive)

        expected = expected_sha256(version)
        if actual != expected:
            raise ValueError(f"SHA256 mismatch — expected {expected}, got {actual}")
        logger.info("Checksum verified")

        ndjson = tmp / "mb_artists.ndjson"
        extract_and_filter(archive, ndjson)
        stage_to_gcs(ndjson, bucket)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
