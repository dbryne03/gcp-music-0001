import hashlib
import json
import logging
import os
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from google.cloud import storage

logger = logging.getLogger(__name__)

BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/json-dumps"
ARCHIVE_NAME = "artist.tar.xz"
GCS_BLOB = "raw/batch/musicbrainz/mb_artists.ndjson"


def get_latest_version() -> str:
    """Fetch the identifier of the most recent MusicBrainz JSON dump.

    Reads the LATEST file published alongside each dump, which contains
    the dump timestamp in YYYYMMDD-HHMMSS format.

    Returns:
        Dump version string, e.g. '20260516-001002'.

    Raises:
        requests.HTTPError: If the LATEST file cannot be retrieved.
    """
    resp = requests.get(f"{BASE_URL}/LATEST", timeout=30)
    resp.raise_for_status()
    version = resp.text.strip()
    if not version:
        raise ValueError("LATEST file is empty — MusicBrainz dump may be in progress")
    return version


def expected_sha256(version: str) -> str:
    """Look up the expected SHA256 hash for the artist archive from the dump manifest.

    Parses the SHA256SUMS file published with each dump and returns the hash
    for ARCHIVE_NAME. Used to verify the integrity of the downloaded archive
    before extraction.

    Args:
        version: Dump version string returned by get_latest_version().

    Returns:
        Lowercase hex SHA256 digest for the artist archive.

    Raises:
        ValueError: If ARCHIVE_NAME is not listed in SHA256SUMS.
        requests.HTTPError: If the SHA256SUMS file cannot be retrieved.
    """
    resp = requests.get(f"{BASE_URL}/{version}/SHA256SUMS", timeout=30)
    resp.raise_for_status()
    for line in resp.text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == ARCHIVE_NAME:
            return parts[0]
    raise ValueError(f"{ARCHIVE_NAME} not found in SHA256SUMS")


def download(version: str, dest: Path) -> str:
    """Stream-download the artist archive and compute its SHA256 hash.

    Downloads in 8 MB chunks to keep memory usage flat regardless of archive
    size. The hash is computed incrementally over the same chunks so the file
    is only read once.

    Args:
        version: Dump version string used to construct the download URL.
        dest: Local path to write the downloaded archive to.

    Returns:
        Lowercase hex SHA256 digest of the downloaded file.

    Raises:
        requests.HTTPError: If the download request fails.
    """
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
    """Extract the artist NDJSON from the archive and filter to pipeline fields.

    Locates the artist member inside the XZ tarball, reads it line by line,
    and writes a compact NDJSON file containing only the fields the pipeline
    needs. Hyphenated MusicBrainz keys are normalised to snake_case and the
    life-span object is flattened to begin_date, end_date, and ended. Genre
    objects are reduced to a plain list of name strings.

    Args:
        archive: Path to the downloaded artist.tar.xz archive.
        out: Path to write the filtered NDJSON output to.

    Returns:
        Number of artist records written.
    """
    count = 0
    ingested_at = datetime.now(timezone.utc).isoformat()
    with tarfile.open(archive, "r:xz") as tar:
        member = next(
            (m for m in tar.getmembers() if m.isfile() and m.name.split("/")[-1] == "artist"),
            None,
        )
        if member is None:
            raise FileNotFoundError(f"No 'artist' member found in {archive.name}")
        with tar.extractfile(member) as src, out.open("w") as dst:
            for raw_line in src:
                rec = json.loads(raw_line)
                span = rec.get("life-span") or {}
                dst.write(json.dumps({
                    "id":           rec.get("id"),
                    "name":         rec.get("name"),
                    "sort_name":    rec.get("sort-name"),
                    "type":         rec.get("type"),
                    "country":      rec.get("country"),
                    "begin_date":   span.get("begin"),
                    "end_date":     span.get("end"),
                    "ended":        span.get("ended"),
                    "genres":       [g["name"] for g in rec.get("genres") or []],
                    "_ingested_at": ingested_at,
                }) + "\n")
                count += 1
    logger.info("Extracted %d artist records", count)
    return count


def stage_to_gcs(local: Path, bucket: str) -> None:
    """Upload the filtered artist NDJSON to GCS.

    Args:
        local: Path to the local NDJSON file to upload.
        bucket: GCS bucket name (without gs:// prefix).
    """
    client = storage.Client()
    client.bucket(bucket).blob(GCS_BLOB).upload_from_filename(str(local))
    logger.info("Staged to gs://%s/%s", bucket, GCS_BLOB)


def main() -> None:
    """Entry point for the MusicBrainz extractor Cloud Run Job.

    Resolves the latest dump version, downloads and verifies the artist
    archive, filters it to pipeline fields, and stages the result to GCS.
    All intermediate files are written to a temporary directory that is
    cleaned up automatically on exit.

    Raises:
        ValueError: If the downloaded archive fails the SHA256 checksum.
    """
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
