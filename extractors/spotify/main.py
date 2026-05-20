import os
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from huggingface_hub import hf_hub_download
from google.cloud import storage

logger = logging.getLogger(__name__)

REPO_ID = "maharshipandya/spotify-tracks-dataset"
PARQUET_FILE = "default/train/0000.parquet"
PARQUET_REVISION = "refs/convert/parquet"
GCS_BLOB = "raw/batch/spotify/spotify_tracks.parquet"


def download_parquet(local_dir: str) -> str:
    """Download the Spotify tracks Parquet file from HuggingFace.

    Targets the auto-generated Parquet export on the refs/convert/parquet
    revision rather than the original CSV, avoiding a local format conversion
    step. The file is cached by hf_hub_download so repeat runs in the same
    environment skip the network fetch.

    Args:
        local_dir: Local directory to download the file into.

    Returns:
        Absolute path to the downloaded Parquet file.
    """
    logger.info("Downloading %s from HuggingFace", REPO_ID)
    return hf_hub_download(
        repo_id=REPO_ID,
        filename=PARQUET_FILE,
        repo_type="dataset",
        revision=PARQUET_REVISION,
        local_dir=local_dir,
    )


def prepare(raw_path: str, out_path: str) -> None:
    """Clean the raw Parquet and stamp an ingestion timestamp.

    Drops the serialised DataFrame index column (Unnamed: 0) that the
    HuggingFace dataset includes as an artefact of its CSV origin. Adds
    a UTC _ingested_at column so the raw layer carries a consistent
    ingestion time without relying on GCS object metadata.

    Args:
        raw_path: Path to the downloaded raw Parquet file.
        out_path: Path to write the prepared Parquet file to.
    """
    df = pd.read_parquet(raw_path)
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    df["_ingested_at"] = datetime.now(timezone.utc)
    df.to_parquet(out_path, index=False)
    logger.info("Prepared %d rows → %s", len(df), out_path)


def stage_to_gcs(local_path: str, bucket: str) -> None:
    """Upload the prepared Parquet file to GCS.

    Args:
        local_path: Path to the local Parquet file to upload.
        bucket: GCS bucket name (without gs:// prefix).
    """
    client = storage.Client()
    client.bucket(bucket).blob(GCS_BLOB).upload_from_filename(local_path)
    logger.info("Staged to gs://%s/%s", bucket, GCS_BLOB)


def main() -> None:
    """Entry point for the Spotify extractor Cloud Run Job.

    Downloads the Spotify tracks dataset from HuggingFace, drops the index
    column artefact, stamps an ingestion timestamp, and stages the result
    to GCS as a single Parquet file. All intermediate files are written to
    a temporary directory that is cleaned up automatically on exit.
    """
    bucket = os.environ["GCS_BUCKET_RAW"]
    with tempfile.TemporaryDirectory() as tmp:
        raw_path = download_parquet(tmp)
        out_path = str(Path(tmp) / "spotify_tracks.parquet")
        prepare(raw_path, out_path)
        stage_to_gcs(out_path, bucket)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
