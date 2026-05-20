import os
import logging
from datetime import datetime, timezone

import pandas as pd
from huggingface_hub import hf_hub_download
from google.cloud import storage

logger = logging.getLogger(__name__)

REPO_ID = "maharshipandya/spotify-tracks-dataset"
PARQUET_FILE = "default/train/0000.parquet"
PARQUET_REVISION = "refs/convert/parquet"
GCS_BLOB = "raw/batch/spotify/spotify_tracks.parquet"


def download_parquet(local_dir: str) -> str:
    logger.info("Downloading %s from HuggingFace", REPO_ID)
    return hf_hub_download(
        repo_id=REPO_ID,
        filename=PARQUET_FILE,
        repo_type="dataset",
        revision=PARQUET_REVISION,
        local_dir=local_dir,
    )


def prepare(raw_path: str, out_path: str) -> None:
    df = pd.read_parquet(raw_path)
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    df["_ingested_at"] = datetime.now(timezone.utc)
    df.to_parquet(out_path, index=False)
    logger.info("Prepared %d rows → %s", len(df), out_path)


def stage_to_gcs(local_path: str, bucket: str) -> None:
    client = storage.Client()
    client.bucket(bucket).blob(GCS_BLOB).upload_from_filename(local_path)
    logger.info("Staged to gs://%s/%s", bucket, GCS_BLOB)


def main() -> None:
    bucket = os.environ["GCS_BUCKET_RAW"]
    raw_path = download_parquet("/tmp/hf")
    out_path = "/tmp/spotify_tracks.parquet"
    prepare(raw_path, out_path)
    stage_to_gcs(out_path, bucket)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
