import os
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)

DATASET_URL = "https://huggingface.co/datasets/maharshipandya/spotify-tracks-dataset"


def download_parquet(destination: str) -> str:
    # TODO: download Spotify tracks Parquet dataset
    # Source: maharshipandya/spotify-tracks-dataset on HuggingFace
    raise NotImplementedError


def stage_to_gcs(local_path: str, bucket: str, blob_name: str) -> None:
    client = storage.Client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(blob_name)
    blob.upload_from_filename(local_path)
    logger.info("Staged %s to gs://%s/%s", local_path, bucket, blob_name)


def main() -> None:
    bucket = os.environ["GCS_BUCKET_RAW"]

    local_path = download_parquet(destination="/tmp/spotify_tracks.parquet")
    stage_to_gcs(
        local_path=local_path,
        bucket=bucket,
        blob_name="raw/batch/spotify/spotify_tracks.parquet",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
