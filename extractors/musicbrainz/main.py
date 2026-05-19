import os
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)

DUMP_URL = "https://data.metabrainz.org/pub/musicbrainz/data/json-dumps/"


def download_dump(destination: str) -> str:
    # TODO: fetch latest MusicBrainz JSON dump
    # Dumps listed at DUMP_URL — target mbdump-artist.tar.bz2
    raise NotImplementedError


def stage_to_gcs(local_path: str, bucket: str, blob_name: str) -> None:
    client = storage.Client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(blob_name)
    blob.upload_from_filename(local_path)
    logger.info("Staged %s to gs://%s/%s", local_path, bucket, blob_name)


def main() -> None:
    bucket = os.environ["GCS_BUCKET_RAW"]

    local_path = download_dump(destination="/tmp/mb_dump.json")
    stage_to_gcs(
        local_path=local_path,
        bucket=bucket,
        blob_name="raw/batch/musicbrainz/mb_dump.json",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
