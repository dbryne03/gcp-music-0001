from datetime import timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from main import GCS_BLOB, prepare, stage_to_gcs


@pytest.fixture
def raw_parquet(tmp_path):
    """Minimal Parquet matching the HuggingFace dataset schema."""
    df = pd.DataFrame({
        "Unnamed: 0": [0, 1],
        "track_id": ["abc123", "def456"],
        "track_name": ["Song A", "Song B"],
        "artists": ["Artist X", "Artist Y"],
        "album_name": ["Album 1", "Album 2"],
        "track_genre": ["pop", "rock"],
        "popularity": [80, 60],
        "duration_ms": [200_000, 180_000],
        "explicit": [False, True],
        "danceability": [0.8, 0.6],
        "energy": [0.9, 0.7],
        "key": [5, 3],
        "loudness": [-5.0, -8.0],
        "mode": [1, 0],
        "speechiness": [0.05, 0.10],
        "acousticness": [0.1, 0.3],
        "instrumentalness": [0.0, 0.0],
        "liveness": [0.1, 0.2],
        "valence": [0.7, 0.4],
        "tempo": [120.0, 95.0],
        "time_signature": [4, 4],
    })
    path = tmp_path / "raw.parquet"
    df.to_parquet(path, index=False)
    return str(path)


@pytest.fixture
def prepared_parquet(raw_parquet, tmp_path):
    out = str(tmp_path / "out.parquet")
    prepare(raw_parquet, out)
    return pd.read_parquet(out)


def test_prepare_drops_index_column(prepared_parquet):
    assert "Unnamed: 0" not in prepared_parquet.columns


def test_prepare_adds_ingested_at(prepared_parquet):
    assert "_ingested_at" in prepared_parquet.columns
    assert prepared_parquet["_ingested_at"].notna().all()
    assert prepared_parquet["_ingested_at"].iloc[0].utcoffset() == timedelta(0)


def test_prepare_preserves_row_count(prepared_parquet):
    assert len(prepared_parquet) == 2


def test_prepare_preserves_payload_columns(prepared_parquet):
    expected = {
        "track_id", "track_name", "artists", "album_name", "track_genre",
        "popularity", "duration_ms", "explicit", "danceability", "energy",
        "key", "loudness", "mode", "speechiness", "acousticness",
        "instrumentalness", "liveness", "valence", "tempo", "time_signature",
    }
    assert expected.issubset(prepared_parquet.columns)


def test_prepare_graceful_if_no_index_column(tmp_path):
    df = pd.DataFrame({"track_id": ["x"], "track_name": ["y"]})
    raw = str(tmp_path / "no_index.parquet")
    out = str(tmp_path / "out.parquet")
    df.to_parquet(raw, index=False)
    prepare(raw, out)
    result = pd.read_parquet(out)
    assert "track_id" in result.columns


def test_stage_to_gcs_uploads_to_correct_blob(tmp_path):
    local_file = tmp_path / "spotify_tracks.parquet"
    local_file.write_bytes(b"parquet-data")

    mock_client = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value.blob.return_value = mock_blob

    with patch("main.storage.Client", return_value=mock_client):
        stage_to_gcs(str(local_file), "test-bucket")

    mock_client.bucket.assert_called_once_with("test-bucket")
    mock_client.bucket.return_value.blob.assert_called_once_with(GCS_BLOB)
    mock_blob.upload_from_filename.assert_called_once_with(str(local_file))
