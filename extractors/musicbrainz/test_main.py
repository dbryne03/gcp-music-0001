import io
import json
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from main import (
    GCS_BLOB,
    ARCHIVE_NAME,
    expected_sha256,
    extract_and_filter,
    get_latest_version,
    stage_to_gcs,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

ARTIST_RECORDS = [
    {
        "id": "f27ec8db-af05-4f36-916e-3d57f91ecf5e",
        "name": "Michael Jackson",
        "sort-name": "Jackson, Michael",
        "type": "Person",
        "country": "US",
        "life-span": {"begin": "1958-08-29", "end": "2009-06-25", "ended": True},
        "genres": [{"name": "pop", "count": 10}, {"name": "soul", "count": 5}],
        "disambiguation": "",
        "relations": [],
    },
    {
        "id": "db92a151-1ac2-438b-bc43-b82e149ddd50",
        "name": "Rick Astley",
        "sort-name": "Astley, Rick",
        "type": "Person",
        "country": "GB",
        "life-span": {"begin": "1966-02-06", "end": None, "ended": False},
        "genres": [],
        "relations": [],
    },
    {
        "id": "9c9f1380-2516-4fc9-a3e6-f9f61941d090",
        "name": "The Beatles",
        "sort-name": "Beatles, The",
        "type": "Group",
        "country": "GB",
        "life-span": {"begin": "1960", "end": "1970", "ended": True},
        "genres": [{"name": "rock", "count": 20}],
        "relations": [],
    },
]


@pytest.fixture
def test_archive(tmp_path) -> Path:
    """Minimal artist.tar.xz matching the MusicBrainz dump structure."""
    ndjson = "\n".join(json.dumps(r) for r in ARTIST_RECORDS).encode()
    archive = tmp_path / ARCHIVE_NAME
    with tarfile.open(archive, "w:xz") as tar:
        info = tarfile.TarInfo(name="mbdump/artist")
        info.size = len(ndjson)
        tar.addfile(info, io.BytesIO(ndjson))
    return archive


# ── extract_and_filter ────────────────────────────────────────────────────────

def test_extract_returns_correct_row_count(test_archive, tmp_path):
    out = tmp_path / "out.ndjson"
    count = extract_and_filter(test_archive, out)
    assert count == len(ARTIST_RECORDS)


def test_extract_normalises_field_names(test_archive, tmp_path):
    out = tmp_path / "out.ndjson"
    extract_and_filter(test_archive, out)
    rows = [json.loads(l) for l in out.read_text().splitlines()]
    assert "sort_name" in rows[0]
    assert "begin_date" in rows[0]
    assert "end_date" in rows[0]
    assert "sort-name" not in rows[0]
    assert "life-span" not in rows[0]


def test_extract_flattens_genres_to_name_list(test_archive, tmp_path):
    out = tmp_path / "out.ndjson"
    extract_and_filter(test_archive, out)
    rows = [json.loads(l) for l in out.read_text().splitlines()]
    assert rows[0]["genres"] == ["pop", "soul"]
    assert rows[1]["genres"] == []


def test_extract_maps_life_span_fields(test_archive, tmp_path):
    out = tmp_path / "out.ndjson"
    extract_and_filter(test_archive, out)
    rows = [json.loads(l) for l in out.read_text().splitlines()]
    mj = rows[0]
    assert mj["begin_date"] == "1958-08-29"
    assert mj["end_date"] == "2009-06-25"
    assert mj["ended"] is True

    rick = rows[1]
    assert rick["end_date"] is None
    assert rick["ended"] is False


def test_extract_drops_unrequired_fields(test_archive, tmp_path):
    out = tmp_path / "out.ndjson"
    extract_and_filter(test_archive, out)
    rows = [json.loads(l) for l in out.read_text().splitlines()]
    for row in rows:
        assert "relations" not in row
        assert "disambiguation" not in row


# ── get_latest_version ────────────────────────────────────────────────────────

def test_get_latest_version_strips_whitespace():
    mock_resp = MagicMock()
    mock_resp.text = "20260516-001002\n"
    with patch("main.requests.get", return_value=mock_resp):
        assert get_latest_version() == "20260516-001002"


# ── expected_sha256 ───────────────────────────────────────────────────────────

def test_expected_sha256_parses_correctly():
    sha = "8413259ab765727db253a547fc9b15210c6f8e95cb53ad102643ff1b8f46c193"
    mock_resp = MagicMock()
    mock_resp.text = f"abc123  area.tar.xz\n{sha}  {ARCHIVE_NAME}\n"
    with patch("main.requests.get", return_value=mock_resp):
        assert expected_sha256("20260516-001002") == sha


def test_expected_sha256_raises_if_not_found():
    mock_resp = MagicMock()
    mock_resp.text = "abc123  area.tar.xz\n"
    with patch("main.requests.get", return_value=mock_resp):
        with pytest.raises(ValueError, match="not found in SHA256SUMS"):
            expected_sha256("20260516-001002")


# ── stage_to_gcs ──────────────────────────────────────────────────────────────

def test_stage_to_gcs_uploads_to_correct_blob(tmp_path):
    local = tmp_path / "mb_artists.ndjson"
    local.write_text('{"id":"x"}\n')

    mock_client = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value.blob.return_value = mock_blob

    with patch("main.storage.Client", return_value=mock_client):
        stage_to_gcs(local, "test-bucket")

    mock_client.bucket.assert_called_once_with("test-bucket")
    mock_client.bucket.return_value.blob.assert_called_once_with(GCS_BLOB)
    mock_blob.upload_from_filename.assert_called_once_with(str(local))
