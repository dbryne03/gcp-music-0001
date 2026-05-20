import json
from unittest.mock import MagicMock, call, patch

import pytest

from main import GCS_BLOB_PREFIX, GROUP_ID, drain, stage_to_gcs


def _make_msg(record: dict, error=None):
    msg = MagicMock()
    msg.error.return_value = error
    msg.value.return_value = json.dumps(record).encode()
    return msg


def _chart_record(name: str = "Artist", week: str = "2026-05-19") -> dict:
    return {
        "artist_mbid": "abc-123",
        "artist_name": name,
        "chart_week": week,
        "rank": 1,
        "listeners": 1000,
        "playcount": 5000,
    }


# ── drain ─────────────────────────────────────────────────────────────────────

def test_drain_returns_all_messages():
    records = [_chart_record(f"Artist {i}") for i in range(3)]
    msgs = [_make_msg(r) for r in records] + [None] * 6

    mock_consumer = MagicMock()
    mock_consumer.poll.side_effect = msgs

    result = drain(mock_consumer, "lastfm.charts")
    assert len(result) == 3


def test_drain_adds_ingested_at():
    msgs = [_make_msg(_chart_record())] + [None] * 6

    mock_consumer = MagicMock()
    mock_consumer.poll.side_effect = msgs

    result = drain(mock_consumer, "lastfm.charts")
    assert "_ingested_at" in result[0]


def test_drain_stamps_consistent_ingested_at():
    records = [_chart_record(f"Artist {i}") for i in range(3)]
    msgs = [_make_msg(r) for r in records] + [None] * 6

    mock_consumer = MagicMock()
    mock_consumer.poll.side_effect = msgs

    result = drain(mock_consumer, "lastfm.charts")
    timestamps = {r["_ingested_at"] for r in result}
    assert len(timestamps) == 1


def test_drain_returns_empty_on_no_messages():
    mock_consumer = MagicMock()
    mock_consumer.poll.return_value = None

    result = drain(mock_consumer, "lastfm.charts")
    assert result == []


def test_drain_raises_on_kafka_error():
    from confluent_kafka import KafkaError, KafkaException

    mock_error = MagicMock()
    mock_error.code.return_value = KafkaError.UNKNOWN_TOPIC_OR_PART

    mock_consumer = MagicMock()
    mock_consumer.poll.return_value = _make_msg({}, error=mock_error)

    with pytest.raises(RuntimeError, match="Kafka error"):
        drain(mock_consumer, "lastfm.charts")


# ── stage_to_gcs ──────────────────────────────────────────────────────────────

def test_stage_to_gcs_uses_chart_week_in_blob_name(tmp_path):
    records = [_chart_record(week="2026-05-19")]

    mock_client = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value.blob.return_value = mock_blob

    with patch("main.storage.Client", return_value=mock_client):
        blob_name = stage_to_gcs(records, "test-bucket")

    assert blob_name == "raw/api/lastfm/2026-05-19.ndjson"
    mock_client.bucket.return_value.blob.assert_called_once_with(
        "raw/api/lastfm/2026-05-19.ndjson"
    )


def test_stage_to_gcs_writes_valid_ndjson():
    records = [_chart_record("A"), _chart_record("B")]

    mock_client = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value.blob.return_value = mock_blob

    with patch("main.storage.Client", return_value=mock_client):
        stage_to_gcs(records, "test-bucket")

    payload = mock_blob.upload_from_string.call_args[0][0]
    lines = payload.strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["artist_name"] == "A"
    assert json.loads(lines[1])["artist_name"] == "B"
