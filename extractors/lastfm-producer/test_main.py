import re
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from main import ArtistChart, PAGE_LIMIT, _chart_week, _publish_page, fetch_charts


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_artist(name: str, rank: int, mbid: str = "abc-123") -> dict:
    return {
        "name": name,
        "mbid": mbid,
        "listeners": str(rank * 1000),
        "playcount": str(rank * 5000),
        "url": f"https://last.fm/music/{name}",
        "streamable": "0",
        "image": [],
    }


def _make_response(artists: list[dict], page: int = 1, total_pages: int = 1) -> dict:
    return {
        "artists": {
            "artist": artists,
            "@attr": {
                "page": str(page),
                "perPage": str(PAGE_LIMIT),
                "totalPages": str(total_pages),
                "total": str(len(artists) * total_pages),
            },
        }
    }


# ── _chart_week ───────────────────────────────────────────────────────────────

def test_chart_week_is_monday():
    date_str = _chart_week()
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    assert dt.weekday() == 0  # Monday


def test_chart_week_format():
    assert re.match(r"^\d{4}-\d{2}-\d{2}$", _chart_week())


# ── fetch_charts — yields pages ───────────────────────────────────────────────

def test_fetch_charts_yields_page_as_list():
    artists = [_make_artist(f"Artist {i}", i) for i in range(1, 4)]
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_response(artists)

    with patch("main.requests.get", return_value=mock_resp):
        pages = list(fetch_charts("test-key"))

    assert len(pages) == 1
    assert isinstance(pages[0], list)
    assert len(pages[0]) == 3
    assert all(isinstance(r, ArtistChart) for r in pages[0])


def test_fetch_charts_assigns_sequential_ranks():
    artists = [_make_artist(f"Artist {i}", i) for i in range(1, 4)]
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_response(artists)

    with patch("main.requests.get", return_value=mock_resp):
        pages = list(fetch_charts("test-key"))

    assert [r.rank for r in pages[0]] == [1, 2, 3]


def test_fetch_charts_paginates_into_multiple_pages():
    page1 = [_make_artist(f"Artist {i}", i) for i in range(1, 4)]
    page2 = [_make_artist(f"Artist {i}", i) for i in range(4, 7)]

    responses = [
        MagicMock(json=MagicMock(return_value=_make_response(page1, page=1, total_pages=2))),
        MagicMock(json=MagicMock(return_value=_make_response(page2, page=2, total_pages=2))),
    ]

    with patch("main.requests.get", side_effect=responses), \
         patch("main.time.sleep"):
        pages = list(fetch_charts("test-key"))

    assert len(pages) == 2
    assert len(pages[0]) == 3
    assert len(pages[1]) == 3
    assert pages[1][0].rank == PAGE_LIMIT + 1


def test_fetch_charts_normalises_empty_mbid_to_none():
    artists = [_make_artist("No MBID Artist", 1, mbid="")]
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_response(artists)

    with patch("main.requests.get", return_value=mock_resp):
        pages = list(fetch_charts("test-key"))

    assert pages[0][0].artist_mbid is None


def test_fetch_charts_stamps_chart_week():
    artists = [_make_artist("Artist", 1)]
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_response(artists)

    with patch("main.requests.get", return_value=mock_resp), \
         patch("main._chart_week", return_value="2026-05-19"):
        pages = list(fetch_charts("test-key"))

    assert pages[0][0].chart_week == "2026-05-19"


# ── _publish_page ─────────────────────────────────────────────────────────────

def _make_records(n: int = 3) -> list[ArtistChart]:
    return [
        ArtistChart(
            artist_mbid="abc",
            artist_name=f"Artist {i}",
            chart_week="2026-05-19",
            rank=i,
            listeners=i * 1000,
            playcount=i * 5000,
        )
        for i in range(1, n + 1)
    ]


def test_publish_page_produces_all_records():
    mock_producer = MagicMock()

    _publish_page(mock_producer, _make_records(3), "test-topic")

    assert mock_producer.produce.call_count == 3
    mock_producer.flush.assert_called_once()


def test_publish_page_serialises_to_json():
    mock_producer = MagicMock()
    records = _make_records(1)

    _publish_page(mock_producer, records, "test-topic")

    _, kwargs = mock_producer.produce.call_args
    payload = kwargs["value"]
    assert b"artist_name" in payload
    assert b"Artist 1" in payload


def test_publish_page_raises_on_delivery_failure():
    mock_producer = MagicMock()

    def fake_produce(topic, value, on_delivery):
        err = MagicMock()
        err.__str__ = lambda _: "broker timeout"
        on_delivery(err, None)

    mock_producer.produce.side_effect = fake_produce

    with pytest.raises(RuntimeError, match="messages failed delivery"):
        _publish_page(mock_producer, _make_records(2), "test-topic")
