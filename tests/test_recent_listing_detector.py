"""Testes do recent_listing_detector."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import db
import recent_listing_detector as rld


def _row(first_seen):
    return {"id": "x", "first_seen_at": first_seen}


def test_is_very_recent_inside_window():
    now = datetime.now(timezone.utc)
    fs = (now - timedelta(minutes=10)).isoformat()
    assert rld.is_very_recent(_row(fs), ph_count=0, ev_count=1, now=now,
                              window_min=60) is True


def test_is_very_recent_outside_window():
    now = datetime.now(timezone.utc)
    fs = (now - timedelta(hours=3)).isoformat()
    assert rld.is_very_recent(_row(fs), ph_count=0, ev_count=1, now=now,
                              window_min=60) is False


def test_is_very_recent_too_many_price_points():
    now = datetime.now(timezone.utc)
    fs = (now - timedelta(minutes=5)).isoformat()
    # 2 pontos de price_history → não é "muito recente"
    assert rld.is_very_recent(_row(fs), ph_count=2, ev_count=1, now=now,
                              window_min=60) is False


def test_is_very_recent_too_many_events():
    now = datetime.now(timezone.utc)
    fs = (now - timedelta(minutes=5)).isoformat()
    assert rld.is_very_recent(_row(fs), ph_count=0, ev_count=10, now=now,
                              window_min=60) is False


def test_is_very_recent_naive_timestamp():
    now = datetime.now(timezone.utc)
    naive = (now - timedelta(minutes=10)).replace(tzinfo=None).isoformat()
    assert rld.is_very_recent(_row(naive), ph_count=0, ev_count=1, now=now,
                              window_min=60) is True


def test_is_very_recent_invalid_timestamp():
    now = datetime.now(timezone.utc)
    assert rld.is_very_recent(_row("not-a-date"), ph_count=0, ev_count=0,
                              now=now, window_min=60) is False
    assert rld.is_very_recent(_row(None), ph_count=0, ev_count=0,
                              now=now, window_min=60) is False


@pytest.fixture
def seeded_db(tmp_path, monkeypatch):
    p = tmp_path / "rld.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    original_connect = db.connect
    monkeypatch.setattr(rld, "connect",
                        lambda *a, **kw: original_connect(p))

    now = datetime.now(timezone.utc)
    recent = (now - timedelta(minutes=10)).isoformat(timespec="seconds")
    old = (now - timedelta(hours=5)).isoformat(timespec="seconds")
    with db.connect(p) as conn:
        for id_, fs in [("recent1", recent), ("recent2", recent),
                        ("old1", old)]:
            conn.execute(
                """
                INSERT INTO listings
                  (id, url, first_seen_at, last_seen_at, last_status,
                   current_title, is_removed)
                VALUES (?, ?, ?, ?, 'ok', 'iPhone 13', 0)
                """,
                (id_, f"http://x/{id_}", fs, fs),
            )
    return p


def test_detect_flags_only_in_window(seeded_db):
    result = rld.detect()
    assert result["flagged"] == 2
    assert result["candidates_in_window"] == 2

    with db.connect(seeded_db) as conn:
        recent_flagged = conn.execute(
            "SELECT id FROM listings WHERE very_recent_listing = 1"
        ).fetchall()
    ids = {r["id"] for r in recent_flagged}
    assert ids == {"recent1", "recent2"}


def test_detect_dry_run_does_not_persist(seeded_db):
    result = rld.detect(dry_run=True)
    assert result["flagged"] == 2
    with db.connect(seeded_db) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE very_recent_listing = 1"
        ).fetchone()[0]
    assert n == 0
