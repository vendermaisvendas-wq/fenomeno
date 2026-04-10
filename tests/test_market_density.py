"""Testes de _compute_competition_score e compute() contra DB isolado."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import db
import market_density as md
from market_density import _compute_competition_score


def test_competition_zero_with_no_activity():
    assert _compute_competition_score(active=0, removal_rate=0) == 0


def test_competition_saturates_at_100():
    score = _compute_competition_score(active=10000, removal_rate=1.0)
    assert score == 100


def test_competition_volume_only():
    # 200 ativos, 0% removal → ~60 (só volume)
    score = _compute_competition_score(active=200, removal_rate=0.0)
    assert 55 <= score <= 65


def test_competition_turnover_only():
    # 1 ativo, 100% removal → ~7 volume (log1p(1) não é zero) + 40 turnover
    score = _compute_competition_score(active=1, removal_rate=1.0)
    assert 45 <= score <= 50


@pytest.fixture
def seeded_db(tmp_path, monkeypatch):
    p = tmp_path / "md.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    now = datetime.now(timezone.utc)
    recent = now.isoformat()

    with db.connect(p) as conn:
        # 6 listings com token "hilux": 3 ativos, 3 removidos
        for i in range(3):
            conn.execute(
                """
                INSERT INTO listings
                  (id, url, first_seen_at, last_seen_at, last_status,
                   current_title, is_removed)
                VALUES (?, ?, ?, ?, 'ok', 'Toyota Hilux SRV', 0)
                """,
                (f"a{i}", f"http://x/{i}", recent, recent),
            )
        for i in range(3):
            fs = (now - timedelta(days=10)).isoformat()
            rm = (now - timedelta(days=3)).isoformat()
            conn.execute(
                """
                INSERT INTO listings
                  (id, url, first_seen_at, last_seen_at, last_status,
                   current_title, is_removed, removed_at)
                VALUES (?, ?, ?, ?, 'not_found', 'Toyota Hilux SR', 1, ?)
                """,
                (f"r{i}", f"http://x/r{i}", fs, rm, rm),
            )
    return p


def test_compute_aggregates_by_token(seeded_db, monkeypatch):
    original_connect = md.connect
    monkeypatch.setattr(md, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    rows = md.compute(min_count=5)
    by_token = {r["token"]: r for r in rows}
    assert "hilux" in by_token
    hilux = by_token["hilux"]
    assert hilux["total_listings"] == 6
    assert hilux["active_listings"] == 3
    assert hilux["removed_listings"] == 3
    assert 0.4 < hilux["removal_rate"] < 0.6
    assert hilux["avg_velocity_days"] == 7.0  # exato: 10d − 3d = 7d
    assert hilux["competition_score"] > 0


def test_compute_respects_min_count(seeded_db, monkeypatch):
    original_connect = md.connect
    monkeypatch.setattr(md, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    rows = md.compute(min_count=100)
    assert rows == []  # nenhum token tem 100 listings


def test_persist_roundtrip(seeded_db, monkeypatch):
    original_connect = md.connect
    monkeypatch.setattr(md, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    rows = md.compute(min_count=5)
    n = md.persist(rows)
    assert n > 0

    with db.connect(seeded_db) as conn:
        stored = conn.execute(
            "SELECT * FROM market_density WHERE token = 'hilux'"
        ).fetchone()
        assert stored is not None
        assert stored["total_listings"] == 6
