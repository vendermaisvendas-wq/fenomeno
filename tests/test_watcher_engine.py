"""
Testes do watcher_engine.

- matches_watcher: pura, testa filtros de keyword/region/price isoladamente
- create_watch + SQLite: roundtrip de schema
- run_due_watchers: cenário com DB isolado, mock de monitor_watch
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import db
from extract_item import Listing
from watcher_engine import (
    _norm, _parse_iso, create_watch, matches_watcher, run_due_watchers,
)


def _listing(**overrides):
    base = Listing(
        id="123", url="http://x/123",
        fetched_at="2026-04-10T12:00:00+00:00",
        status="ok",
        title="iPhone 13 128GB preto seminovo",
        price_amount="3500",
        price_currency="BRL",
        price_formatted="R$ 3.500",
        location_text="Araçatuba, SP",
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def _w(keyword="iphone", region=None, min_price=None, max_price=None):
    return {
        "keyword": keyword, "region": region,
        "min_price": min_price, "max_price": max_price,
    }


# --- matches_watcher puras -------------------------------------------------

def test_match_keyword_only():
    ok, reason = matches_watcher(_listing(), _w(keyword="iphone"))
    assert ok is True
    assert reason == "ok"


def test_match_keyword_case_insensitive():
    ok, _ = matches_watcher(
        _listing(title="Vendo IPHONE 13"), _w(keyword="iphone"),
    )
    assert ok is True


def test_match_keyword_accent_insensitive():
    # "maçã" deve bater "maca"
    ok, _ = matches_watcher(
        _listing(title="iPad promoção maçã"), _w(keyword="maca"),
    )
    assert ok is True


def test_no_match_when_keyword_absent():
    ok, reason = matches_watcher(
        _listing(title="Samsung Galaxy"), _w(keyword="iphone"),
    )
    assert ok is False
    assert reason == "keyword_mismatch"


def test_match_region_exact():
    ok, _ = matches_watcher(
        _listing(location_text="Araçatuba, SP"),
        _w(keyword="iphone", region="Araçatuba"),
    )
    assert ok is True


def test_match_region_case_accent_insensitive():
    ok, _ = matches_watcher(
        _listing(location_text="São Paulo, SP"),
        _w(keyword="iphone", region="sao paulo"),
    )
    assert ok is True


def test_no_match_wrong_region():
    ok, reason = matches_watcher(
        _listing(location_text="Rio de Janeiro, RJ"),
        _w(keyword="iphone", region="Araçatuba"),
    )
    assert ok is False
    assert reason == "region_mismatch"


def test_match_within_price_range():
    ok, _ = matches_watcher(
        _listing(price_amount="3500"),
        _w(keyword="iphone", min_price=3000, max_price=4000),
    )
    assert ok is True


def test_no_match_below_min_price():
    ok, reason = matches_watcher(
        _listing(price_amount="2000"),
        _w(keyword="iphone", min_price=3000),
    )
    assert ok is False
    assert reason == "below_min"


def test_no_match_above_max_price():
    ok, reason = matches_watcher(
        _listing(price_amount="5000"),
        _w(keyword="iphone", max_price=4000),
    )
    assert ok is False
    assert reason == "above_max"


def test_price_filter_requires_price_on_listing():
    ok, reason = matches_watcher(
        _listing(price_amount=None, price_formatted=None),
        _w(keyword="iphone", min_price=1000),
    )
    assert ok is False
    assert reason == "no_price_for_filter"


def test_no_price_filter_when_no_bounds():
    # sem min/max, price ausente não bloqueia
    ok, _ = matches_watcher(
        _listing(price_amount=None, price_formatted=None),
        _w(keyword="iphone"),
    )
    assert ok is True


def test_empty_keyword_rejects():
    ok, reason = matches_watcher(_listing(), _w(keyword=""))
    assert ok is False
    assert reason == "watcher_keyword_empty"


def test_norm_handles_none():
    assert _norm(None) == ""
    assert _norm("") == ""


def test_norm_strips_accents_and_lowercases():
    assert _norm("São Paulo") == "sao paulo"
    assert _norm("AÇAÍ") == "acai"


# --- DB + create_watch -----------------------------------------------------

@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    p = tmp_path / "watch.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    import watcher_engine as we
    original_connect = db.connect
    monkeypatch.setattr(we, "connect",
                        lambda *a, **kw: original_connect(p))
    return p


def test_create_watch_persists(isolated_db):
    wid = create_watch(keyword="iphone", region="Araçatuba", min_price=2000)
    assert wid is not None
    with db.connect(isolated_db) as conn:
        row = conn.execute(
            "SELECT * FROM watchers WHERE watch_id = ?", (wid,)
        ).fetchone()
    assert row["keyword"] == "iphone"
    assert row["region"] == "Araçatuba"
    assert row["min_price"] == 2000
    assert row["is_active"] == 1


def test_create_watch_empty_keyword_raises(isolated_db):
    with pytest.raises(ValueError):
        create_watch(keyword="")
    with pytest.raises(ValueError):
        create_watch(keyword="   ")


def test_schema_has_watcher_tables(isolated_db):
    with db.connect(isolated_db) as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
    assert "watchers" in tables
    assert "watcher_results" in tables


def test_watcher_results_unique_constraint(isolated_db):
    wid = create_watch(keyword="iphone")
    from db import now_iso
    with db.connect(isolated_db) as conn:
        conn.execute(
            "INSERT INTO watcher_results (watch_id, listing_id, first_seen) "
            "VALUES (?, ?, ?)",
            (wid, "abc", now_iso()),
        )
        # Segundo INSERT OR IGNORE não deve dar erro nem duplicar
        conn.execute(
            "INSERT OR IGNORE INTO watcher_results (watch_id, listing_id, first_seen) "
            "VALUES (?, ?, ?)",
            (wid, "abc", now_iso()),
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM watcher_results WHERE watch_id = ?", (wid,),
        ).fetchone()[0]
    assert count == 1


# --- run_due_watchers -----------------------------------------------------

def test_run_due_watchers_respects_interval(isolated_db, monkeypatch):
    wid = create_watch(keyword="iphone")

    # Marca como "rodou agora" → não está devido
    from db import now_iso
    with db.connect(isolated_db) as conn:
        conn.execute(
            "UPDATE watchers SET last_run_at = ? WHERE watch_id = ?",
            (now_iso(), wid),
        )

    call_count = []
    def _fake_monitor(watch_id):
        call_count.append(watch_id)
        return {"new_matches": 0}

    import watcher_engine as we
    monkeypatch.setattr(we, "monitor_watch", _fake_monitor)

    result = we.run_due_watchers(min_interval_seconds=3600)
    assert result["due"] == 0
    assert result["ran"] == 0
    assert call_count == []


def test_run_due_watchers_runs_stale(isolated_db, monkeypatch):
    wid = create_watch(keyword="iphone")

    # last_run_at antigo → devido
    past = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat(timespec="seconds")
    with db.connect(isolated_db) as conn:
        conn.execute(
            "UPDATE watchers SET last_run_at = ? WHERE watch_id = ?",
            (past, wid),
        )

    called = []
    def _fake_monitor(watch_id):
        called.append(watch_id)
        return {"new_matches": 3}

    import watcher_engine as we
    monkeypatch.setattr(we, "monitor_watch", _fake_monitor)

    result = we.run_due_watchers(min_interval_seconds=3600)
    assert result["due"] == 1
    assert result["ran"] == 1
    assert result["total_new_matches"] == 3
    assert called == [wid]


def test_run_due_watchers_ignores_inactive(isolated_db, monkeypatch):
    wid = create_watch(keyword="iphone")
    with db.connect(isolated_db) as conn:
        conn.execute(
            "UPDATE watchers SET is_active = 0 WHERE watch_id = ?", (wid,),
        )

    import watcher_engine as we
    monkeypatch.setattr(we, "monitor_watch",
                        lambda x: pytest.fail("should not be called"))
    result = we.run_due_watchers(min_interval_seconds=3600)
    assert result["total_active"] == 0
    assert result["ran"] == 0


def test_run_due_watchers_survives_exception(isolated_db, monkeypatch):
    wid = create_watch(keyword="iphone")
    past = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat(timespec="seconds")
    with db.connect(isolated_db) as conn:
        conn.execute(
            "UPDATE watchers SET last_run_at = ? WHERE watch_id = ?", (past, wid),
        )

    def _boom(watch_id):
        raise RuntimeError("discovery exploded")

    import watcher_engine as we
    monkeypatch.setattr(we, "monitor_watch", _boom)

    result = we.run_due_watchers(min_interval_seconds=3600)
    assert result["failures"] == 1
    assert result["ran"] == 0


def test_parse_iso_handles_naive():
    dt = _parse_iso("2026-04-10T12:00:00")
    assert dt is not None
    assert dt.tzinfo is not None  # convertido para UTC
