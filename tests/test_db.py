"""
Testes de schema + inserts do db.py usando SQLite in-memory via tmp_path.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import db


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    p = tmp_path / "test.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    return p


def test_schema_creates_all_tables(isolated_db):
    with db.connect(isolated_db) as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
    assert {"listings", "snapshots", "events", "price_history"} <= tables


def test_schema_has_intelligence_columns(isolated_db):
    with db.connect(isolated_db) as conn:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(listings)"
        ).fetchall()}
    assert {"estimated_market_value", "discount_percentage",
            "opportunity_score", "cluster_id"} <= cols


def test_migration_adds_missing_columns(tmp_path):
    """Simula um banco legado sem as colunas novas e garante que init_db
    aplica a migração sem recriar a tabela."""
    import sqlite3
    legacy_path = tmp_path / "legacy.sqlite3"
    # Schema mínimo sem as colunas de inteligência
    legacy = sqlite3.connect(str(legacy_path))
    legacy.executescript("""
        CREATE TABLE listings (
            id TEXT PRIMARY KEY, url TEXT NOT NULL, source TEXT,
            first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
            last_status TEXT NOT NULL, is_removed INTEGER DEFAULT 0,
            removed_at TEXT, reappeared_at TEXT,
            current_title TEXT, current_price TEXT,
            current_currency TEXT, current_location TEXT
        );
        INSERT INTO listings (id, url, first_seen_at, last_seen_at, last_status)
        VALUES ('legacy1', 'http://x', '2024-01-01', '2024-01-01', 'ok');
    """)
    legacy.commit()
    legacy.close()

    db.init_db(legacy_path)

    with db.connect(legacy_path) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(listings)").fetchall()}
        assert "opportunity_score" in cols
        assert "discount_percentage" in cols
        # Dado legacy preservado
        row = conn.execute("SELECT id FROM listings").fetchone()
        assert row["id"] == "legacy1"


def test_discover_insert_is_idempotent(isolated_db):
    with db.connect(isolated_db) as conn:
        assert db.discover_insert(conn, "abc", "http://x/abc", "test") is True
        assert db.discover_insert(conn, "abc", "http://x/abc", "test") is False
        row = db.listing_by_id(conn, "abc")
        assert row["source"] == "test"
        assert row["last_status"] == "pending"


def test_insert_snapshot_and_fetch(isolated_db):
    with db.connect(isolated_db) as conn:
        db.discover_insert(conn, "lid1", "http://x", "seed")
        db.insert_snapshot(conn, "lid1", "2026-04-10T12:00:00",
                           "ok", "hash123", {"title": "foo", "price": 100})
    with db.connect(isolated_db) as conn:
        snaps = db.snapshots_for(conn, "lid1")
        assert len(snaps) == 1
        assert snaps[0]["status"] == "ok"
        payload = db.latest_snapshot_payload(conn, "lid1")
        assert payload["title"] == "foo"


def test_insert_event_and_query(isolated_db):
    with db.connect(isolated_db) as conn:
        db.discover_insert(conn, "lid2", "http://x", "seed")
        db.insert_event(conn, "lid2", "2026-04-10T12:00:00",
                        "price_change", "100", "90")
    with db.connect(isolated_db) as conn:
        events = db.events_for(conn, "lid2")
        assert len(events) == 1
        assert events[0]["event_type"] == "price_change"
        assert events[0]["new_value"] == "90"


def test_price_history_ordering(isolated_db):
    with db.connect(isolated_db) as conn:
        db.discover_insert(conn, "lid3", "http://x", "seed")
        for i, t in enumerate(["2026-04-10T10:00:00", "2026-04-10T11:00:00",
                                "2026-04-10T12:00:00"]):
            db.insert_price_history(conn, "lid3", 100.0 - i * 5,
                                    f"R$ {100 - i * 5}", "BRL", t)
    with db.connect(isolated_db) as conn:
        history = db.price_history_for(conn, "lid3")
        assert len(history) == 3
        # asc por recorded_at
        assert history[0]["price"] == 100.0
        assert history[-1]["price"] == 90.0


def test_vacuum_returns_metrics(isolated_db):
    result = db.vacuum_database(isolated_db)
    assert "size_before" in result
    assert "size_after" in result
    assert "bytes_reclaimed" in result
