"""Testes do discovery_cache contra DB isolado."""

from __future__ import annotations

import pytest

import db
import discovery_cache as dc


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    p = tmp_path / "dc.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    original_connect = db.connect
    monkeypatch.setattr(dc, "connect",
                        lambda *a, **kw: original_connect(p))
    return p


def test_get_returns_none_when_empty(isolated_db):
    assert dc.get("iphone", "Araçatuba") is None


def test_put_then_get_roundtrip(isolated_db):
    hits = [{"url": "http://x/1", "item_id": "1", "title": "iPhone 13"}]
    dc.put("iphone", "Araçatuba", hits, ttl_seconds=300)
    cached = dc.get("iphone", "Araçatuba")
    assert cached is not None
    assert len(cached) == 1
    assert cached[0]["item_id"] == "1"


def test_key_is_normalized():
    # case + whitespace devem ser normalizados
    assert dc._key("iPhone", "SP") == dc._key("iphone", "sp")
    assert dc._key(" iphone ", " SP ") == dc._key("iphone", "sp")


def test_key_distinguishes_different_queries():
    assert dc._key("iphone", "SP") != dc._key("iphone", "RJ")
    assert dc._key("iphone", "SP") != dc._key("ipad", "SP")


def test_put_overwrites_same_key(isolated_db):
    dc.put("iphone", "SP", [{"item_id": "1"}])
    dc.put("iphone", "SP", [{"item_id": "2"}, {"item_id": "3"}])
    cached = dc.get("iphone", "SP")
    assert len(cached) == 2


def test_expired_entry_returns_none(isolated_db):
    # TTL = -100s significa expirado imediatamente
    dc.put("iphone", "SP", [{"item_id": "1"}], ttl_seconds=-100)
    assert dc.get("iphone", "SP") is None


def test_cleanup_removes_only_expired(isolated_db):
    dc.put("iphone", "SP", [{"item_id": "1"}], ttl_seconds=-1)
    dc.put("ipad", "SP", [{"item_id": "2"}], ttl_seconds=300)
    removed = dc.cleanup_expired()
    assert removed == 1
    assert dc.get("iphone", "SP") is None
    assert dc.get("ipad", "SP") is not None


def test_invalidate_specific(isolated_db):
    dc.put("iphone", "SP", [{"item_id": "1"}])
    dc.put("ipad", "SP", [{"item_id": "2"}])
    dc.invalidate("iphone", "SP")
    assert dc.get("iphone", "SP") is None
    assert dc.get("ipad", "SP") is not None


def test_invalidate_all(isolated_db):
    dc.put("iphone", "SP", [{"item_id": "1"}])
    dc.put("ipad", "SP", [{"item_id": "2"}])
    n = dc.invalidate()
    assert n == 2
    assert dc.get("iphone", "SP") is None
    assert dc.get("ipad", "SP") is None


def test_info_counts(isolated_db):
    dc.put("a", None, [{"item_id": "1"}], ttl_seconds=300)
    dc.put("b", None, [{"item_id": "2"}], ttl_seconds=-1)
    info = dc.info()
    assert info["total"] == 2
    assert info["live"] == 1
    assert info["expired"] == 1


def test_corrupted_json_returns_none(isolated_db):
    # Insere row com JSON inválido manualmente
    from db import now_iso
    from datetime import datetime, timedelta, timezone
    expires = (datetime.now(timezone.utc) + timedelta(seconds=300)).isoformat(timespec="seconds")
    with db.connect(isolated_db) as conn:
        conn.execute(
            "INSERT INTO discovery_cache "
            "(query_hash, query_text, region, result_json, expires_at, created_at) "
            "VALUES (?, 'x', '', '{not valid json', ?, ?)",
            (dc._key("x", ""), expires, now_iso()),
        )
    assert dc.get("x", "") is None
