"""Testes de formatação e dedup do alert_engine."""

from __future__ import annotations

import pytest

import alert_engine
import db


def test_format_watcher_alert_full():
    watcher = {
        "watch_id": 7, "keyword": "iphone", "region": "Araçatuba",
    }
    listing = {
        "current_title": "iPhone 13 128GB preto seminovo",
        "current_price": "3500", "current_currency": "BRL",
        "current_location": "Araçatuba, SP",
        "url": "https://facebook.com/marketplace/item/123/",
    }
    msg = alert_engine.format_watcher_alert(listing, watcher)
    assert "watcher #7" in msg
    assert "iphone" in msg
    assert "Araçatuba" in msg
    assert "iPhone 13 128GB" in msg
    assert "3500" in msg
    assert "BRL" in msg
    assert "marketplace/item/123" in msg


def test_format_watcher_alert_missing_region():
    watcher = {"watch_id": 3, "keyword": "bicicleta", "region": None}
    listing = {
        "current_title": "Bicicleta aro 29",
        "current_price": None, "current_currency": None,
        "current_location": None,
        "url": "http://x",
    }
    msg = alert_engine.format_watcher_alert(listing, watcher)
    assert "qualquer região" in msg
    assert "Bicicleta aro 29" in msg


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    p = tmp_path / "alert.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    original_connect = db.connect
    monkeypatch.setattr(alert_engine, "connect",
                        lambda *a, **kw: original_connect(p))
    return p


def _seed_watcher_and_match(db_path, watch_id: int = 1):
    from db import now_iso
    now = now_iso()
    with db.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO watchers
              (watch_id, keyword, region, is_active, created_at)
            VALUES (?, 'iphone', 'Araçatuba', 1, ?)
            """,
            (watch_id, now),
        )
        conn.execute(
            """
            INSERT INTO listings
              (id, url, first_seen_at, last_seen_at, last_status,
               current_title, current_price, current_currency)
            VALUES ('abc', 'http://fb/abc', ?, ?, 'ok',
                    'iPhone 13', '3500', 'BRL')
            """,
            (now, now),
        )
        conn.execute(
            """
            INSERT INTO events (listing_id, at, event_type, new_value)
            VALUES ('abc', ?, 'watcher_match', ?)
            """,
            (now, f"watch_id={watch_id}"),
        )


def test_process_pending_skips_missing_env(isolated_db, monkeypatch):
    _seed_watcher_and_match(isolated_db)

    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)

    stats = alert_engine.process_pending_watcher_matches()
    assert stats["matches_scanned"] >= 1
    # Sem env vars → canais retornam None → unconfigured
    assert stats["unconfigured"] >= 2  # telegram + discord


def test_process_pending_dedup_via_alert_sent(isolated_db, monkeypatch):
    _seed_watcher_and_match(isolated_db)
    # Pré-marca como já alertado
    from db import now_iso
    with db.connect(isolated_db) as conn:
        conn.execute(
            """
            INSERT INTO events (listing_id, at, event_type, old_value, new_value)
            VALUES ('abc', ?, 'alert_sent', 'watcher_telegram_1', 'ok')
            """,
            (now_iso(),),
        )

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "fake")

    sent_calls = []
    def _fake_tg(msg):
        sent_calls.append(msg)
        return True
    monkeypatch.setattr(alert_engine, "send_telegram", _fake_tg)
    monkeypatch.setattr(alert_engine, "send_discord", lambda msg: None)

    stats = alert_engine.process_pending_watcher_matches()
    # dedup deve ter pulado telegram sem chamar send_telegram
    assert sent_calls == []
    assert stats["dedup_skipped"] >= 1


def test_process_pending_dry_run(isolated_db, monkeypatch):
    _seed_watcher_and_match(isolated_db)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "x")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "y")

    calls = []
    monkeypatch.setattr(alert_engine, "send_telegram",
                        lambda msg: calls.append(msg) or True)
    monkeypatch.setattr(alert_engine, "send_discord", lambda msg: None)

    alert_engine.process_pending_watcher_matches(dry_run=True)
    assert calls == []  # dry run não envia
