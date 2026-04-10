"""Testes do price_model no backend fallback (sem sklearn)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import db
from price_model import train_and_predict


@pytest.fixture
def seeded_db(tmp_path, monkeypatch):
    p = tmp_path / "pm.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # Preciso de >= MIN_TRAINING_SAMPLES (30)
    with db.connect(p) as conn:
        for i in range(40):
            price = 100000 + (i * 500)
            conn.execute(
                """
                INSERT INTO listings
                  (id, url, first_seen_at, last_seen_at, last_status,
                   current_title, current_price, is_removed)
                VALUES (?, ?, ?, ?, 'ok', ?, ?, 0)
                """,
                (
                    f"id{i}", f"http://x/{i}", now, now,
                    f"Toyota Hilux SRV 2020 unidade {i}",
                    str(price),
                ),
            )
    return p


def test_insufficient_data_returns_clean_status(tmp_path, monkeypatch):
    p = tmp_path / "empty.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)

    import price_model as pm
    original_connect = pm.connect
    monkeypatch.setattr(pm, "connect",
                        lambda *a, **kw: original_connect(p))

    result = train_and_predict(backend="fallback", dry_run=True)
    assert result["status"] == "insufficient_data"


def test_fallback_backend_produces_predictions(seeded_db, monkeypatch):
    import market_value
    import price_model as pm
    original_connect = db.connect
    for mod in (pm, market_value):
        monkeypatch.setattr(
            mod, "connect",
            lambda *a, _p=seeded_db, **kw: original_connect(_p),
        )

    result = train_and_predict(backend="fallback", dry_run=False)
    assert result["status"] == "ok"
    assert result["backend"] == "fallback"
    assert result["updated"] > 0

    # Verifica que predicted_price foi gravado em pelo menos alguns listings
    with db.connect(seeded_db) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE predicted_price IS NOT NULL"
        ).fetchone()
        assert row[0] > 0

        # price_gap também
        row = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE price_gap IS NOT NULL"
        ).fetchone()
        assert row[0] > 0


def test_dry_run_does_not_persist(seeded_db, monkeypatch):
    import market_value
    import price_model as pm
    original_connect = db.connect
    for mod in (pm, market_value):
        monkeypatch.setattr(
            mod, "connect",
            lambda *a, _p=seeded_db, **kw: original_connect(_p),
        )

    result = train_and_predict(backend="fallback", dry_run=True)
    assert result["status"] == "ok"
    with db.connect(seeded_db) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM listings WHERE predicted_price IS NOT NULL"
        ).fetchone()
        assert row[0] == 0
