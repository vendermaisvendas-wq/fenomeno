"""Teste de integração simples do deal_simulator contra DB isolado."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import db
from deal_simulator import simulate


@pytest.fixture
def seeded_db(tmp_path, monkeypatch):
    p = tmp_path / "sim.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with db.connect(p) as conn:
        data = [
            # (id, title, price, emv, discount, score, removed)
            ("1", "Hilux SRV 2020", "100000", 130000.0, 23.0, 85, True),
            ("2", "Hilux SR 2019",  "90000",  115000.0, 22.0, 78, True),
            ("3", "Civic 2018",     "55000",  70000.0,  21.0, 72, False),
            ("4", "Corola 2017",    "48000",  55000.0,  13.0, 40, False),  # score baixo
        ]
        for id_, title, price, emv, disc, score, removed in data:
            conn.execute(
                """
                INSERT INTO listings
                  (id, url, first_seen_at, last_seen_at, last_status,
                   current_title, current_price, estimated_market_value,
                   discount_percentage, opportunity_score, is_removed)
                VALUES (?, ?, ?, ?, 'ok', ?, ?, ?, ?, ?, 0)
                """,
                (id_, f"http://x/{id_}", now, now, title, price, emv, disc, score),
            )
            if removed:
                conn.execute(
                    "INSERT INTO events (listing_id, at, event_type) "
                    "VALUES (?, ?, 'removed')",
                    (id_, now),
                )
    return p


def test_simulate_respects_score_filter(seeded_db, monkeypatch):
    import deal_simulator as ds
    original_connect = ds.connect
    monkeypatch.setattr(ds, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    result, picks = simulate(capital=300000, min_score=70)
    # id=4 tem score 40 → excluído
    picked_ids = {p["id"] for p in picks}
    assert "4" not in picked_ids
    assert picked_ids == {"1", "2", "3"}


def test_simulate_respects_capital_limit(seeded_db, monkeypatch):
    import deal_simulator as ds
    original_connect = ds.connect
    monkeypatch.setattr(ds, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    # Capital de 100k — só cabe o Hilux SRV (100k) ou o Civic (55k) + nada mais
    result, picks = simulate(capital=100000, min_score=70)
    total = sum(p["_price_parsed"] for p in picks)
    assert total <= 100000


def test_simulate_computes_hit_rate(seeded_db, monkeypatch):
    import deal_simulator as ds
    original_connect = ds.connect
    monkeypatch.setattr(ds, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    result, picks = simulate(capital=500000, min_score=70)
    # 2 dos 3 foram removed → hit_rate = 2/3 ≈ 0.667
    assert 0.5 < result.hit_rate < 0.8


def test_simulate_keyword_filter(seeded_db, monkeypatch):
    import deal_simulator as ds
    original_connect = ds.connect
    monkeypatch.setattr(ds, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    result, picks = simulate(capital=500000, min_score=0, keyword="hilux")
    picked_titles = {p["current_title"] for p in picks}
    assert all("hilux" in t.lower() for t in picked_titles)
    assert len(picks) == 2
