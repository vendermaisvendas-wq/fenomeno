"""Teste de smoke do weekly_report: gera HTML a partir de DB seedado."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import db
import weekly_report as wr


@pytest.fixture
def seeded_db(tmp_path, monkeypatch):
    p = tmp_path / "wr.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(days=2)).isoformat(timespec="seconds")
    old = (now - timedelta(days=60)).isoformat(timespec="seconds")

    with db.connect(p) as conn:
        # 2 novos na janela, 1 antigo
        for id_, fs, title, price, score, disc in [
            ("new1", recent, "Hilux SRV 2020", "150000", 85, 25.0),
            ("new2", recent, "iPhone 13", "3500", 70, 20.0),
            ("old1", old, "Civic antigo", "40000", 50, 10.0),
        ]:
            conn.execute(
                """
                INSERT INTO listings
                  (id, url, first_seen_at, last_seen_at, last_status,
                   current_title, current_price, opportunity_score,
                   discount_percentage, is_removed)
                VALUES (?, ?, ?, ?, 'ok', ?, ?, ?, ?, 0)
                """,
                (id_, f"http://x/{id_}", fs, fs, title, price, score, disc),
            )

        # Evento de price_change recente
        conn.execute(
            "INSERT INTO events (listing_id, at, event_type, old_value, new_value) "
            "VALUES ('new1', ?, 'price_change', '200000', '150000')",
            (recent,),
        )
    return p


def test_collect_finds_recent_listings(seeded_db, monkeypatch):
    original_connect = wr.connect
    monkeypatch.setattr(wr, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    sections = wr.collect(days=7)
    assert sections.new_in_window == 2
    assert sections.price_changes == 1
    assert len(sections.top_deals) >= 1


def test_biggest_drops_computed_from_events(seeded_db, monkeypatch):
    original_connect = wr.connect
    monkeypatch.setattr(wr, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    sections = wr.collect(days=7)
    assert len(sections.biggest_drops) == 1
    drop = sections.biggest_drops[0]
    assert drop["old_price"] == 200000
    assert drop["new_price"] == 150000
    assert drop["drop_pct"] == 25.0


def test_render_produces_valid_html(seeded_db, monkeypatch):
    original_connect = wr.connect
    monkeypatch.setattr(wr, "connect",
                        lambda *a, **kw: original_connect(seeded_db))

    sections = wr.collect(days=7)
    html = wr.render(sections)
    assert "<html" in html
    assert "Weekly report" in html
    assert "Hilux" in html  # deve aparecer na seção top deals
    assert "25.0" in html   # drop %


def test_render_empty_db_still_produces_html(tmp_path, monkeypatch):
    p = tmp_path / "empty.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    original_connect = wr.connect
    monkeypatch.setattr(wr, "connect", lambda *a, **kw: original_connect(p))

    sections = wr.collect(days=7)
    html = wr.render(sections)
    assert "<html" in html
    assert "(nenhum)" in html  # _table com lista vazia
