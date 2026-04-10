"""Testes do outlier_detector usando DB isolado."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import db
from outlier_detector import detect_outliers


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    p = tmp_path / "test.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    return p


def _insert(conn, id_, title, price):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO listings
          (id, url, first_seen_at, last_seen_at, last_status,
           current_title, current_price)
        VALUES (?, ?, ?, ?, 'ok', ?, ?)
        """,
        (id_, f"http://x/{id_}", now, now, title, price),
    )


def test_outlier_detector_flags_extreme_values(isolated_db, monkeypatch):
    # precisa apontar conexões default para o isolated_db
    import market_value
    import outlier_detector as od
    original_connect = od.connect
    monkeypatch.setattr(od, "connect",
                        lambda *a, **kw: original_connect(isolated_db))
    monkeypatch.setattr(market_value, "connect",
                        lambda *a, **kw: original_connect(isolated_db))

    with db.connect(isolated_db) as conn:
        # grupo de 6 Hondas com preço agrupado ~10000 e um outlier absurdo
        _insert(conn, "1", "Honda CG 160 Titan 2020", "10000")
        _insert(conn, "2", "Honda CG 160 Titan 2019", "9500")
        _insert(conn, "3", "Honda CG 160 Titan 2021", "10500")
        _insert(conn, "4", "Honda CG 160 Start 2020", "9000")
        _insert(conn, "5", "Honda CG 160 Fan 2020", "9800")
        _insert(conn, "6", "Honda CG 160 Cargo 2020", "99999999")  # outlier absurdo

    result = detect_outliers(dry_run=False)
    assert result["outliers"] >= 1

    with db.connect(isolated_db) as conn:
        row = conn.execute(
            "SELECT price_outlier FROM listings WHERE id = '6'"
        ).fetchone()
        assert row["price_outlier"] == 1


def test_outlier_detector_skips_small_groups(isolated_db, monkeypatch):
    import market_value
    import outlier_detector as od
    original_connect = od.connect
    monkeypatch.setattr(od, "connect",
                        lambda *a, **kw: original_connect(isolated_db))
    monkeypatch.setattr(market_value, "connect",
                        lambda *a, **kw: original_connect(isolated_db))

    with db.connect(isolated_db) as conn:
        # só 3 itens → menos que MIN_GROUP_FOR_IQR (5); não deve flagar nada
        _insert(conn, "1", "Xiaomi Mi 11 5G", "1500")
        _insert(conn, "2", "Xiaomi Note 10", "1200")
        _insert(conn, "3", "Xiaomi Redmi 9", "800")

    result = detect_outliers(dry_run=False)
    assert result["outliers"] == 0
