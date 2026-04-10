"""Testes de export_data: filtros + writers csv/json."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone

import pytest

import db
from export_data import _build_query, write_csv, write_json


@pytest.fixture
def seeded_db(tmp_path, monkeypatch):
    p = tmp_path / "test.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with db.connect(p) as conn:
        for id_, title, price, score, city, outlier in [
            ("1", "Hilux SRV", "180000", 85, "São Paulo, SP", 0),
            ("2", "Hilux SR",  "160000", 50, "Campinas, SP",  0),
            ("3", "Civic",     "85000",  30, "Rio, RJ",       0),
            ("4", "Corola",    "90000",  60, "Belo Horizonte", 1),  # outlier
        ]:
            conn.execute(
                """
                INSERT INTO listings
                  (id, url, first_seen_at, last_seen_at, last_status,
                   current_title, current_price, current_location,
                   opportunity_score, price_outlier)
                VALUES (?, ?, ?, ?, 'ok', ?, ?, ?, ?, ?)
                """,
                (id_, f"http://x/{id_}", now, now, title, price, city, score, outlier),
            )
    return p


def test_build_query_keyword_filter():
    sql, params = _build_query(
        keyword="hilux", city=None, min_score=None, min_discount=None,
        exclude_outliers=False, limit=None,
    )
    assert "LOWER(COALESCE(current_title" in sql
    assert params == ["%hilux%"]


def test_build_query_multiple_filters():
    sql, params = _build_query(
        keyword="hilux", city="sp", min_score=60, min_discount=None,
        exclude_outliers=True, limit=10,
    )
    assert "%hilux%" in params
    assert "%sp%" in params
    assert 60 in params
    assert "price_outlier" in sql
    assert "LIMIT 10" in sql


def test_write_csv_creates_file(seeded_db, tmp_path, monkeypatch):
    import export_data
    original_connect = export_data.connect
    monkeypatch.setattr(export_data, "connect",
                        lambda *a, **kw: original_connect(seeded_db))
    rows = export_data._fetch(
        keyword=None, city=None, min_score=None, min_discount=None,
        exclude_outliers=False, limit=None,
    )
    out = tmp_path / "dump.csv"
    write_csv(rows, out)
    assert out.exists()
    with out.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        records = list(reader)
    assert len(records) == 4
    assert "current_title" in records[0]


def test_write_json_creates_file(seeded_db, tmp_path, monkeypatch):
    import export_data
    original_connect = export_data.connect
    monkeypatch.setattr(export_data, "connect",
                        lambda *a, **kw: original_connect(seeded_db))
    rows = export_data._fetch(
        keyword="hilux", city=None, min_score=None, min_discount=None,
        exclude_outliers=False, limit=None,
    )
    out = tmp_path / "dump.json"
    write_json(rows, out)
    data = json.loads(out.read_text(encoding="utf-8"))
    assert len(data) == 2  # apenas hilux
    assert all("hilux" in r["current_title"].lower() for r in data)


def test_exclude_outliers_filter(seeded_db, monkeypatch):
    import export_data
    original_connect = export_data.connect
    monkeypatch.setattr(export_data, "connect",
                        lambda *a, **kw: original_connect(seeded_db))
    rows = export_data._fetch(
        keyword=None, city=None, min_score=None, min_discount=None,
        exclude_outliers=True, limit=None,
    )
    # Corola foi marcada como outlier → deve sumir
    titles = {r["current_title"] for r in rows}
    assert "Corola" not in titles
    assert len(rows) == 3
