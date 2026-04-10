"""Testes do parser_regression_detector contra DB isolado."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import db
import parser_regression_detector as prd


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    p = tmp_path / "prd.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    return p


def _insert_history(conn, at: str, ok: float, og: float, relay: float):
    conn.execute(
        """
        INSERT INTO parser_health_history
          (at, sample_size, ok_rate, jsonld_rate, og_rate, relay_rate, dom_rate, verdict)
        VALUES (?, 10, ?, 0, ?, ?, 0, 'healthy')
        """,
        (at, ok, og, relay),
    )


def _fake_report(ok_n: int, og_pct: float = 100.0,
                 relay_pct: float = 100.0, jsonld_pct: float = 0.0):
    return {
        "sample_size": 10,
        "statuses": {"ok": ok_n, "login_wall": 10 - ok_n},
        "layer_coverage_pct": {
            "jsonld": jsonld_pct, "og": og_pct, "relay": relay_pct,
            "json_walk": 50.0, "dom": 0.0,
        },
        "field_coverage": {},
        "non_ok_items": [],
        "verdict": "healthy",
    }


def test_baseline_insufficient_no_alert(isolated_db, monkeypatch):
    original_connect = prd.connect
    monkeypatch.setattr(prd, "connect",
                        lambda *a, **kw: original_connect(isolated_db))

    broken, reasons = prd.detect_regression(_fake_report(ok_n=9))
    assert broken is False
    assert "baseline_insufficient" in reasons


def test_healthy_run_when_baseline_stable(isolated_db, monkeypatch):
    original_connect = prd.connect
    monkeypatch.setattr(prd, "connect",
                        lambda *a, **kw: original_connect(isolated_db))

    # 5 reports anteriores saudáveis
    with db.connect(isolated_db) as conn:
        for i in range(5):
            _insert_history(conn, f"2026-04-0{i+1}T12:00:00", 95.0, 100.0, 95.0)

    broken, reasons = prd.detect_regression(_fake_report(ok_n=9))
    assert broken is False


def test_detects_abrupt_og_drop(isolated_db, monkeypatch):
    original_connect = prd.connect
    monkeypatch.setattr(prd, "connect",
                        lambda *a, **kw: original_connect(isolated_db))

    # Baseline: OG em 100%
    with db.connect(isolated_db) as conn:
        for i in range(5):
            _insert_history(conn, f"2026-04-0{i+1}T12:00:00", 95.0, 100.0, 95.0)

    # Novo report: OG despencou para 50%
    broken, reasons = prd.detect_regression(
        _fake_report(ok_n=9, og_pct=50.0)
    )
    assert broken is True
    assert any("og_rate" in r for r in reasons)


def test_detects_abrupt_ok_rate_drop(isolated_db, monkeypatch):
    original_connect = prd.connect
    monkeypatch.setattr(prd, "connect",
                        lambda *a, **kw: original_connect(isolated_db))

    # Baseline: ok_rate em 90%
    with db.connect(isolated_db) as conn:
        for i in range(5):
            _insert_history(conn, f"2026-04-0{i+1}T12:00:00", 90.0, 100.0, 95.0)

    # Novo: apenas 4 ok de 10 = 40%
    broken, reasons = prd.detect_regression(_fake_report(ok_n=4))
    assert broken is True
    assert any("ok_rate" in r for r in reasons)
