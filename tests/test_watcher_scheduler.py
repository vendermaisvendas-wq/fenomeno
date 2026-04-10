"""Testes do watcher_scheduler — função pura compute_dynamic_priority + integração."""

from __future__ import annotations

import pytest

import db
import watcher_engine as we
import watcher_scheduler as ws


# --- pure tests ---------------------------------------------------------

def test_priority_static_only():
    w = {"priority": 1, "plan": None}
    score = ws.compute_dynamic_priority(w, num_users=1, match_count=0)
    # base = 100 - 1*20 = 80; +5 (1 user) +0 (0 matches) +0 (no plan) = 85
    assert score == 85.0


def test_priority_3_lowest_base():
    w = {"priority": 3, "plan": None}
    score = ws.compute_dynamic_priority(w, num_users=1, match_count=0)
    # base = 100 - 60 = 40; + 5 = 45
    assert score == 45.0


def test_popularity_boost_capped():
    w = {"priority": 2, "plan": None}
    score = ws.compute_dynamic_priority(w, num_users=100, match_count=0)
    # base = 60; + 20 (capped) + 0 = 80
    assert score == 80.0


def test_match_history_boost():
    w = {"priority": 2, "plan": None}
    score = ws.compute_dynamic_priority(w, num_users=1, match_count=20)
    # base = 60; +5 (1 user) +10 (20*0.5) +0 = 75
    assert score == 75.0


def test_match_history_capped():
    w = {"priority": 2, "plan": None}
    score = ws.compute_dynamic_priority(w, num_users=1, match_count=1000)
    # base = 60; +5 +20 (capped) +0 = 85
    assert score == 85.0


def test_premium_plan_boost():
    w = {"priority": 2, "plan": "premium"}
    score = ws.compute_dynamic_priority(w, num_users=1, match_count=0)
    # base = 60; +5 +0 +30 = 95
    assert score == 95.0


def test_pro_plan_boost():
    w = {"priority": 2, "plan": "pro"}
    score = ws.compute_dynamic_priority(w, num_users=1, match_count=0)
    # base = 60; +5 +0 +15 = 80
    assert score == 80.0


def test_free_plan_no_extra():
    w = {"priority": 2, "plan": "free"}
    score = ws.compute_dynamic_priority(w, num_users=1, match_count=0)
    assert score == 65.0  # base 60 + popularity 5


# --- integração com DB --------------------------------------------------

@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    p = tmp_path / "ws.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    original_connect = db.connect
    monkeypatch.setattr(we, "connect", lambda *a, **kw: original_connect(p))
    monkeypatch.setattr(ws, "connect", lambda *a, **kw: original_connect(p))
    return p


def test_schedule_due_orders_by_priority_descending(isolated_db):
    w_low = we.create_watch(keyword="abajur", priority=3)        # base 40+5
    w_mid = we.create_watch(keyword="iphone", priority=2)        # base 60+5
    w_high = we.create_watch(keyword="hilux", priority=1)        # base 80+5

    ordered = ws.schedule_due()
    # high priority (P1) deve vir primeiro
    assert ordered[0] == w_high
    assert ordered[-1] == w_low


def test_schedule_due_premium_overrides_priority(isolated_db):
    # Premium P3 (40+5+30=75) deve vencer Free P1 (80+5+0=85)? Não.
    # Vamos testar caso onde premium ganha: P2+premium = 95 vs P1+free = 85
    free_p1 = we.create_watch(keyword="ipad", priority=1, plan="free")
    premium_p2 = we.create_watch(keyword="macbook", priority=2, plan="premium")

    ordered = ws.schedule_due()
    # Premium P2 (60+5+30=95) > Free P1 (80+5+0=85)
    assert ordered.index(premium_p2) < ordered.index(free_p1)


def test_schedule_due_excludes_inactive(isolated_db):
    w1 = we.create_watch(keyword="iphone", priority=1)
    w2 = we.create_watch(keyword="ipad", priority=1)

    with db.connect(isolated_db) as conn:
        conn.execute("UPDATE watchers SET is_active = 0 WHERE watch_id = ?", (w1,))

    ordered = ws.schedule_due()
    assert w1 not in ordered
    assert w2 in ordered


def test_debug_returns_all_active_with_scores(isolated_db):
    we.create_watch(keyword="iphone", priority=1, plan="premium")
    we.create_watch(keyword="ipad", priority=2, plan="free")
    we.create_watch(keyword="hilux", priority=3)

    rows = ws.debug()
    assert len(rows) == 3
    # Sorted desc por dynamic_score
    for i in range(len(rows) - 1):
        assert rows[i]["dynamic_score"] >= rows[i + 1]["dynamic_score"]
