"""Testes v9: priority, run_due_watchers_async, watcher_optimizer."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import db
import watcher_engine as we
import watcher_optimizer as wo


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    p = tmp_path / "watch_v9.sqlite3"
    monkeypatch.setattr(db, "DB_PATH", p)
    db.init_db(p)
    original_connect = db.connect
    monkeypatch.setattr(we, "connect", lambda *a, **kw: original_connect(p))
    monkeypatch.setattr(wo, "connect", lambda *a, **kw: original_connect(p))
    # v10: run_due_watchers_async chama watcher_scheduler.schedule_due por
    # default, então a fixture precisa patchear o connect dele também.
    import watcher_scheduler as ws
    monkeypatch.setattr(ws, "connect", lambda *a, **kw: original_connect(p))
    return p


# --- priority ----------------------------------------------------------

def test_create_watch_with_priority(isolated_db):
    wid = we.create_watch(keyword="iphone", priority=1)
    with db.connect(isolated_db) as conn:
        row = conn.execute(
            "SELECT priority FROM watchers WHERE watch_id = ?", (wid,)
        ).fetchone()
    assert row["priority"] == 1


def test_create_watch_default_priority(isolated_db):
    wid = we.create_watch(keyword="iphone")
    with db.connect(isolated_db) as conn:
        row = conn.execute(
            "SELECT priority FROM watchers WHERE watch_id = ?", (wid,)
        ).fetchone()
    assert row["priority"] == 2


def test_create_watch_invalid_priority(isolated_db):
    with pytest.raises(ValueError):
        we.create_watch(keyword="iphone", priority=99)


def test_interval_for_priority():
    assert we.interval_for_priority(1) == 600
    assert we.interval_for_priority(2) == 1800
    assert we.interval_for_priority(3) == 3600
    assert we.interval_for_priority(99, fallback=42) == 42
    assert we.interval_for_priority(None, fallback=42) == 42


# --- _select_due_ids respects priority ---------------------------------

def test_select_due_priority_high_runs_more_often(isolated_db):
    """Watcher P1 (10min) com last_run de 15min atrás → due.
       Watcher P3 (1h) com last_run de 15min atrás → NOT due."""
    wid_high = we.create_watch(keyword="iphone", priority=1)
    wid_low = we.create_watch(keyword="ipad", priority=3)

    past = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat(timespec="seconds")
    with db.connect(isolated_db) as conn:
        conn.execute("UPDATE watchers SET last_run_at = ?", (past,))

    rows = we._load_active_watchers()
    due = we._select_due_ids(rows, fallback_interval=3600)
    assert wid_high in due
    assert wid_low not in due


def test_select_due_never_run_is_due(isolated_db):
    wid = we.create_watch(keyword="iphone", priority=2)
    rows = we._load_active_watchers()
    due = we._select_due_ids(rows, fallback_interval=3600)
    assert wid in due


# --- run_due_watchers_async --------------------------------------------

def test_run_due_watchers_async_basic(isolated_db, monkeypatch):
    wid1 = we.create_watch(keyword="iphone", priority=1)
    wid2 = we.create_watch(keyword="ipad", priority=1)

    called = []
    def _fake_monitor(watch_id):
        called.append(watch_id)
        return {"new_matches": 2}

    monkeypatch.setattr(we, "monitor_watch", _fake_monitor)

    result = asyncio.run(we.run_due_watchers_async(concurrency=2))
    assert result["due"] == 2
    assert result["ran"] == 2
    assert result["total_new_matches"] == 4
    assert result["failures"] == 0
    assert set(called) == {wid1, wid2}


def test_run_due_watchers_async_isolates_failures(isolated_db, monkeypatch):
    we.create_watch(keyword="iphone", priority=1)
    we.create_watch(keyword="ipad", priority=1)
    we.create_watch(keyword="macbook", priority=1)

    def _flaky(watch_id):
        if watch_id == 2:
            raise RuntimeError("ddg exploded")
        return {"new_matches": 1}

    monkeypatch.setattr(we, "monitor_watch", _flaky)

    result = asyncio.run(we.run_due_watchers_async(concurrency=3))
    assert result["due"] == 3
    assert result["ran"] == 2
    assert result["failures"] == 1
    assert result["total_new_matches"] == 2


def test_run_due_watchers_async_skips_inactive(isolated_db, monkeypatch):
    wid = we.create_watch(keyword="iphone", priority=1)
    with db.connect(isolated_db) as conn:
        conn.execute("UPDATE watchers SET is_active = 0 WHERE watch_id = ?", (wid,))

    monkeypatch.setattr(we, "monitor_watch",
                        lambda x: pytest.fail("should not be called"))
    result = asyncio.run(we.run_due_watchers_async(concurrency=3))
    assert result["total_active"] == 0
    assert result["ran"] == 0


def test_run_due_watchers_async_no_due(isolated_db, monkeypatch):
    wid = we.create_watch(keyword="iphone", priority=1)
    # Marca como rodou agora
    with db.connect(isolated_db) as conn:
        from db import now_iso
        conn.execute("UPDATE watchers SET last_run_at = ? WHERE watch_id = ?",
                     (now_iso(), wid))

    monkeypatch.setattr(we, "monitor_watch",
                        lambda x: pytest.fail("should not be called"))
    result = asyncio.run(we.run_due_watchers_async(concurrency=3))
    assert result["due"] == 0
    assert result["ran"] == 0


# --- watcher_optimizer.find_popular_groups -----------------------------

def test_find_popular_groups_groups_by_keyword_region(isolated_db):
    we.create_watch(keyword="iphone", region="Araçatuba")
    we.create_watch(keyword="iphone", region="Araçatuba")
    we.create_watch(keyword="iphone", region="Araçatuba")
    we.create_watch(keyword="ipad", region="SP")

    groups = wo.find_popular_groups(min_users=2)
    assert len(groups) == 1
    g = groups[0]
    assert g.keyword.lower() == "iphone"
    assert g.region == "Araçatuba"
    assert g.watchers == 3


def test_find_popular_groups_case_insensitive(isolated_db):
    we.create_watch(keyword="iPhone", region="ARAÇATUBA")
    we.create_watch(keyword="iphone", region="araçatuba")
    groups = wo.find_popular_groups(min_users=2)
    assert len(groups) == 1
    assert groups[0].watchers == 2


def test_find_popular_groups_excludes_inactive(isolated_db):
    wid1 = we.create_watch(keyword="iphone")
    we.create_watch(keyword="iphone")
    with db.connect(isolated_db) as conn:
        conn.execute("UPDATE watchers SET is_active = 0 WHERE watch_id = ?", (wid1,))
    groups = wo.find_popular_groups(min_users=2)
    # apenas 1 ativo → não forma grupo
    assert groups == []


def test_find_popular_groups_min_users_threshold(isolated_db):
    we.create_watch(keyword="iphone")
    we.create_watch(keyword="iphone")
    groups = wo.find_popular_groups(min_users=3)
    assert groups == []
    groups = wo.find_popular_groups(min_users=2)
    assert len(groups) == 1
